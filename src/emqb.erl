%%--------------------------------------------------------------------
%% @doc emqb client
%% @end
%%
%% Copyright (c) 2024 Peer Stritzinger GmbH. All Rights Reserved.
%%
%% Portions of this software are derived from the emqtt project,
%% which is licensed under the Apache License, Version 2.0.
%% For more details, see https://github.com/emqx/emqtt or the project's
%% official website.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqb).

-behaviour(gen_statem).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("emqtt/include/emqtt.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API Functions
-export([start_link/0, start_link/1]).
-export([subscribe/2, subscribe/3, subscribe/4]).
-export([publish/3, publish/4, publish/5]).
-export([unsubscribe/2, unsubscribe/3]).

% Behaviour gen_server callback functions
-export([init/1]).
-export([callback_mode/0]).
-export([connecting/3]).
-export([online/3]).
-export([offline/3]).
-export([handle_event/4]).
-export([terminate/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(data, {
    name :: atom(),
    owner :: undefined | pid(),
    mode = offline :: mode(),
    conn_type = tcp :: conn_type(),
    clientid :: undefined | binary(),
    auto_ack = true :: boolean(),
    emqtt_opts = [] :: [emqtt:option()],
    client :: undefined | pid(),
    conn_props = #{} :: emqtt:properties()
}).

-type client() :: pid() | atom().
-type payload() :: term().
-type mode() :: offline | online.
-type conn_type() :: tcp | ws.

% Change to use emqtt:qos() when supporting QoS 2
-type qos() :: ?QOS_0 | ?QOS_1.
% Change to use emqtt:qos_name() when supporting QoS 2
-type qos_name() :: qos0 | at_most_once |
                    qos1 | at_least_once.

-type message() :: #{
    qos => qos(),
    dup => boolean(),
    retain => boolean(),
    packet_id => emqtt:packet_id(),
    topic => emqtt:topic(),
    properties => emqtt:properties(),
    payload => payload(),
    client_pid => pid()
}.

% The emqtt option proto_ver is not allowed, but for simplicity...
-type option() :: emqtt:options()
                | {mode, mode()}
                | {conn_type, conn_type()}.
-type option_map() :: #{
    name => atom(),
    owner => pid(),
    % If the emqtt client should be started and connect or it should only
    % use the internal queues. Default: offline.
    mode => mode(),
    % The connection mode to use to connect to the MQTT broker. Default: tcp.
    conn_type => conn_type(),

    % These options are used by both emqb and emqtt
    clientid => iodata(),

    % Passed as-is to emqtt if specified
    host => emqtt:host(),
    hosts => [{emqtt:host(), inet:port_number()}],
    port => inet:port_number(),
    tcp_opts => [gen_tcp:option()],
    ssl => boolean(),
    ssl_opts => [ssl:ssl_option()],
    ws_path => string(),
    connect_timeout => pos_integer(),
    bridge_mode => boolean(),
    clean_start => boolean(),
    username => iodata(),
    password => iodata(),
    keepalive => non_neg_integer(),
    max_inflight => pos_integer(),
    retry_interval => timeout(),
    will_topic => iodata(),
    will_payload => iodata(),
    will_retain => boolean(),
    will_qos => qos(),
    will_props => emqtt:properties(),
    auto_ack => boolean(),
    ack_timeout => pos_integer(),
    force_ping => boolean(),
    properties => emqtt:properties()
}.
-type start_opts() :: option_map() | [option()].


% Change to use emqtt:subopt() when supporting QoS 2
-type subopt() :: {rh, 0 | 1 | 2}
                | {rap, boolean()}
                | {nl,  boolean()}
                | {qos, qos() | qos_name()}.
-type subopts() :: qos() | qos_name() | [subopt()].
-type topic_spec() :: emqtt:topic()
                    | {emqtt:topic(), subopts()}
                    | [{emqtt:topic(), subopts()}].
-type subscribe_ret() :: {ok, emqtt:properties(), [emqtt:reason_code()]}
                       | {error, term()}.

% Add an extra option that would bypass the external MQTT broker if a local
% queue exists when publishing a message.
-type pubopt() :: {retain, boolean()}
                | {qos, qos() | qos_name()}
                | {bypass, boolean()}.
-type pubopts() :: qos() | qos_name() | [pubopt()].

-export_type([message/0]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link() -> gen_statem:start_ret().
start_link() ->
    gen_server:start_link(?MODULE, [#{}], []).

-spec start_link(start_opts()) -> gen_statem:start_ret().
start_link(Opts) when is_map(Opts) ->
    start_link(maps:to_list(Opts));
start_link(Opts) when is_list(Opts) ->
    case proplists:get_value(name, Opts) of
        undefined ->
            gen_statem:start_link(?MODULE, [with_owner(Opts)], []);
        Name when is_atom(Name) ->
            gen_statem:start_link({local, Name}, ?MODULE, [with_owner(Opts)], [])
    end.

-spec subscribe(client(), topic_spec()) -> subscribe_ret().
subscribe(Client, Topic)
  when is_binary(Topic) ->
    subscribe(Client, {Topic, ?QOS_0});
subscribe(Client, {Topic, QoS})
  when is_binary(Topic), is_atom(QoS) ->
    subscribe(Client, {Topic, atom2qos(QoS)});
subscribe(Client, {Topic, QoS})
  when is_binary(Topic), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    subscribe(Client, [{Topic, ?QOS_I(QoS)}]);
subscribe(Client, Topics)
  when is_list(Topics) ->
    subscribe(Client, #{}, Topics).

-dialyzer({nowarn_function, subscribe/3}).
-spec(subscribe(client(), emqtt:topic(), subopts()) -> subscribe_ret();
               (client(), emqtt:properties(), topic_spec()) -> subscribe_ret()).
subscribe(Client, Topic, QoS)
  when is_binary(Topic), is_atom(QoS) ->
    subscribe(Client, Topic, atom2qos(QoS));
subscribe(Client, Topic, QoS)
  when is_binary(Topic), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    subscribe(Client, Topic, [{qos, QoS}]);
subscribe(Client, Topic, Opts)
  when is_binary(Topic), is_list(Opts) ->
    subscribe(Client, #{}, [{Topic, Opts}]);
subscribe(Client, Properties, Topics)
  when is_map(Properties), is_list(Topics) ->
    % Check that QoS 2 is not specified, this can be removed when supporting it
    lists:foreach(fun({_, QoS}) -> validate_qos(QoS); (_) -> ok end, Topics),
    gen_statem:call(Client, {subscribe, Properties, Topics}).

-dialyzer({nowarn_function, subscribe/4}).
-spec subscribe(client(), emqtt:properties(), emqtt:topic(), subopts())
    -> subscribe_ret().
subscribe(Client, Properties, Topic, QoS)
  when is_map(Properties), is_binary(Topic), is_atom(QoS) ->
    subscribe(Client, Properties, Topic, atom2qos(QoS));
subscribe(Client, Properties, Topic, QoS)
  when is_map(Properties), is_binary(Topic), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    subscribe(Client, Properties, Topic, [{qos, QoS}]);
subscribe(Client, Properties, Topic, Opts)
  when is_map(Properties), is_binary(Topic), is_list(Opts) ->
    subscribe(Client, Properties, [{Topic, Opts}]).

-spec publish(client(), emqtt:topic(), payload()) -> ok | {error, term()}.
publish(Client, Topic, Payload) when is_binary(Topic) ->
    publish(Client, Topic, #{}, Payload, []).

-spec publish(client(), emqtt:topic(), payload(), pubopts())
    -> ok | {ok, emqtt:packet_id()} | {error, term()}.
publish(Client, Topic, Payload, QoS)
  when is_binary(Topic), is_atom(QoS) ->
    publish(Client, Topic, Payload, [{qos, atom2qos(QoS)}]);
publish(Client, Topic, Payload, QoS)
  when is_binary(Topic), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    publish(Client, Topic, Payload, [{qos, QoS}]);
publish(Client, Topic, Payload, Opts)
  when is_binary(Topic), is_list(Opts) ->
    publish(Client, Topic, #{}, Payload, Opts).

-spec publish(client(), emqtt:topic(), emqtt:properties(), payload(), pubopts())
    -> ok | {ok, emqtt:packet_id()} | {error, term()}.
publish(Client, Topic, Properties, Payload, QoS)
  when is_binary(Topic), is_map(Properties), is_atom(QoS) ->
    publish(Client, Topic, Properties, Payload, [{qos, atom2qos(QoS)}]);
publish(Client, Topic, Properties, Payload, QoS)
  when is_binary(Topic), is_map(Properties), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    publish(Client, Topic, Properties, Payload, [{qos, QoS}]);
publish(Client, Topic, Properties, Payload, Opts)
  when is_binary(Topic), is_map(Properties), is_list(Opts) ->
    % Check that QoS 2 is not specified, this can be removed when supporting it
    validate_qos(Opts),
    gen_statem:call(Client, {publish, Topic, Properties, Payload, Opts}).

-spec unsubscribe(client(), emqtt:topic() | [emqtt:topic()])
    -> subscribe_ret().
unsubscribe(Client, Topic)
  when is_binary(Topic) ->
    unsubscribe(Client, [Topic]);
unsubscribe(Client, Topics)
  when is_list(Topics) ->
    unsubscribe(Client, #{}, Topics).

-spec unsubscribe(client(), emqtt:properties(), emqtt:topic() | [emqtt:topic()])
    -> subscribe_ret().
unsubscribe(Client, Properties, Topic)
  when is_map(Properties), is_binary(Topic) ->
    unsubscribe(Client, Properties, [Topic]);
unsubscribe(Client, Properties, Topics)
  when is_map(Properties), is_list(Topics) ->
    gen_statem:call(Client, {unsubscribe, Properties, Topics}).


%%% BEHAVIOUR gen_statem CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Opts]) ->
    % embq only support MQTT v5
    Data = init(Opts, #data{emqtt_opts = [{proto_ver, v5}]}),
    case Data#data.mode of
        online -> {ok, connecting, Data, [{next_event, internal, connect}]};
        offline -> {ok, offline, Data}
    end.

callback_mode() ->
    state_functions.


%-- Connecting State Event Handler ---------------------------------------------

connecting(internal, connect, Data = #data{emqtt_opts = Opts}) ->
    case emqtt:start_link(Opts) of
        {error, Reason} -> {stop, Reason};
        {ok, Client} ->
            case emqtt:connect(Client) of
                {error, Reason} -> {stop, Reason};
                {ok, ConnectProps} ->
                    Data2 = Data#data{client = Client,
                                      conn_props = ConnectProps},
                    {next_state, online, Data2}
            end
    end.


%-- Online State Event Handler -------------------------------------------------

online({call, From}, {subscribe, _Properties, _Topics}, _Data) ->
    {keep_state_and_data, [{reply, From, {error, not_implemented}}]};
online({call, From}, {publish, _Topic, _Properties, _Payload, _Opts}, _Data) ->
    {keep_state_and_data, [{reply, From, {error, not_implemented}}]};
online({call, From}, {unsubscribe, _Properties, _Topics}, _Data) ->
    {keep_state_and_data, [{reply, From, {error, not_implemented}}]}.

% {disconnect, ReasonCode, Properties} ->
% {publish, Message}
% {puback, {PacketId, ReasonCode, Properties}}

%-- Offline State Event Handler ------------------------------------------------

offline({call, From}, {subscribe, _Properties, _Topics}, _Data) ->
    {keep_state_and_data, [{reply, From, {error, not_implemented}}]};
offline({call, From}, {publish, _Topic, _Properties, _Payload, _Opts}, _Data) ->
    {keep_state_and_data, [{reply, From, {error, not_implemented}}]};
offline({call, From}, {unsubscribe, _Properties, _Topics}, _Data) ->
    {keep_state_and_data, [{reply, From, {error, not_implemented}}]}.


%-- Generic State Event Handler ------------------------------------------------

handle_event({call, From}, Request, State, _Data) ->
    ?LOG_WARNING("Unexpected call ~p from ~p while ~s", [Request, From, State]),
    {keep_state_and_data, [{reply, From, {error, unexpected_call}}]};
handle_event(cast, Msg, State, _Data) ->
    ?LOG_WARNING("Unexpected cast ~p while ~s", [Msg, State]),
    keep_state_and_data;
handle_event(info, Info, State, _Data) ->
    ?LOG_WARNING("Unexpected message ~p while ~s", [Info, State]),
    keep_state_and_data.

terminate(_Reason, _State, _Data) ->
    ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

atom2qos(qos0) -> 0;
atom2qos(at_most_once) -> 0;
atom2qos(qos1) -> 1;
atom2qos(at_least_once) -> 1.

% As emqb doesn't support QoS 2 yet, this help validating it is not required.
validate_qos(0) -> ok;
validate_qos(1) -> ok;
validate_qos(qos0) -> ok;
validate_qos(qos1) -> ok;
validate_qos(at_most_once) -> ok;
validate_qos(at_least_once) -> ok;
validate_qos([]) -> ok;
validate_qos([{qos, QoS} | Rest]) ->
    validate_qos(QoS),
    validate_qos(Rest);
validate_qos(QoS) ->
    throw({mqtt_qos_not_supported, QoS}).

with_owner(Opts) ->
    case proplists:get_value(owner, Opts) of
        Owner when is_pid(Owner) -> Opts;
        undefined -> [{owner, self()} | Opts]
    end.

init([], Data) ->
    Data;
init([{name, Name} | Opts], Data) ->
    init(Opts, Data#data{name = Name});
init([{owner, Owner} | Opts], Data)
  when is_pid(Owner) ->
    link(Owner),
    init(Opts, Data#data{owner = Owner});
init([{mode, Mode} | Opts], Data)
  when Mode =:= online; Mode =:= offline ->
    init(Opts, Data#data{mode = Mode});
init([{conn_type, ConnType} | Opts], Data)
  when ConnType =:= tcp; ConnType =:= ws ->
    init(Opts, Data#data{conn_type = ConnType});
init([{clientid, Value} = Opt | Opts], Data = #data{emqtt_opts = EmqttOpts}) ->
    ClientId = iolist_to_binary(Value),
    init(Opts, Data#data{clientid = ClientId, emqtt_opts = [Opt | EmqttOpts]});
init([auto_ack = Opt | Opts], Data = #data{emqtt_opts = EmqttOpts}) ->
    init(Opts, Data#data{auto_ack = true, emqtt_opts = [Opt | EmqttOpts]});
init([{auto_ack, AutoAck} = Opt | Opts], Data = #data{emqtt_opts = EmqttOpts})
  when is_boolean(AutoAck) ->
    init(Opts, Data#data{auto_ack = AutoAck, emqtt_opts = [Opt | EmqttOpts]});
init([{Key, _} = Opt | Opts], Data = #data{emqtt_opts = EmqttOpts})
  when Key =:= host; Key =:= port; Key =:= hosts; Key =:= tcp_opts;
       Key =:= ssl; Key =:= ssl_opts; Key =:= ws_path; Key =:= clean_start;
       Key =:= username; Key =:= password; Key =:= keepalive;
       Key =:= will_topic; Key =:= will_props; Key =:= will_payload;
       Key =:= will_retain; Key =:= will_qos; Key =:= connect_timeout;
       Key =:= ack_timeout; Key =:= force_ping; Key =:= properties;
       Key =:= max_inflight; Key =:= retry_interval; Key =:= bridge_mode ->
    init(Opts, Data#data{emqtt_opts = [Opt | EmqttOpts]});
init([Opt | Opts], Data = #data{emqtt_opts = EmqttOpts})
  when Opt =:= force_ping ->
    init(Opts, Data#data{emqtt_opts = [{Opt, true} | EmqttOpts]});
init([_Opt | Opts], Data) ->
    init(Opts, Data).
