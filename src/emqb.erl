%%--------------------------------------------------------------------
%% Copyright (c) 2024 Peer Stritzinger GmbH. All Rights Reserved.
%%
%% @author Sebastien Merle <s.merle@gmail.com>
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


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("emqtt/include/emqtt.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API Functions
-export([start_link/0, start_link/1]).
-export([subscribe/2, subscribe/3, subscribe/4]).
-export([publish/3, publish/4, publish/5]).
-export([unsubscribe/2, unsubscribe/3]).
-export([puback/2, puback/3, puback/4]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
    % use the internal queues. Default: online.
    mode => mode(),
    % If the bridge should fallback to offline mode if maximum number of
    % connection retries is reached. Note that the bridge will not retry
    % to connect anymore and will stay offline untile it is restarted.
    % Default: false.
    offline_fallback => boolean(),
    % The connection mode to use to connect to the MQTT broker. Default: tcp.
    conn_type => conn_type(),
    % The maximum number of time the client will retry to connect to the MQTT
    % broker. Default: 8.
    reconn_max_retries => infinity | non_neg_integer(),
    % The base delay in milliseconds for reconnecting to the MQTT broker after
    % disconnection. Default: 1000.
    reconn_base_delay => pos_integer(),
    % The maximum delay in milliseconds for reconnection. Default: 32000.
    reconn_max_delay => pos_integer(),
    % The multiplier for reconnection exponential backoff. Default: 2.
    reconn_multiplier => pos_integer(),
    % The jitter in milliseconds for reconnection exponential backoff.
    % The random value added to the connection delay will be btween
    % (-(V div 2) - (V rem 2)) and (V div 2). Default: 1000.
    reconn_jitter => pos_integer(),
    % The codec callback module to use for encoding and decoding MQTT payloads
    % Default: emqb_codec_json
    codec => module(),

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

% Change to use emqtt:pubopt() when supporting QoS 2
-type pubopt() :: {retain, boolean()}
                | {qos, qos() | qos_name()}.
-type pubopts() :: qos() | qos_name() | [pubopt()].

-export_type([message/0]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link() -> gen_statem:start_ret().
start_link() ->
    emqb_client:start_link([]).

-spec start_link(start_opts()) -> gen_statem:start_ret().
start_link(StartOpts) when is_map(StartOpts) ->
    emqb_client:start_link(maps:to_list(StartOpts));
start_link(StartOpts) when is_list(StartOpts) ->
    emqb_client:start_link(StartOpts).

-spec subscribe(client(), topic_spec()) -> subscribe_ret().
subscribe(Client, Topic)
  when is_binary(Topic) ->
    emqb_client:subscribe(Client, #{}, [{Topic, [{qos, ?QOS_0}]}]);
subscribe(Client, {Topic, QoS})
  when is_binary(Topic), is_atom(QoS) ->
    emqb_client:subscribe(Client, #{}, [{Topic, [{qos, atom2qos(QoS)}]}]);
subscribe(Client, {Topic, QoS})
  when is_binary(Topic), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    emqb_client:subscribe(Client, #{}, [{Topic, [{qos, QoS}]}]);
subscribe(Client, TopicSpec)
  when is_list(TopicSpec) ->
    lists:foreach(fun({_, Opts}) -> validate_qos(Opts) end, TopicSpec),
    emqb_client:subscribe(Client, #{}, TopicSpec).

-dialyzer({nowarn_function, subscribe/3}).
-spec(subscribe(client(), emqtt:topic(), subopts()) -> subscribe_ret();
               (client(), emqtt:properties(), topic_spec()) -> subscribe_ret()).
subscribe(Client, Topic, QoS)
  when is_binary(Topic), is_atom(QoS) ->
    emqb_client:subscribe(Client, #{}, [{Topic, [{qos, atom2qos(QoS)}]}]);
subscribe(Client, Topic, QoS)
  when is_binary(Topic), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    emqb_client:subscribe(Client, #{}, [{Topic, [{qos, QoS}]}]);
subscribe(Client, Topic, SubOpts)
  when is_binary(Topic), is_list(SubOpts) ->
    emqb_client:subscribe(Client, #{}, [{Topic, SubOpts}]);
subscribe(Client, Properties, TopicSpec)
  when is_map(Properties), is_list(TopicSpec) ->
    lists:foreach(fun({_, SubOpts}) -> validate_qos(SubOpts) end, TopicSpec),
    emqb_client:subscribe(Client, Properties, TopicSpec).

% -dialyzer({nowarn_function, subscribe/4}).
-spec subscribe(client(), emqtt:properties(), emqtt:topic(), subopts())
    -> subscribe_ret().
subscribe(Client, Properties, Topic, QoS)
  when is_map(Properties), is_binary(Topic), is_atom(QoS) ->
    emqb_client:subscribe(Client, Properties, [{Topic, [{qos, atom2qos(QoS)}]}]);
subscribe(Client, Properties, Topic, QoS)
  when is_map(Properties), is_binary(Topic), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    emqb_client:subscribe(Client, Properties, [{Topic, [{qos, QoS}]}]);
subscribe(Client, Properties, Topic, Opts)
  when is_map(Properties), is_binary(Topic), is_list(Opts) ->
    validate_qos(Opts),
    emqb_client:subscribe(Client, Properties, [{Topic, Opts}]).

-spec publish(client(), emqtt:topic(), payload()) -> ok | {error, term()}.
publish(Client, Topic, Payload) when is_binary(Topic) ->
    emqb_client:publish(Client, Topic, #{}, Payload, [{qos, ?QOS_0}]).

-spec publish(client(), emqtt:topic(), payload(), pubopts())
    -> ok | {ok, emqtt:packet_id()} | {error, term()}.
publish(Client, Topic, Payload, QoS)
  when is_binary(Topic), is_atom(QoS) ->
    emqb_client:publish(Client, Topic, #{}, Payload, [{qos, atom2qos(QoS)}]);
publish(Client, Topic, Payload, QoS)
  when is_binary(Topic), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    emqb_client:publish(Client, Topic, #{}, Payload, [{qos, QoS}]);
publish(Client, Topic, Payload, PubOpts)
  when is_binary(Topic), is_list(PubOpts) ->
    validate_qos(PubOpts),
    emqb_client:publish(Client, Topic, #{}, Payload, PubOpts).

-spec publish(client(), emqtt:topic(), emqtt:properties(), payload(), pubopts())
    -> ok | {ok, emqtt:packet_id()} | {error, term()}.
publish(Client, Topic, Properties, Payload, QoS)
  when is_binary(Topic), is_map(Properties), is_atom(QoS) ->
    emqb_client:publish(Client, Topic, Properties, Payload,
                        [{qos, atom2qos(QoS)}]);
publish(Client, Topic, Properties, Payload, QoS)
  when is_binary(Topic), is_map(Properties), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    emqb_client:publish(Client, Topic, Properties, Payload, [{qos, QoS}]);
publish(Client, Topic, Properties, Payload, PubOpts)
  when is_binary(Topic), is_map(Properties), is_list(PubOpts) ->
    validate_qos(PubOpts),
    emqb_client:publish(Client, Topic, Properties, Payload, PubOpts).

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
    emqb_client:unsubscribe(Client, Properties, Topics).

-spec puback(client(), emqtt:packet_id()) -> ok.
puback(Client, PacketId) when is_integer(PacketId) ->
    emqb_client:puback(Client, PacketId, ?RC_SUCCESS, #{}).

-spec puback(client(), emqtt:packet_id(), emqtt:reason_code()) -> ok.
puback(Client, PacketId, ReasonCode)
  when is_integer(PacketId), is_integer(ReasonCode) ->
    emqb_client:puback(Client, PacketId, ReasonCode, #{}).

-spec puback(client(), emqtt:packet_id(), emqtt:reason_code(), emqtt:properties()) -> ok.
puback(Client, PacketId, ReasonCode, Properties)
  when is_integer(PacketId), is_integer(ReasonCode), is_map(Properties) ->
    emqb_client:puback(Client, PacketId, ReasonCode, Properties).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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

atom2qos(qos0) -> 0;
atom2qos(at_most_once) -> 0;
atom2qos(qos1) -> 1;
atom2qos(at_least_once) -> 1;
atom2qos(Other) -> throw({mqtt_qos_not_supported, Other}).
