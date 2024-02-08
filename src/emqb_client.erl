%%--------------------------------------------------------------------
%% @doc emqb client
%% @end
%%
%% @author Sebastien Merle <s.merle@gmail.com>
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

-module(emqb_client).

-behaviour(gen_statem).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("emqtt/include/emqtt.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API Functions
-export([start_link/1]).
-export([subscribe/3]).
-export([publish/5]).
-export([unsubscribe/3]).
-export([puback/4]).

% Behaviour gen_statem callback functions
-export([init/1]).
-export([callback_mode/0]).
-export([disconnected/3]).
-export([connecting/3]).
-export([online/3]).
-export([offline/3]).
-export([handle_event/4]).
-export([terminate/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(data, {
    name :: atom(),
    owner :: undefined | pid(),
    mode = online :: emqb:mode(),
    offline_fallback = false :: boolean(),
    conn_type = tcp :: emqb:conn_type(),
    codec = emqb_codec_json :: module(),
    clientid :: undefined | binary(),
    auto_ack = true :: boolean(),
    emqtt_opts = [] :: [emqtt:option()],
    % undefined for the first reconnection
    reconn_delay = undefined :: undefined | pos_integer(),
    % Next retry count, 0 means the initial connection so it should
    % be reset to 1 after a successful connection.
    reconn_retries = 0 :: non_neg_integer(),
    % The last connection error reason
    reconn_error :: term(),
    reconn_max_retries = 8 :: infinity | non_neg_integer(),
    reconn_base_delay = 1000 :: pos_integer(),
    reconn_max_delay = 32000 :: pos_integer(),
    reconn_multiplier = 2 :: pos_integer(),
    reconn_jitter = 1000 :: pos_integer(),
    client :: undefined | pid(),
    client_mon :: undefined | reference(),
    last_packet_id = 0 :: emqtt:packet_id(),
    conn_props = #{} :: emqtt:properties(),
    subscriptions :: emqb_topic_tree:topic_tree()
}).

% Change to use emqtt:subopt() when supporting QoS 2
% Same as emqb:subopt() but the qos is required to be an integer
-type subopt() :: {rh, 0 | 1 | 2}
                | {rap, boolean()}
                | {nl,  boolean()}
                | {qos, emqb:qos()}.

% Change to use emqtt:pubopt() when supporting QoS 2
% Same as emqb:pubopt() but the qos is required to be an integer
-type pubopt() :: {retain, boolean()}
                | {qos, emqb:qos()}.


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link([emqb:option()]) -> gen_statem:start_ret().
start_link(Opts) when is_list(Opts) ->
    case proplists:get_value(name, Opts) of
        undefined ->
            gen_statem:start_link(?MODULE, [with_owner(Opts)], []);
        Name when is_atom(Name) ->
            gen_statem:start_link({local, Name}, ?MODULE, [with_owner(Opts)], [])
    end.

-spec subscribe(emqb:client(), emqtt:properties(),
                [{emqtt:topic(), [subopt()]}])
    -> emqb:subscribe_ret().
subscribe(Client, Properties, Topics)
  when is_map(Properties), is_list(Topics) ->
    gen_statem:call(Client, {subscribe, Properties, Topics}).

-spec publish(emqb:client(), emqtt:topic(), emqtt:properties(),
              emqb:payload(), [pubopt()])
    -> ok | {ok, emqtt:packet_id()} | {error, term()}.
publish(Client, Topic, Properties, Payload, PubOpts)
  when is_binary(Topic), is_map(Properties), is_list(PubOpts) ->
    gen_statem:call(Client, {publish, Topic, Properties, Payload, PubOpts}).

-spec unsubscribe(amqb:client(), emqtt:properties(), [emqtt:topic()])
    -> amqb:subscribe_ret().
unsubscribe(Client, Properties, Topics)
  when is_map(Properties), is_list(Topics) ->
    gen_statem:call(Client, {unsubscribe, Properties, Topics}).

-spec puback(amqb:client(), emqtt:packet_id(), emqtt:reason_code(),
             emqtt:properties()) -> ok.
puback(Client, PacketId, ReasonCode, Properties)
  when is_integer(PacketId), is_integer(ReasonCode), is_map(Properties) ->
    gen_statem:cast(Client, {send_puback, PacketId, ReasonCode, Properties}).


%%% BEHAVIOUR gen_statem CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Opts]) ->
    process_flag(trap_exit, true),
    % embq only support MQTT v5
    Data = init(Opts, #data{
        emqtt_opts = [{proto_ver, v5}],
        subscriptions = emqb_topic_tree:new()
    }),
    case Data#data.mode of
        offline ->
            {ok, offline, Data};
        online ->
            case emqtt_start(Data) of
                {error, Reason} -> {stop, Reason};
                {ok, Data2} -> {ok, disconnected, Data2}
            end
    end.

callback_mode() ->
    [state_functions, state_enter].


%-- Disconnected State Event Handler -------------------------------------------

disconnected(enter, _OldState, #data{reconn_retries = Retries,
                                     reconn_max_retries = MaxRetries})
  when MaxRetries =/= infinity, Retries > MaxRetries ->
    {keep_state_and_data, [{state_timeout, 0, retries_exausted}]};
disconnected(enter, _OldState, #data{reconn_delay = undefined}) ->
    % First connection do not have any delay
    {keep_state_and_data, [{state_timeout, 0, reconnect}]};
disconnected(enter, _OldState, #data{reconn_retries = Retries,
                                     reconn_delay = Delay}) ->
    ?LOG_DEBUG("Reconnecting to MQTT broker in ~w ms (~w)", [Delay, Retries]),
    {keep_state_and_data, [{state_timeout, Delay, reconnect}]};
disconnected(state_timeout, reconnect, Data) ->
    {next_state, connecting, Data};
disconnected(state_timeout, retries_exausted,
             Data = #data{offline_fallback = false, reconn_error = Reason}) ->
    ?LOG_ERROR("Maximum number of reconnection attempt reached, terminating"),
    {stop, Reason, emqtt_stop(Data, Reason)};
disconnected(state_timeout, retries_exausted,
             Data = #data{offline_fallback = true, reconn_error = Reason}) ->
    ?LOG_WARNING("Maximum number of reconnection attempt reached, falling back to offline mode"),
    {next_state, offline, emqtt_stop(Data, Reason)};
disconnected(info, {'DOWN', MonRef, process, _Pid, Reason},
             Data = #data{client_mon = MonRef}) ->
    {repeat_state, emqtt_crashed(Data, Reason)}.


%-- Connecting State Event Handler ---------------------------------------------

connecting(enter, _OldState, _Data) ->
    {keep_state_and_data, [{state_timeout, 0, connect}]};
connecting(state_timeout, connect, Data = #data{reconn_retries = Retries}) ->
    BrokerAddr = format_broker_address(Data),
    ?LOG_NOTICE("Connecting to MQTT broker ~s", [BrokerAddr]),
    case emqtt_connect(Data) of
        {error, Reason, Data2} ->
            ?LOG_WARNING("Failed to connect to broker ~s : ~p",
                         [BrokerAddr, Reason]),
            Data3 = Data2#data{
                reconn_error = Reason,
                reconn_retries = Retries + 1
            },
            {next_state, disconnected, Data3};
        {ok, Data2} ->
            ?LOG_INFO("Connected to MQTT broker ~s", [BrokerAddr]),
            Data3 = Data2#data{
                reconn_error = undefined,
                reconn_retries = 1
            },
            {next_state, online, Data3}
    end.


%-- Online State Event Handler -------------------------------------------------

online(enter, _OldState, _Data) ->
    keep_state_and_data;
online({call, From}, {subscribe, Properties, Topics}, Data) ->
    case subscribe_external(Data, Properties, Topics) of
        {error, Reason1, Data2} ->
            ?LOG_WARNING("External MQTT subscribe failed: ~p", [Reason1]),
            {keep_state, Data2, [{reply, From, {error, Reason1}}]};
        {ok, ExtProps, ExtCodes, Data2} ->
            case subscribe_internal(Data2, Properties, Topics) of
                % {error, Reason2, Data3} ->
                %     ?LOG_WARNING("Internal MQTT subscribe failed: ~p", [Reason2]),
                %     {keep_state, Data2, [{reply, From, {error, Reason2}}]};
                {ok,  IntProps, IntCodes, Data3} ->
                    {ResProps, ResCodes} =
                        merge_sub_results(ExtProps, ExtCodes, IntProps, IntCodes),
                    {keep_state, Data3, [{reply, From, {ok, ResProps, ResCodes}}]}
            end
    end;
online({call, From}, {unsubscribe, Properties, Topics}, Data) ->
    case unsubscribe_external(Data, Properties, Topics) of
        {error, Reason1, Data2} ->
            ?LOG_WARNING("External MQTT unsubscribe failed: ~p", [Reason1]),
            {keep_state, Data2, [{reply, From, {error, Reason1}}]};
        {ok, ExtProps, ExtCodes, Data2} ->
            case unsubscribe_internal(Data2, Properties, Topics) of
                % {error, Reason2, Data3} ->
                %     ?LOG_WARNING("Internal MQTT unsubscribe failed: ~p", [Reason2]),
                %     {keep_state, Data2, [{reply, From, {error, Reason2}}]};
                {ok,  IntProps, IntCodes, Data3} ->
                    {ResProps, ResCodes} =
                        merge_sub_results(ExtProps, ExtCodes, IntProps, IntCodes),
                    {keep_state, Data3, [{reply, From, {ok, ResProps, ResCodes}}]}
            end
    end;
online({call, From}, {publish, Topic, Properties, Payload, Opts}, Data) ->
    case publish_external(Data, Topic, Properties, Payload, Opts) of
        {error, Reason1, Data2} ->
            ?LOG_WARNING("External MQTT publish failed: ~p", [Reason1]),
            {keep_state, Data2, [{reply, From, {error, Reason1}}]};
        {ok, Data2} ->
            case publish_internal(Data2, Topic, Properties, Payload, Opts, undefined) of
                % {error, Reason2, Data3} ->
                %     ?LOG_WARNING("Internal MQTT publish failed: ~p", [Reason2]),
                %     {keep_state, Data3, [{reply, From, {error, Reason2}}]};
                {ok, Data3} ->
                    {keep_state, Data3, [{reply, From, ok}]}
            end;
        {ok, PacketId, Data2} ->
            Data3 = update_last_packet_id(Data2, PacketId),
            case publish_internal(Data3, Topic, Properties, Payload, Opts, PacketId) of
                % {error, Reason2, Data4} ->
                %     ?LOG_WARNING("Internal MQTT publish failed: ~p", [Reason2]),
                %     {keep_state, Data4, [{reply, From, {error, Reason2}}]};
                {ok, PacketId, Data4} ->
                    {keep_state, Data4, [{reply, From, {ok, PacketId}}]}
            end
    end;
online(cast, {send_puback, PacketId, ReasonCode, Properties}, Data) ->
    {keep_state, puback_external(Data, PacketId, ReasonCode, Properties)};
online(info, {disconnected, ReasonCode, _Properties}, Data) ->
    %TODO: Extract relevent information from the properties if available
    ?LOG_WARNING("Got disconnected from the MQTT broker: ~s",
                 [emqb_utils:mqtt_discode2reason(ReasonCode)]),
    {next_state, disconnected, Data};
online(info, {publish, Msg}, Data) ->
    {keep_state, dispatch_external(Data, Msg)};
online(info, {puback, #{packet_id:= PacketId, reason_code:= Code}} = Msg,
       #data{owner = Owner}) ->
    ?LOG_DEBUG("Received MQTT puback for packet ~w: ~s",
               [PacketId, emqb_utils:mqtt_code2reason(Code)]),
    Owner ! {puback, Msg},
    keep_state_and_data;
online(info, {'DOWN', MonRef, process, _Pid, Reason},
       Data = #data{client_mon = MonRef}) ->
    {next_state, disconnected, emqtt_crashed(Data, Reason)}.


%-- Offline State Event Handler ------------------------------------------------

offline(enter, _OldState, _Data) ->
    keep_state_and_data;
offline({call, From}, {subscribe, Properties, Topics}, Data) ->
    case subscribe_internal(Data, Properties, Topics) of
        % {error, Reason, Data2} ->
        %     {keep_state, Data2, [{reply, From, {error, Reason}}]};
        {ok,  Properties, ReasonCodes, Data2} ->
            {keep_state, Data2, [{reply, From, {ok, Properties, ReasonCodes}}]}
    end;
offline({call, From}, {unsubscribe, Properties, Topics}, Data) ->
    case unsubscribe_internal(Data, Properties, Topics) of
        % {error, Reason, Data2} ->
        %     {keep_state, Data2, [{reply, From, {error, Reason}}]};
        {ok,  Properties, ReasonCodes, Data2} ->
            {keep_state, Data2, [{reply, From, {ok, Properties, ReasonCodes}}]}
    end;
offline({call, From}, {publish, Topic, Properties, Payload, Opts}, Data) ->
    {PacketId, Data2} = next_packet_id(Data, Opts),
    case publish_internal(Data2, Topic, Properties, Payload, Opts, PacketId) of
        % {error, Reason, Data3} ->
        %     {keep_state, Data3, [{reply, From, {error, Reason}}]};
        {ok,  Data3} ->
            {keep_state, Data3, [{reply, From, ok}]};
        {ok,  PacketId, Data3} ->
            {keep_state, Data3, [{reply, From, {ok, PacketId}}]}
    end.


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

terminate(Reason, _State, Data) ->
    emqtt_stop(Data, Reason),
    ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

opt_qos(PropList) ->
    ?QOS_I(proplists:get_value(qos, PropList, ?QOS_0)).

with_owner(Opts) ->
    case proplists:get_value(owner, Opts) of
        Owner when is_pid(Owner) -> Opts;
        undefined -> [{owner, self()} | Opts]
    end.

%% @doc Calculate the next reconnection delay.
%% NextDelay = min(MaxDelay, LastDelay * Multiplier + Jitter)
%% Where jitter is a number that in function of J being the given
%% startup option reconn_jitter is a random number between
%% (-(V div 2) - (V rem 2)) and (V div 2).
update_backoff(Data = #data{reconn_delay = undefined}) ->
    #data{reconn_base_delay = BaseDelay} = Data,
    Data#data{reconn_delay = BaseDelay};
update_backoff(Data = #data{reconn_delay = LastDelay}) ->
    #data{reconn_max_delay = MaxDelay,
          reconn_multiplier = Multipier,
          reconn_jitter = Jitter} = Data,
    Extra = rand:uniform(Jitter + 1) - (Jitter div 2) - (Jitter rem 2) - 1,
    NewDelay = min(MaxDelay, LastDelay * Multipier + Extra),
    Data#data{reconn_delay = NewDelay}.

%% Reset the reconnection backoff.
reset_backoff(Data) ->
    #data{reconn_base_delay = BaseDelay} = Data,
    Data#data{reconn_delay = BaseDelay}.

%% Extract and format the broker(s) address for EMQTT options
%% the hosts options is not yet supported.
format_broker_address(#data{conn_type = ConnType, emqtt_opts = Opts}) ->
    Host = proplists:get_value(host, Opts, {127, 0, 0, 1}),
    Port = proplists:get_value(host, Opts, undefined),
    Ssl = proplists:get_value(ssl, Opts, undefined),
    Proto = case {ConnType, Ssl} of
        {tcp, true} -> <<"mqtts://">>;
        {tcp, _} -> <<"mqtt://">>;
        {ws, true} -> <<"wss://">>;
        {ws, _} -> <<"ws://">>
    end,
    Addr = emqb_utils:format_hostname(Host, Port, 1883),
    <<Proto/binary, Addr/binary>>.

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
init([{offline_fallback, Flag} | Opts], Data)
  when Flag =:= true; Flag =:= false ->
    init(Opts, Data#data{offline_fallback = Flag});
init([{conn_type, ConnType} | Opts], Data)
  when ConnType =:= tcp; ConnType =:= ws ->
    init(Opts, Data#data{conn_type = ConnType});
init([{reconn_max_retries, MaxRetries} | Opts], Data)
  when is_integer(MaxRetries), MaxRetries > 0; MaxRetries =:= infinity ->
    init(Opts, Data#data{reconn_max_retries = MaxRetries});
init([{reconn_base_delay, Delay} | Opts], Data)
  when is_integer(Delay), Delay > 0 ->
    init(Opts, Data#data{reconn_base_delay = Delay});
init([{reconn_max_delay, Delay} | Opts], Data)
  when is_integer(Delay), Delay > 0 ->
    init(Opts, Data#data{reconn_max_delay = Delay});
init([{reconn_multiplier, Mul} | Opts], Data)
  when is_integer(Mul), Mul > 0 ->
    init(Opts, Data#data{reconn_multiplier = Mul});
init([{reconn_jitter, Jitter} | Opts], Data)
  when is_integer(Jitter), Jitter > 0 ->
    init(Opts, Data#data{reconn_jitter = Jitter});
init([{codec, Mod} | Opts], Data) when is_atom(Mod)->
    init(Opts, Data#data{codec = Mod});
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
init([Opt | _Opts], _Data) ->
    erlang:error({unsupported_option, Opt}).

next_packet_id(Data = #data{last_packet_id = LastId}, PubOpts) ->
    case opt_qos(PubOpts) of
        ?QOS_0 -> {undefined, Data};
        ?QOS_1 -> {LastId + 1, Data#data{last_packet_id = LastId + 1}}
    end.

update_last_packet_id(Data, PacketId) ->
    Data#data{last_packet_id = PacketId}.

emqtt_start(Data = #data{client = undefined, emqtt_opts = Opts}) ->
    case emqtt:start_link(Opts) of
        {error, Reason} -> {error, Reason};
        {ok, Client} ->
            % We don't want to crash if EMQTT client crashes, so we unlink and monitor
            erlang:unlink(Client),
            MonRef = erlang:monitor(process, Client),
            {ok, Data#data{client = Client, client_mon = MonRef}}
    end.

emqtt_stop(Data = #data{client = undefined}, _Reason) ->
    Data;
emqtt_stop(Data = #data{client = Client, client_mon = MonRef}, Reason) ->
    erlang:demonitor(MonRef, [flush]),
    Code = emqb_utils:mqtt_reason2discode(Reason),
    catch emqtt:disconnect(Client, Code),
    catch emqtt:stop(Client),
    Data#data{client = undefined}.

emqtt_crashed(Data = #data{client_mon = undefined}, _Reason) ->
    Data;
emqtt_crashed(Data = #data{client_mon = MonRef}, _Reason) ->
    erlang:demonitor(MonRef, [flush]),
    Data#data{client = undefined, client_mon = undefined}.

emqtt_connect(Data = #data{client = undefined}) ->
    case emqtt_start(Data) of
        {error, Reason} -> {error, Reason, Data};
        {ok, Data2} -> emqtt_connect(Data2)
    end;
emqtt_connect(Data = #data{client = Client}) ->
    case emqtt:connect(Client) of
        {error, Reason} ->
            {error, Reason, update_backoff(Data)};
        {ok, ConnProps} ->
            {ok, reset_backoff(update_conn_opts(Data, ConnProps))}
    end.

update_conn_opts(Data, PropMap) when is_map(PropMap) ->
    update_conn_opts(Data, maps:to_list(PropMap));
update_conn_opts(Data, []) -> Data;
update_conn_opts(Data, [{'Assigned-Client-Identifier', ClientId} | Rest]) ->
    #data{emqtt_opts = Opts} = Data,
    NewOpts = [{clientid, ClientId} | proplists:delete(clientid, Opts)],
    NewData = Data#data{clientid = ClientId, emqtt_opts = NewOpts},
    update_conn_opts(NewData, Rest);
update_conn_opts(_Data, [{'Retain-Available', false} | _Rest]) ->
    erlang:throw(mqtt_broker_do_not_support_retain);
update_conn_opts(_Data, [{'Wildcard-Subscription-Available', false} | _Rest]) ->
    erlang:throw(mqtt_broker_do_not_support_wildcard);
update_conn_opts(Data, [{_PropName, _PropValue} | Rest]) ->
    update_conn_opts(Data, Rest).

add_custom_prop(Key, Value, Props) when is_atom(Key) ->
    add_custom_prop(atom_to_binary(Key), Value, Props);
add_custom_prop(Key, Value, #{'User-Property' := UserProps} = Props)
  when is_binary(Key), is_binary(Value) ->
    Props#{'User-Property' => [{Key, Value} | UserProps]};
add_custom_prop(Key, Value, #{} = Props)
  when is_binary(Key), is_binary(Value) ->
    Props#{'User-Property' => [{Key, Value}]}.

publish_external(Data, Topic, Properties, Payload, Opts) ->
    ?LOG_DEBUG("PUBLISHING to ~p with ~p and ~p (~p)", [Topic, Properties, Payload, Opts]),
    #data{client = Client, codec = Codec} = Data,
    case Codec:encode(Properties, Payload) of
        {error, Reason} -> {error, Reason, Data};
        {ok, Properties2, PayloadBin} ->
            InstanceId = emqb_app:instance_id(),
            Properties3 = add_custom_prop(bid, InstanceId, Properties2),
            case emqtt:publish(Client, Topic, Properties3, PayloadBin, Opts) of
                ok -> {ok, Data};
                {ok, PacketId} -> {ok, PacketId, Data};
                {error, Reason} -> {error, Reason, Data}
            end
    end.

dispatch_external(Data = #data{codec = Codec},
                  Msg = #{payload := Payload, properties := Props}) ->
    %TODO: Filter out the message comming from this VM instance
    case Codec:decode(Props, Payload) of
        {ok, DecodedPayload} ->
            dispatch_external_send(Data, Msg#{payload => DecodedPayload});
        {error, Reason} ->
            ?LOG_WARNING("Failed to decode MQTT packet: ~p", [Reason]),
            Data
    end.

dispatch_external_send(Data = #data{owner = Owner},
                       Msg = #{qos := ?QOS_0, payload := Payload}) ->
    ?LOG_DEBUG("Received MQTT QoS0 Message: ~p", [Payload]),
    Owner ! {publish, Msg#{client_pid => self()}},
    Data;
dispatch_external_send(Data = #data{owner = Owner},
                       Msg = #{qos := ?QOS_1, packet_id := PacketId,
                               payload := Payload}) ->
    ?LOG_DEBUG("Received MQTT QoS1 Message ~w: ~p", [PacketId, Payload]),
    Owner ! {publish, Msg#{client_pid => self()}},
    Data.

subscribe_external(Data, Properties, TopicSpec) ->
    #data{client = Client} = Data,
    case emqtt:subscribe(Client, Properties, TopicSpec) of
        {ok, Props, Codes} -> {ok, Props, Codes, Data};
        {error, Reason} -> {error, Reason, Data}
    end.

unsubscribe_external(Data, Properties, Topics) ->
    #data{client = Client} = Data,
    case emqtt:unsubscribe(Client, Properties, Topics) of
        {ok, Props, Codes} -> {ok, Props, Codes, Data};
        {error, Reason} -> {error, Reason, Data}
    end.

puback_external(Data, PacketId, ReasonCode, Properties) ->
    #data{client = Client} = Data,
    emqtt:puback(Client, PacketId, ReasonCode, Properties),
    Data.

publish_internal(Data, _Topic, _Properties, _Payload, _Opts, undefined) ->
    {ok, Data};
publish_internal(Data, _Topic, _Properties, _Payload, _Opts, PacketId) ->
    {ok, PacketId, Data}.

subscribe_internal(Data, _Properties, _Topics) ->
    {ok, #{}, [], Data}.

unsubscribe_internal(Data, _Properties, _Topics) ->
    {ok, #{}, [], Data}.

%TODO: Figure out how to actually merge this correctly
merge_sub_results(Props1, Codes1, Props2, Codes2) ->
    {maps:merge(Props2, Props1), Codes2 ++ Codes1}.
