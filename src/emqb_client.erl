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
-include("emqb_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API Functions
-export([start_link/1]).
-export([subscribe/3]).
-export([unsubscribe/3]).
-export([publish/5]).
-export([puback/4]).

% Registry notification functions
-export([async_new/0]).
-export([async_topic_added/5]).
-export([async_topic_removed/4]).
-export([async_check/2]).

% Topic notification functions
-export([dispatch/6]).

% Behaviour gen_statem callback functions
-export([init/1]).
-export([callback_mode/0]).
-export([disconnected/3]).
-export([connecting/3]).
-export([external_mode/3]).
-export([hybrid_mode/3]).
-export([internal_mode/3]).
-export([handle_event/4]).
-export([terminate/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-opaque client() :: {pid(), pid(), emqb:mode()}.
-opaque async_context() :: gen_statem:request_id_collection().

-record(data, {
    name :: atom(),
    owner :: undefined | pid(),
    mode = hybrid :: emqb:mode(),
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
    connected = false :: boolean(),
    client_mon :: undefined | reference(),
    conn_props = #{} :: emqtt:properties(),
    subscriptions = #{} :: #{reference() => topic_subscription()},
    patterns :: emqb_topic_tree:topic_tree(reference()),
    topics = #{} :: #{reference() => #{emqb_topic:path() => pid()}}
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
                | {qos, emqb:qos()}
                | {bypass, boolean()}.

-export_type([client/0, async_context/0, subopt/0, pubopt/0]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc As this function do not return a normal pid like most of the start_link
%% functions, it cannot be used in a supervisor. This shouldn't be an issue as
%% the client should be started in the owning process anyway.
-spec start_link([emqb:option()]) -> {ok, client()} | ignore | {error, term()}.
start_link(Opts) when is_list(Opts) ->
    Opts2 = case proplists:get_value(name, Opts) of
        undefined -> with_owner(Opts);
        Name when is_atom(Name) -> Opts
    end,
    ?LOG_INFO(">>>>> emqb_client API start_link ~p -> ???", [self()]),
    case gen_statem:start_link(?MODULE, [Opts2], []) of
        {ok, ClientPid} ->
            ?LOG_INFO("<<<<< emqb_client API start_link ~p <- ~p", [self(), ClientPid]),
            {ok, {ClientPid, opt_owner(Opts2), opt_mode(Opts2)}};
        {error, _Reason} = Error -> Error;
        ignore -> ignore
    end.

-spec subscribe(client(), emqtt:properties(),
                [{emqtt:topic(), [subopt()]}])
    -> emqb:subscribe_ret().
subscribe({ClientPid, _Owner, _Mode}, Properties, TopicSpec)
  when is_map(Properties), is_list(TopicSpec) ->
    ParsedTopicSpec = [{{T, emqb_topic:parse(T)}, O} || {T, O} <- TopicSpec],
    ?LOG_INFO(">>>>> emqb_client API subscribe(~p) ~p -> ~p", [TopicSpec, self(), ClientPid]),
    Res = gen_statem:call(ClientPid, {subscribe, Properties, ParsedTopicSpec}),
    ?LOG_INFO("<<<<< emqb_client API subscribe(~p) ~p <- ~p", [TopicSpec, self(), ClientPid]),
    Res.

-spec unsubscribe(client(), emqtt:properties(), [emqtt:topic()])
    -> emqb:subscribe_ret().
unsubscribe({ClientPid, _Owner, _Mode}, Properties, Topics)
  when is_map(Properties), is_list(Topics) ->
    ParsedTopics = [{T, emqb_topic:parse(T)} || T <- Topics],
    ?LOG_INFO(">>>>> emqb_client API unsubscribe(~p) ~p -> ~p", [Topics, self(), ClientPid]),
    Res = gen_statem:call(ClientPid, {unsubscribe, Properties, ParsedTopics}),
    ?LOG_INFO("<<<<< emqb_client API unsubscribe(~p) ~p <- ~p", [Topics, self(), ClientPid]),
    Res.

-spec publish(client(), emqtt:topic(), emqtt:properties(),
              emqb:payload(), [pubopt()])
    -> ok | {ok, emqtt:packet_id() | reference()} | {error, term()}.
publish({_ClientPid, Owner, internal}, Topic, Properties, Payload, PubOpts)
  when is_binary(Topic), is_map(Properties), is_list(PubOpts) ->
    TopicPath = emqb_topic:parse(Topic),
    case publish_internal(Owner, TopicPath, Properties, Payload, PubOpts) of
        {error, _Reason} = Error -> Error;
        {ok, _, undefined} -> ok;
        {ok, _, PacketRef} -> {ok, PacketRef}
    end;
publish({ClientPid, Owner, hybrid}, Topic, Properties, Payload, PubOpts)
  when is_binary(Topic), is_map(Properties), is_list(PubOpts) ->
    TopicPath = emqb_topic:parse(Topic),
    case {opt_bypass(PubOpts),
          publish_internal(Owner, TopicPath, Properties, Payload, PubOpts)} of
        {_, {error, _Reason} = Error} -> Error;
        {true, {ok, true, undefined}} -> ok;
        {true, {ok, true, PacketRef}} -> {ok, PacketRef};
        {_, {ok, _, _}} ->
            Req = {publish_external, Topic, Properties, Payload, PubOpts},
            ?LOG_INFO(">>>>> emqb_client API publish 1 ~p -> ~p", [self(), ClientPid]),
            Res = gen_statem:call(ClientPid, Req),
            ?LOG_INFO("<<<<< emqb_client API publish 1 ~p <- ~p", [self(), ClientPid]),
            Res
    end;
publish({ClientPid, _Owner, external}, Topic, Properties, Payload, PubOpts)
  when is_binary(Topic), is_map(Properties), is_list(PubOpts) ->
    Req = {publish_external, Topic, Properties, Payload, PubOpts},
    ?LOG_INFO(">>>>> emqb_client API publish 2 ~p -> ~p", [self(), ClientPid]),
    Res = gen_statem:call(ClientPid, Req),
    ?LOG_INFO("<<<<< emqb_client API publish 2 ~p <- ~p", [self(), ClientPid]),
    Res.

-spec puback(client(), emqtt:packet_id() | reference(),
             emqb:reason_code(), emqtt:properties()) -> ok.
puback({_ClientPid, _Owner, internal}, _PacketId, _ReasonCode, _Properties) ->
    % We just ignore PUBACK in internal mode
    ok;
puback({_ClientPid, _Owner, hybrid}, PacketId, _ReasonCode, _Properties)
  when is_reference(PacketId) ->
    % In hybrid mode, we ignore PUBACK for internally delivered messages
    ok;
puback({ClientPid, _Owner, _Mode}, PacketId, ReasonCode, Properties)
  when is_integer(PacketId), is_integer(ReasonCode), is_map(Properties) ->
    gen_statem:cast(ClientPid, {send_puback, PacketId, ReasonCode, Properties}).


%%% REGISTRY NOTIFICATION FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% -spec topic_added(pid(), emqb_topic:path(), pid()) -> [topic_subscription()].
% topic_added(ClientPid, TopicPath, TopicPid) ->
%     ?LOG_INFO(">>>>> emqb_client API topic_added ~p -> ~p", [self(), ClientPid]),
%     Res = gen_statem:call(ClientPid, {topic_added, TopicPath, TopicPid}),
%     ?LOG_INFO("<<<<< emqb_client API topic_added ~p <- ~p", [self(), ClientPid]),
%     Res.

% -spec topic_removed(pid(), emqb_topic:path()) -> [reference()].
% topic_removed(ClientPid, TopicPath) ->
%     ?LOG_INFO(">>>>> emqb_client API topic_removed ~p -> ~p", [self(), ClientPid]),
%     Res = gen_statem:call(ClientPid, {topic_removed, TopicPath}),
%     ?LOG_INFO("<<<<< emqb_client API topic_removed ~p <- ~p", [self(), ClientPid]),
%     Res.

-spec async_new() -> async_context().
async_new() ->
    gen_statem:reqids_new().

-spec async_topic_added(async_context(), pid(), emqb_topic:path(), pid(), term()) -> async_context().
async_topic_added(ReqIdCol, ClientPid, TopicPath, TopicPid, Label) ->
    ?LOG_INFO(">>>>> emqb_client API async_topic_added(~p, ~p) ~p -> ~p", [TopicPath, TopicPid, self(), ClientPid]),
    Req = {topic_added, TopicPath, TopicPid},
    ReqIdCol2 = gen_statem:send_request(ClientPid, Req, Label, ReqIdCol),
    ?LOG_INFO("<<<<< emqb_client API async_topic_added(~p, ~p) ~p <- ~p", [TopicPath, TopicPid, self(), ClientPid]),
    ReqIdCol2.

-spec async_topic_removed(async_context(), pid(), emqb_topic:path(), term()) -> async_context().
async_topic_removed(ReqIdCol, ClientPid, TopicPath, Label) ->
    ?LOG_INFO(">>>>> emqb_client API async_topic_removed(~p) ~p -> ~p", [TopicPath, self(), ClientPid]),
    Req = {topic_removed, TopicPath},
    ReqIdCol2 = gen_statem:send_request(ClientPid, Req, Label, ReqIdCol),
    ?LOG_INFO("<<<<< emqb_client API async_topic_removed(~p) ~p <- ~p", [TopicPath, self(), ClientPid]),
    ReqIdCol2.

-spec async_check(Message :: term(), async_context()) ->
    no_reply | no_request | {Response, Label :: term(), async_context()}
  when
    Response :: {reply, Reply :: term()}
              | {error, {Reason :: term(), gen_statem:server_ref()}}.
async_check(Msg, ReqIdCol) ->
    gen_statem:check_response(Msg, ReqIdCol, true).


%%% TOPIC NOTIFICATION FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec dispatch(pid(), reference(), emqtt:properties(), emqb_topic:path(),
               term(), [pubopt()]) -> ok.
dispatch(ClientPid, SubRef, Props, TopicPath, Payload, PubOpts) ->
    Msg = {dispatch, SubRef, Props, TopicPath, Payload, PubOpts},
    gen_statem:cast(ClientPid, Msg).


%%% BEHAVIOUR gen_statem CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Opts]) ->
    process_flag(trap_exit, true),
    % embq only support MQTT v5
    Data = init(Opts, #data{
        emqtt_opts = [{proto_ver, v5}],
        patterns = emqb_topic_tree:new()
    }),
    {StartMqtt, Register, InitialState} = case Data#data.mode of
        internal -> {false, true, internal_mode};
        hybrid -> {true, true, disconnected};
        external -> {true, false, disconnected}
    end,
    case Register of
        true -> emqb_registry:register_client(self());
        false -> ok
    end,
    case StartMqtt of
        false -> {ok, InitialState, Data};
        true ->
            case emqtt_start(Data) of
                {error, Reason} -> {stop, Reason};
                {ok, Data2} -> {ok, InitialState, Data2}
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
             Data = #data{reconn_error = Reason}) ->
    ?LOG_ERROR("Maximum number of reconnection attempt reached, terminating"),
    {stop, Reason, emqtt_stop(Data, Reason)};
disconnected({call, _From}, {subscribe, _, _}, _Data) ->
    ?LOG_INFO("!!!!! ~p emqb_client subscribe postponed 1", [self()]),
    {keep_state_and_data, [postpone]};
disconnected({call, _From}, {unsubscribe, _, _}, _Data) ->
    ?LOG_INFO("!!!!! ~p emqb_client unsubscribe postponed 1", [self()]),
    {keep_state_and_data, [postpone]};
disconnected({call, _From}, {publish, _, _, _, _}, _Data) ->
    ?LOG_INFO("!!!!! ~p emqb_client publish postponed 1", [self()]),
    {keep_state_and_data, [postpone]};
disconnected(cast, {send_puback, _, _, _}, _Data) ->
    {keep_state_and_data, [postpone]};
disconnected(info, {'DOWN', MonRef, process, _Pid, Reason},
             Data = #data{client_mon = MonRef}) ->
    ?LOG_INFO("XXXXX ~p emqb_client EMQTT crached 1", [self()]),
    {repeat_state, emqtt_crashed(Data, Reason)};
disconnected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, disconnected, Data).


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
                connected = false,
                reconn_error = Reason,
                reconn_retries = Retries + 1
            },
            {next_state, disconnected, Data3};
        {ok, Data2} ->
            ?LOG_INFO("Connected to MQTT broker ~s", [BrokerAddr]),
            Data3 = Data2#data{
                connected = true,
                reconn_error = undefined,
                reconn_retries = 1
            },
            case Data3#data.mode of
                hybrid -> {next_state, hybrid_mode, Data3};
                external -> {next_state, external_mode, Data3}
            end
    end;
connecting({call, _From}, {subscribe, _, _}, _Data) ->
    ?LOG_INFO("!!!!! ~p emqb_client subscribe postponed 2", [self()]),
    {keep_state_and_data, [postpone]};
connecting({call, _From}, {unsubscribe, _, _}, _Data) ->
    ?LOG_INFO("!!!!! ~p emqb_client unsubscribe postponed 2", [self()]),
    {keep_state_and_data, [postpone]};
connecting({call, _From}, {publish, _, _, _, _}, _Data) ->
    ?LOG_INFO("!!!!! ~p emqb_client publish postponed 2", [self()]),
    {keep_state_and_data, [postpone]};
connecting(cast, {send_puback, _, _, _}, _Data) ->
    {keep_state_and_data, [postpone]};
connecting(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, connecting, Data).


%-- External Mode State Event Handler ------------------------------------------

external_mode(enter, _OldState, Data) ->
    resubscribe_external(Data),
    keep_state_and_data;
external_mode({call, From}, {subscribe, Properties, TopicSpec}, Data) ->
    {PrepTopicSpec, Data2} = subscribe_prepare(Data, Properties, TopicSpec),
    case subscribe_external(Data2, Properties, PrepTopicSpec) of
        {error, Reason, Data3} ->
            ?LOG_WARNING("External MQTT subscribe failed: ~p", [Reason]),
            {keep_state, Data3, [{reply, From, {error, Reason}}]};
        {ok, ExtProps, ExtCodes, Data3} ->
            {keep_state, Data3, [{reply, From, {ok, ExtProps, ExtCodes}}]}
    end;
external_mode({call, From}, {unsubscribe, Properties, Topics}, Data) ->
    {PrepTopics, Data2} = unsubscribe_prepare(Data, Topics),
    case unsubscribe_external(Data2, Properties, PrepTopics) of
        {error, Reason1, Data3} ->
            ?LOG_WARNING("External MQTT unsubscribe failed: ~p", [Reason1]),
            {keep_state, Data3, [{reply, From, {error, Reason1}}]};
        {ok, ResProps, ResCodes, Data3} ->
            {keep_state, Data3, [{reply, From, {ok, ResProps, ResCodes}}]}
    end;
external_mode({call, From}, {publish_external, Topic, Properties, Payload, Opts}, Data) ->
    case publish_external(Data, Topic, Properties, Payload, Opts) of
        {error, Reason, Data2} ->
            ?LOG_WARNING("Extenal MQTT publish failed: ~p", [Reason]),
            {keep_state, Data2, [{reply, From, {error, Reason}}]};
        {ok, Data2} ->
            {keep_state, Data2, [{reply, From, ok}]};
        {ok, PacketId, Data2} ->
            {keep_state, Data2, [{reply, From, {ok, PacketId}}]}
    end;
external_mode(cast, {send_puback, PacketId, ReasonCode, Properties}, Data)
  when is_integer(PacketId) ->
    {keep_state, puback_external(Data, PacketId, ReasonCode, Properties)};
external_mode(info, {disconnected, ReasonCode, _Properties}, Data) ->
    %TODO: Extract relevent information from the properties if available.
    ?LOG_WARNING("Got disconnected from the MQTT broker: ~s",
                 [emqb_utils:mqtt_discode2reason(ReasonCode)]),
    {next_state, disconnected, Data#data{connected = false}};
external_mode(info, {publish, Msg}, Data) ->
    {keep_state, dispatch_external(Data, Msg)};
external_mode(info, {puback, #{packet_id:= PacketId, reason_code:= Code}} = Msg,
       #data{owner = Owner}) ->
    ?LOG_DEBUG("Received MQTT puback for packet ~w: ~s",
               [PacketId, emqb_utils:mqtt_code2reason(Code)]),
    Owner ! {puback, Msg},
    keep_state_and_data;
external_mode(info, {'DOWN', MonRef, process, _Pid, Reason},
       Data = #data{client_mon = MonRef}) ->
    {next_state, disconnected, emqtt_crashed(Data, Reason)};
external_mode(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, external_mode, Data).


%-- Hybrid Mode State Event Handler --------------------------------------------

hybrid_mode(enter, _OldState, Data) ->
    resubscribe_external(Data),
    keep_state_and_data;
hybrid_mode({call, From}, {subscribe, Properties, TopicSpec}, Data) ->
    ?LOG_INFO("CCCCC ~p emqb_client subscribe(~p)", [self(), TopicSpec]),
    {PrepTopicSpec, Data2} = subscribe_prepare(Data, Properties, TopicSpec),
    case subscribe_external(Data2, Properties, PrepTopicSpec) of
        {error, Reason1, Data3} ->
            ?LOG_WARNING("External MQTT subscribe failed: ~p", [Reason1]),
            {keep_state, Data3, [{reply, From, {error, Reason1}}]};
        {ok, ExtProps, ExtCodes, Data3} ->
            {_, _, Data4} = subscribe_internal(Data3, Properties, PrepTopicSpec),
            {keep_state, Data4, [{reply, From, {ok, ExtProps, ExtCodes}}]}
    end;
hybrid_mode({call, From}, {unsubscribe, Properties, Topics}, Data) ->
    ?LOG_INFO("CCCCC ~p emqb_client unsubscribe(~p)", [self(), Topics]),
    {PrepTopics, Data2} = unsubscribe_prepare(Data, Topics),
    case unsubscribe_external(Data2, Properties, PrepTopics) of
        {error, Reason1, Data3} ->
            ?LOG_WARNING("External MQTT unsubscribe failed: ~p", [Reason1]),
            {keep_state, Data3, [{reply, From, {error, Reason1}}]};
        {ok, ExtProps, ExtCodes, Data3} ->
            {_, _, Data4} = unsubscribe_internal(Data3, Properties, PrepTopics),
            {keep_state, Data4, [{reply, From, {ok, ExtProps, ExtCodes}}]}
    end;
hybrid_mode({call, From}, {publish_external, Topic, Properties, Payload, Opts}, Data) ->
    ?LOG_INFO("CCCCC ~p emqb_client publish_external", [self()]),
    case publish_external(Data, Topic, Properties, Payload, Opts) of
        {error, Reason, Data2} ->
            ?LOG_WARNING("External MQTT publish failed: ~p", [Reason]),
            {keep_state, Data2, [{reply, From, {error, Reason}}]};
        {ok, Data2} ->
            {keep_state, Data2, [{reply, From, ok}]};
        {ok, PacketId, Data2} ->
            {keep_state, Data2, [{reply, From, {ok, PacketId}}]}
    end;
hybrid_mode({call, From}, {topic_added, TopicPath, TopicPid}, Data) ->
    ?LOG_INFO("CCCCC ~p emqb_client topic_added(~p)", [self(), TopicPath]),
    {Data2, Subs} = topic_added_internal(Data, TopicPath, TopicPid),
    {keep_state, Data2, [{reply, From, Subs}]};
hybrid_mode({call, From}, {topic_removed, TopicPath}, Data) ->
    ?LOG_INFO("CCCCC ~p emqb_client topic_removed(~p)", [self(), TopicPath]),
    {Data2, Refs} = topic_removed_internal(Data, TopicPath),
    {keep_state, Data2, [{reply, From, Refs}]};
hybrid_mode(cast, {send_puback, PacketId, ReasonCode, Properties}, Data)
  when is_integer(PacketId) ->
    % PUBACK for a broker-generated packet identifier
    {keep_state, puback_external(Data, PacketId, ReasonCode, Properties)};
hybrid_mode(cast, {dispatch, SubRef, Props, TopicPath, Payload, PubOpts}, Data) ->
    {keep_state, dispatch_internal(Data, SubRef, TopicPath,
                                   Props, Payload, PubOpts)};
hybrid_mode(info, {disconnected, ReasonCode, _Properties}, Data) ->
    %TODO: Extract relevent information from the properties if available.
    ?LOG_WARNING("Got disconnected from the MQTT broker: ~s",
                 [emqb_utils:mqtt_discode2reason(ReasonCode)]),
    {next_state, disconnected, Data#data{connected = false}};
hybrid_mode(info, {publish, Msg}, Data) ->
    {keep_state, dispatch_external(Data, Msg)};
hybrid_mode(info, {puback, #{packet_id:= PacketId, reason_code:= Code}} = Msg,
       #data{owner = Owner}) ->
    ?LOG_DEBUG("Received MQTT puback for packet ~w: ~s",
               [PacketId, emqb_utils:mqtt_code2reason(Code)]),
    Owner ! {puback, Msg},
    keep_state_and_data;
hybrid_mode(info, {'DOWN', MonRef, process, _Pid, Reason},
       Data = #data{client_mon = MonRef}) ->
    ?LOG_INFO("XXXXX ~p emqb_client EMQTT crached 3", [self()]),
    {next_state, disconnected, emqtt_crashed(Data, Reason)};
hybrid_mode(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, hybrid_mode, Data).


%-- Internal Mode State Event Handler ------------------------------------------

internal_mode(enter, _OldState, _Data) ->
    keep_state_and_data;
internal_mode({call, From}, {subscribe, Properties, TopicSpec}, Data) ->
    {PrepTopicSpec, Data2} = subscribe_prepare(Data, Properties, TopicSpec),
    {ResProps, ResCodes, Data3} = subscribe_internal(Data2, Properties, PrepTopicSpec),
    {keep_state, Data3, [{reply, From, {ok, ResProps, ResCodes}}]};
internal_mode({call, From}, {unsubscribe, Properties, Topics}, Data) ->
    {PrepTopics, Data2} = unsubscribe_prepare(Data, Topics),
    {ResProps, ResCodes, Data3} = unsubscribe_internal(Data2, Properties, PrepTopics),
    {keep_state, Data3, [{reply, From, {ok, ResProps, ResCodes}}]};
internal_mode({call, From}, {topic_added, TopicPath, TopicPid}, Data) ->
    {Data2, Subs} = topic_added_internal(Data, TopicPath, TopicPid),
    {keep_state, Data2, [{reply, From, Subs}]};
internal_mode({call, From}, {topic_removed, TopicPath}, Data) ->
    {Data2, Refs} = topic_removed_internal(Data, TopicPath),
    {keep_state, Data2, [{reply, From, Refs}]};
internal_mode(cast, {dispatch, SubRef, Props, TopicPath, Payload, PubOpts}, Data) ->
    {keep_state, dispatch_internal(Data, SubRef, TopicPath, Props,
                                   Payload, PubOpts)};
internal_mode(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, internal_mode, Data).


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
    proplists:get_value(qos, PropList, ?QOS_0).

opt_bypass(PropList) ->
    proplists:get_value(bypass, PropList, false).

opt_mode(PropList) ->
    proplists:get_value(mode, PropList, hybrid).

opt_owner(PropList) ->
    proplists:get_value(owner, PropList).

with_owner(Opts) ->
    case proplists:get_value(owner, Opts) of
        Owner when is_pid(Owner) -> Opts;
        undefined -> [{owner, self()} | Opts]
    end.

parse_subopts(Sub, []) -> Sub;
parse_subopts(Sub, [{qos, QoS} | Rest]) ->
    parse_subopts(Sub#topic_subscription{qos = QoS}, Rest);
parse_subopts(Sub, [{rh, RH} | Rest]) ->
    parse_subopts(Sub#topic_subscription{rh = RH}, Rest);
parse_subopts(Sub, [{rap, RAP} | Rest]) ->
    parse_subopts(Sub#topic_subscription{rap = RAP}, Rest);
parse_subopts(Sub, [{nl, NL} | Rest]) ->
    parse_subopts(Sub#topic_subscription{nl = NL}, Rest).

format_subparams(Sub) ->
    #topic_subscription{sid = Sid, qos = QoS, rh = RH, rap = RAP, nl = NL} = Sub,
    KV = [{qos, QoS}, {rh, RH}, {rap, RAP}, {nl, NL}],
    SubOpts = [{K, V} || {K, V} <- KV, V =/= undefined],
    SubProps = if Sid =:= undefined -> #{};
        true -> #{'Subscription-Identifier' => Sid}
    end,
    {SubProps, SubOpts}.

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
    Port = proplists:get_value(port, Opts, undefined),
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
  when Mode =:= internal; Mode =:= external; Mode =:= hybrid ->
    init(Opts, Data#data{mode = Mode});
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
init([{Key, undefined} | Opts], Data = #data{emqtt_opts = EmqttOpts})
  when Key =:= host; Key =:= port; Key =:= username; Key =:= password ->
    % If undefined, we just ignore the option
    init(Opts, Data#data{emqtt_opts = EmqttOpts});
init([{Key, _} = Opt | Opts], Data = #data{emqtt_opts = EmqttOpts})
  when Key =:= host; Key =:= port; Key =:= hosts; Key =:= tcp_opts;
       Key =:= ssl; Key =:= ssl_opts; Key =:= ws_path;
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
emqtt_stop(Data = #data{client = Client, connected = IsConnected,
                        client_mon = MonRef}, Reason) ->
    erlang:demonitor(MonRef, [flush]),
    case IsConnected of
        false -> ok;
        true ->
            Code = emqb_utils:mqtt_reason2discode(Reason),
            catch emqtt:disconnect(Client, Code)
    end,
    catch emqtt:stop(Client),
    Data#data{client = undefined, connected = false, client_mon = undefined}.

emqtt_crashed(Data = #data{client_mon = undefined}, _Reason) ->
    Data#data{client = undefined, connected = false};
emqtt_crashed(Data = #data{client_mon = MonRef}, _Reason) ->
    erlang:demonitor(MonRef, [flush]),
    Data#data{client = undefined, connected = false, client_mon = undefined}.

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
update_conn_opts(Data = #data{clientid = undefined}, []) ->
    Data;
update_conn_opts(Data = #data{emqtt_opts = Opts}, []) ->
    % If there is a defined client identifier, we set clean_start option to
    % false for the next time we will connect to the broker.
    Opts2 = [{clean_start, false} | proplists:delete(clean_start, Opts)],
    Data#data{emqtt_opts = Opts2};
update_conn_opts(Data, [{'Assigned-Client-Identifier', ClientId} | Rest]) ->
    #data{emqtt_opts = Opts} = Data,
    Opts2 = [{clientid, ClientId} | proplists:delete(clientid, Opts)],
    Data2 = Data#data{clientid = ClientId, emqtt_opts = Opts2},
    update_conn_opts(Data2, Rest);
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

del_custom_prop(Key, Props) when is_atom(Key) ->
    del_custom_prop(atom_to_binary(Key), Props);
del_custom_prop(Key,  #{'User-Property' := UserProps} = Props)
  when is_binary(Key) ->
    Props#{'User-Property' := proplists:delete(Key, UserProps)};
del_custom_prop(_Key,  #{} = Props) ->
    Props.

get_custom_prop(Key, Props, Default) when is_atom(Key) ->
    get_custom_prop(atom_to_binary(Key), Props, Default);
get_custom_prop(Key,  #{'User-Property' := UserProps}, Default)
  when is_binary(Key) ->
    proplists:get_value(Key, UserProps, Default);
get_custom_prop(_Key,  #{}, Default) ->
    Default.

subscribe_prepare(Data, SubProps, TopicSpec) ->
    subscribe_prepare(Data, SubProps, TopicSpec, []).

subscribe_prepare(Data, _SubProps, [], Acc) ->
    {lists:reverse(Acc), Data};
subscribe_prepare(Data, SubProps,
                  [{{_, TopicPattern}, SubOpts} = Spec | Rest], Acc) ->
    #data{owner = Owner, subscriptions = Subs, patterns = Tree} = Data,
    SubRef = make_ref(),
    SubId = maps:get('Subscription-Identifier', SubProps, undefined),
    SubData = parse_subopts(#topic_subscription{
        ref = SubRef,
        client = self(),
        owner = Owner,
        pattern = TopicPattern,
        sid = SubId,
        qos = ?QOS_0
    }, SubOpts),
    Subs2 = Subs#{SubRef => SubData},
    Tree2 = emqb_topic_tree:update(TopicPattern, SubRef, Tree),
    Data2 = Data#data{subscriptions = Subs2, patterns = Tree2},
    subscribe_prepare(Data2, SubProps, Rest, [{Spec, SubData} | Acc]).

unsubscribe_prepare(Data, Topics) ->
    unsubscribe_prepare(Data, Topics, []).

unsubscribe_prepare(Data, [], Acc) ->
    {lists:reverse(Acc), Data};
unsubscribe_prepare(Data = #data{subscriptions = Subs, patterns = Tree},
                    [{_, TopicPattern} = Topic | Rest], Acc) ->
    case emqb_topic_tree:find(TopicPattern, Tree) of
        error ->
            unsubscribe_prepare(Data, Rest, [{Topic, undefined} | Acc]);
        {ok, SubRef} ->
            {SubData, Subs2} = maps:take(SubRef, Subs),
            Tree2 = emqb_topic_tree:remove(TopicPattern, Tree),
            Data2 = Data#data{subscriptions = Subs2, patterns = Tree2},
            unsubscribe_prepare(Data2, Rest, [{Topic, SubData} | Acc])
    end.

publish_external(Data, Topic, Properties, Payload, Opts) ->
    #data{mode = Mode, client = Client, codec = Codec} = Data,
    case Codec:encode(Properties, Payload) of
        {error, Reason} -> {error, Reason, Data};
        {ok, Properties2, PayloadBin} ->
            % In hybrid mode, we add a custom property to be able to filter
            % out our own message comming back from us from the MQTT broker.
            Properties3 = case Mode of
                hybrid ->
                    InstanceId = emqb_app:instance_id(),
                    add_custom_prop(bid, InstanceId, Properties2);
                _ ->
                    Properties2
            end,
            % Removes emqb-specific options
            Opts2 = proplists:delete(bypass, Opts),
            case emqtt:publish(Client, Topic, Properties3, PayloadBin, Opts2) of
                ok -> {ok, Data};
                {ok, PacketId} -> {ok, PacketId, Data};
                {error, Reason} -> {error, Reason, Data}
            end
    end.

dispatch_external(Data = #data{codec = Codec},
                  Msg = #{payload := Payload, properties := Props}) ->
    InstanceId = emqb_app:instance_id(),
    case get_custom_prop(bid, Props, undefined) of
        InstanceId ->
            % A client on the same VM instance sent this message,
            % we should get it internally.
            Data;
        undefined ->
            Props2 = del_custom_prop(bid, Props),
            case Codec:decode(Props2, Payload) of
                {ok, DecPayload} ->
                    Msg2 = Msg#{payload := DecPayload, properties := Props2},
                    dispatch_external_send(Data, Msg2);
                {error, Reason} ->
                    ?LOG_WARNING("Failed to decode MQTT packet: ~p", [Reason]),
                    Data
            end
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

subscribe_external(Data, Properties, PreparedTopicSpec) ->
    #data{client = Client} = Data,
    % Filter out the parsed topics
    FilteredSpec = [{T, O} || {{{T, _}, O}, _} <- PreparedTopicSpec],
    case emqtt:subscribe(Client, Properties, FilteredSpec) of
        {ok, Props, Codes} -> {ok, Props, Codes, Data};
        {error, Reason} -> {error, Reason, Data}
    end.

unsubscribe_external(Data, Properties, PreparedTopics) ->
    #data{client = Client} = Data,
    % Filter out the parsed topics
    FilteredTopics = [T || {{T, _}, _} <- PreparedTopics],
    case emqtt:unsubscribe(Client, Properties, FilteredTopics) of
        {ok, Props, Codes} -> {ok, Props, Codes, Data};
        {error, Reason} -> {error, Reason, Data}
    end.

resubscribe_external(Data) ->
    #data{client = Client, subscriptions = Subs, patterns = Tree} = Data,
    emqb_topic_tree:fold(fun(TopicPath, SubRef, undefined) ->
        SubInfo = maps:get(SubRef, Subs),
        {SubProps, SubOpts} = format_subparams(SubInfo),
        Topic = emqb_topic:format(TopicPath),
        case emqtt:subscribe(Client, SubProps, Topic, SubOpts) of
            {ok, _ResProps, [?RC_GRANTED_QOS_0]} -> undefined;
            {ok, _ResProps, [?RC_GRANTED_QOS_1]} -> undefined;
            {ok, _ResProps, [Code]} ->
                ?LOG_WARNING("Failed to restore subscription to topic ~s: ~s",
                             [Topic, emqb_utils:mqtt_code2reason(Code)]),
                undefined;
            {error, Reason} ->
                ?LOG_WARNING("Failed to restore subscription to topic ~s: ~p",
                             [Topic, Reason]),
                undefined
        end
    end, undefined, Tree),
    ok.

puback_external(Data, PacketId, ReasonCode, Properties) ->
    #data{client = Client} = Data,
    emqtt:puback(Client, PacketId, ReasonCode, Properties),
    Data.

%% Will be called from the process calling emqb_client:publish/5
publish_internal(Owner, TopicPath, Props, Payload, PubOpts) ->
    % The topic process need to be created, as it triggers the subscription
    % of all the clients with matching patterns. After the creation, the
    % registry will contains all the subscriptions information to send the
    % message directly to the owners of the clients without calling the topics.
    % We still want to send an asynchronous message to the topic process in
    % order to keep it alive.
    case emqb_manager:topic(TopicPath) of
        {error, Reason} -> {error, Reason};
        {ok, TopicPid} ->
            emqb_topic:keep_alive(TopicPid),
            PubQoS = opt_qos(PubOpts),
            FilteredProps = maps:with([
                'Payload-Format-Indicator',
                'Message-Expiry-Interval',
                'Content-Type',
                'Response-Topic',
                'Correlation-Data',
                'User-Property'
            ], Props),
            Subscriptions = emqb_registry:get_subscriptions(TopicPath),
            Dispatched = maps:fold(fun(_, SubData, _) ->
                dispatch_internal(SubData, TopicPath, FilteredProps,
                                  Payload, PubOpts),
                true
            end, false, Subscriptions),
            case PubQoS of
                ?QOS_0 ->
                    {ok, Dispatched, undefined};
                ?QOS_1 ->
                    PacketRef = make_ref(),
                    % Then simulate the puback
                    PubAck = #{packet_id => PacketRef, properties => #{},
                               reason_code => ?RC_SUCCESS},
                    Owner ! {puback, PubAck},
                    {ok, Dispatched, PacketRef}
            end
    end.

subscribe_internal(Data, Properties, PreparedTopics) ->
    subscribe_internal(Data, Properties, PreparedTopics, #{}, []).

subscribe_internal(Data, _SubProps, [], ResProps, Acc) ->
    {ResProps, lists:reverse(Acc), Data};
subscribe_internal(Data = #data{topics = Topics}, SubProps,
                   [{{{_, TopicPattern}, _}, SubData} | Rest], ResProps, Acc) ->
    #topic_subscription{ref = SubRef, qos = QoS} = SubData,
    ?LOG_INFO(">>>>> emqb_client PROC subscribe_internal call emqb_registry:match_topics(~p) ~p -> registry", [TopicPattern, self()]),
    TopicPids = emqb_registry:match_topics(TopicPattern),
    ?LOG_INFO("<<<<< emqb_client PROC subscribe_internal call emqb_registry:match_topics(~p) ~p <- registry", [TopicPattern, self()]),
    Topics2 = Topics#{SubRef => maps:from_list(TopicPids)},
    Data2 = Data#data{topics = Topics2},
    lists:foreach(fun({_, TopicPid}) ->
        ?LOG_INFO(">>>>> emqb_client PROC subscribe_internal call emqb_topic:subscribe(~p) ~p -> ~p", [SubData, self(), TopicPid]),
        emqb_topic:subscribe(TopicPid, SubData),
        ?LOG_INFO("<<<<< emqb_client PROC subscribe_internal call emqb_topic:subscribe(~p) ~p <- ~p", [SubData, self(), TopicPid])
    end, TopicPids),
    ResCode = case QoS of
        ?QOS_0 -> ?RC_GRANTED_QOS_0;
        ?QOS_1 -> ?RC_GRANTED_QOS_1
    end,
    subscribe_internal(Data2, SubProps, Rest, ResProps, [ResCode | Acc]).

unsubscribe_internal(Data, Properties, PreparedTopics) ->
    unsubscribe_internal(Data, Properties, PreparedTopics, #{}, []).

unsubscribe_internal(Data, _SubProps, [], ResProps, Acc) ->
    {ResProps, lists:reverse(Acc), Data};
unsubscribe_internal(Data, SubProps, [{_, undefined} | Rest], ResProps, Acc) ->
    ResCode = ?RC_NO_SUBSCRIPTION_EXISTED,
    unsubscribe_internal(Data, SubProps, Rest, ResProps, [ResCode | Acc]);
unsubscribe_internal(Data = #data{topics = Topics}, SubProps,
                     [{_, SubData} | Rest], ResProps, Acc) ->
    #topic_subscription{ref = SubRef} = SubData,
    #{SubRef := TopicMap} = Topics,
    Topics2 = maps:remove(SubRef, Topics),
    Data2 = Data#data{topics = Topics2},
    maps:foreach(fun(_, TopicPid) ->
        ?LOG_INFO(">>>>> emqb_client PROC unsubscribe_internal call emqb_topic:unsubscribe(~p) ~p -> ~p", [SubRef, self(), TopicPid]),
        emqb_topic:unsubscribe(TopicPid, SubRef),
        ?LOG_INFO("<<<<< emqb_client PROC unsubscribe_internal call emqb_topic:unsubscribe(~p) ~p <- ~p", [SubRef, self(), TopicPid])
    end, TopicMap),
    ResCode = ?RC_SUCCESS,
    unsubscribe_internal(Data2, SubProps, Rest, ResProps, [ResCode | Acc]).

dispatch_internal(Data = #data{subscriptions = Subscriptions},
                  SubRef, TopicPath, Props, Payload, PubOpts) ->
    case maps:find(SubRef, Subscriptions) of
        error -> Data;
        {ok, SubData} ->
            dispatch_internal(SubData, TopicPath, Props, Payload, PubOpts),
            Data
    end.

%% Can be called either from the client's process or from the publisher's process
dispatch_internal(#topic_subscription{owner = Owner} = SubData,
                  TopicPath, Props, Payload, PubOpts) ->
    Msg = format_msg(SubData, TopicPath, Props, Payload, PubOpts),
    Owner ! {publish, Msg},
    ok.

format_msg(#topic_subscription{client = ClientPid, qos = QoS, sid = SubId},
           TopicPath, Properties, Payload, PubOpts) ->
    NewProps = case SubId of
        undefined -> Properties;
        Id -> Properties#{'Subscription-Identifier' => Id}
    end,
    PacketId = case min(QoS, opt_qos(PubOpts)) of
        ?QOS_0 -> undefined;
        ?QOS_1 -> make_ref()
    end,
    format_msg(ClientPid, QoS, PacketId, TopicPath, NewProps, Payload).

format_msg(ClientPid, QoS, PacketId, TopicPath, Properties, Payload) ->
    #{
        qos => QoS,
        dup => false,
        retain => false,
        packet_id => PacketId,
        topic => emqb_topic:format(TopicPath),
        properties => Properties,
        payload => Payload,
        client_pid => ClientPid
    }.

topic_added_internal(Data, TopicPath, TopicPid) ->
    #data{subscriptions = Subs, patterns = Tree, topics = Topics} = Data,
    % Get all the subscriptions that match the topic
    SubRefs = emqb_topic_tree:resolve(fun(_Pattern, SubRef, Acc) ->
        [SubRef | Acc]
    end, [], TopicPath, Tree),
    % Then return all the subscriptions for the new topic
    {Topics2, NewSubs} = lists:foldl(fun(Ref, {TMaps, Acc}) ->
        #{Ref := TMap} = TMaps,
        #{Ref := SubData} = Subs,
        TMaps2 = TMaps#{Ref => TMap#{TopicPath => TopicPid}},
        {TMaps2, [SubData | Acc]}
    end, {Topics, []}, SubRefs),
    {Data#data{topics = Topics2}, NewSubs}.

topic_removed_internal(Data, TopicPath) ->
    #data{patterns = Tree, topics = Topics} = Data,
    % Get all the subscriptions that match the topic
    SubRefs = emqb_topic_tree:resolve(fun(_Pattern, SubRef, Acc) ->
        [SubRef | Acc]
    end, [], TopicPath, Tree),
    % Then return all the reference for the subscriptions to be removed
    {Topics2, OldRefs} = lists:foldl(fun(Ref, {TMaps, Acc}) ->
        #{Ref := TMap} = TMaps,
        TMaps2 = TMaps#{Ref => maps:remove(TopicPath, TMap)},
        {TMaps2, [Ref | Acc]}
    end, {Topics, []}, SubRefs),
    {Data#data{topics = Topics2}, OldRefs}.
