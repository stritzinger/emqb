%%--------------------------------------------------------------------
%% @doc MQTT bridge client.
%%
%% It offers an interface similar to <c>emqtt</c>, but allows the clients
%% running in the same VM to comunicate directly without waiting for the MQTT
%% broker messages to get back.
%%
%% Only supports MQTT version 5 brokers.
%%
%% Currently not supporting QoS 2 and QoS 1 is pretty much simulated when
%% messages are delivered internally.
%%
%% Implement automatic reconnection to the broker, with exponential backoff.
%%
%% The owner of the client will receive the following messages types:
%% <ul>
%%   <li><c>{publish, emqb:message()}</c></li>
%%   <li><c>{puback, emqb:puback()}</c></li>
%% </ul>
%%
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

-module(emqb).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("emqtt/include/emqtt.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API Functions
-export([start_link/0, start_link/1]).
-export([subscribe/2, subscribe/3, subscribe/4]).
-export([unsubscribe/2, unsubscribe/3]).
-export([publish/3, publish/4, publish/5]).
-export([puback/2, puback/3, puback/4]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type payload() :: term().
-type mode() :: internal | external | hybride.
-type conn_type() :: tcp | ws.

% Change to use emqtt:qos() when supporting QoS 2
-type qos() :: ?QOS_0 | ?QOS_1.
% Change to use emqtt:qos_name() when supporting QoS 2
-type qos_name() :: qos0 | at_most_once |
                    qos1 | at_least_once.

-type message() :: #{
    qos := qos(),
    dup := boolean(),
    retain := boolean(),
    packet_id := emqtt:packet_id() | reference(),
    topic := emqtt:topic(),
    properties := emqtt:properties(),
    payload := payload(),
    client_pid := pid()
}.

-type puback() :: #{
    packet_id := emqtt:packet_id() | reference(),
    properties := emqtt:properties(),
    reason_code := emqtt:reason_code()
}.

% The emqtt option proto_ver is not allowed, but for simplicity...
-type option() :: emqtt:options()
                | {mode, mode()}
                | {conn_type, conn_type()}.
-type option_map() :: #{
    name => atom(),
    owner => pid(),
    mode => mode(),
    conn_type => conn_type(),
    reconn_max_retries => infinity | non_neg_integer(),
    reconn_base_delay => pos_integer(),
    reconn_max_delay => pos_integer(),
    reconn_multiplier => pos_integer(),
    reconn_jitter => pos_integer(),
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
% If the bypass option is true, the message will not be published to the
% external broker if there was at least one local consumer.
-type pubopt() :: {retain, boolean()}
                | {qos, qos() | qos_name()}
                | {bypass, boolean()}.
-type pubopts() :: qos() | qos_name() | [pubopt()].

-export_type([
    mode/0, conn_type/0, qos/0, option/0, subscribe_ret/0, payload/0,
    message/0, puback/0
]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts a amqb client.
%% Same as `amqb:start_link(#{})'.
-spec start_link() -> gen_statem:start_ret().
start_link() ->
    emqb_client:start_link([]).

%% @doc Starts a amqb client.
%% <p>This does <b>NOT</b> return a simple PID, it returns an opaque data
%% structure. It means that this cannot be called by a supervidor.
%% Options are the same as <c>emqtt</c> minus the protocol version, as
%% <c>emqb</c> only supports MQTT version 5.</p>
%% <p>The extra possible options specific to <c>emqbq</c> are:
%% <ul>
%%     <li><c>mode</c>: the startup mode of the MQTT bridge, <c>external</c>,
%%       <c>internal</c> or <c>hybride</c>. In internal mode, the bridge will
%%       not try to connect to a MQTT broker and will only work internally.
%%       In external mode the bridge will connect to the MQTT broker and
%%       only use external MQTT message. In hybride mode, the bridge will
%%       connect to the MQTT broker and post the message both internally
%%       and to the broker, and if the publish option <c>bypass</c> is
%%       specified and there is a local consumer for the message, it will
%%       not be published to the MQTT broker. Default: <c>hybride</c>.
%%     </li>
%%     <li><c>conn_type</c>: the connection type to use to connect to the MQTT
%%      broker, could be either <c>tcp</c> or <c>ws</c> and the default is
%%      <c>tcp</c>.
%%    </li>
%%    <li><c>reconn_max_retries</c>: the maximum number of time the client will
%%      retry to connect to the MQTT broker. Default: <c>8</c>.
%%    </li>
%%    <li><c>reconn_base_delay</c>: the base delay in milliseconds for
%%      reconnecting to the MQTT broker after disconnection.
%%      Default: <c>1000</c>.
%%    </li>
%%    <li><c>reconn_max_delay</c>: the maximum delay in milliseconds for
%%      reconnection. Default: <c>32000</c>.
%%    </li>
%%    <li><c>reconn_multiplier</c>: the multiplier for reconnection exponential
%%      backoff. Default: <c>2</c>.
%%    </li>
%%    <li><c>reconn_jitter</c>: the jitter in milliseconds for reconnection
%%      exponential backoff. The random value added to the connection delay
%%      will be between <c>(-(J div 2) - (J rem 2))</c> and <c>(J div 2)</c>
%%      where <c>J</c> is the value of this options. Default: <c>1000</c>.
%%    </li>
%%    <li><c>codec</c>: the codec callback module implementing behaviour
%%      <c>emqb_codec</c> to use for encoding and decoding MQTT payloads.
%%      Default: <c>emqb_codec_json</c>.
%%    </li>
%%  </ul></p>
-spec start_link(start_opts()) -> gen_statem:start_ret().
start_link(StartOpts) when is_map(StartOpts) ->
    emqb_client:start_link(maps:to_list(StartOpts));
start_link(StartOpts) when is_list(StartOpts) ->
    emqb_client:start_link(StartOpts).

%% @doc Subscribes to a to MQTT topics.
-spec subscribe(emqb_client:client(), topic_spec()) -> subscribe_ret().
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
    TopicSpec2 = lists:map(fun(SpecItem) ->
        validate_topic_spec(SpecItem)
    end, TopicSpec),
    emqb_client:subscribe(Client, #{}, TopicSpec2).

%% @doc Subscribes to a to MQTT topics.
-dialyzer({nowarn_function, subscribe/3}).
-spec(subscribe(emqb_client:client(), emqtt:topic(), subopts()) -> subscribe_ret();
               (emqb_client:client(), emqtt:properties(), topic_spec()) -> subscribe_ret()).
subscribe(Client, Topic, QoS)
  when is_binary(Topic), is_atom(QoS) ->
    emqb_client:subscribe(Client, #{}, [{Topic, [{qos, atom2qos(QoS)}]}]);
subscribe(Client, Topic, QoS)
  when is_binary(Topic), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    emqb_client:subscribe(Client, #{}, [{Topic, [{qos, QoS}]}]);
subscribe(Client, Topic, SubOpts)
  when is_binary(Topic), is_list(SubOpts) ->
    emqb_client:subscribe(Client, #{}, [{Topic, SubOpts}]);
subscribe(Client, Properties, Topic)
  when is_map(Properties), is_binary(Topic) ->
    emqb_client:subscribe(Client, Properties, [{Topic, [{qos, ?QOS_0}]}]);
subscribe(Client, Properties, {Topic, QoS})
  when is_map(Properties), is_binary(Topic), is_atom(QoS) ->
    emqb_client:subscribe(Client, Properties, [{Topic, [{qos, atom2qos(QoS)}]}]);
subscribe(Client, Properties, {Topic, QoS})
  when is_map(Properties), is_binary(Topic), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    emqb_client:subscribe(Client, Properties, [{Topic, [{qos, QoS}]}]);
subscribe(Client, Properties, TopicSpec)
  when is_map(Properties), is_list(TopicSpec) ->
    TopicSpec2 = lists:map(fun(SpecItem) ->
        validate_topic_spec(SpecItem)
    end, TopicSpec),
    emqb_client:subscribe(Client, Properties, TopicSpec2).

%% @doc Subscribes to a to MQTT topics.
-spec subscribe(emqb_client:client(), emqtt:properties(), emqtt:topic(), subopts())
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

%% @doc Publishes to a to MQTT topics.
-spec publish(emqb_client:client(), emqtt:topic(), payload()) -> ok | {error, term()}.
publish(Client, Topic, Payload) when is_binary(Topic) ->
    emqb_client:publish(Client, Topic, #{}, Payload, [{qos, ?QOS_0}]).

%% @doc Unsubscribes from MQTT topics.
-spec unsubscribe(emqb_client:client(), emqtt:topic() | [emqtt:topic()])
    -> subscribe_ret().
unsubscribe(Client, Topic)
  when is_binary(Topic) ->
    unsubscribe(Client, [Topic]);
unsubscribe(Client, Topics)
  when is_list(Topics) ->
    unsubscribe(Client, #{}, Topics).

%% @doc Unsubscribes from MQTT topics.
-spec unsubscribe(emqb_client:client(), emqtt:properties(), emqtt:topic() | [emqtt:topic()])
    -> subscribe_ret().
unsubscribe(Client, Properties, Topic)
  when is_map(Properties), is_binary(Topic) ->
    unsubscribe(Client, Properties, [Topic]);
unsubscribe(Client, Properties, Topics)
  when is_map(Properties), is_list(Topics) ->
    emqb_client:unsubscribe(Client, Properties, Topics).

%% @doc Publishes to a to an MQTT topic.
-spec publish(emqb_client:client(), emqtt:topic(), payload(), pubopts())
    -> ok | {ok, emqtt:packet_id() | reference()} | {error, term()}.
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

%% @doc Publishes to a to an MQTT topic.
-spec publish(emqb_client:client(), emqtt:topic(), emqtt:properties(), payload(), pubopts())
    -> ok | {ok, emqtt:packet_id() | reference()} | {error, term()}.
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

%% @doc Acknowledge a message received with QoS 1 and no `auto_ack'.
-spec puback(emqb_client:client(), emqtt:packet_id()) -> ok.
puback(Client, PacketId) when is_integer(PacketId) ->
    emqb_client:puback(Client, PacketId, ?RC_SUCCESS, #{}).

%% @doc Acknowledge a message received with QoS 1 and no `auto_ack'.
-spec puback(emqb_client:client(), emqtt:packet_id(), emqtt:reason_code()) -> ok.
puback(Client, PacketId, ReasonCode)
  when is_integer(PacketId), is_integer(ReasonCode) ->
    emqb_client:puback(Client, PacketId, ReasonCode, #{}).

%% @doc Acknowledge a message received with QoS 1 and no `auto_ack'.
-spec puback(emqb_client:client(), emqtt:packet_id(), emqtt:reason_code(), emqtt:properties()) -> ok.
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

validate_topic_spec(Topic)
  when is_binary(Topic) ->
    {Topic, [{qos, ?QOS_0}]};
validate_topic_spec({Topic, QoS})
 when is_binary(Topic), QoS >= ?QOS_0, QoS =< ?QOS_1 ->
    {Topic, [{qos, QoS}]};
validate_topic_spec({Topic, QoS})
 when is_binary(Topic), ?IS_QOS_NAME(QoS) ->
    {Topic, [{qos, atom2qos(QoS)}]};
validate_topic_spec({Topic, SubOpts} = Result)
 when is_binary(Topic) ->
    validate_qos(SubOpts),
    Result;
validate_topic_spec(Other) ->
    throw({invlid_topic_spec, Other}).

atom2qos(qos0) -> 0;
atom2qos(at_most_once) -> 0;
atom2qos(qos1) -> 1;
atom2qos(at_least_once) -> 1;
atom2qos(Other) -> throw({mqtt_qos_not_supported, Other}).
