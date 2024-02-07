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

-module(emqb_utils).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("emqtt/include/emqtt.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export([format_hostname/1, format_hostname/2, format_hostname/3]).
-export([mqtt_code2reason/1]).
-export([mqtt_discode2reason/1]).
-export([mqtt_reason2discode/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

format_hostname({_, _, _, _} = Ip) ->
    list_to_binary(inet:ntoa(Ip));
format_hostname({_, _, _, _, _, _, _, _} = Ip) ->
    list_to_binary(inet:ntoa(Ip));
format_hostname(Hostname) when is_atom(Hostname) ->
    atom_to_binary(Hostname);
format_hostname(Hostname) when is_list(Hostname) ->
    list_to_binary(Hostname);
format_hostname(Hostname) when is_binary(Hostname) ->
    Hostname.

format_hostname(Input, undefined) ->
    format_hostname(Input);
format_hostname(Input, Port) when is_integer(Port), Port > 0, Port < 65536 ->
    Hostname = format_hostname(Input),
    PortStr = integer_to_binary(Port),
    <<Hostname/binary, $:, PortStr/binary>>.

format_hostname(Input, undefined, _DefaultPort) ->
    format_hostname(Input);
format_hostname(Input, DefaultPort, DefaultPort) ->
    format_hostname(Input);
format_hostname(Input, OtherPort, _DefaultPort) ->
    format_hostname(Input, OtherPort).

mqtt_code2reason(?RC_SUCCESS) ->
    success;
mqtt_code2reason(?RC_GRANTED_QOS_1) ->
    granted_qos_1;
mqtt_code2reason(?RC_GRANTED_QOS_2) ->
    granted_qos_2;
mqtt_code2reason(?RC_DISCONNECT_WITH_WILL_MESSAGE) ->
    disconnect_with_will_message;
mqtt_code2reason(?RC_NO_MATCHING_SUBSCRIBERS) ->
    no_matching_subscribers;
mqtt_code2reason(?RC_NO_SUBSCRIPTION_EXISTED) ->
    no_subscription_existed;
mqtt_code2reason(?RC_CONTINUE_AUTHENTICATION) ->
    continue_authentication;
mqtt_code2reason(?RC_RE_AUTHENTICATE) ->
    re_authenticate;
mqtt_code2reason(?RC_UNSPECIFIED_ERROR) ->
    unspecified_error;
mqtt_code2reason(?RC_MALFORMED_PACKET) ->
    malformed_packet;
mqtt_code2reason(?RC_PROTOCOL_ERROR) ->
    protocol_error;
mqtt_code2reason(?RC_IMPLEMENTATION_SPECIFIC_ERROR) ->
    implementation_specific_error;
mqtt_code2reason(?RC_UNSUPPORTED_PROTOCOL_VERSION) ->
    unsupported_protocol_version;
mqtt_code2reason(?RC_CLIENT_IDENTIFIER_NOT_VALID) ->
    client_identifier_not_valid;
mqtt_code2reason(?RC_BAD_USER_NAME_OR_PASSWORD) ->
    bad_user_name_or_password;
mqtt_code2reason(?RC_NOT_AUTHORIZED) ->
    not_authorized;
mqtt_code2reason(?RC_SERVER_UNAVAILABLE) ->
    server_unavailable;
mqtt_code2reason(?RC_SERVER_BUSY) ->
    server_busy;
mqtt_code2reason(?RC_BANNED) ->
    banned;
mqtt_code2reason(?RC_SERVER_SHUTTING_DOWN) ->
    server_shutting_down;
mqtt_code2reason(?RC_BAD_AUTHENTICATION_METHOD) ->
    bad_authentication_method;
mqtt_code2reason(?RC_KEEP_ALIVE_TIMEOUT) ->
    keep_alive_timeout;
mqtt_code2reason(?RC_SESSION_TAKEN_OVER) ->
    session_taken_over;
mqtt_code2reason(?RC_TOPIC_FILTER_INVALID) ->
    topic_filter_invalid;
mqtt_code2reason(?RC_TOPIC_NAME_INVALID) ->
    topic_name_invalid;
mqtt_code2reason(?RC_PACKET_IDENTIFIER_IN_USE) ->
    packet_identifier_in_use;
mqtt_code2reason(?RC_PACKET_IDENTIFIER_NOT_FOUND) ->
    packet_identifier_not_found;
mqtt_code2reason(?RC_RECEIVE_MAXIMUM_EXCEEDED) ->
    receive_maximum_exceeded;
mqtt_code2reason(?RC_TOPIC_ALIAS_INVALID) ->
    topic_alias_invalid;
mqtt_code2reason(?RC_PACKET_TOO_LARGE) ->
    packet_too_large;
mqtt_code2reason(?RC_MESSAGE_RATE_TOO_HIGH) ->
    message_rate_too_high;
mqtt_code2reason(?RC_QUOTA_EXCEEDED) ->
    quota_exceeded;
mqtt_code2reason(?RC_ADMINISTRATIVE_ACTION) ->
    administrative_action;
mqtt_code2reason(?RC_PAYLOAD_FORMAT_INVALID) ->
    payload_format_invalid;
mqtt_code2reason(?RC_RETAIN_NOT_SUPPORTED) ->
    retain_not_supported;
mqtt_code2reason(?RC_QOS_NOT_SUPPORTED) ->
    qos_not_supported;
mqtt_code2reason(?RC_USE_ANOTHER_SERVER) ->
    use_another_server;
mqtt_code2reason(?RC_SERVER_MOVED) ->
    server_moved;
mqtt_code2reason(?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED) ->
    shared_subscriptions_not_supported;
mqtt_code2reason(?RC_CONNECTION_RATE_EXCEEDED) ->
    connection_rate_exceeded;
mqtt_code2reason(?RC_MAXIMUM_CONNECT_TIME) ->
    maximum_connect_time;
mqtt_code2reason(?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED) ->
    subscription_identifiers_not_supported;
mqtt_code2reason(?RC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED) ->
    wildcard_subscriptions_not_supported;
mqtt_code2reason(UnknownCode) ->
    UnknownCode.

mqtt_discode2reason(?RC_NORMAL_DISCONNECTION) ->
    normal_disconnection;
mqtt_discode2reason(Other) ->
    mqtt_code2reason(Other).

mqtt_reason2discode(normal) -> ?RC_NORMAL_DISCONNECTION;
mqtt_reason2discode(shutdown) -> ?RC_SERVER_SHUTTING_DOWN;
mqtt_reason2discode(_Other) -> ?RC_UNSPECIFIED_ERROR.
