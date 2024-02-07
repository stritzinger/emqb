%%--------------------------------------------------------------------
%% Copyright (c) 2024 Peer Stritzinger GmbH. All Rights Reserved.
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

-module(emqb_codec_json).

-behaviour(emqb_codec).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Behaviour embqb_codec callback functions
-export([encode/2]).
-export([decode/2]).


%%% BEHAVIOUR amqb_codec CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

encode(Properties, PayloadTerm) ->
    ContentType = maps:get('Content-Type', Properties, undefined),
    Format = maps:get('Payload-Format-Indicator', Properties, undefined),
    try
        case {ContentType, Format} of
            {<<"application/json">>, 1} ->
                {ok, Properties, jsx:encode(PayloadTerm)};
            {<<"application/json">>, undefined} ->
                NewProperties = Properties#{'Payload-Format-Indicator' => 1},
                {ok, NewProperties, jsx:encode(PayloadTerm)};
            {undefined, undefined} ->
                NewProperties = Properties#{
                    'Payload-Format-Indicator' => 1,
                    'Content-Type' => <<"application/json">>
                },
                {ok, NewProperties, jsx:encode(PayloadTerm)};
            {Type, Format} ->
                {error, {unsupported_content_type, {Type, Format}}}
        end
    catch
        error:badarg ->
            {error, {invalid_payload, PayloadTerm}}
    end.

decode(Properties, Payload) ->
    ContentType = maps:get('Content-Type', Properties, undefined),
    Format = maps:get('Payload-Format-Indicator', Properties, undefined),
    try
        case {ContentType, Format} of
            {<<"application/json">>, 1} ->
                {ok, json_decode(Payload)};
            {undefined, _} ->
                % Tries JSON anyway if there is no explicit content type
                {ok, json_decode(Payload)};
            {Type, Format} ->
                {error, {unsupported_content_type, {Type, Format}}}
        end
    catch
        error:badarg ->
            {error, {invalid_payload, Payload}}
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

json_decode(Data) ->
    jsx:decode(Data, [return_maps, {labels, attempt_atom}]).
