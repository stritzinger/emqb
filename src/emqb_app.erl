%%%-------------------------------------------------------------------
%% @doc emqb main application callback module
%% @end
%%
%% @author Sebastien Merle <s.merle@gmail.com>
%%
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

-module(emqb_app).

-behaviour(application).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API functions
-export([instance_id/0]).

% Behaviour application callback functions
-export([start/2]).
-export([stop/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

instance_id() -> persistent_term:get(trag_instance_id).


%%% BEHAVIOUR application CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start(_StartType, _StartArgs) ->
    InstanceId = base64:encode(crypto:strong_rand_bytes(12)),
    persistent_term:put(trag_instance_id, InstanceId),
    emqb_sup:start_link().

stop(_State) ->
    ok.
