%%--------------------------------------------------------------------
%% @doc emqb root supervisor
%% @end
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

-module(emqb_sup).

-behaviour(supervisor).

%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Public functions
-export([start_link/0]).

% Behaviour supervisor callback functions
-export([init/1]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER, ?MODULE).
-define(SUPERVISOR(MOD), #{
    id => MOD,
    start => {MOD, start_link, []},
    restart => permanent,
    shutdown => infinity,
    type => supervisor,
    modules => [MOD]
}).
-define(WORKER(MOD, ARGS), #{
    id => MOD,
    start => {MOD, start_link, ARGS},
    restart => permanent,
    shutdown => 2000,
    type => worker,
    modules => [MOD]
}).
-define(WORKER(MOD), ?WORKER(MOD, [])).


%%% PUBLIC FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).


%%% BEHAVIOUR supervisor CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 1,
        period => 120
    },
    ChildSpecs = [
        ?SUPERVISOR(emqb_topic_sup),
        ?WORKER(emqb_registry),
        ?WORKER(emqb_manager)
    ],
    {ok, {SupFlags, ChildSpecs}}.
