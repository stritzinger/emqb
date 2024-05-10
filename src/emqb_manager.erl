%%--------------------------------------------------------------------
%% @doc emqb topic manager
%% Manage the creation of the topic processes.
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

-module(emqb_manager).

-behaviour(gen_server).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API Functions
-export([start_link/0]).
-export([topic/1]).

% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {}).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER, ?MODULE).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec topic(emqb_topic:path()) -> {ok, pid()} | {error, term()}.
topic(TopicPath) ->
    case emqb_registry:lookup_topic(TopicPath) of
        {ok, TopicPid} -> {ok, TopicPid};
        error ->
            % ?LOG_INFO(">>>>> ~p emqb_manager API topic", [self()]),
            Res = gen_server:call(?SERVER, {create_topic, TopicPath}),
            % ?LOG_INFO("<<<<< ~p emqb_manager API topic", [self()]),
            Res
    end.


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    {ok, #state{}}.

handle_call({create_topic, TopicPath}, _From, State) ->
    case emqb_registry:lookup_topic(TopicPath) of
        {ok, TopicPid} -> {reply, {ok, TopicPid}, State};
        error ->
            % ?LOG_INFO(">>>>> ~p emqb_manager process call emqb_topic_sup:start_topic", [self()]),
            Pid = emqb_topic_sup:start_topic(TopicPath),
            % ?LOG_INFO("<<<<< ~p emqb_manager process call emqb_topic_sup:start_topic", [self()]),
            {reply, Pid, State}
            % {reply, emqb_topic_sup:start_topic(TopicPath), State}
    end;
handle_call(Request, From, State) ->
    ?LOG_WARNING("Unexpected call ~p from ~p", [Request, From]),
    {reply, {error, unexpected_call}, State}.

handle_cast(Msg, State) ->
    ?LOG_WARNING("Unexpected cast ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG_WARNING("Unexpected message ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
