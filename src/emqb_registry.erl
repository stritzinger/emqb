%%--------------------------------------------------------------------
%% @doc emqb topic registry
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

-module(emqb_registry).

-behaviour(gen_server).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API Functions
-export([start_link/0]).
-export([lookup_topic/1]).
-export([match_topics/1]).
-export([register_client/1]).
-export([register_topic/2]).

% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(topic_data, {
    path :: emqb_topic:path(),
    pid :: pid(),
    mon :: reference()
}).

-record(client_data, {
    pid :: pid(),
    mon :: reference()
}).

-record(state, {
    clients = #{} :: #{pid() => #client_data{}},
    topics :: emqb_topic_tree:topic_tree(#topic_data{}),
    monitors = #{} :: #{reference() => #topic_data{} | #client_data{}}
}).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER, ?MODULE).
-define(TOPIC_TABLE, emqb_topic_table).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec lookup_topic(emqb_topic:path()) -> {ok, pid()} | error.
lookup_topic(TopicPath) ->
    case ets:lookup(?TOPIC_TABLE, TopicPath) of
        [{_, TopicPid}] -> {ok, TopicPid};
        [] -> error
    end.

-spec match_topics(emqb_topic:path()) -> [{emqb_topic:path(), pid()}].
match_topics(TopicPattern) ->
    gen_server:call(?SERVER, {match_topics, TopicPattern}).

-spec register_client(pid()) -> ok.
register_client(ClientPid) ->
    gen_server:call(?SERVER, {register_client, ClientPid}).

-spec register_topic(pid(), emqb_topic:path()) -> ok.
register_topic(TopicPid, TopicPath) ->
    gen_server:call(?SERVER, {register_topic, TopicPid, TopicPath}).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    ets:new(?TOPIC_TABLE, [protected, named_table, set, {read_concurrency, true}]),
    {ok, #state{topics = emqb_topic_tree:new()}}.

handle_call({match_topics, TopicPattern}, _From,
            State = #state{topics = TopicTree}) ->
    Result = emqb_topic_tree:match(
        fun(TopicPath, #topic_data{pid = TopicPid}, Acc) ->
            [{TopicPath, TopicPid} | Acc]
        end, [], TopicPattern, TopicTree),
    {reply, Result, State};
handle_call({register_client, ClientPid}, _From,
            State = #state{clients = Clients, monitors = Monitors}) ->
    MonRef = erlang:monitor(process, ClientPid),
    Data = #client_data{pid = ClientPid, mon = MonRef},
    NewClients = Clients#{ClientPid => Data},
    NewMonitors = Monitors#{MonRef => Data},
    {reply, ok, State#state{clients = NewClients, monitors = NewMonitors}};
handle_call({register_topic, TopicPid, TopicPath}, _From,
            State = #state{clients = Clients, topics = TopicTree,
                           monitors = Monitors}) ->
    MonRef = erlang:monitor(process, TopicPid),
    Data = #topic_data{path = TopicPath, pid = TopicPid, mon = MonRef},
    NewMonitors = Monitors#{MonRef => Data},
    ets:insert(?TOPIC_TABLE, {TopicPath, TopicPid}),
    NewTopicTree = emqb_topic_tree:update(TopicPath, Data, TopicTree),
    NewState = State#state{topics = NewTopicTree, monitors = NewMonitors},
    maps:foreach(fun(ClientPid, _) ->
        emqb_client:topic_added(ClientPid, TopicPath, TopicPid)
    end, Clients),
    {reply, ok, NewState};
handle_call(Request, From, State) ->
    ?LOG_WARNING("Unexpected call ~p from ~p", [Request, From]),
    {reply, {error, unexpected_call}, State}.

handle_cast(Msg, State) ->
    ?LOG_WARNING("Unexpected cast ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', MonRef, process, TopicPid, _Reason},
            State = #state{topics = TopicTree, clients = Clients,
                           monitors = Monitors}) ->
    case maps:take(MonRef, Monitors) of
        error -> {noreply, State};
        {#topic_data{path = TopicPath}, NewMonitors} ->
            % Topic process died
            ets:delete(?TOPIC_TABLE, TopicPath),
            NewTopicTree = emqb_topic_tree:remove(TopicPath, TopicTree),
            NewState = State#state{topics = NewTopicTree,
                                   monitors = NewMonitors},
            maps:foreach(fun(ClientPid, _) ->
                emqb_client:topic_removed(ClientPid, TopicPath, TopicPid)
            end, Clients),
            {noreply, NewState};
        {#client_data{pid = ClientPid}, NewMonitors} ->
            % Client process died
            NewClients = maps:remove(ClientPid, Clients),
            NewState = State#state{clients = NewClients,
                                   monitors = NewMonitors},
            {noreply, NewState}
    end;
handle_info(Info, State) ->
    ?LOG_WARNING("Unexpected message ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
