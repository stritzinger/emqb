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
-include("emqb_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API Functions
-export([start_link/0]).
-export([lookup_topic/1]).
-export([match_topics/1]).
-export([register_client/1]).
-export([register_topic/2]).
-export([add_subscriptions/2]).
-export([del_subscriptions/2]).
-export([get_subscriptions/1]).

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
    monitors = #{} :: #{reference() => #topic_data{} | #client_data{}},
    async :: emqb_client:async_context(),
    pending = #{} :: #{reference() => {pos_integer(), gen_server:from(), Acc :: list()}}
}).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER, ?MODULE).
-define(TOPIC_TABLE, emqb_topics).
-define(SUBSCRIPTION_TABLE, emqb_subscriptions).


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
    ?LOG_INFO(">>>>> emqb_registry API match_topics(~p) ~p -> ~p", [TopicPattern, self(), ?SERVER]),
    Res = gen_server:call(?SERVER, {match_topics, TopicPattern}),
    ?LOG_INFO("<<<<< emqb_registry API match_topics(~p) ~p <- ~p", [TopicPattern, self(), ?SERVER]),
    Res.

-spec register_client(pid()) -> ok.
register_client(ClientPid) ->
    ?LOG_INFO(">>>>> emqb_registry API register_client(~p) ~p -> ~p", [ClientPid, self(), ?SERVER]),
    Res = gen_server:call(?SERVER, {register_client, ClientPid}),
    ?LOG_INFO("<<<<< emqb_registry API register_client(~p) ~p <- ~p", [ClientPid, self(), ?SERVER]),
    Res.

-spec register_topic(pid(), emqb_topic:path()) -> [topic_subscription()].
register_topic(TopicPid, TopicPath) ->
    ?LOG_INFO(">>>>> emqb_registry API register_topic(~p, ~p) ~p -> ~p", [TopicPid, TopicPath, self(), ?SERVER]),
    Res = gen_server:call(?SERVER, {register_topic, TopicPid, TopicPath}),
    ?LOG_INFO("<<<<< emqb_registry API register_topic(~p, ~p) ~p <- ~p", [TopicPid, TopicPath, self(), ?SERVER]),
    Res.

-spec add_subscriptions(emqb_topic:path(), topic_subscription() | [topic_subscription()]) -> ok.
add_subscriptions(TopicPath, Subscriptions) ->
    ?LOG_INFO(">>>>> emqb_registry API add_subscriptions(~p, ~p) ~p -> ~p", [TopicPath, Subscriptions, self(), ?SERVER]),
    Res = gen_server:call(?SERVER, {add_subscriptions, TopicPath, Subscriptions}),
    ?LOG_INFO("<<<<< emqb_registry API add_subscriptions(~p, ~p) ~p <- ~p", [TopicPath, Subscriptions, self(), ?SERVER]),
    Res.

-spec del_subscriptions(emqb_topic:path(), reference() | [reference()]) -> ok.
del_subscriptions(TopicPath, SubRefs) ->
    ?LOG_INFO(">>>>> emqb_registry API del_subscriptions(~p, ~p) ~p -> ~p", [TopicPath, SubRefs, self(), ?SERVER]),
    Res = gen_server:call(?SERVER, {del_subscriptions, TopicPath, SubRefs}),
    ?LOG_INFO("<<<<< emqb_registry API del_subscriptions(~p, ~p) ~p <- ~p", [TopicPath, SubRefs, self(), ?SERVER]),
    Res.

-spec get_subscriptions(emqb_topic:path()) -> #{reference => topic_subscription()}.
get_subscriptions(TopicPath) ->
    try ets:lookup(?SUBSCRIPTION_TABLE, TopicPath) of
        [{_TopicPath, Subscriptions}] -> Subscriptions;
        [] -> #{}
    catch
        %% Case where the table does not exist yet.
        error:badarg -> #{}
    end.


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    ets:new(?TOPIC_TABLE, [protected, named_table, set, {read_concurrency, true}]),
    ets:new(?SUBSCRIPTION_TABLE, [protected, named_table, set, {read_concurrency, true}]),
    AsyncCtx = emqb_client:async_new(),
    {ok, #state{topics = emqb_topic_tree:new(), async = AsyncCtx}}.

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
handle_call({register_topic, TopicPid, TopicPath}, From,
            State = #state{clients = Clients, topics = TopicTree,
                           monitors = Monitors, async = Async,
                           pending = Pending}) ->
    MonRef = erlang:monitor(process, TopicPid),
    Data = #topic_data{path = TopicPath, pid = TopicPid, mon = MonRef},
    NewMonitors = Monitors#{MonRef => Data},
    ets:insert(?TOPIC_TABLE, {TopicPath, TopicPid}),
    NewTopicTree = emqb_topic_tree:update(TopicPath, Data, TopicTree),
    PendingRef = make_ref(),
    Pending2 = Pending#{PendingRef => {maps:size(Clients), From, []}},
    Async2 = maps:fold(fun(ClientPid, _, A) ->
        Label = {topic_added, PendingRef, TopicPath},
        emqb_client:async_topic_added(A, ClientPid, TopicPath, TopicPid, Label)
    end, Async, Clients),
    NewState = State#state{topics = NewTopicTree, monitors = NewMonitors,
                           async = Async2, pending = Pending2},
    {noreply, NewState};
handle_call({add_subscriptions, TopicPath, Subscriptions}, _From, State) ->
    {reply, addsub(TopicPath, Subscriptions), State};
handle_call({del_subscriptions, TopicPath, SubRefs}, _From, State) ->
    {reply, delsub(TopicPath, SubRefs), State};
handle_call(Request, From, State) ->
    ?LOG_WARNING("Unexpected call ~p from ~p", [Request, From]),
    {reply, {error, unexpected_call}, State}.

handle_cast(Msg, State) ->
    ?LOG_WARNING("Unexpected cast ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', MonRef, process, _TopicPid, _Reason},
            State = #state{topics = TopicTree, clients = Clients,
                           monitors = Monitors, async = Async}) ->
    case maps:take(MonRef, Monitors) of
        error -> {noreply, State};
        {#topic_data{path = TopicPath}, NewMonitors} ->
            % Topic process died
            ets:delete(?TOPIC_TABLE, TopicPath),
            NewTopicTree = emqb_topic_tree:remove(TopicPath, TopicTree),
            Async2 = maps:fold(fun(ClientPid, _, A) ->
                Label = {topic_removed, TopicPath},
                emqb_client:async_topic_removed(A, ClientPid, TopicPath, Label)
            end, Async, Clients),
            NewState = State#state{topics = NewTopicTree,
                                   monitors = NewMonitors,
                                   async = Async2},
            {noreply, NewState};
        {#client_data{pid = ClientPid}, NewMonitors} ->
            % Client process died
            NewClients = maps:remove(ClientPid, Clients),
            NewState = State#state{clients = NewClients,
                                   monitors = NewMonitors},
            {noreply, NewState}
    end;
handle_info(Info, State = #state{async = Async}) ->
    case emqb_client:async_check(Info, Async) of
        A when A =:= no_reply; A =:= no_request ->
            ?LOG_WARNING("Unexpected message ~p", [Info]),
            {noreply, State};
        {{reply, Subs}, {topic_added, Ref, TopicPath}, Async2} ->
            addsub(TopicPath, Subs),
            State2 = async_topic_added(State, Ref, Subs),
            {noreply, State2#state{async = Async2}};
        {{error, {Reason, ServerRef}}, {topic_added, Ref, _TopicPath}, Async2} ->
            ?LOG_WARNING("Failed to notify added topic to ~p: ~p",
                         [ServerRef, Reason]),
            State2 = async_topic_added(State, Ref, []),
            {noreply, State2#state{async = Async2}};
        {{reply, SubRefs}, {topic_removed, TopicPath}, Async2} ->
            delsub(TopicPath, SubRefs),
            {noreply, State#state{async = Async2}};
        {{error, {Reason, ServerRef}}, {topic_removed, _TopicPath}, Async2} ->
            ?LOG_WARNING("Failed to notify removed topic to ~p: ~p",
                         [ServerRef, Reason]),
            {noreply, State#state{async = Async2}}
    end.

terminate(_Reason, _State) ->
    ok.


%%% INTERNAL FUNCTION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

async_topic_added(State = #state{pending = Pending}, Ref, Subs) ->
    #{Ref := {Count, From, Acc}} = Pending,
    case Count > 1 of
        true ->
            Pending2 = Pending#{Ref := {Count - 1, From, [Subs | Acc]}},
            State#state{pending = Pending2};
        false ->
            gen_server:reply(From, lists:flatten([Subs | Acc])),
            Pending2 = maps:remove(Ref, Pending),
            State#state{pending = Pending2}
    end.

addsub(TopicPath, #topic_subscription{ref = SubRef} = Sub) ->
    case ets:lookup(?SUBSCRIPTION_TABLE, TopicPath) of
        [{_TopicPath, CurrSubs}] ->
            NewSubs = CurrSubs#{SubRef => Sub},
            ets:insert(?SUBSCRIPTION_TABLE, {TopicPath, NewSubs});
        [] ->
            ets:insert(?SUBSCRIPTION_TABLE, {TopicPath, #{SubRef => Sub}})
    end,
    ok;
addsub(TopicPath, Subs) when is_list(Subs) ->
    SubMap = maps:from_list([{R, S} || #topic_subscription{ref = R} = S <- Subs]),
    case ets:lookup(?SUBSCRIPTION_TABLE, TopicPath) of
        [{_TopicPath, CurrSubs}] ->
            ets:insert(?SUBSCRIPTION_TABLE, {TopicPath, maps:merge(CurrSubs, SubMap)});
        [] ->
            ets:insert(?SUBSCRIPTION_TABLE, {TopicPath, SubMap})
    end,
    ok.

delsub(TopicPath, SubRef) when is_reference(SubRef) ->
    case ets:lookup(?SUBSCRIPTION_TABLE, TopicPath) of
        [{_TopicPath, CurrSubs}] ->
            case maps:remove(SubRef, CurrSubs) of
                NewSubs when map_size(NewSubs) =:= 0 ->
                    ets:delete(?SUBSCRIPTION_TABLE, TopicPath);
                NewSubs ->
                    ets:insert(?SUBSCRIPTION_TABLE, {TopicPath, NewSubs})
            end;
        [] -> true
    end,
    ok;
delsub(TopicPath, SubRefs) when is_list(SubRefs) ->
    case ets:lookup(?SUBSCRIPTION_TABLE, TopicPath) of
        [{_TopicPath, CurrSubs}] ->
            case maps:without(SubRefs, CurrSubs) of
                NewSubs when map_size(NewSubs) =:= 0 ->
                    ets:delete(?SUBSCRIPTION_TABLE, TopicPath);
                NewSubs ->
                    ets:insert(?SUBSCRIPTION_TABLE, {TopicPath, NewSubs})
            end;
        [] -> true
    end,
    ok.
