%%--------------------------------------------------------------------
%% @doc emqb topic process
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

-module(emqb_topic).

-behaviour(gen_server).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API Functions
-export([format/1]).
-export([parse/1]).
-export([validate/1]).
-export([start_link/1]).
-export([subscribe/3]).
-export([unsubscribe/3]).
-export([publish/4]).

% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type path() :: [level()].
-type level() :: binary() | $+ | $#.

-record(subscriber_data, {
    pid :: pid(),
    sub :: pos_integer(),
    mon :: reference()
}).

-record(state, {
    path :: emqb_topic:path(),
    subscribers = #{} :: #{{pid(), pos_integer()} => #subscriber_data{}},
    monitors = #{} :: #{reference() => #subscriber_data{}}
}).

-export_type([path/0, level/0]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Time without any published message after which a topic without any subscribers
% will be shutdown.
-define(INACTIVITY_TIMOUT, 60000).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec format(binary() | emqb_topic:path()) -> binary().
format(<<"">>) -> erlang:error(badarg);
format([]) -> erlang:error(badarg);
format(Topic) when is_binary(Topic) -> Topic;
format(TopicPath) when is_list(TopicPath) ->
    erlang:iolist_to_binary(lists:join($/, TopicPath)).

-spec parse(binary() | emqb_topic:path()) -> emqb_topic:path().
parse(<<"">>) -> erlang:error(badarg);
parse([]) -> erlang:error(badarg);
parse(TopicPath) when is_list(TopicPath) ->
    validate(TopicPath),
    TopicPath;
parse(Topic) when is_binary(Topic) ->
    Levels = binary:split(Topic, <<"/">>, [global]),
    TopicPath = lists:map(fun
        (<<"+">>) -> $+;
        (<<"#">>) -> $#;
        (Level) -> Level
    end, Levels),
    validate(TopicPath),
    TopicPath.

% This doesn't validate the levels name does not contain '+' or '#'
-spec validate(emqb_topic:path()) -> ok.
validate([]) -> ok;
validate([$#]) -> ok;
validate([Bin | Rest]) when is_binary(Bin) ->
    validate(Rest);
validate([$+ | Rest]) ->
    validate(Rest);
validate(_) ->
    erlang:error(badarg).

-spec start_link(emqb_topic:path()) -> gen_server:start_ret().
start_link(TopicPath) ->
    gen_server:start_link(?MODULE, [TopicPath], []).

-spec subscribe(pid(), pid(), term()) -> ok.
subscribe(TopicPid, ClientPid, SubRef) ->
    gen_server:cast(TopicPid, {subscribe, ClientPid, SubRef}).

-spec unsubscribe(pid(), pid(), term()) -> ok.
unsubscribe(TopicPid, ClientPid, SubRef) ->
    gen_server:cast(TopicPid, {unsubscribe, ClientPid, SubRef}).

-spec publish(pid(), emqtt:properties(), emqb:payload(), [emqb_client:pubopt()])
    -> boolean().
publish(TopicPid, Properties, Payload, Opts) ->
    gen_server:call(TopicPid, {publish, Properties, Payload, Opts}).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([TopicPath]) ->
    ?LOG_INFO("Local topic ~s process started", [format(TopicPath)]),
    emqb_registry:register_topic(self(), TopicPath),
    State =#state{path = TopicPath},
    {ok, State, timeout(State)}.

handle_call({publish, Properties, Payload, _Opts}, _From,
            State = #state{path = TopicPath, subscribers = Subscribers}) ->
    FilteredProps = maps:with(['Payload-Format-Indicator',
                               'Message-Expiry-Interval', 'Content-Type',
                               'Response-Topic', 'Correlation-Data',
                               'User-Property'], Properties),
    maps:foreach(fun(_, #subscriber_data{pid = ClientPid, sub = SubRef}) ->
        emqb_client:dispatch(ClientPid, SubRef, FilteredProps, TopicPath, Payload)
    end, Subscribers),
    %TODO: Implements retain
    {reply, maps:size(Subscribers) > 0, State, timeout(State)};
handle_call(Request, From, State) ->
    ?LOG_WARNING("Unexpected call ~p from ~p", [Request, From]),
    {reply, {error, unexpected_call}, State, timeout(State)}.

handle_cast({subscribe, ClientPid, SubRef},
            State = #state{subscribers = Subscribers, monitors = Monitors}) ->
    SubscriberKey = {ClientPid, SubRef},
    case maps:find(SubscriberKey, Subscribers) of
        {ok, _} ->
            {noreply, State, timeout(State)};
        error ->
            MonRef = erlang:monitor(process, ClientPid),
            Data = #subscriber_data{pid = ClientPid, sub = SubRef,
                                    mon = MonRef},
            NewSubscribers = Subscribers#{SubscriberKey => Data},
            NewMonitors = Monitors#{MonRef => Data},
            State2 = State#state{subscribers = NewSubscribers,
                                 monitors = NewMonitors},
            {noreply, State2, timeout(State2)}
    end;
handle_cast({unsubscribe, ClientPid, SubRef},
            State = #state{subscribers = Subscribers, monitors = Monitors}) ->
    SubscriberKey = {ClientPid, SubRef},
    case maps:take(SubscriberKey, Subscribers) of
        error ->
            {noreply, State, timeout(State)};
        {#subscriber_data{mon = MonRef}, NewSubscribers} ->
            erlang:demonitor(MonRef, [flush]),
            NewMonitors = maps:remove(MonRef, Monitors),
            State2 = State#state{subscribers = NewSubscribers,
                                 monitors = NewMonitors},
            {noreply, State2, timeout(State2)}
    end;
handle_cast(Msg, State) ->
    ?LOG_WARNING("Unexpected cast ~p", [Msg]),
    {noreply, State, timeout(State)}.

handle_info(timeout, State = #state{path = TopicPath}) ->
    ?LOG_INFO("Inactive topic ~s shutdown", [format(TopicPath)]),
    {stop, shutdown, State};
handle_info(Info, State) ->
    ?LOG_WARNING("Unexpected message ~p", [Info]),
    {noreply, State, timeout(State)}.

terminate(_Reason, _State) ->
    ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

timeout(#state{subscribers = Subscribers}) when map_size(Subscribers) =:= 0 ->
    ?INACTIVITY_TIMOUT;
timeout(#state{}) ->
    infinity.
