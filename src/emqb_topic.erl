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
-include("emqb_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API Functions
-export([format/1]).
-export([parse/1]).
-export([validate/1]).
-export([start_link/1]).
-export([subscribe/2]).
-export([unsubscribe/2]).
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
    ref :: reference(),
    mon :: reference()
}).

-record(state, {
    path :: emqb_topic:path(),
    subscribers = #{} :: #{reference() => #subscriber_data{}},
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

-spec subscribe(pid(), topic_subscription()) -> ok.
subscribe(TopicPid, Sub) ->
    gen_server:call(TopicPid, {subscribe, Sub}).

-spec unsubscribe(pid(), reference()) -> ok.
unsubscribe(TopicPid, SubRef) ->
    gen_server:call(TopicPid, {unsubscribe, SubRef}).

-spec publish(pid(), emqtt:properties(), emqb:payload(), [emqb_client:pubopt()])
    -> boolean().
publish(TopicPid, Properties, Payload, Opts) ->
    gen_server:call(TopicPid, {publish, Properties, Payload, Opts}).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([TopicPath]) ->
    ?LOG_INFO("Local topic ~s process started", [format(TopicPath)]),
    Subscriptions = emqb_registry:register_topic(self(), TopicPath),
    State = add_subscribers(#state{path = TopicPath}, Subscriptions),
    {ok, State, ?INACTIVITY_TIMOUT}.

handle_call({publish, Properties, Payload, PubOpts}, _From,
            State = #state{path = TopicPath, subscribers = Subscribers}) ->
    FilteredProps = maps:with(['Payload-Format-Indicator',
                               'Message-Expiry-Interval', 'Content-Type',
                               'Response-Topic', 'Correlation-Data',
                               'User-Property'], Properties),
    maps:foreach(fun(_, #subscriber_data{pid = ClientPid, ref = SubRef}) ->
        emqb_client:dispatch(ClientPid, SubRef, FilteredProps, TopicPath,
                             Payload, PubOpts)
    end, Subscribers),
    %TODO: Implements retain
    {reply, maps:size(Subscribers) > 0, State, ?INACTIVITY_TIMOUT};
handle_call({subscribe, Subscription}, _From, State = #state{path = TopicPath}) ->
    State2 = add_subscribers(State, Subscription),
    emqb_registry:add_subscriptions(TopicPath, Subscription),
    {reply, ok, State2, ?INACTIVITY_TIMOUT};
handle_call({unsubscribe, SubRef}, _From, State = #state{path = TopicPath}) ->
    State2 = del_subscribers(State, SubRef),
    emqb_registry:del_subscriptions(TopicPath, SubRef),
    {reply, ok, State2, ?INACTIVITY_TIMOUT};
handle_call(Request, From, State) ->
    ?LOG_WARNING("Unexpected call ~p from ~p", [Request, From]),
    {reply, {error, unexpected_call}, State, ?INACTIVITY_TIMOUT}.

handle_cast(Msg, State) ->
    ?LOG_WARNING("Unexpected cast ~p", [Msg]),
    {noreply, State, ?INACTIVITY_TIMOUT}.

handle_info(timeout, State = #state{path = TopicPath}) ->
    ?LOG_INFO("Inactive topic ~s shutdown", [format(TopicPath)]),
    {stop, shutdown, State};
handle_info(Info, State) ->
    ?LOG_WARNING("Unexpected message ~p", [Info]),
    {noreply, State, ?INACTIVITY_TIMOUT}.

terminate(_Reason, _State) ->
    ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_subscribers(State, []) -> State;
add_subscribers(State, [Sub | Rest]) ->
    add_subscribers(add_subscribers(State, Sub), Rest);
add_subscribers(State = #state{subscribers = Subscribers, monitors = Monitors},
                #topic_subscription{ref = SubRef, client = ClientPid}) ->
    case maps:find(SubRef, Subscribers) of
        {ok, _} -> State;
        error ->
            MonRef = erlang:monitor(process, ClientPid),
            Data = #subscriber_data{pid = ClientPid, ref = SubRef, mon = MonRef},
            NewSubscribers = Subscribers#{SubRef => Data},
            NewMonitors = Monitors#{MonRef => Data},
            State#state{subscribers = NewSubscribers, monitors = NewMonitors}
    end.

del_subscribers(State, []) -> State;
del_subscribers(State, [SubRef | Rest]) ->
    del_subscribers(del_subscribers(State, SubRef), Rest);
del_subscribers(State = #state{subscribers = Subscribers, monitors = Monitors}, SubRef) ->
    case maps:take(SubRef, Subscribers) of
        error -> State;
        {#subscriber_data{mon = MonRef}, NewSubscribers} ->
            erlang:demonitor(MonRef, [flush]),
            NewMonitors = maps:remove(MonRef, Monitors),
            State#state{subscribers = NewSubscribers, monitors = NewMonitors}
    end.
