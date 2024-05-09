%%--------------------------------------------------------------------
%% Copyright (c) 2024 Peer Stritzinger GmbH. All Rights Reserved.
%%
%% @author Sebastien Merle <s.merle@gmail.com>
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

-module(emqb_SUITE).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("emqtt/include/emqtt.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Common test functions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

% Test cases functions
-export([simple_publish_test_config/0, simple_publish_test/1]).
-export([subscribe_spec_test_config/0, subscribe_spec_test/1]).
-export([unsubscribe_test_config/0, unsubscribe_test/1]).
-export([topic_timeout_test_config/0, topic_timeout_test/1]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(assertMessage(FROM, MSG), fun() ->
    receive
        {msg, FROM, MSG} -> ok
    after
        1000 -> ct:fail({message_expected, ?FUNCTION_NAME, ?LINE})
    end
end()).
-define(assertNoMoreMessages(), fun() ->
    receive
        {msg, From, Msg} ->
            ct:fail({unexpected_message, From, Msg})
    after
        200 -> ok
    end
end()).

-define(GENERIC_TESTS, [
    simple_publish_test,
    subscribe_spec_test,
    unsubscribe_test
]).

-define(INTERNAL_TESTS, [
    topic_timeout_test
]).


%%% COMMON TEST FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() -> [
    {group, internal},
    {group, external},
    {group, hybrid}
].

groups() ->
    [
        {internal, [sequence], ?GENERIC_TESTS ++ ?INTERNAL_TESTS},
        {external, [sequence], ?GENERIC_TESTS},
        {hybrid, [sequence], ?GENERIC_TESTS ++ ?INTERNAL_TESTS}
    ].

init_per_suite(Config) ->
    ct_utils:setup_tracing(),
    BrokerHost = os:getenv("MQTT_BROKER_HOST", "localhost"),
    BrokerPort = list_to_integer(os:getenv("MQTT_BROKER_PORT", "1883")),
    BrokerUser = os:getenv("MQTT_BROKER_USERNAME", undefined),
    BrokerPass = os:getenv("MQTT_BROKER_PASSWORD", undefined),
    BaseClientOpts = #{
        host => BrokerHost,
        port => BrokerPort,
        reconn_max_retries => 2
    },
    ClientOpts = if
        (BrokerUser =/= undefined) and (BrokerPass =/= undefined) ->
            BaseClientOpts#{username => BrokerUser, password => BrokerPass};
        true ->
            BaseClientOpts
    end,
    case check_mqtt_broker_availability(ClientOpts) of
        ok ->
            [{mqtt_available, true},
             {client_opts, ClientOpts}
             | Config];
        {error, Reason} ->
            ct:pal("Warning: MQTT broker is not available (~p), skipping online tests.", [Reason]),
            [{mqtt_available, false},
             {client_opts, #{}}
             | Config]
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(Mode, Config) when Mode =:= external; Mode =:= hybrid ->
    case proplists:get_value(mqtt_available, Config, false) of
        true -> client_opts_update(Config, mode, Mode);
        false -> {skip, mqtt_broker_unavailable}
    end;
init_per_group(internal, Config) ->
     client_opts_update(Config, mode, internal).

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(TestName, Config) ->
    % process_flag(trap_exit, true),
    TestConfFunName = list_to_atom(lists:flatten(io_lib:format("~w_config", [TestName]))),
    TestConf = try ?MODULE:TestConfFunName() catch error:undef -> [] end,
    AllConfig = merge_config(TestConf, Config),
    ProcNames = proplists:get_value(proc_names, AllConfig, []),
    ClientOpts = proplists:get_value(client_opts, AllConfig),
    case proplists:get_value(tracing, AllConfig, false) of
        false -> ok;
        true ->
            ct_utils:start_tracing(TestName),
            dbg:p(all, c),
            ok
    end,
    {ok, _} = application:ensure_all_started(emqb),
    ProcMap = lists:foldl(fun(ProcName, Map) ->
        Map#{ProcName => start_process(ProcName, self(), ClientOpts)}
    end, #{}, ProcNames),
    [{proc_map, ProcMap} | AllConfig].

end_per_testcase(TestName, Config) ->
    ProcMap = proplists:get_value(proc_map, Config, #{}),
    maps:foreach(fun(_ProcName, ProcPid) ->
        stop_process(ProcPid)
    end, ProcMap),
    application:stop(emqb),
    ct_utils:stop_tracing(TestName),
    ok.


%%% TEST CASES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

simple_publish_test_config() -> [
    % tracing,
    {proc_names, [producer, consumer1, consumer2]}
].

simple_publish_test(_Config) ->
    subscribe(consumer1, <<"foo/bar">>),
    subscribe(consumer1, <<"foo/+/bar">>),
    subscribe(consumer1, <<"foo/boz/#">>),
    subscribe(consumer2, <<"foo/bar">>),
    subscribe(consumer2, <<"foo/+/biz">>),
    subscribe(consumer2, <<"foo/biz/bar">>),

    publish(producer, <<"foo/bar">>, <<"msg1">>),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg1">>}}),
    ?assertMessage(consumer2, {publish, #{payload := <<"msg1">>}}),

    publish(producer, <<"foo/biz/bar">>, <<"msg2">>),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg2">>}}),
    ?assertMessage(consumer2, {publish, #{payload := <<"msg2">>}}),

    publish(producer, <<"foo/bez/bar">>, <<"msg3">>),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg3">>}}),

    publish(producer, <<"foo/boz/bar">>, <<"msg4">>),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg4">>}}),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg4">>}}),

    publish(producer, <<"foo/boz/biz">>, <<"msg5">>),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg5">>}}),
    ?assertMessage(consumer2, {publish, #{payload := <<"msg5">>}}),

    publish(producer, <<"foo/boz/biz/buz">>, <<"msg5">>),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg5">>}}),

    publish(producer, <<"foo/buz/biz">>, <<"msg6">>),
    ?assertMessage(consumer2, {publish, #{payload := <<"msg6">>}}),

    ?assertNoMoreMessages(),
    ok.

subscribe_spec_test_config() -> [
    % tracing,
    {proc_names, [producer, consumer1]}
].

subscribe_spec_test(_Config) ->
    subscribe(consumer1, [
        <<"foo/bar">>,
        {<<"foo/+/bar">>, qos0},
        {<<"foo/boz/#">>, ?QOS_0},
        {<<"foo/boz/#">>, [{qos, qos0}]}
    ]),

    publish(producer, <<"foo/bar">>, <<"msg1">>),
    publish(producer, <<"foo/biz/bar">>, <<"msg2">>),
    publish(producer, <<"foo/bez/bar">>, <<"msg3">>),
    publish(producer, <<"foo/boz/bar">>, <<"msg4">>),
    publish(producer, <<"foo/boz/biz">>, <<"msg5">>),
    publish(producer, <<"foo/boz/biz/buz">>, <<"msg5">>),

    ?assertMessage(consumer1, {publish, #{payload := <<"msg1">>}}),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg2">>}}),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg3">>}}),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg4">>}}),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg4">>}}),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg5">>}}),
    ?assertMessage(consumer1, {publish, #{payload := <<"msg5">>}}),
    ?assertNoMoreMessages(),

    ok.

unsubscribe_test_config() -> [
    %tracing,
    {proc_names, [producer, consumer]}
].

unsubscribe_test(_Config) ->
    subscribe(consumer, #{'Subscription-Identifier' => 1}, <<"foo/bar">>),
    subscribe(consumer, #{'Subscription-Identifier' => 2}, <<"foo/+">>),
    subscribe(consumer, #{'Subscription-Identifier' => 3}, <<"foo/#">>),

    publish(producer, <<"foo/bar">>, <<"msg1">>),
    ?assertMessage(consumer,
        {publish, #{topic := <<"foo/bar">>, payload := <<"msg1">>,
                    properties := #{'Subscription-Identifier' := 1}}}),
    ?assertMessage(consumer,
        {publish, #{topic := <<"foo/bar">>, payload := <<"msg1">>,
                    properties := #{'Subscription-Identifier' := 2}}}),
    ?assertMessage(consumer,
        {publish, #{topic := <<"foo/bar">>, payload := <<"msg1">>,
                    properties := #{'Subscription-Identifier' := 3}}}),

    publish(producer, <<"foo/boz">>, <<"msg2">>),
    ?assertMessage(consumer,
        {publish, #{topic := <<"foo/boz">>, payload := <<"msg2">>,
                    properties := #{'Subscription-Identifier' := 2}}}),
    ?assertMessage(consumer,
        {publish, #{topic := <<"foo/boz">>, payload := <<"msg2">>,
                    properties := #{'Subscription-Identifier' := 3}}}),

    publish(producer, <<"foo/bar/boz">>, <<"msg3">>),
    ?assertMessage(consumer,
        {publish, #{topic := <<"foo/bar/boz">>, payload := <<"msg3">>,
                    properties := #{'Subscription-Identifier' := 3}}}),

    ?assertNoMoreMessages(),

    unsubscribe(consumer, <<"foo/+">>),

    publish(producer, <<"foo/bar">>, <<"msg1">>),
    ?assertMessage(consumer,
        {publish, #{topic := <<"foo/bar">>, payload := <<"msg1">>,
                    properties := #{'Subscription-Identifier' := 1}}}),
    ?assertMessage(consumer,
        {publish, #{topic := <<"foo/bar">>, payload := <<"msg1">>,
                    properties := #{'Subscription-Identifier' := 3}}}),

    publish(producer, <<"foo/boz">>, <<"msg2">>),
    ?assertMessage(consumer,
        {publish, #{topic := <<"foo/boz">>, payload := <<"msg2">>,
                    properties := #{'Subscription-Identifier' := 3}}}),

    publish(producer, <<"foo/bar/boz">>, <<"msg3">>),
    ?assertMessage(consumer,
        {publish, #{topic := <<"foo/bar/boz">>, payload := <<"msg3">>,
                    properties := #{'Subscription-Identifier' := 3}}}),

    ?assertNoMoreMessages(),

    unsubscribe(consumer, <<"foo/#">>),

    publish(producer, <<"foo/bar">>, <<"msg1">>),
    ?assertMessage(consumer,
        {publish, #{topic := <<"foo/bar">>, payload := <<"msg1">>,
                    properties := #{'Subscription-Identifier' := 1}}}),

    publish(producer, <<"foo/boz">>, <<"msg2">>),
    publish(producer, <<"foo/bar/boz">>, <<"msg3">>),

    ?assertNoMoreMessages(),

    ok.

topic_timeout_test_config() -> [
    % tracing,
    {proc_names, [client1, client2, client3]}
].

topic_timeout_test(_Config) ->
    subscribe(client1, <<"client1/foo">>),
    subscribe(client1, <<"client1/bar">>),
    subscribe(client2, <<"client2/foo">>),
    subscribe(client2, <<"client2/bar">>),
    subscribe(client3, <<"client3/foo">>),
    subscribe(client3, <<"client3/bar">>),

    publish(client1, <<"client2/foo">>, <<"req1">>),
    ?assertMessage(client2, {publish, #{payload := <<"req1">>}}),

    publish(client2, <<"client1/bar">>, <<"req2">>),
    ?assertMessage(client1, {publish, #{payload := <<"req2">>}}),

    publish(client2, <<"client3/foo">>, <<"req3">>),
    ?assertMessage(client3, {publish, #{payload := <<"req3">>}}),

    publish(client3, <<"client2/bar">>, <<"req4">>),
    ?assertMessage(client2, {publish, #{payload := <<"req4">>}}),

    lists:foreach(fun(T) ->
        {ok, Pid} = emqb_registry:lookup_topic(T),
        Pid ! timeout
    end, [
        [<<"client1">>, <<"bar">>],
        [<<"client3">>, <<"foo">>],
        [<<"client2">>, <<"bar">>],
        [<<"client2">>, <<"foo">>]
    ]),

    timer:sleep(1000),

    lists:foreach(fun(T) ->
        ?assertMatch(error, emqb_registry:lookup_topic(T))
    end, [
        [<<"client1">>, <<"bar">>],
        [<<"client3">>, <<"foo">>],
        [<<"client2">>, <<"bar">>],
        [<<"client2">>, <<"foo">>]
    ]),

    publish(client1, <<"client2/foo">>, <<"req5">>),
    ?assertMessage(client2, {publish, #{payload := <<"req5">>}}),

    publish(client2, <<"client1/bar">>, <<"req6">>),
    ?assertMessage(client1, {publish, #{payload := <<"req6">>}}),

    publish(client2, <<"client3/foo">>, <<"req7">>),
    ?assertMessage(client3, {publish, #{payload := <<"req7">>}}),

    publish(client3, <<"client2/bar">>, <<"req8">>),
    ?assertMessage(client2, {publish, #{payload := <<"req8">>}}),

    ?assertNoMoreMessages(),

    ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

subscribe(Name, Arg1) ->
  call(Name, subscribe, [Arg1]).

subscribe(Name, Arg1, Arg2) ->
  call(Name, subscribe, [Arg1, Arg2]).

% subscribe(Name, Arg1, Arg2, Arg3) ->
%   call(Name, subscribe, [Arg1, Arg2, Arg3]).

publish(Name, Arg1, Arg2) ->
    call(Name, publish, [Arg1, Arg2]).

% publish(Name, Arg1, Arg2, Arg3) ->
%     call(Name, publish, [Arg1, Arg2, Arg3]).

% publish(Name, Arg1, Arg2, Arg3, Arg4) ->
%     call(Name, publish, [Arg1, Arg2, Arg3, Arg4]).

unsubscribe(Name, Arg1) ->
    call(Name, unsubscribe, [Arg1]).

% unsubscribe(Name, Arg1, Arg2) ->
%     call(Name, unsubscribe, [Arg1, Arg2]).

% puback(Name, Arg1, Arg2) ->
%     call(Name, puback, [Arg1, Arg2]).

% puback(Name, Arg1, Arg2, Arg3) ->
%     call(Name, puback, [Arg1, Arg2, Arg3]).

call(Name, FunName, Args) when is_atom(Name) ->
    case erlang:whereis(Name) of
        undefined -> erlang:error(noproc);
        Pid -> call(Pid, FunName, Args)
    end;
call(Pid, FunName, Args) ->
    CallRef = make_ref(),
    MonRef = erlang:monitor(process, Pid),
    Pid ! {call, self(), CallRef, FunName, Args},
    receive
        {'DOWN', MonRef, process, Pid, Reason} ->
            erlang:error(Reason);
        {result, CallRef, Result} ->
            erlang:demonitor(MonRef, [flush]),
            Result;
        {exception, CallRef, {C, R, S}} ->
            erlang:raise(C, R, S)
    end.

start_process(Name, Owner, Opts) ->
    CallRef = make_ref(),
    Pid = erlang:spawn_link(fun() ->
        process_init(Name, Owner, CallRef, Opts)
    end),
    MonRef = erlang:monitor(process, Pid),
    receive
        {started, CallRef} -> Pid;
        {error, CallRef, Reason} ->
            erlang:error(Reason);
        {exception, CallRef, {C, R, S}} ->
            erlang:raise(C, R, S);
        {'DOWN', MonRef, process, Pid, Reason} ->
            ct:fail({process_startup_failed, Reason})
    after
        5000 -> ct:fail(process_startup_timeout)
    end.

stop_process(Pid) ->
    MonRef = erlang:monitor(process, Pid),
    Pid ! stop,
    receive
        {'DOWN', MonRef, process, Pid, _Reason} -> ok
    after
        5000 -> ok
    end.

process_init(Name, Owner, Ref, Opts) ->
    erlang:register(Name, self()),
    try emqb:start_link(Opts) of
        {error, Reason}  ->
            Owner ! {error, Ref, Reason};
        {ok, Client} ->
            Owner ! {started, Ref},
            process_loop(Name, Client, Owner)
    catch
        C:R:S ->
            Owner ! {exception, Ref, {C, R, S}}
    end.

process_loop(Name, Client, Owner) ->
    receive
        stop -> ok;
        {call, From, Ref, FunName, Args} ->
            try erlang:apply(emqb, FunName, [Client | Args]) of
                Result ->
                    From ! {result, Ref, Result},
                    process_loop(Name, Client, Owner)
            catch
                C:R:S ->
                    From ! {exception, Ref, {C, R, S}},
                    process_loop(Name, Client, Owner)
            end;
        Msg ->
            Owner ! {msg, Name, Msg},
            process_loop(Name, Client, Owner)
    end.

merge_config(A, B) ->
    DupKeys = proplists:get_keys(A) ++ proplists:get_keys(B),
    Keys = maps:keys(maps:from_list([{K, undefined} || K <- DupKeys])),
    merge_config(Keys, A, B, []).

merge_config([], _, _, Acc) -> Acc;
merge_config([Key | Rest], A, B, Acc) ->
    case {proplists:get_value(Key, A), proplists:get_value(Key, B)} of
        {Va, Vb} when is_list(Va), is_list(Vb) ->
            merge_config(Rest, A, B, [{Key, Va ++ Vb} | Acc]);
        {undefined, Vb} ->
            merge_config(Rest, A, B, [{Key, Vb} | Acc]);
        {Va, undefined} ->
            merge_config(Rest, A, B, [{Key, Va} | Acc])
    end.

client_opts_update(Config, Key, Value) ->
    ClientOpts = proplists:get_value(client_opts, Config),
    NewClientOpts = ClientOpts#{Key => Value},
    [{client_opts, NewClientOpts} | proplists:delete(client_opts, Config)].

check_mqtt_broker_availability(Opts) ->
    case emqtt:start_link(Opts) of
        {error, _Reason} = Error -> Error;
        {ok, Pid} ->
            erlang:unlink(Pid),
            try emqtt:connect(Pid) of
                {error, _Reason} = Error -> Error;
                {ok, _Props} -> ok
            after
                catch emqtt:stop(Pid)
            end
    end.
