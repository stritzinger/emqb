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

-module(emqb_topic_tree_SUITE).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Common test functions
-export([all/0]).

% Test cases
-export([tree_test/1]).
-export([resolve_test/1]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(FOLD_FUN, fun(__T, __D, __A) -> [{__T, __D} | __A] end).
-define(assertTreeFind(TREE, TOPIC, EXP),
    ?assertEqual(EXP, emqb_topic_tree:find(TOPIC, TREE))).
-define(assertTreeFold(TREE, EXP),
    ?assertEqual(lists:sort(EXP),
                 lists:sort(emqb_topic_tree:fold(?FOLD_FUN, [], TREE)))).
-define(assertTreeMatch(TREE, PATTERN, EXP),
    ?assertEqual(lists:sort(EXP),
                 lists:sort(emqb_topic_tree:match(?FOLD_FUN, [], PATTERN, TREE)))).
-define(assertTreeResolve(TREE, TOPIC, EXP),
    ?assertEqual(lists:sort(EXP),
                 lists:sort(emqb_topic_tree:resolve(?FOLD_FUN, [], TOPIC, TREE)))).

%%% COMMON TEST FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() -> [
    tree_test,
    resolve_test
].


%%% TEST CASES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

tree_test(_Config) ->
    T1 = emqb_topic_tree:new(),
    ?assertEqual(0, emqb_topic_tree:size(T1)),
    ?assertTreeFind(T1, <<"foo">>, error),
    ?assertTreeFind(T1, <<"foo/bar">>, error),
    ?assertTreeFind(T1, <<"foo/bar/boz">>, error),
    ?assertTreeFind(T1, <<"foo/biz/boz">>, error),
    ?assertTreeFind(T1, <<"foo/biz/boz/">>, error),

    ?assertEqual(T1, emqb_topic_tree:remove(<<"foo/bar">>, T1)),

    ?assertTreeFold(T1, []),

    ?assertTreeMatch(T1, <<"#">>, []),
    ?assertTreeMatch(T1, <<"foo/bar">>, []),
    ?assertTreeMatch(T1, <<"foo/+/boz">>, []),
    ?assertTreeMatch(T1, <<"foo/+/boz/">>, []),
    ?assertTreeMatch(T1, <<"+">>, []),
    ?assertTreeMatch(T1, <<"foo/biz/#">>, []),

    T2 = emqb_topic_tree:update(<<"foo/bar">>, toto, T1),
    ?assertEqual(1, emqb_topic_tree:size(T2)),
    ?assertTreeFind(T2, <<"foo">>, error),
    ?assertTreeFind(T2, <<"foo/bar">>, {ok, toto}),
    ?assertTreeFind(T2, <<"foo/bar/boz">>, error),
    ?assertTreeFind(T2, <<"foo/biz/boz">>, error),
    ?assertTreeFind(T2, <<"foo/biz/boz/">>, error),

    ?assertTreeFold(T2, [{[<<"foo">>, <<"bar">>], toto}]),

    ?assertEqual(lists:sort(emqb_topic_tree:fold(?FOLD_FUN, [], T2)),
                 lists:sort(emqb_topic_tree:match(?FOLD_FUN, [], <<"#">>, T2))),
    ?assertTreeMatch(T2, <<"foo/bar">>, [{[<<"foo">>, <<"bar">>], toto}]),
    ?assertTreeMatch(T2, <<"foo/+/boz">>, []),
    ?assertTreeMatch(T2, <<"foo/+/boz/">>, []),
    ?assertTreeMatch(T2, <<"+">>, []),
    ?assertTreeMatch(T2, <<"foo/biz/#">>, []),

    T3 = emqb_topic_tree:update(<<"foo/bar/boz">>, tata, T2),
    ?assertEqual(2, emqb_topic_tree:size(T3)),
    ?assertTreeFind(T3, <<"foo">>, error),
    ?assertTreeFind(T3, <<"foo/bar">>, {ok, toto}),
    ?assertTreeFind(T3, <<"foo/bar/boz">>, {ok, tata}),
    ?assertTreeFind(T3, <<"foo/biz/boz">>, error),
    ?assertTreeFind(T3, <<"foo/biz/boz/">>, error),

    ?assertTreeFold(T3, [
        {[<<"foo">>, <<"bar">>], toto},
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata}]),

    ?assertEqual(lists:sort(emqb_topic_tree:fold(?FOLD_FUN, [], T3)),
                 lists:sort(emqb_topic_tree:match(?FOLD_FUN, [], <<"#">>, T3))),
    ?assertTreeMatch(T3, <<"foo/bar">>, [
        {[<<"foo">>, <<"bar">>], toto}]),
    ?assertTreeMatch(T3, <<"foo/+/boz">>, [
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata}]),
    ?assertTreeMatch(T3, <<"foo/+/boz/">>, []),
    ?assertTreeMatch(T3, <<"+">>, []),
    ?assertTreeMatch(T3, <<"foo/biz/#">>, []),

    T4 = emqb_topic_tree:update(<<"foo/biz/boz">>, tutu, T3),
    ?assertEqual(3, emqb_topic_tree:size(T4)),
    ?assertTreeFind(T4, <<"foo">>, error),
    ?assertTreeFind(T4, <<"foo/bar">>, {ok, toto}),
    ?assertTreeFind(T4, <<"foo/bar/boz">>, {ok, tata}),
    ?assertTreeFind(T4, <<"foo/biz/boz">>, {ok, tutu}),
    ?assertTreeFind(T4, <<"foo/biz/boz/">>, error),

    ?assertTreeFold(T4, [
        {[<<"foo">>, <<"bar">>], toto},
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
        {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}]),

    ?assertEqual(lists:sort(emqb_topic_tree:fold(?FOLD_FUN, [], T4)),
                 lists:sort(emqb_topic_tree:match(?FOLD_FUN, [], <<"#">>, T4))),
    ?assertTreeMatch(T4, <<"foo/bar">>, [
        {[<<"foo">>, <<"bar">>], toto}]),
    ?assertTreeMatch(T4, <<"foo/+/boz">>, [
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
        {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}]),
    ?assertTreeMatch(T4, <<"foo/+/boz/">>, []),
    ?assertTreeMatch(T4, <<"+">>, []),
    ?assertTreeMatch(T4, <<"foo/biz/#">>, [
        {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}]),

    T5 = emqb_topic_tree:update(<<"foo/biz/boz/">>, tete, T4),
    ?assertEqual(4, emqb_topic_tree:size(T5)),
    ?assertTreeFind(T5, <<"foo">>, error),
    ?assertTreeFind(T5, <<"foo/bar">>, {ok, toto}),
    ?assertTreeFind(T5, <<"foo/bar/boz">>, {ok, tata}),
    ?assertTreeFind(T5, <<"foo/biz/boz">>, {ok, tutu}),
    ?assertTreeFind(T5, <<"foo/biz/boz/">>, {ok, tete}),

    ?assertTreeFold(T5, [
        {[<<"foo">>, <<"bar">>], toto},
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
        {[<<"foo">>, <<"biz">>, <<"boz">>], tutu},
        {[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}]),

    ?assertEqual(lists:sort(emqb_topic_tree:fold(?FOLD_FUN, [], T5)),
                 lists:sort(emqb_topic_tree:match(?FOLD_FUN, [], <<"#">>, T5))),
    ?assertTreeMatch(T5, <<"foo/bar">>, [
        {[<<"foo">>, <<"bar">>], toto}]),
    ?assertTreeMatch(T5, <<"foo/+/boz">>, [
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
        {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}]),
    ?assertTreeMatch(T5, <<"foo/+/boz/">>, [
        {[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}]),
    ?assertTreeMatch(T5, <<"+">>, []),
    ?assertTreeMatch(T5, <<"foo/biz/#">>, [
        {[<<"foo">>, <<"biz">>, <<"boz">>], tutu},
        {[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}]),

    T6 = emqb_topic_tree:update(<<"foo">>, titi, T5),
    ?assertEqual(5, emqb_topic_tree:size(T6)),
    ?assertTreeFind(T6, <<"foo">>, {ok, titi}),
    ?assertTreeFind(T6, <<"foo/bar">>, {ok, toto}),
    ?assertTreeFind(T6, <<"foo/bar/boz">>, {ok, tata}),
    ?assertTreeFind(T6, <<"foo/biz/boz">>, {ok, tutu}),
    ?assertTreeFind(T6, <<"foo/biz/boz/">>, {ok, tete}),

    ?assertTreeFold(T6, [
        {[<<"foo">>], titi},
        {[<<"foo">>, <<"bar">>], toto},
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
        {[<<"foo">>, <<"biz">>, <<"boz">>], tutu},
        {[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}]),

    ?assertEqual(lists:sort(emqb_topic_tree:fold(?FOLD_FUN, [], T6)),
                 lists:sort(emqb_topic_tree:match(?FOLD_FUN, [], <<"#">>, T6))),
    ?assertTreeMatch(T6, <<"foo/bar">>, [
        {[<<"foo">>, <<"bar">>], toto}]),
    ?assertTreeMatch(T6, <<"foo/+/boz">>, [
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
        {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}]),
    ?assertTreeMatch(T6, <<"foo/+/boz/">>, [
        {[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}]),
    ?assertTreeMatch(T6, <<"+">>, [
        {[<<"foo">>], titi}]),
    ?assertTreeMatch(T6, <<"foo/biz/#">>, [
        {[<<"foo">>, <<"biz">>, <<"boz">>], tutu},
        {[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}]),

    T7 = emqb_topic_tree:remove(<<"foo/biz/boz/">>, T6),
    ?assertEqual(4, emqb_topic_tree:size(T7)),
    ?assertTreeFind(T7, <<"foo">>, {ok, titi}),
    ?assertTreeFind(T7, <<"foo/bar">>, {ok, toto}),
    ?assertTreeFind(T7, <<"foo/bar/boz">>, {ok, tata}),
    ?assertTreeFind(T7, <<"foo/biz/boz">>, {ok, tutu}),
    ?assertTreeFind(T7, <<"foo/biz/boz/">>, error),

    ?assertTreeFold(T7, [
        {[<<"foo">>], titi},
        {[<<"foo">>, <<"bar">>], toto},
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
        {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}]),

    T8 = emqb_topic_tree:remove(<<"foo">>, T7),
    ?assertEqual(3, emqb_topic_tree:size(T8)),
    ?assertTreeFind(T8, <<"foo">>, error),
    ?assertTreeFind(T8, <<"foo/bar">>, {ok, toto}),
    ?assertTreeFind(T8, <<"foo/bar/boz">>, {ok, tata}),
    ?assertTreeFind(T8, <<"foo/biz/boz">>, {ok, tutu}),
    ?assertTreeFind(T8, <<"foo/biz/boz/">>, error),

    ?assertTreeFold(T8, [
        {[<<"foo">>, <<"bar">>], toto},
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
        {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}]),

    T9 = emqb_topic_tree:remove(<<"foo/biz/boz">>, T8),
    ?assertEqual(2, emqb_topic_tree:size(T9)),
    ?assertTreeFind(T9, <<"foo">>, error),
    ?assertTreeFind(T9, <<"foo/bar">>, {ok, toto}),
    ?assertTreeFind(T9, <<"foo/bar/boz">>, {ok, tata}),
    ?assertTreeFind(T9, <<"foo/biz/boz">>, error),
    ?assertTreeFind(T9, <<"foo/biz/boz/">>, error),

    ?assertTreeFold(T9, [
        {[<<"foo">>, <<"bar">>], toto},
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata}]),

    T10 = emqb_topic_tree:remove(<<"foo/bar">>, T9),
    ?assertEqual(1, emqb_topic_tree:size(T10)),
    ?assertTreeFind(T10, <<"foo">>, error),
    ?assertTreeFind(T10, <<"foo/bar">>, error),
    ?assertTreeFind(T10, <<"foo/bar/boz">>, {ok, tata}),
    ?assertTreeFind(T10, <<"foo/biz/boz">>, error),
    ?assertTreeFind(T10, <<"foo/biz/boz/">>, error),

    ?assertEqual(T10, emqb_topic_tree:remove(<<"foo/bar/">>, T10)),

    ?assertTreeFold(T10, [
        {[<<"foo">>, <<"bar">>, <<"boz">>], tata}]),

    T11 = emqb_topic_tree:remove(<<"foo/bar/boz">>, T10),
    ?assertEqual(0, emqb_topic_tree:size(T11)),
    ?assertTreeFind(T11, <<"foo">>, error),
    ?assertTreeFind(T11, <<"foo/bar">>, error),
    ?assertTreeFind(T11, <<"foo/bar/boz">>, error),
    ?assertTreeFind(T11, <<"foo/biz/boz">>, error),
    ?assertTreeFind(T11, <<"foo/biz/boz/">>, error),

    ?assertTreeFold(T11, []),

    ok.

resolve_test(_Config) ->
    Patterns = [
        {<<"foo">>, data1},
        {<<"+">>, data2},
        {<<"bar/#">>, data3},
        {<<"buz/+/boz">>, data4},
        {<<"biz/+/boz/bez">>, data5},
        {<<"biz/+/boz/+">>, data6}
    ],

    T = lists:foldl(fun({P, D}, T) ->
        emqb_topic_tree:update(P, D, T)
    end, emqb_topic_tree:new(), Patterns),

    ?assertEqual(6, emqb_topic_tree:size(T)),
    ?assertTreeFind(T, <<"foo">>, {ok, data1}),
    ?assertTreeFind(T, <<"+">>, {ok, data2}),
    ?assertTreeFind(T, <<"bar/#">>, {ok, data3}),
    ?assertTreeFind(T, <<"buz/+/boz">>, {ok, data4}),
    ?assertTreeFind(T, <<"biz/+/boz/bez">>, {ok, data5}),
    ?assertTreeFind(T, <<"biz/+/boz/+">>, {ok, data6}),

    ?assertEqual(lists:sort([{emqb_topic:parse(A), B} || {A, B} <- Patterns]),
                 lists:sort(emqb_topic_tree:fold(?FOLD_FUN, [], T))),

    ?assertTreeResolve(T, <<"foo">>, [
        {[<<"foo">>], data1},
        {[$+], data2}]),
    ?assertTreeResolve(T, <<"toto">>, [
        {[$+], data2}]),
    ?assertTreeResolve(T, <<"foo/bar">>, []),
    ?assertTreeResolve(T, <<"bar">>, [
        {[$+], data2}]),
    ?assertTreeResolve(T, <<"bar/toto">>, [
        {[<<"bar">>, $#], data3}]),
    ?assertTreeResolve(T, <<"bar/toto/tata">>, [
        {[<<"bar">>, $#], data3}]),
    ?assertTreeResolve(T, <<"buz/tata">>, []),
    ?assertTreeResolve(T, <<"buz/tata/boz">>, [
        {[<<"buz">>, $+, <<"boz">>], data4}]),
    ?assertTreeResolve(T, <<"buz/tata/boz/bez">>, []),
    ?assertTreeResolve(T, <<"biz/tata/boz/bez">>, [
        {[<<"biz">>, $+, <<"boz">>, <<"bez">>], data5},
        {[<<"biz">>, $+, <<"boz">>, $+], data6}]),
    ?assertTreeResolve(T, <<"biz/tata/boz/toto">>, [
        {[<<"biz">>, $+, <<"boz">>, $+], data6}]),
    ok.
