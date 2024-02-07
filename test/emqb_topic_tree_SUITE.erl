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
-export([parse_topic_test/1]).
-export([tree_test/1]).


%%% COMMON TEST FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() ->
    [parse_topic_test, tree_test].


%%% TEST CASES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_topic_test(_Config) ->
    ?assertError(badarg, emqb_topic_tree:parse_topic(<<"">>)),
    ?assertError(badarg, emqb_topic_tree:parse_topic([])),
    ?assertEqual([<<"">>, <<"">>],
                 emqb_topic_tree:parse_topic(<<"/">>)),
    ?assertEqual([<<"foo">>, <<"bar">>],
                 emqb_topic_tree:parse_topic(<<"foo/bar">>)),
    ?assertEqual([<<"">>, <<"foo">>, <<"bar">>],
                 emqb_topic_tree:parse_topic(<<"/foo/bar">>)),
    ?assertEqual([<<"foo">>, <<"bar">>, <<"">>],
                 emqb_topic_tree:parse_topic(<<"foo/bar/">>)),
    ?assertEqual([<<"foo">>, '+', <<"bar">>, '#'],
                 emqb_topic_tree:parse_topic(<<"foo/+/bar/#">>)),
    ?assertError(badarg, emqb_topic_tree:parse_topic(<<"foo/#/bar/+">>)),
    ?assertError(badarg, emqb_topic_tree:parse_topic([<<"foo">>, '#', <<"bar">>])),
    ok.

tree_test(_Config) ->
    FoldFun = fun(T, D, A) -> [{T, D} | A] end,

    T1 = emqb_topic_tree:new(),
    ?assertEqual(0, emqb_topic_tree:size(T1)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo">>, T1)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/bar">>, T1)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/bar/boz">>, T1)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz">>, T1)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz/">>, T1)),

    ?assertEqual(T1, emqb_topic_tree:remove(<<"foo/bar">>, T1)),

    ?assertEqual([], lists:sort(emqb_topic_tree:fold(FoldFun, [], T1))),

    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"#">>, T1)),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"foo/bar">>, T1)),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz">>, T1)),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz/">>, T1)),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"+">>, T1)),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"foo/biz/#">>, T1)),

    T2 = emqb_topic_tree:update(<<"foo/bar">>, toto, T1),
    ?assertEqual(1, emqb_topic_tree:size(T2)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo">>, T2)),
    ?assertEqual({ok, toto}, emqb_topic_tree:find(<<"foo/bar">>, T2)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/bar/boz">>, T2)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz">>, T2)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz/">>, T2)),

    ?assertEqual([{[<<"foo">>, <<"bar">>], toto}],
                 lists:sort(emqb_topic_tree:fold(FoldFun, [], T2))),

    ?assertEqual(lists:sort(emqb_topic_tree:fold(FoldFun, [], T2)),
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"#">>, T2))),
    ?assertEqual([{[<<"foo">>, <<"bar">>], toto}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/bar">>, T2))),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz">>, T2)),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz/">>, T2)),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"+">>, T2)),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"foo/biz/#">>, T2)),

    T3 = emqb_topic_tree:update(<<"foo/bar/boz">>, tata, T2),
    ?assertEqual(2, emqb_topic_tree:size(T3)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo">>, T3)),
    ?assertEqual({ok, toto}, emqb_topic_tree:find(<<"foo/bar">>, T3)),
    ?assertEqual({ok, tata}, emqb_topic_tree:find(<<"foo/bar/boz">>, T3)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz">>, T3)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz/">>, T3)),

    ?assertEqual([{[<<"foo">>, <<"bar">>], toto},
                  {[<<"foo">>, <<"bar">>, <<"boz">>], tata}],
                 lists:sort(emqb_topic_tree:fold(FoldFun, [], T3))),

    ?assertEqual(lists:sort(emqb_topic_tree:fold(FoldFun, [], T3)),
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"#">>, T3))),
    ?assertEqual([{[<<"foo">>, <<"bar">>], toto}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [],
                                                  <<"foo/bar">>, T3))),
    ?assertEqual([{[<<"foo">>, <<"bar">>, <<"boz">>], tata}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz">>, T3))),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz/">>, T3)),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"+">>, T3)),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"foo/biz/#">>, T3)),

    T4 = emqb_topic_tree:update(<<"foo/biz/boz">>, tutu, T3),
    ?assertEqual(3, emqb_topic_tree:size(T4)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo">>, T4)),
    ?assertEqual({ok, toto}, emqb_topic_tree:find(<<"foo/bar">>, T4)),
    ?assertEqual({ok, tata}, emqb_topic_tree:find(<<"foo/bar/boz">>, T4)),
    ?assertEqual({ok, tutu}, emqb_topic_tree:find(<<"foo/biz/boz">>, T4)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz/">>, T4)),

    ?assertEqual([{[<<"foo">>, <<"bar">>], toto},
                  {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
                  {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}],
                 lists:sort(emqb_topic_tree:fold(FoldFun, [], T4))),

    ?assertEqual(lists:sort(emqb_topic_tree:fold(FoldFun, [], T4)),
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"#">>, T4))),
    ?assertEqual([{[<<"foo">>, <<"bar">>], toto}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/bar">>, T4))),
    ?assertEqual([{[<<"foo">>, <<"bar">>, <<"boz">>], tata},
                  {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz">>, T4))),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz/">>, T4)),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"+">>, T4)),
    ?assertEqual([{[<<"foo">>, <<"biz">>, <<"boz">>], tutu}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/biz/#">>, T4))),

    T5 = emqb_topic_tree:update(<<"foo/biz/boz/">>, tete, T4),
    ?assertEqual(4, emqb_topic_tree:size(T5)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo">>, T5)),
    ?assertEqual({ok, toto}, emqb_topic_tree:find(<<"foo/bar">>, T5)),
    ?assertEqual({ok, tata}, emqb_topic_tree:find(<<"foo/bar/boz">>, T5)),
    ?assertEqual({ok, tutu}, emqb_topic_tree:find(<<"foo/biz/boz">>, T5)),
    ?assertEqual({ok, tete}, emqb_topic_tree:find(<<"foo/biz/boz/">>, T5)),

    ?assertEqual([{[<<"foo">>, <<"bar">>], toto},
                  {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
                  {[<<"foo">>, <<"biz">>, <<"boz">>], tutu},
                  {[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}],
                 lists:sort(emqb_topic_tree:fold(FoldFun, [], T5))),

    ?assertEqual(lists:sort(emqb_topic_tree:fold(FoldFun, [], T5)),
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"#">>, T5))),
    ?assertEqual([{[<<"foo">>, <<"bar">>], toto}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/bar">>, T5))),
    ?assertEqual([{[<<"foo">>, <<"bar">>, <<"boz">>], tata},
                  {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz">>, T5))),
    ?assertEqual([{[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz/">>, T5))),
    ?assertEqual([], emqb_topic_tree:match(FoldFun, [], <<"+">>, T5)),
    ?assertEqual([{[<<"foo">>, <<"biz">>, <<"boz">>], tutu},
                 {[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/biz/#">>, T5))),

    T6 = emqb_topic_tree:update(<<"foo">>, titi, T5),
    ?assertEqual(5, emqb_topic_tree:size(T6)),
    ?assertEqual({ok, titi}, emqb_topic_tree:find(<<"foo">>, T6)),
    ?assertEqual({ok, toto}, emqb_topic_tree:find(<<"foo/bar">>, T6)),
    ?assertEqual({ok, tata}, emqb_topic_tree:find(<<"foo/bar/boz">>, T6)),
    ?assertEqual({ok, tutu}, emqb_topic_tree:find(<<"foo/biz/boz">>, T6)),
    ?assertEqual({ok, tete}, emqb_topic_tree:find(<<"foo/biz/boz/">>, T6)),

    ?assertEqual([{[<<"foo">>], titi},
                  {[<<"foo">>, <<"bar">>], toto},
                  {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
                  {[<<"foo">>, <<"biz">>, <<"boz">>], tutu},
                  {[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}],
                 lists:sort(emqb_topic_tree:fold(FoldFun, [], T6))),

    ?assertEqual(lists:sort(emqb_topic_tree:fold(FoldFun, [], T6)),
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"#">>, T6))),
    ?assertEqual([{[<<"foo">>, <<"bar">>], toto}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/bar">>, T6))),
    ?assertEqual([{[<<"foo">>, <<"bar">>, <<"boz">>], tata},
                  {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz">>, T6))),
    ?assertEqual([{[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/+/boz/">>, T6))),
    ?assertEqual([{[<<"foo">>], titi}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"+">>, T6))),
    ?assertEqual([{[<<"foo">>, <<"biz">>, <<"boz">>], tutu},
                 {[<<"foo">>, <<"biz">>, <<"boz">>, <<"">>], tete}],
                 lists:sort(emqb_topic_tree:match(FoldFun, [], <<"foo/biz/#">>, T6))),

    T7 = emqb_topic_tree:remove(<<"foo/biz/boz/">>, T6),
    ?assertEqual(4, emqb_topic_tree:size(T7)),
    ?assertEqual({ok, titi}, emqb_topic_tree:find(<<"foo">>, T7)),
    ?assertEqual({ok, toto}, emqb_topic_tree:find(<<"foo/bar">>, T7)),
    ?assertEqual({ok, tata}, emqb_topic_tree:find(<<"foo/bar/boz">>, T7)),
    ?assertEqual({ok, tutu}, emqb_topic_tree:find(<<"foo/biz/boz">>, T7)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz/">>, T7)),

    ?assertEqual([{[<<"foo">>], titi},
                  {[<<"foo">>, <<"bar">>], toto},
                  {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
                  {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}],
                 lists:sort(emqb_topic_tree:fold(FoldFun, [], T7))),

    T8 = emqb_topic_tree:remove(<<"foo">>, T7),
    ?assertEqual(3, emqb_topic_tree:size(T8)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo">>, T8)),
    ?assertEqual({ok, toto}, emqb_topic_tree:find(<<"foo/bar">>, T8)),
    ?assertEqual({ok, tata}, emqb_topic_tree:find(<<"foo/bar/boz">>, T8)),
    ?assertEqual({ok, tutu}, emqb_topic_tree:find(<<"foo/biz/boz">>, T8)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz/">>, T8)),

    ?assertEqual([{[<<"foo">>, <<"bar">>], toto},
                  {[<<"foo">>, <<"bar">>, <<"boz">>], tata},
                  {[<<"foo">>, <<"biz">>, <<"boz">>], tutu}],
                 lists:sort(emqb_topic_tree:fold(FoldFun, [], T8))),

    T9 = emqb_topic_tree:remove(<<"foo/biz/boz">>, T8),
    ?assertEqual(2, emqb_topic_tree:size(T9)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo">>, T9)),
    ?assertEqual({ok, toto}, emqb_topic_tree:find(<<"foo/bar">>, T9)),
    ?assertEqual({ok, tata}, emqb_topic_tree:find(<<"foo/bar/boz">>, T9)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz">>, T9)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz/">>, T9)),

    ?assertEqual([{[<<"foo">>, <<"bar">>], toto},
                  {[<<"foo">>, <<"bar">>, <<"boz">>], tata}],
                 lists:sort(emqb_topic_tree:fold(FoldFun, [], T9))),

    T10 = emqb_topic_tree:remove(<<"foo/bar">>, T9),
    ?assertEqual(1, emqb_topic_tree:size(T10)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo">>, T10)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/bar">>, T10)),
    ?assertEqual({ok, tata}, emqb_topic_tree:find(<<"foo/bar/boz">>, T10)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz">>, T10)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz/">>, T10)),

    ?assertEqual(T10, emqb_topic_tree:remove(<<"foo/bar/">>, T10)),

    ?assertEqual([{[<<"foo">>, <<"bar">>, <<"boz">>], tata}],
                 lists:sort(emqb_topic_tree:fold(FoldFun, [], T10))),

    T11 = emqb_topic_tree:remove(<<"foo/bar/boz">>, T10),
    ?assertEqual(0, emqb_topic_tree:size(T11)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo">>, T11)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/bar">>, T11)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/bar/boz">>, T11)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz">>, T11)),
    ?assertEqual(error, emqb_topic_tree:find(<<"foo/biz/boz/">>, T11)),

    ?assertEqual([], lists:sort(emqb_topic_tree:fold(FoldFun, [], T11))),

    ok.
