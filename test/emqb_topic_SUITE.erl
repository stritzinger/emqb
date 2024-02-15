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

-module(emqb_topic_SUITE).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Common test functions
-export([all/0]).

% Test cases
-export([parse_topic_test/1]).
-export([format_topic_test/1]).


%%% COMMON TEST FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() ->
    [parse_topic_test, format_topic_test].


%%% TEST CASES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_topic_test(_Config) ->
    ?assertError(badarg, emqb_topic:parse(<<"">>)),
    ?assertError(badarg, emqb_topic:parse([])),
    ?assertEqual([<<"">>, <<"">>],
                 emqb_topic:parse(<<"/">>)),
    ?assertEqual([<<"foo">>, <<"bar">>],
                 emqb_topic:parse(<<"foo/bar">>)),
    ?assertEqual([<<"foo">>, <<"bar">>],
                 emqb_topic:parse([<<"foo">>, <<"bar">>])),
    ?assertEqual([<<"">>, <<"foo">>, <<"bar">>],
                 emqb_topic:parse(<<"/foo/bar">>)),
    ?assertEqual([<<"foo">>, <<"bar">>, <<"">>],
                 emqb_topic:parse(<<"foo/bar/">>)),
    ?assertEqual([<<"foo">>, $+, <<"bar">>, $#],
                 emqb_topic:parse(<<"foo/+/bar/#">>)),
    ?assertError(badarg, emqb_topic:parse(<<"foo/#/bar/+">>)),
    ?assertError(badarg, emqb_topic:parse([<<"foo">>, $#, <<"bar">>])),
    ok.

format_topic_test(_Config) ->
    ?assertError(badarg, emqb_topic:format([])),
    ?assertError(badarg, emqb_topic:format(<<"">>)),
    ?assertEqual(<<"/">>,
                 emqb_topic:format([<<"">>, <<"">>])),
    ?assertEqual(<<"foo/bar">>,
                 emqb_topic:format([<<"foo">>, <<"bar">>])),
    ?assertEqual(<<"/foo/bar">>,
                 emqb_topic:format([<<"">>, <<"foo">>, <<"bar">>])),
    ?assertEqual(<<"foo/bar/">>,
                 emqb_topic:format([<<"foo">>, <<"bar">>, <<"">>])),
    ?assertEqual(<<"foo/+/bar/#">>,
                 emqb_topic:format([<<"foo">>, $+, <<"bar">>, $#])),
    ok.
