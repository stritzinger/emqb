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

-module(emqb_topic_tree).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API Functions
-export([new/0]).
-export([parse_topic/1]).
-export([size/1]).
-export([update/3]).
-export([remove/2]).
-export([find/2]).
-export([fold/3]).
-export([match/4]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type topic() :: [topic_level()].
-type topic_level() :: binary() | '+' | '#'.
-type topic_data() :: undefined | {data, term()}.
-type topic_node() :: #{topic_level() => {topic_data(), topic_node()}}.
-type topic_tree() :: topic_node().
-type fold_fun() :: fun((topic(), topic_data(), term()) -> term()).


-export_type([topic_tree/0]).


%%% API Functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec new() -> topic_tree().
new() -> #{}.

-spec parse_topic(binary() | topic()) -> topic().
parse_topic(<<"">>) -> erlang:error(badarg);
parse_topic([]) -> erlang:error(badarg);
parse_topic(TopicBin) when is_binary(TopicBin) ->
    Levels = binary:split(TopicBin, <<"/">>, [global]),
    Topic = lists:map(fun
        (<<"+">>) -> '+';
        (<<"#">>) -> '#';
        (Level) -> Level
    end, Levels),
    parse_topic(Topic);
parse_topic(Topic) when is_list(Topic) ->
    validate_topic(Topic),
    Topic.

%% @doc Returns the number of topics in the tree. This is not a constant-time
%% operation, the whole tree will be scanned so it is slow and only intended
%% to be used for debugging.
-spec size(topic_tree()) -> non_neg_integer().
size(Tree) ->
    tree_size(Tree, 0).

-spec update(topic() | binary(), topic_data(), topic_tree()) -> topic_tree().
update(Topic, Data, Tree) ->
    tree_update(Tree, parse_topic(Topic), Data).

-spec remove(topic() | binary(), topic_tree()) -> topic_tree().
remove(Topic, Tree) ->
    tree_remove(Tree, parse_topic(Topic)).

-spec find(topic() | binary(), topic_tree()) -> {ok, topic_data()} | error.
find(Topic, Tree) ->
    tree_find(Tree, parse_topic(Topic)).

-spec fold(fold_fun(), term(), topic_tree()) -> term().
fold(Fun, Init, Tree) ->
    tree_fold(Tree, [], Fun, Init).

-spec match(fold_fun(), term(), topic() | binary(), topic_tree()) -> term().
match(Fun, Init, Topic, Tree) ->
    tree_match(Tree, parse_topic(Topic), [], Fun, Init).


%%% Internal Functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% This doesn't validate the levels name does not contain '+' or '#'
validate_topic([]) -> ok;
validate_topic(['#']) -> ok;
validate_topic([Bin | Rest]) when is_binary(Bin) ->
    validate_topic(Rest);
validate_topic(['+' | Rest]) ->
    validate_topic(Rest);
validate_topic(_) ->
    erlang:error(badarg).

tree_size(Tree, Acc) ->
    maps:fold(fun
        (_, {undefined, SubTree}, CurrAcc) -> tree_size(SubTree, CurrAcc);
        (_, {{data, _}, SubTree}, CurrAcc) -> 1 + tree_size(SubTree, CurrAcc)
    end, Acc, Tree).

tree_update(Tree, [Level], Data) ->
    case Tree of
        #{Level := {_OldData, SubTree}} ->
            Tree#{Level => {{data, Data}, SubTree}};
        #{} ->
            Tree#{Level => {{data, Data}, #{}}}
    end;
tree_update(Tree, [Level | Rest], Data) ->
    case Tree of
        #{Level := {LevelData, SubTree}} ->
            NewSubTree = tree_update(SubTree, Rest, Data),
            Tree#{Level => {LevelData, NewSubTree}};
        #{} ->
            SubTree = tree_update(#{}, Rest, Data),
            Tree#{Level => {undefined, SubTree}}
    end.

tree_remove(Tree, [Level]) ->
    case Tree of
        #{Level := {_Data, SubTree}} when map_size(SubTree) =:= 0 ->
            maps:remove(Level, Tree);
        #{Level := {_Data, SubTree}} ->
            Tree#{Level => {undefined, SubTree}};
        #{} ->
            Tree
    end;
tree_remove(Tree, [Level | Rest]) ->
    case Tree of
        #{Level := {undefined, SubTree}} ->
            case tree_remove(SubTree, Rest) of
                NewSubTree when map_size(NewSubTree) =:= 0 ->
                    maps:remove(Level, Tree);
                NewSubTree ->
                    Tree#{Level => {undefined, NewSubTree}}
            end;
        #{Level := {LevelData, SubTree}} ->
            NewSubTree = tree_remove(SubTree, Rest),
            Tree#{Level => {LevelData, NewSubTree}};
        #{} ->
            Tree
    end.

tree_find(Tree, [Level]) ->
    case Tree of
        #{Level := {{data, Data}, _SubTree}} -> {ok, Data};
        #{} -> error
    end;
tree_find(Tree, [Level | Rest]) ->
    case Tree of
        #{Level := {_LevelData, SubTree}} -> tree_find(SubTree, Rest);
        #{} -> error
    end.

tree_fold(Tree, InvPrefix, Fun, Acc) ->
    maps:fold(fun
        (Level, {undefined, SubTree}, CurrAcc) ->
            tree_fold(SubTree, [Level | InvPrefix], Fun, CurrAcc);
        (Level, {{data, Data}, SubTree}, CurrAcc) ->
            NewInvPrefix = [Level | InvPrefix],
            Topic = lists:reverse(NewInvPrefix),
            NewAcc = Fun(Topic, Data, CurrAcc),
            tree_fold(SubTree, NewInvPrefix, Fun, NewAcc)
    end, Acc, Tree).

tree_match(Tree, ['#'], InvPrefix, Fun, Acc) ->
    tree_fold(Tree, InvPrefix, Fun, Acc);
tree_match(Tree, ['+'], InvPrefix, Fun, Acc) ->
    maps:fold(fun
        (_Level, {undefined, _SubTree}, CurrAcc) ->
            CurrAcc;
        (Level, {{data, Data}, _SubTree}, CurrAcc) ->
            NewInvPrefix = [Level | InvPrefix],
            Topic = lists:reverse(NewInvPrefix),
            Fun(Topic, Data, CurrAcc)
    end, Acc, Tree);
tree_match(Tree, ['+' | Rest], InvPrefix, Fun, Acc) ->
    maps:fold(fun(Level, {_LevelData, SubTree}, CurrAcc) ->
        tree_match(SubTree, Rest, [Level | InvPrefix], Fun, CurrAcc)
    end, Acc, Tree);
tree_match(Tree, [Level], InvPrefix, Fun, Acc) when is_binary(Level) ->
    case Tree of
        #{Level := {{data, Data}, _SubTree}} ->
            Fun(lists:reverse([Level | InvPrefix]), Data, Acc);
        #{} ->
            Acc
    end;
tree_match(Tree, [Level | Rest], InvPrefix, Fun, Acc) when is_binary(Level) ->
    case Tree of
        #{Level := {_LevelData, SubTree}} ->
            tree_match(SubTree, Rest, [Level | InvPrefix], Fun, Acc);
        #{} ->
            Acc
    end.
