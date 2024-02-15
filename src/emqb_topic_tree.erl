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
-export([size/1]).
-export([update/3]).
-export([remove/2]).
-export([find/2]).
-export([fold/3]).
-export([match/4]).
-export([resolve/4]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type topic_data(T) :: undefined | {data, T}.
-type topic_node(T) :: #{emqb_topic:level() => {topic_data(T), topic_node(T)}}.
-type topic_tree(T) :: topic_node(T).
-type fold_fun(T) :: fun((emqb_topic:path(), T, term()) -> term()).


-export_type([topic_tree/1]).


%%% API Functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec new() -> topic_tree(any()).
new() -> #{}.


%% @doc Returns the number of topics in the tree. This is not a constant-time
%% operation, the whole tree will be scanned so it is slow and only intended
%% to be used for debugging.
-spec size(topic_tree(any())) -> non_neg_integer().
size(Tree) ->
    tree_size(Tree, 0).

-spec update(emqb_topic:path() | binary(), T, topic_tree(T)) -> topic_tree(T).
update(Topic, Data, Tree) ->
    tree_update(Tree, emqb_topic:parse(Topic), Data).

-spec remove(emqb_topic:path() | binary(), topic_tree(T)) -> topic_tree(T).
remove(Topic, Tree) ->
    tree_remove(Tree, emqb_topic:parse(Topic)).

%% @doc Finds the exact topic, the given topic could be either a fully qualified
%% topic or a topic pattern, but it will only find if the exact pattern/topic
%% is in the tree.
-spec find(emqb_topic:path() | binary(), topic_tree(T)) -> {ok, T} | error.
find(Topic, Tree) ->
    tree_find(Tree, emqb_topic:parse(Topic)).

-spec fold(fold_fun(T), term(), topic_tree(T)) -> term().
fold(Fun, Init, Tree) ->
    tree_fold(Tree, [], Fun, Init).

%% @doc Calls the given function for each fully qualified topic in the tree
%% that match the given pattern or fully qualified topic.
%% The tree is not expected to contain any pattern, only fully qualified topics.
-spec match(fold_fun(T), term(), emqb_topic:path() | binary(), topic_tree(T))
    -> term().
match(Fun, Init, TopicPattern, Tree) ->
    tree_match(Tree, emqb_topic:parse(TopicPattern), [], Fun, Init).

%% @doc Calls the given function for each patterns or fully qualified topics in
%% the tree that match the given fully qualified topic.
resolve(Fun, Init, TopicPath, Tree) ->
    tree_resolve(Tree, emqb_topic:parse(TopicPath), [], Fun, Init).


%%% Internal Functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

tree_size(undefined, Acc) -> Acc;
tree_size(Tree, Acc) ->
    maps:fold(fun
        (_, {undefined, SubTree}, CurrAcc) -> tree_size(SubTree, CurrAcc);
        (_, {{data, _}, SubTree}, CurrAcc) -> 1 + tree_size(SubTree, CurrAcc)
    end, Acc, Tree).

tree_update(undefined, TopicPath, Data) ->
    tree_update(#{}, TopicPath, Data);
tree_update(Tree, [Level], Data) ->
    case Tree of
        #{Level := {_OldData, SubTree}} ->
            Tree#{Level => {{data, Data}, SubTree}};
        #{} ->
            Tree#{Level => {{data, Data}, undefined}}
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

tree_find(undefined, _) -> error;
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

tree_fold(undefined, _InvPrefix, _Fun, Acc) -> Acc;
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

tree_match(undefined, _Pattern, _InvPrefix, _Fun, Acc) -> Acc;
tree_match(Tree, [$#], InvPrefix, Fun, Acc) ->
    tree_fold(Tree, InvPrefix, Fun, Acc);
tree_match(Tree, [$+], InvPrefix, Fun, Acc) ->
    maps:fold(fun
        (_Level, {undefined, _SubTree}, CurrAcc) ->
            CurrAcc;
        (Level, {{data, Data}, _SubTree}, CurrAcc) ->
            NewInvPrefix = [Level | InvPrefix],
            Topic = lists:reverse(NewInvPrefix),
            Fun(Topic, Data, CurrAcc)
    end, Acc, Tree);
tree_match(Tree, [$+ | Rest], InvPrefix, Fun, Acc) ->
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

tree_resolve(_Tree, [], _InvPrefix, _Fun, Acc) ->
    Acc;
tree_resolve(Tree, [Level], InvPrefix, Fun, Acc) ->
    Acc2 = case maps:find($#, Tree) of
        {ok, {{data, LevelData1}, undefined}} ->
            Fun(lists:reverse([$# | InvPrefix]), LevelData1, Acc);
        _ -> Acc
    end,
    Acc3 = case maps:find($+, Tree) of
        {ok, {{data, LevelData2}, _SubTree1}} ->
            Fun(lists:reverse([$+ | InvPrefix]), LevelData2, Acc2);
        _ -> Acc2
    end,
    Acc4 = case maps:find(Level, Tree) of
        {ok, {{data, LevelData3}, _SubTree2}} ->
            Fun(lists:reverse([Level | InvPrefix]), LevelData3, Acc3);
        _ -> Acc3
    end,
    Acc4;
tree_resolve(Tree, [Level | Rest], InvPrefix, Fun, Acc) ->
    Acc2 = case maps:find($#, Tree) of
        {ok, {{data, LevelData}, undefined}} ->
            Fun(lists:reverse([$# | InvPrefix]), LevelData, Acc);
        _ -> Acc
    end,
    Acc3 = case maps:find($+, Tree) of
        {ok, {_, SubTree1}} when SubTree1 =/= undefined ->
            tree_resolve(SubTree1, Rest, [$+ | InvPrefix], Fun, Acc2);
        _ -> Acc2
    end,
    Acc4 = case maps:find(Level, Tree) of
        {ok, {_, SubTree2}} when SubTree2 =/= undefined ->
            tree_resolve(SubTree2, Rest, [Level | InvPrefix], Fun, Acc3);
        _ -> Acc3
    end,
    Acc4.
