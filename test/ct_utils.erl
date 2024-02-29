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

-module(ct_utils).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("common_test/include/ct.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API functions
-export([setup_tracing/0]).
-export([start_tracing/1]).
-export([stop_tracing/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup_tracing() ->
	dbg:start().

start_tracing(_TestName) ->
	dbg:tracer(process, {fun trace_handler/2, #{}}),
    ok.

stop_tracing(_TestName) ->
    dbg:stop().


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

trace_handler(Msg, State) ->
    try iolist_to_binary(format_trace(Msg)) of
        Trace -> ct:print("~s", [Trace])
    catch
        T:E:S -> ct:print("TRACE ERROR ~w:~w~nSTACK:~n~p~nTRACE:~n~p",
                          [T, E, S, Msg])
    end,
    State.

format_trace({trace, Pid, call, {Mod, Fun, Args}}) ->
    io_lib:format("~w CALL TO ~w:~w(~s)", [Pid, Mod, Fun, format_args(Args)]);
format_trace({trace, Pid, return_from, {Mod, Fun, Arity}, Result}) ->
    io_lib:format("~w RETURN FROM ~w:~w/~w : ~s",
                  [Pid, Mod, Fun, Arity, format_multipline_term(Result)]);
format_trace({trace, Pid, exception_from, {Mod, Fun, Arity}, Value}) ->
    io_lib:format("~w EXCEPTION IN ~w:~w/~w : ~s",
                  [Pid, Mod, Fun, Arity, format_multipline_term(Value)]).

is_multiline(Binary) when is_binary(Binary) ->
    case binary:match(Binary, <<"\n">>) of
        {Position, _Length} when Position > 0 -> true;
        nomatch -> false
    end.

format_multipline_term(T) ->
    B = iolist_to_binary(io_lib:format("~p", [T])),
    case is_multiline(B) of
        true -> io_lib:format("~n~s", [B]);
        false -> io_lib:format("~s", [B])
    end.

format_args(Args) ->
    FormatedArgs = [iolist_to_binary(format_arg(A)) || A <- Args],
    IsMultiline = lists:any(fun(X) -> X =:= true end,
                            [is_multiline(A) || A <- FormatedArgs]),
    case IsMultiline of
        false ->
            lists:join(", ", FormatedArgs);
        true ->
            Nl = io_lib:format("~n", []),
            Sep = io_lib:format(",~n", []),
            [Nl, lists:join(Sep, FormatedArgs), Nl]
    end.

format_arg(Arg) ->
    io_lib:format("~p", [Arg]).
