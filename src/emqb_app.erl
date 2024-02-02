%%%-------------------------------------------------------------------
%% @doc emqb main application callback module
%% @end
%%%-------------------------------------------------------------------

-module(emqb_app).

-behaviour(application).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Behaviour application callback functions
-export([start/2]).
-export([stop/1]).


%%% BEHAVIOUR application CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start(_StartType, _StartArgs) ->
    emqb_sup:start_link().

stop(_State) ->
    ok.
