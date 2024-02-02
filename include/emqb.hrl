%%--------------------------------------------------------------------
%% Copyright (c) 2024 Peer Stritzinger GmbH. All Rights Reserved.
%%
%% Portions of this software are derived from the emqtt project,
%% which is licensed under the Apache License, Version 2.0.
%% For more details, see https://github.com/emqx/emqtt or the project's
%% official website.
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

-ifndef(EMQB_HRL).
-define(EMQB_HRL, true).


%%--------------------------------------------------------------------
%% MQTT QoS Levels, QoS 2 is not yet supported.
%%--------------------------------------------------------------------

-define(QOS_0, 0). %% At most once
-define(QOS_1, 1). %% At least once

-define(IS_QOS(I), (I >= ?QOS_0 andalso I =< ?QOS_1)).

-define(QOS_I(Name),
    begin
        (case Name of
            ?QOS_0        -> ?QOS_0;
            qos0          -> ?QOS_0;
            at_most_once  -> ?QOS_0;
            ?QOS_1        -> ?QOS_1;
            qos1          -> ?QOS_1;
            at_least_once -> ?QOS_1
        end)
    end).

-define(IS_QOS_NAME(I),
        (I =:= qos0 orelse I =:= at_most_once orelse
         I =:= qos1 orelse I =:= at_least_once)).

-endif.
