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

-ifndef(EMQB_INTERNAL_HRL).
-define(EMQB_INTERNAL_HRL, true).

-record(topic_subscription, {
    ref :: reference(),
    pattern :: emqb_topic:path(),
    client :: pid(),
    owner :: pid(),
    qos :: emqb:qos(),
    sid :: undefined | non_neg_integer()
}).

-type topic_subscription() :: #topic_subscription{}.

-endif.
