-module(jc_utils_test).

-include("common_types.hrl").
-include("disco.hrl").
-include("job_coordinator.hrl").
-include("pipeline.hrl").
-include_lib("eunit/include/eunit.hrl").

% All of these tests assume that MIN_RATION_OF_FIRST_ACTIVE_TO_REST is set to 2.

can1_test() ->
    P = [{stage1, split, false}],
    G = gb_trees:from_orddict([{stage1, #stage_info{}}]),
    true = jc_utils:can_run_task(P, stage1, G).

can2_test() ->
    P = [{stage1, split, false}, {stage2, group_node, true}],
    G = gb_trees:from_orddict([{stage1, #stage_info{finished = true}}]),
    true = jc_utils:can_run_task(P, stage2, G).

can3_test() ->
    P = [{stage1, split, false}, {stage2, group_node, true}],
    G = gb_trees:from_orddict([
            {stage1, #stage_info{finished = false, n_running = 4}},
            {stage2, #stage_info{finished = false, n_running = 1}}
        ]),
    true = jc_utils:can_run_task(P, stage2, G).

can4_test() ->
    P = [{stage1, split, false}, {stage2, group_node, true}],
    G = gb_trees:from_orddict([
            {stage1, #stage_info{finished = false, n_running = 4}},
            {stage2, #stage_info{finished = false, n_running = 2}}
        ]),
    false = jc_utils:can_run_task(P, stage2, G).
