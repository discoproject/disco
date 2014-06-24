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
    true = jc_utils:can_run_task(P, stage1, G, #task_schedule{}).

can2_test() ->
    P = [{stage1, split, false}, {stage2, group_node, true}],
    G = gb_trees:from_orddict([
            {stage1, #stage_info{finished = true}},
            {stage2, #stage_info{finished = false, n_running = 0}}
        ]),
    true = jc_utils:can_run_task(P, stage2, G, #task_schedule{max_cores = 1}).

can3_test() ->
    P = [{stage1, split, false}, {stage2, group_node, true}],
    G = gb_trees:from_orddict([
            {stage1, #stage_info{finished = false, n_running = 4}},
            {stage2, #stage_info{finished = false, n_running = 1}}
        ]),
    true = jc_utils:can_run_task(P, stage2, G, #task_schedule{}).

can4_test() ->
    P = [{stage1, split, false}, {stage2, group_node, true}],
    G = gb_trees:from_orddict([
            {stage1, #stage_info{finished = false, n_running = 4}},
            {stage2, #stage_info{finished = false, n_running = 2}}
        ]),
    false = jc_utils:can_run_task(P, stage2, G, #task_schedule{}).

can5_test() ->
    P = [{stage1, split, false}, {stage2, group_node, true}],
    G = gb_trees:from_orddict([
            {stage1, #stage_info{finished = false, n_running = 4}},
            {stage2, #stage_info{finished = false, n_running = 1}}
        ]),
    false = jc_utils:can_run_task(P, stage2, G, #task_schedule{max_cores = 2}).

can6_test() ->
    P = [{stage1, split, false}, {stage2, group_node, true}],
    G = gb_trees:from_orddict([
            {stage1, #stage_info{finished = false, n_running = 4}},
            {stage2, #stage_info{finished = false, n_running = 1}}
        ]),
    false = jc_utils:can_run_task(P, stage2, G, #task_schedule{max_cores = 5}).

can7_test() ->
    P = [{stage1, split, false}],
    G = gb_trees:from_orddict([{stage1, #stage_info{finished = false, n_running = 4}}]),
    false = jc_utils:can_run_task(P, stage2, G, #task_schedule{max_cores = 4}).

can8_test() ->
    P = [{stage1, split, false}],
    G = gb_trees:from_orddict([{stage1, #stage_info{finished = false, n_running = 4}}]),
    true = jc_utils:can_run_task(P, stage2, G, #task_schedule{max_cores = 5}).

can9_test() ->
    P = [{stage1, split, false}, {stage2, group_node, true}],
    G = gb_trees:from_orddict([
            {stage1, #stage_info{finished = true}},
            {stage2, #stage_info{finished = false, n_running = 10}}
        ]),
    false = jc_utils:can_run_task(P, stage2, G, #task_schedule{max_cores = 5}).
