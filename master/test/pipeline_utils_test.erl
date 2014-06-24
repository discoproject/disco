-module(pipeline_utils_test).

-include("common_types.hrl").
-include("disco.hrl").
-include("job_coordinator.hrl").
-include("pipeline.hrl").
-include_lib("eunit/include/eunit.hrl").

data_split_test() ->
    DataSpec = {data, {5, 0, [{"out", "http://output"}]}},
    Outputs = [{11, [{10, DataSpec}]}],
    GOutputs = pipeline_utils:group_outputs(split, Outputs),
    GOutputs = [{{5,"http://output"},[{{11,10}, DataSpec}]}].

dir_split_test() ->
    DirSpec = {dir, {"localhost", "localhost", [{9, "out"}]}},
    Outputs = [{11, [{10, DirSpec}]}],
    GOutputs = pipeline_utils:group_outputs(split, Outputs),
    GOutputs = [{{9,"localhost"}, [{{11,10},DirSpec}]}].

dir_data_split_test() ->
    DirSpec = {dir, {"localhost", "localhost", [{9, "out"}]}},
    DataSpec = {data, {0, 0, [{"other", "http://otherhost"}]}},
    Outputs = [{11, [{10, DirSpec}, {12, DataSpec}]}],
    GOutputs = pipeline_utils:group_outputs(split, Outputs),
    GOutputs = [{{9,"localhost"}, [{{11,10},DirSpec}]},
           {{0,"http://otherhost"},[{{11,12},DataSpec}]}].

data_node_label_test() ->
    DataSpec = {data, {5, 0, [{"out", "http://output"}]}},
    Outputs = [{11, [{10, DataSpec}]}],
    GOutputs = pipeline_utils:group_outputs(group_node_label, Outputs),
    GOutputs = [{{5,"http://output"},[{{11,10}, DataSpec}]}].

data_node_label_multi_test() ->
    DataSpec1 = {data, {5, 600, [{"out1", "http://output1"}]}},
    DataSpec2 = {data, {5, 700, [{"out2", "http://output2"}]}},
    Outputs = [{11, [{10, DataSpec1}, {10, DataSpec2}]}],
    GOutputs = pipeline_utils:group_outputs(group_node_label, Outputs),
    Elem1 = {{5,"http://output1"}, [{{11,10},DataSpec1}]},
    Elem2 = {{5,"http://output2"}, [{{11,10},DataSpec2}]},
    case GOutputs of
        [Elem1,Elem2] -> ok;
        [Elem2,Elem1] -> ok
    end.

data_label_multi_test() ->
    DataSpec1 = {data, {5, 600, [{"out1", "http://output1"}]}},
    DataSpec2 = {data, {5, 700, [{"out2", "http://output2"}]}},
    Outputs = [{11, [{10, DataSpec1}, {10, DataSpec2}]}],
    GOutputs = pipeline_utils:group_outputs(group_label, Outputs),
    GOutputs = [{{5,none}, [{{11,10},DataSpec1}, {{11,10},DataSpec2}]}].


empty_pipeline_test() ->
    true = pipeline_utils:all_deps_finished([], <<"stage">>, none).

input_stage_test() ->
    true = pipeline_utils:all_deps_finished([{a,b, false}], ?INPUT, none).

done_stage_test() ->
    true = pipeline_utils:all_deps_finished([{a,b, false}], a, none).

undone_stage_test() ->
    M = gb_trees:from_orddict([{a, #stage_info{finished = false}}]),
    false = pipeline_utils:all_deps_finished([{a,b, false}], c, M).

dep_done_stage_test() ->
    M = gb_trees:from_orddict([{a, #stage_info{finished = true}}]),
    true = pipeline_utils:all_deps_finished([{a,b, false}], c, M).

get_grouping_test_group(Grouping, TaskId, NewOutput) ->
    DataSpec1 = {data, {5, 600, [{"out1", "node1"}]}},
    DataSpec2 = {data, {5, 700, [{"out2", "node2"}]}},
    Outputs = [{11, [{10, DataSpec1}, {10, DataSpec2}]}],
    pipeline_utils:get_grouping_lists(Grouping, Outputs, TaskId, NewOutput).

get_grouping_node_label_test() ->
    DataSpec3 = {data, {5, 800, [{"out3", "node3"}]}},
    Output = [{13, DataSpec3}],
    TaskId = 14,
    {L1, []} = get_grouping_test_group(group_node_label, TaskId, Output),
    L1 = [{{5,"node3"}, [{{TaskId,13},DataSpec3}]}].

get_grouping_all_test() ->
    DataSpec3 = {data, {5, 800, [{"out3", "node3"}]}},
    Output = [{13, DataSpec3}],
    TaskId = 14,
    {[], L2} = get_grouping_test_group(group_all, TaskId, Output),
    1 = length(L2).

get_grouping_node1_test() ->
    DataSpec3 = {data, {5, 800, [{"out3", "node3"}]}},
    Output = [{13, DataSpec3}],
    TaskId = 14,
    {L1, []} = get_grouping_test_group(group_node, TaskId, Output),
    L1 = [{{0,"node3"}, [{{TaskId,13},DataSpec3}]}].

get_grouping_node2_test() ->
    DataSpec3 = {data, {5, 800, [{"out3", "node1"}]}},
    Output = [{13, DataSpec3}],
    TaskId = 14,
    {[], L2} = get_grouping_test_group(group_node, TaskId, Output),
    1 = length(L2).

get_grouping_dir_test() ->
   DirSpec = {dir, {"localhost", <<"dir://data">>, [{9, "out"}]}},
   Outputs = [{0, DirSpec}],
   {L1, []} = pipeline_utils:get_grouping_lists(group_all, [], 1, Outputs),
   1 = length(L1).
