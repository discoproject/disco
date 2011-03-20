-module(master_tests).
-export([main/0]).

main() ->
    io:fwrite("Running ddfs_util prop tests ...~n", []),
    ddfs_util_test:prop_test(),
    io:fwrite("Running ddfs_tag prop tests ...~n", []),
    ddfs_tag_test:prop_test(),
    io:fwrite("Running ddfs_tag tests ...~n", []),
    ddfs_tag_test:test().
