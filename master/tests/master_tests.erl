-module(master_tests).
-export([main/0]).

main() ->
    ddfs_util_test:prop_test().
