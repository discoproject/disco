-type jobname() :: nonempty_string().

-type cores() :: non_neg_integer().

-record(nodeinfo, {name  :: host(),
                   slots :: cores(),
                   connected   :: boolean(),
                   blacklisted :: boolean(),
                   num_running :: non_neg_integer(),
                   stats_ok      :: non_neg_integer(),
                   stats_failed  :: non_neg_integer(),
                   stats_crashed :: non_neg_integer()}).
-type nodeinfo() :: #nodeinfo{}.

% types used for local-cluster mode

%                          {NextPort, {host() -> {GetPort, PutPort}}}.
-type port_map() :: none | {non_neg_integer(), gb_tree()}.

-record(node_ports, {get_port :: non_neg_integer(),
                     put_port :: non_neg_integer()}).
-type node_ports() :: #node_ports{}.
