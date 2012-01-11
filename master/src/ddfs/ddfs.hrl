-type host() :: nonempty_string().
-type path() :: nonempty_string().
-type volume_name() :: nonempty_string().

% Diskinfo is {FreeSpace, UsedSpace}.
-type diskinfo() :: {non_neg_integer(), non_neg_integer()}.
-type volume() :: {diskinfo(), volume_name()}.

-type object_type() :: 'blob' | 'tag'.
-type object_name() :: binary().
-type url() :: binary().
-type taginfo() :: {erlang:timestamp(), volume_name()}.
