-type path() :: nonempty_string().
-type volume_name() :: nonempty_string().

% Diskinfo is {FreeSpace, UsedSpace}.
-type diskinfo() :: {non_neg_integer(), non_neg_integer()}.
-type volume() :: {diskinfo(), volume_name()}.
