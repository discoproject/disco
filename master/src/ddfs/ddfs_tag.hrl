
-type tokentype() :: 'read' | 'write'.
-type user_attr() :: [{binary(), binary()}].
% An 'internal' token is also used by internal consumers, but never stored.
-type token() :: 'null' | binary().

-record(tagcontent, {id :: binary(),
                     last_modified :: binary(),
                     read_token = null :: token(),
                     write_token = null :: token(),
                     urls = [] :: [binary()],
                     user = [] :: user_attr()}).

-type tagcontent() :: #tagcontent{}.
