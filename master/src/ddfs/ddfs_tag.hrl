
-type tokentype() :: 'read' | 'write'.
-type user_attr() :: [{binary(), binary()}].
% An 'internal' token is also used by internal consumers, but never stored.
-type token() :: 'null' | binary().

-type tagname() :: binary().
-type tagid() :: binary().

-type attrib() :: 'urls' | 'read_token' | 'write_token' | {'user', binary()}.

-record(tagcontent, {id :: tagid(),
                     last_modified :: binary(),
                     read_token = null :: token(),
                     write_token = null :: token(),
                     urls = [] :: [[binary()]],
                     user = [] :: user_attr()}).

-type tagcontent() :: #tagcontent{}.

% API messages.
-type get_msg() :: {get, attrib() | all, token()}.
-type put_msg() :: {put, attrib(), string(), token()}.
-type update_msg() :: {update, [url()], token(), proplists:proplist()}.
-type delayed_update_msg() :: {delayed_update, [url()], token(), proplists:proplist()}.
-type delete_attrib_msg()  :: {delete_attrib, attrib(), token()}.
-type delete_msg() :: {delete, token()}.

% Special msg for GC.
-type gc_get_msg() :: gc_get.

% Notifications.
-type notify_msg() :: {notify, term()}.

% Special messages for the +deleted tag.
-type has_tagname_msg() :: {has_tagname, tagname()}.
-type get_tagnames_msg() :: get_tagnames.
-type delete_tagname_msg() :: {delete_tagname, tagname()}.

