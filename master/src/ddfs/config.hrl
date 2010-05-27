
-define(SECOND, 1000).
-define(MINUTE, 60 * ?SECOND).
-define(HOUR, 60 * ?MINUTE).
-define(DAY, 24 * ?HOUR).
-define(MB, 1024 * 1024).

% Maximum length of tag/blob prefix
-define(NAME_MAX, 511).

% How many replicas by default
% -define(DEFAULT_REPLICAS, 3).

% How long to wait for replies from nodes
-define(NODE_TIMEOUT, 5 * ?SECOND).

% How long ddfs node startup can take
-define(NODE_STARTUP, 1 * ?MINUTE).

% How long to wait for a reply from and operation that accesses nodes
% (>NODE_TIMEOUT)
-define(NODEOP_TIMEOUT, 1 * ?MINUTE).

% How many HTTP connections are kept in queue if the system is busy
-define(HTTP_QUEUE_LENGTH, 100).

% How long to keep a PUT request in queue if the system is busy
-define(PUT_WAIT_TIMEOUT, 1 * ?MINUTE).

% How long to keep a GET request in queue if the system is busy
-define(GET_WAIT_TIMEOUT, 1 * ?MINUTE).

% Tag cache expires in TAG_EXPIRES milliseconds.
% Note that must be TAG_EXPIRES < GC_INTERVAL, otherwise tags never expire
-define(TAG_EXPIRES, 10 * ?HOUR).

% Interval for refreshing the list of all available tags
% (This is only needed to purge non-existent tags eventually from the tag
% cache. It doesn't harm to have a long interval).
-define(TAG_CACHE_INTERVAL, 10 * ?MINUTE).

% Time to wait between garbage collection runs
-define(GC_INTERVAL, ?DAY).

% Tag cache expires in this many milliseconds if tag can't be fetched
-define(TAG_EXPIRES_ONERROR, 1 * ?MINUTE).

% Number of tag replicas: min(length(Nodes), ?TAG_REPLICAS)
% -define(TAG_REPLICAS, 3).

% Permissions for blobs and tags
-define(FILE_MODE, 8#00400).

% Interval for checking available disk space in ddfs_node
-define(DISKSPACE_INTERVAL, 10 * ?SECOND).

% How often node should refresh its tag cache from disk
-define(FIND_TAGS_INTERVAL, ?DAY).

% /dfs/tag/ requests can be ?MAX_TAG_BODY_SIZE bytes at most
-define(MAX_TAG_BODY_SIZE, (512 * ?MB)).

% How long to http requests should wait for the tag updates to
% finish (a long time)
-define(TAG_UPDATE_TIMEOUT, ?DAY).

% Timeout for re-replicating a single blob over HTTP PUT
-define(GC_PUT_TIMEOUT, 10 * ?MINUTE).

% Delete !partial leftovers after this many milliseconds
-define(PARTIAL_EXPIRES, ?DAY).

% When orphaned blob can be deleted 
-define(ORPHANED_BLOB_EXPIRES, 5 * ?DAY).

% When orphaned tag can be deleted 
-define(ORPHANED_TAG_EXPIRES, 5 * ?DAY).

% How long a tag has to stay on the deleted list before
% we can permanently forget it, after all known instances
% of the tag object have been removed. This quarantine period 
% ensures that a node that was temporarily unavailable 
% and reactivates can't resurrect deleted tags. You
% must ensure that all temporarily inactive nodes
% are reactivated (or cleaned) within the ?DELETED_TAG_EXPIRES
% time frame. To be on the safe side, make the period long
% enough.
-define(DELETED_TAG_EXPIRES, 30 * ?DAY).
