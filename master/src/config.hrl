
-define(MB, 1024 * 1024).

-define(MAX_JSON_POST, 32 * ?MB).

-define(MAX_JOB_PACKET, 1024 * ?MB).

% Make sure the number is well below the maximum number
% of file descriptors
-define(MAX_HTTP_CONNECTIONS, 128).
