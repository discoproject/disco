-define(SECOND, 1000).
-define(MINUTE, 60 * ?SECOND).
-define(HOUR, 60 * ?MINUTE).
-define(DAY, 24 * ?HOUR).
-define(MB, 1024 * 1024).

-define(MAX_JSON_POST, 32 * ?MB).

-define(MAX_JOB_PACKET, 1024 * ?MB).

% Make sure the number is well below the maximum number
% of file descriptors
-define(MAX_HTTP_CONNECTIONS, 128).

% Default setting for the maximum number of failures
% allowed per task
-define(TASK_MAX_FAILURES, 10000000).

% A failed task is paused for
% min(C * FAILED_MIN_PAUSE, FAILED_MAX_PAUSE) +
%    randint(0, FAILED_PAUSE_RANDOMIZE)
% seconds where C is the failure count for the task.

-define(FAILED_MIN_PAUSE, 1000).
-define(FAILED_MAX_PAUSE, 60 * 1000).
-define(FAILED_PAUSE_RANDOMIZE, 30 * 1000).
