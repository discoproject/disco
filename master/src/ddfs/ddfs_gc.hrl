-type local_object() :: {object_name(), node()}.
-type phase() :: 'start' | 'build_map' | 'map_wait' | 'gc'
               | 'rr_blobs' | 'rr_blobs_wait' | 'rr_tags'.
-type protocol_msg() :: {'check', object_type(), object_name()} | 'start_gc' | 'end_rr'.

-type blob_update() :: {object_name(), 'filter' | [url()]}.
