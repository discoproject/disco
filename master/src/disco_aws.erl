%%%-------------------------------------------------------------------
%%% @author Tristan Sloughter <>
%%% @copyright (C) 2012, Tristan Sloughter
%%% @doc
%%%
%%% @end
%%% Created : 30 May 2012 by Tristan Sloughter <>
%%%-------------------------------------------------------------------
-module(disco_aws).

%% API
-export([spawn_put/3,
         set_aws_creds/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec spawn_put(string(), string(), binary()) -> pid().
spawn_put(Bucket, Dir, Contents) ->
    Creds = get_config(),
    proc_lib:spawn(fun()->                       
                           erlcloud_s3:put_object(Bucket, binary_to_list(Dir), Contents, Creds) 
                   end).

-spec set_aws_creds() -> record().
set_aws_creds() ->
    CredFilePath = os:getenv("AWS_CREDENTIAL_FILE"),
    {ok, CredFile} = file:read_file(CredFilePath), 
    AwsCreds =[list_to_tuple(string:tokens(Line, "=")) || Line <- string:tokens(binary_to_list(CredFile), "\n")],

    AccessKeyId = aws_access_key_id(AwsCreds),
    SecretKey = aws_secret_key(AwsCreds),

    erlcloud_s3:configure(AccessKeyId, SecretKey),
    erlcloud_s3:new(AccessKeyId, SecretKey).    

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_config() ->
    case get(aws_config) of
        undefined ->
            set_aws_creds();
        Config ->
            Config
    end.
    
aws_access_key_id(AwsCreds) ->
    {"AWSAccessKeyId", AccessKeyId} = lists:keyfind("AWSAccessKeyId", 1, AwsCreds),    
    AccessKeyId.

aws_secret_key(AwsCreds) ->
    {"AWSSecretKey", SecretKey} = lists:keyfind("AWSSecretKey", 1, AwsCreds),    
    SecretKey.
