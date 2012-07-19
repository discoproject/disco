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
-export([put/3,
         spawn_put/3,
         set_aws_creds/0,
         try_s3_tagdata/2,
         delete/2,
         list_objects/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec put(string(), string(), binary()) -> term().
put(Bucket, Dir, Contents) ->
    Creds = get_config(),
    erlcloud_s3:put_object(Bucket, Dir, Contents, Creds).

-spec spawn_put(string(), string(), binary()) -> pid().
spawn_put(Bucket, Dir, Contents) ->
    Creds = get_config(),
    proc_lib:spawn(fun()->                       
                           erlcloud_s3:put_object(Bucket, Dir, Contents, Creds) 
                   end).

-spec delete(string(), string()) -> list().
delete(Bucket, Key) ->
    Creds = get_config(),
    erlcloud_s3:delete_object(Bucket, Key, Creds).

-spec list_objects(string()) -> list().
list_objects(Bucket) ->
    Creds = get_config(),
    erlcloud_s3:list_objects(Bucket, Creds).    

-spec set_aws_creds() -> record().
set_aws_creds() ->
    CredFilePath = os:getenv("AWS_CREDENTIAL_FILE"),
    {ok, CredFile} = file:read_file(CredFilePath), 
    AwsCreds =[list_to_tuple(string:tokens(Line, "=")) || Line <- string:tokens(binary_to_list(CredFile), "\n")],

    AccessKeyId = aws_access_key_id(AwsCreds),
    SecretKey = aws_secret_key(AwsCreds),

    erlcloud_s3:configure(AccessKeyId, SecretKey),
    erlcloud_s3:new(AccessKeyId, SecretKey).    

-spec try_s3_tagdata(string(), string()) -> {binary(), binary()}.
try_s3_tagdata(Bucket, TagName) ->
    Creds = get_config(),
    case lists:keyfind(contents, 1, erlcloud_s3:list_objects(Bucket, Creds)) of
        {contents, Objects} ->
            case lists:foldl(fun(Tag, Acc) ->
                                     {key, Key} = lists:keyfind(key, 1, Tag),

                                     case string:str(Key, "tag/"++TagName) of
                                         0 ->
                                             Acc;
                                         1 ->
                                             {last_modified, Time} = lists:keyfind(last_modified, 1, Tag),
                                             [{Time, Key} | Acc]
                                     end
                             end, [], Objects) of
                [] ->
                    fail;
                Tags ->
                    {_, TagID} = lists:max(Tags),
                    {TagID, get_tag_data(Bucket, TagID, Creds)}
            end;
        _ ->
            fail
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_tag_data(Bucket, TagID, Creds) ->
    Obj = erlcloud_s3:get_object(Bucket, TagID, Creds),
    {content, Content} = lists:keyfind(content, 1, Obj),
    Content.

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
