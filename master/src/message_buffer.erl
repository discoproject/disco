-module(message_buffer).

-export([new/1, has_overflow/1, to_list/1, to_string/1, append/2]).

-type message_buffer() :: {queue(), 'nooverflow' | 'overflow', non_neg_integer()}.

-spec new(non_neg_integer()) -> message_buffer().
new(MessagesMax) when is_integer(MessagesMax), MessagesMax >= 0 ->
    {queue:new(), nooverflow, MessagesMax}.

-spec has_overflow(message_buffer()) -> 'nooverflow' | 'overflow'.
has_overflow({_MessageQ, Overflow, _MessagesMax}) ->
    Overflow.

-spec to_list(message_buffer()) -> [string()].
to_list({MessageQ, _Overflow, _MessagesMax}) ->
    queue:to_list(MessageQ).

-spec to_string(message_buffer()) -> string().
to_string({_MessageQ, Overflow, _MessagesMax} = MessageBuffer) ->
    case Overflow of
        overflow -> "...\n";
        nooverflow -> ""
    end ++ string:join(to_list(MessageBuffer), "\n").

-spec append(string(), message_buffer()) -> message_buffer().
append(Message, {MessageQ, Overflow, MessagesMax}) ->
    MessageQ_ = queue:in(Message, MessageQ),
    case queue:len(MessageQ_) > MessagesMax of
        true ->
            drop({MessageQ_, Overflow, MessagesMax});
        false ->
            {MessageQ_, Overflow, MessagesMax}
    end.

-spec drop(message_buffer()) -> message_buffer().
drop({MessageQ, _Overflow, MessagesMax}) ->
    {_Old, MessageQ_} = queue:out(MessageQ),
    {MessageQ_, overflow, MessagesMax}.
