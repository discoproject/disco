-module(message_buffer).

-export([new/1, has_overflow/1, to_list/1, to_string/1, append/2]).

new(MessagesMax) when is_integer(MessagesMax), MessagesMax >= 0 ->
    {queue:new(), nooverflow, MessagesMax}.

has_overflow({_MessageQ, Overflow, _MessagesMax}) ->
    Overflow.

to_list({MessageQ, _Overflow, _MessagesMax}) ->
    queue:to_list(MessageQ).

to_string({_MessageQ, Overflow, _MessagesMax} = MessageBuffer) ->
    case Overflow of
        overflow -> "...\n";
        nooverflow -> ""
    end ++ string:join(to_list(MessageBuffer), "\n").

append(Message, {MessageQ, Overflow, MessagesMax}) ->
    MessageQ_ = queue:in(Message, MessageQ),
    case queue:len(MessageQ_) > MessagesMax of
        true ->
            drop({MessageQ_, Overflow, MessagesMax});
        false ->
            {MessageQ_, Overflow, MessagesMax}
    end.

drop({MessageQ, _Overflow, MessagesMax}) ->
    {_Old, MessageQ_} = queue:out(MessageQ),
    {MessageQ_, overflow, MessagesMax}.
