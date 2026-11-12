namespace Jackalope.Protocol;

/// <summary>An AMQP error that the broker reports to the client with a Close method.</summary>
internal abstract class AmqpException : Exception
{
    protected AmqpException(ushort replyCode, string text, uint methodId)
        : base($"{Protocol.ReplyCode.Name(replyCode)} - {text}")
    {
        ReplyCode = replyCode;
        MethodId = methodId;
    }

    public ushort ReplyCode { get; }

    /// <summary>The (class &lt;&lt; 16 | method) id of the method that caused the error, or 0.</summary>
    public uint MethodId { get; }
}

/// <summary>A "soft" error: closes only the channel it occurred on.</summary>
internal sealed class ChannelException(ushort replyCode, string text, uint methodId = 0)
    : AmqpException(replyCode, text, methodId);

/// <summary>A "hard" error: closes the whole connection.</summary>
internal sealed class ConnectionException(ushort replyCode, string text, uint methodId = 0)
    : AmqpException(replyCode, text, methodId);

/// <summary>Malformed wire data; reported as a SYNTAX_ERROR connection close.</summary>
internal sealed class AmqpSyntaxException(string message) : Exception(message);
