namespace Jackalope.Protocol;

internal static class AmqpConstants
{
    public const byte FrameMethod = 1;
    public const byte FrameHeader = 2;
    public const byte FrameBody = 3;
    public const byte FrameHeartbeat = 8;
    public const byte FrameEnd = 0xCE;

    /// <summary>Frame header (type + channel + size) plus the frame-end octet.</summary>
    public const int FrameOverhead = 8;

    /// <summary>Spec minimum frame size; also the limit that applies before Tune/TuneOk completes.</summary>
    public const int MinFrameMax = 4096;

    public static readonly byte[] ProtocolHeader = [(byte)'A', (byte)'M', (byte)'Q', (byte)'P', 0, 0, 9, 1];

    public static readonly byte[] HeartbeatFrame = [FrameHeartbeat, 0, 0, 0, 0, 0, 0, FrameEnd];
}

internal static class ReplyCode
{
    public const ushort Success = 200;
    public const ushort NoRoute = 312;
    public const ushort ConnectionForced = 320;
    public const ushort AccessRefused = 403;
    public const ushort NotFound = 404;
    public const ushort ResourceLocked = 405;
    public const ushort PreconditionFailed = 406;
    public const ushort FrameError = 501;
    public const ushort SyntaxError = 502;
    public const ushort CommandInvalid = 503;
    public const ushort ChannelError = 504;
    public const ushort UnexpectedFrame = 505;
    public const ushort NotAllowed = 530;
    public const ushort NotImplemented = 540;
    public const ushort InternalError = 541;

    public static string Name(ushort code) => code switch
    {
        Success => "OK",
        NoRoute => "NO_ROUTE",
        ConnectionForced => "CONNECTION_FORCED",
        AccessRefused => "ACCESS_REFUSED",
        NotFound => "NOT_FOUND",
        ResourceLocked => "RESOURCE_LOCKED",
        PreconditionFailed => "PRECONDITION_FAILED",
        FrameError => "FRAME_ERROR",
        SyntaxError => "SYNTAX_ERROR",
        CommandInvalid => "COMMAND_INVALID",
        ChannelError => "CHANNEL_ERROR",
        UnexpectedFrame => "UNEXPECTED_FRAME",
        NotAllowed => "NOT_ALLOWED",
        NotImplemented => "NOT_IMPLEMENTED",
        InternalError => "INTERNAL_ERROR",
        _ => "ERROR",
    };
}

/// <summary>Method identifiers encoded as (classId &lt;&lt; 16) | methodId.</summary>
internal static class Method
{
    public static uint Id(ushort classId, ushort methodId) => ((uint)classId << 16) | methodId;

    public const uint ConnectionStart = (10u << 16) | 10;
    public const uint ConnectionStartOk = (10u << 16) | 11;
    public const uint ConnectionTune = (10u << 16) | 30;
    public const uint ConnectionTuneOk = (10u << 16) | 31;
    public const uint ConnectionOpen = (10u << 16) | 40;
    public const uint ConnectionOpenOk = (10u << 16) | 41;
    public const uint ConnectionClose = (10u << 16) | 50;
    public const uint ConnectionCloseOk = (10u << 16) | 51;

    public const uint ChannelOpen = (20u << 16) | 10;
    public const uint ChannelOpenOk = (20u << 16) | 11;
    public const uint ChannelClose = (20u << 16) | 40;
    public const uint ChannelCloseOk = (20u << 16) | 41;

    public const uint ExchangeDeclare = (40u << 16) | 10;
    public const uint ExchangeDeclareOk = (40u << 16) | 11;

    public const uint QueueDeclare = (50u << 16) | 10;
    public const uint QueueDeclareOk = (50u << 16) | 11;
    public const uint QueueBind = (50u << 16) | 20;
    public const uint QueueBindOk = (50u << 16) | 21;
    public const uint QueueUnbind = (50u << 16) | 50;
    public const uint QueueUnbindOk = (50u << 16) | 51;

    public const uint BasicQos = (60u << 16) | 10;
    public const uint BasicQosOk = (60u << 16) | 11;
    public const uint BasicConsume = (60u << 16) | 20;
    public const uint BasicConsumeOk = (60u << 16) | 21;
    public const uint BasicCancel = (60u << 16) | 30;
    public const uint BasicCancelOk = (60u << 16) | 31;
    public const uint BasicPublish = (60u << 16) | 40;
    public const uint BasicReturn = (60u << 16) | 50;
    public const uint BasicDeliver = (60u << 16) | 60;
    public const uint BasicAck = (60u << 16) | 80;
    public const uint BasicReject = (60u << 16) | 90;
    public const uint BasicNack = (60u << 16) | 120;

    public const uint ConfirmSelect = (85u << 16) | 10;
    public const uint ConfirmSelectOk = (85u << 16) | 11;

    private static readonly Dictionary<uint, string> Names = new()
    {
        [Id(10, 10)] = "connection.start", [Id(10, 11)] = "connection.start-ok",
        [Id(10, 20)] = "connection.secure", [Id(10, 21)] = "connection.secure-ok",
        [Id(10, 30)] = "connection.tune", [Id(10, 31)] = "connection.tune-ok",
        [Id(10, 40)] = "connection.open", [Id(10, 41)] = "connection.open-ok",
        [Id(10, 50)] = "connection.close", [Id(10, 51)] = "connection.close-ok",
        [Id(10, 60)] = "connection.blocked", [Id(10, 61)] = "connection.unblocked",
        [Id(10, 70)] = "connection.update-secret",
        [Id(20, 10)] = "channel.open", [Id(20, 11)] = "channel.open-ok",
        [Id(20, 20)] = "channel.flow", [Id(20, 21)] = "channel.flow-ok",
        [Id(20, 40)] = "channel.close", [Id(20, 41)] = "channel.close-ok",
        [Id(40, 10)] = "exchange.declare", [Id(40, 20)] = "exchange.delete",
        [Id(40, 30)] = "exchange.bind", [Id(40, 40)] = "exchange.unbind",
        [Id(50, 10)] = "queue.declare", [Id(50, 20)] = "queue.bind", [Id(50, 30)] = "queue.purge",
        [Id(50, 40)] = "queue.delete", [Id(50, 50)] = "queue.unbind",
        [Id(60, 10)] = "basic.qos", [Id(60, 20)] = "basic.consume", [Id(60, 30)] = "basic.cancel",
        [Id(60, 40)] = "basic.publish", [Id(60, 70)] = "basic.get", [Id(60, 80)] = "basic.ack",
        [Id(60, 90)] = "basic.reject", [Id(60, 100)] = "basic.recover-async", [Id(60, 110)] = "basic.recover",
        [Id(60, 120)] = "basic.nack",
        [Id(85, 10)] = "confirm.select",
        [Id(90, 10)] = "tx.select", [Id(90, 20)] = "tx.commit", [Id(90, 30)] = "tx.rollback",
    };

    public static string Name(uint id) =>
        Names.TryGetValue(id, out var name) ? name : $"method {id >> 16}.{id & 0xFFFF}";
}
