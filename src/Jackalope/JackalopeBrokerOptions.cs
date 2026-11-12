namespace Jackalope;

/// <summary>Settings for one broker instance. Every instance gets its own copy; nothing is shared between instances.</summary>
public sealed record JackalopeBrokerOptions
{
    /// <summary>Address to listen on. Defaults to loopback, which avoids firewall prompts on CI agents.</summary>
    public string Host { get; init; } = "127.0.0.1";

    /// <summary>TCP port to listen on. 0 (the default) asks the OS for a free ephemeral port.</summary>
    public int Port { get; init; }

    /// <summary>Expected PLAIN username. Null accepts any username.</summary>
    public string? Username { get; init; }

    /// <summary>Expected PLAIN password. Null accepts any password.</summary>
    public string? Password { get; init; }

    /// <summary>Heartbeat interval (seconds) proposed in connection.tune. The client may lower it or disable it with 0.</summary>
    public ushort HeartbeatSeconds { get; init; } = 60;

    /// <summary>Maximum frame size proposed in connection.tune (RabbitMQ's default is 131072).</summary>
    public int FrameMax { get; init; } = 131072;

    /// <summary>Maximum channel number proposed in connection.tune (RabbitMQ's default is 2047).</summary>
    public ushort ChannelMax { get; init; } = 2047;

    /// <summary>Largest accepted message body in bytes (RabbitMQ's default is 128 MiB).</summary>
    public long MaxMessageSize { get; init; } = 128L * 1024 * 1024;

    /// <summary>Optional diagnostic log sink (connection lifecycle, protocol errors).</summary>
    public Action<string>? Logger { get; init; }

    internal void Log(string message)
    {
        try
        {
            Logger?.Invoke(message);
        }
        catch
        {
            // A faulty log sink must never take down the broker.
        }
    }

    internal void Validate()
    {
        if (Port is < 0 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(Port), Port, "Port must be between 0 and 65535.");
        }

        if (FrameMax < 4096)
        {
            throw new ArgumentOutOfRangeException(nameof(FrameMax), FrameMax, "FrameMax must be at least 4096.");
        }

        if (ChannelMax == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(ChannelMax), ChannelMax, "ChannelMax must be at least 1.");
        }
    }
}
