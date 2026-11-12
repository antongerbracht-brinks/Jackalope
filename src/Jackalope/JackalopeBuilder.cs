namespace Jackalope;

/// <summary>
/// Fluent builder in the style of Testcontainers' <c>RabbitMqBuilder</c>.
/// </summary>
/// <example>
/// <code>
/// await using var rabbit = new JackalopeBuilder().WithUsername("app").WithPassword("secret").Build();
/// await rabbit.StartAsync();
/// var factory = new ConnectionFactory { Uri = new Uri(rabbit.GetConnectionString()) };
/// </code>
/// </example>
public sealed class JackalopeBuilder
{
    private JackalopeBrokerOptions _options = new();

    public JackalopeBuilder WithHost(string host)
    {
        _options = _options with { Host = host };
        return this;
    }

    /// <summary>Fixed port. Omit (or pass 0) for an OS-assigned free port, which is what parallel CI needs.</summary>
    public JackalopeBuilder WithPort(int port)
    {
        _options = _options with { Port = port };
        return this;
    }

    public JackalopeBuilder WithUsername(string username)
    {
        _options = _options with { Username = username };
        return this;
    }

    public JackalopeBuilder WithPassword(string password)
    {
        _options = _options with { Password = password };
        return this;
    }

    public JackalopeBuilder WithHeartbeat(TimeSpan interval)
    {
        _options = _options with { HeartbeatSeconds = checked((ushort)interval.TotalSeconds) };
        return this;
    }

    public JackalopeBuilder WithFrameMax(int frameMax)
    {
        _options = _options with { FrameMax = frameMax };
        return this;
    }

    public JackalopeBuilder WithLogger(Action<string> logger)
    {
        _options = _options with { Logger = logger };
        return this;
    }

    /// <summary>The options accumulated so far.</summary>
    public JackalopeBrokerOptions Options => _options;

    /// <summary>Builds an in-process broker (fastest; recommended for .NET test projects).</summary>
    public JackalopeBroker Build() => new(_options);

    /// <summary>Builds a broker that runs as a child process. See <see cref="JackalopeProcess"/> for how the server is located.</summary>
    public JackalopeProcess BuildProcess(string? serverPath = null) => new(_options, serverPath);
}
