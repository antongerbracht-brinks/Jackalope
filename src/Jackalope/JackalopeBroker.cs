using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Jackalope.Broker;
using Jackalope.Transport;

namespace Jackalope;

/// <summary>
/// An in-process mock RabbitMQ broker listening on its own TCP port. Each instance is fully independent:
/// separate listener, separate exchanges/queues, no static state.
/// </summary>
/// <example>
/// <code>
/// await using var broker = new JackalopeBroker();
/// await broker.StartAsync();
/// var factory = new ConnectionFactory { Uri = new Uri(broker.GetConnectionString()) };
/// </code>
/// </example>
public sealed class JackalopeBroker : IJackalopeInstance
{
    private readonly JackalopeBrokerOptions _options;
    private readonly ConcurrentDictionary<AmqpConnection, Task> _connections = new();
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private BrokerState? _state;
    private TcpListener? _listener;
    private CancellationTokenSource? _stopping;
    private Task? _acceptLoop;
    private int _connectionIds;
    private bool _started;
    private bool _stopped;

    public JackalopeBroker()
        : this(new JackalopeBrokerOptions())
    {
    }

    public JackalopeBroker(JackalopeBrokerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
    }

    public JackalopeBrokerOptions Options => _options;

    /// <inheritdoc />
    public string Hostname { get; private set; } = "127.0.0.1";

    /// <inheritdoc />
    public int Port { get; private set; }

    /// <inheritdoc />
    public string Username => _options.Username ?? "guest";

    /// <inheritdoc />
    public string Password => _options.Password ?? "guest";

    public bool IsRunning => _started && !_stopped;

    /// <inheritdoc />
    public string GetConnectionString() => ConnectionStrings.Build(Hostname, Port, Username, Password);

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_started)
            {
                throw new InvalidOperationException("The broker has already been started. Create a new instance instead.");
            }

            var address = await ResolveAsync(_options.Host, cancellationToken).ConfigureAwait(false);
            _state = new BrokerState(_options);
            _listener = new TcpListener(address, _options.Port);
            _listener.Start(backlog: 512);
            var endpoint = (IPEndPoint)_listener.LocalEndpoint;
            Port = endpoint.Port;
            Hostname = ConnectionStrings.ConnectableHost(endpoint.Address, _options.Host);
            _stopping = new CancellationTokenSource();
            _acceptLoop = AcceptLoopAsync(_listener, _stopping.Token);
            _started = true;
            _options.Log($"listening on {endpoint}");
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    private static async Task<IPAddress> ResolveAsync(string host, CancellationToken cancellationToken)
    {
        if (IPAddress.TryParse(host, out var parsed))
        {
            return parsed;
        }

        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return IPAddress.Loopback;
        }

        var addresses = await Dns.GetHostAddressesAsync(host, cancellationToken).ConfigureAwait(false);
        return addresses.FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork) ?? addresses[0];
    }

    private async Task AcceptLoopAsync(TcpListener listener, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            Socket socket;
            try
            {
                socket = await listener.AcceptSocketAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException or SocketException)
            {
                return;
            }

            socket.NoDelay = true;
            var connection = new AmqpConnection(_state!, socket, Interlocked.Increment(ref _connectionIds));
            _connections[connection] = Task.Run(async () =>
            {
                try
                {
                    await connection.RunAsync().ConfigureAwait(false);
                }
                finally
                {
                    _connections.TryRemove(connection, out _);
                }
            }, CancellationToken.None);
        }
    }

    /// <summary>
    /// Stops listening and closes all client connections with 320 CONNECTION_FORCED, as a real broker shutdown would.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_started || _stopped)
            {
                return;
            }

            _stopped = true;
            _stopping!.Cancel();
            _listener!.Stop();
            await _acceptLoop!.ConfigureAwait(false);

            lock (_state!.Sync)
            {
                foreach (var connection in _connections.Keys)
                {
                    connection.BeginServerClose(new Protocol.ConnectionException(Protocol.ReplyCode.ConnectionForced,
                        "broker forced connection closure with reason 'shutdown'"));
                }
            }

            try
            {
                await Task.WhenAll(_connections.Values).WaitAsync(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                foreach (var connection in _connections.Keys)
                {
                    connection.Abort();
                }
            }

            _options.Log("stopped");
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _stopping?.Dispose();
    }

    // ---- introspection (handy for test assertions; not part of AMQP) ------------------------------

    /// <summary>Number of currently connected AMQP clients.</summary>
    public int ConnectionCount => _connections.Count;

    /// <summary>A point-in-time snapshot of all queues.</summary>
    public IReadOnlyList<QueueInfo> GetQueues()
    {
        var state = RequireState();
        lock (state.Sync)
        {
            return state.Queues.Values
                .Select(q => new QueueInfo(q.Name, q.Ready.Count, q.Consumers.Count, q.Durable, q.Exclusive, q.AutoDelete,
                    new Dictionary<string, object?>(q.Arguments)))
                .OrderBy(q => q.Name, StringComparer.Ordinal)
                .ToList();
        }
    }

    /// <summary>A point-in-time snapshot of one queue, or null if it does not exist.</summary>
    public QueueInfo? GetQueue(string name) => GetQueues().FirstOrDefault(q => q.Name == name);

    /// <summary>A point-in-time snapshot of all exchanges, including the built-in ones.</summary>
    public IReadOnlyList<ExchangeInfo> GetExchanges()
    {
        var state = RequireState();
        lock (state.Sync)
        {
            return state.Exchanges.Values
                .Select(e => new ExchangeInfo(e.Name, e.TypeName, e.Durable, e.AutoDelete, e.Internal,
                    e.Bindings.Select(b => new BindingInfo(e.Name, b.Queue.Name, b.RoutingKey)).ToList()))
                .OrderBy(e => e.Name, StringComparer.Ordinal)
                .ToList();
        }
    }

    private BrokerState RequireState() =>
        _state ?? throw new InvalidOperationException("The broker has not been started.");
}

public sealed record QueueInfo(string Name, int MessageCount, int ConsumerCount, bool Durable, bool Exclusive, bool AutoDelete,
    IReadOnlyDictionary<string, object?> Arguments);

public sealed record ExchangeInfo(string Name, string Type, bool Durable, bool AutoDelete, bool Internal, IReadOnlyList<BindingInfo> Bindings);

public sealed record BindingInfo(string Exchange, string Queue, string RoutingKey);
