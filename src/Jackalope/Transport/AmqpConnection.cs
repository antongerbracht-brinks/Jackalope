using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using Jackalope.Broker;
using Jackalope.Protocol;

namespace Jackalope.Transport;

/// <summary>
/// One client TCP connection. Frames are read sequentially by <see cref="RunAsync"/> and processed while
/// holding the broker lock; everything sent to the client goes through a single unbounded queue drained by one
/// writer task, so frames from different channels never interleave mid-frame and content frames stay contiguous.
/// </summary>
internal sealed class AmqpConnection
{
    private static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan CloseOkTimeout = TimeSpan.FromSeconds(3);

    private readonly BrokerState _broker;
    private readonly JackalopeBrokerOptions _options;
    private readonly Socket _socket;
    private readonly Stream _input;
    private readonly Stream _output;
    private readonly Channel<byte[]> _outbound = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true });
    private readonly Dictionary<ushort, ServerChannel> _channels = new();
    private readonly CancellationTokenSource _abort = new();

    private State _state = State.AwaitingProtocolHeader;
    private int _incomingFrameMax = AmqpConstants.MinFrameMax;
    private int _outgoingFrameMax = AmqpConstants.MinFrameMax;
    private ushort _channelMax;
    private long _lastReceivedTicks = Environment.TickCount64;
    private bool _cleanedUp;

    private enum State
    {
        AwaitingProtocolHeader,
        AwaitingStartOk,
        AwaitingTuneOk,
        AwaitingOpen,
        Open,
        Closing,
    }

    public AmqpConnection(BrokerState broker, Socket socket, int id)
    {
        _broker = broker;
        _options = broker.Options;
        _socket = socket;
        var stream = new NetworkStream(socket, ownsSocket: false);
        _input = new BufferedStream(stream, 64 * 1024);
        _output = new BufferedStream(stream, 64 * 1024);
        Id = id;
        RemoteEndPoint = socket.RemoteEndPoint?.ToString() ?? "unknown";
    }

    public int Id { get; }

    public string RemoteEndPoint { get; }

    public string VirtualHost { get; private set; } = "/";

    public string? UserName { get; private set; }

    public ushort HeartbeatSeconds { get; private set; }

    public bool IsOpen => _state == State.Open;

    /// <summary>Largest body-frame payload we may send, from the negotiated frame-max.</summary>
    public int MaxBodyFramePayload => _outgoingFrameMax - AmqpConstants.FrameOverhead;

    public int ChannelCount => _channels.Count;

    private void Log(string message) => _broker.Options.Log($"[conn {Id} {RemoteEndPoint}] {message}");

    /// <summary>Queues raw frame bytes for sending. Thread-safe and non-blocking.</summary>
    public void Send(byte[] frames) => _outbound.Writer.TryWrite(frames);

    public void SendMethod(ushort channel, uint methodId, Action<AmqpWriter>? writeArgs = null) =>
        Send(AmqpWriter.MethodFrame(channel, methodId, writeArgs));

    public async Task RunAsync()
    {
        var writer = WriteLoopAsync();
        using var handshakeTimer = new Timer(_ =>
        {
            if (_state < State.Open)
            {
                Log("handshake timed out");
                Abort();
            }
        }, null, HandshakeTimeout, Timeout.InfiniteTimeSpan);

        try
        {
            if (!await ReadProtocolHeaderAsync())
            {
                return;
            }

            lock (_broker.Sync)
            {
                SendConnectionStart();
                _state = State.AwaitingStartOk;
            }

            var header = new byte[7];
            while (true)
            {
                await _input.ReadExactlyAsync(header, _abort.Token);
                byte type = header[0];
                ushort channel = BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(1));
                uint size = BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(3));

                if (size > (uint)(_incomingFrameMax - AmqpConstants.FrameOverhead))
                {
                    lock (_broker.Sync)
                    {
                        BeginServerClose(new ConnectionException(ReplyCode.FrameError,
                            $"frame size {size + AmqpConstants.FrameOverhead} exceeds negotiated frame-max {_incomingFrameMax}"));
                    }

                    // The rest of the stream can no longer be parsed reliably; give the Close a moment to flush.
                    await Task.Delay(100);
                    return;
                }

                var payload = new byte[size + 1];
                await _input.ReadExactlyAsync(payload, _abort.Token);
                Volatile.Write(ref _lastReceivedTicks, Environment.TickCount64);

                bool keepReading;
                lock (_broker.Sync)
                {
                    keepReading = payload[size] == AmqpConstants.FrameEnd
                        ? HandleFrame(type, channel, payload.AsSpan(0, (int)size))
                        : BeginServerClose(new ConnectionException(ReplyCode.FrameError,
                            $"missing frame-end octet (got 0x{payload[size]:X2})"));
                }

                if (!keepReading)
                {
                    return;
                }
            }
        }
        catch (Exception ex) when (ex is IOException or EndOfStreamException or OperationCanceledException or ObjectDisposedException or SocketException)
        {
            // Peer went away or we aborted.
        }
        catch (Exception ex)
        {
            Log($"unexpected error: {ex}");
        }
        finally
        {
            lock (_broker.Sync)
            {
                Cleanup();
            }

            _outbound.Writer.TryComplete();
            try
            {
                await writer.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch
            {
                // Best effort flush.
            }

            Abort();
            _socket.Dispose();
            Log("closed");
        }
    }

    private async Task<bool> ReadProtocolHeaderAsync()
    {
        var header = new byte[8];
        await _input.ReadExactlyAsync(header, _abort.Token);
        if (!header.AsSpan().SequenceEqual(AmqpConstants.ProtocolHeader))
        {
            // Spec: reply with the protocol header we support, then close.
            Log($"rejected protocol header {Convert.ToHexString(header)}");
            Send(AmqpConstants.ProtocolHeader);
            return false;
        }

        return true;
    }

    private async Task WriteLoopAsync()
    {
        var reader = _outbound.Reader;
        try
        {
            while (await reader.WaitToReadAsync())
            {
                while (reader.TryRead(out var frames))
                {
                    await _output.WriteAsync(frames, _abort.Token);
                }

                await _output.FlushAsync(_abort.Token);
            }
        }
        catch (Exception)
        {
            Abort();
        }
    }

    /// <summary>Tears down the socket immediately; the read loop then exits and runs cleanup.</summary>
    public void Abort()
    {
        try
        {
            _abort.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        try
        {
            _socket.Shutdown(SocketShutdown.Both);
        }
        catch
        {
            // Already closed.
        }

        _socket.Close();
    }

    // ---- frame dispatch (all called under the broker lock) -------------------------------------

    private bool HandleFrame(byte type, ushort channelNumber, ReadOnlySpan<byte> payload)
    {
        try
        {
            return HandleFrameCore(type, channelNumber, payload);
        }
        catch (ConnectionException ex)
        {
            return BeginServerClose(ex);
        }
        catch (AmqpSyntaxException ex)
        {
            return BeginServerClose(new ConnectionException(ReplyCode.SyntaxError, ex.Message));
        }
    }

    private bool HandleFrameCore(byte type, ushort channelNumber, ReadOnlySpan<byte> payload)
    {
        if (type == AmqpConstants.FrameHeartbeat)
        {
            if (channelNumber != 0)
            {
                throw new ConnectionException(ReplyCode.FrameError, $"heartbeat frame on non-zero channel {channelNumber}");
            }

            return true;
        }

        if (_state == State.Closing)
        {
            // After sending connection.close we discard everything except close/close-ok.
            if (type == AmqpConstants.FrameMethod && channelNumber == 0 && payload.Length >= 4)
            {
                uint id = PeekMethod(payload);
                if (id == Method.ConnectionCloseOk)
                {
                    return false;
                }

                if (id == Method.ConnectionClose)
                {
                    SendMethod(0, Method.ConnectionCloseOk);
                    return false;
                }
            }

            return true;
        }

        if (channelNumber == 0)
        {
            if (type != AmqpConstants.FrameMethod)
            {
                throw new ConnectionException(ReplyCode.UnexpectedFrame, $"unexpected frame type {type} on channel 0");
            }

            return HandleConnectionMethod(payload);
        }

        if (_state != State.Open)
        {
            throw new ConnectionException(ReplyCode.ChannelError, "channel frame received before connection.open");
        }

        if (channelNumber > _channelMax)
        {
            throw new ConnectionException(ReplyCode.ChannelError,
                $"channel {channelNumber} exceeds negotiated channel-max {_channelMax}");
        }

        if (!_channels.TryGetValue(channelNumber, out var channel))
        {
            if (type == AmqpConstants.FrameMethod && payload.Length >= 4 && PeekMethod(payload) == Method.ChannelOpen)
            {
                _channels[channelNumber] = new ServerChannel(this, _broker, channelNumber);
                SendMethod(channelNumber, Method.ChannelOpenOk, w => w.WriteLongStr(ReadOnlySpan<byte>.Empty));
                return true;
            }

            throw new ConnectionException(ReplyCode.ChannelError, $"expected 'channel.open' on channel {channelNumber}");
        }

        channel.HandleFrame(type, payload);
        return true;
    }

    private static uint PeekMethod(ReadOnlySpan<byte> payload) =>
        Method.Id(BinaryPrimitives.ReadUInt16BigEndian(payload), BinaryPrimitives.ReadUInt16BigEndian(payload[2..]));

    public void RemoveChannel(ushort number) => _channels.Remove(number);

    private bool HandleConnectionMethod(ReadOnlySpan<byte> payload)
    {
        var reader = new AmqpReader(payload);
        uint id = Method.Id(reader.ReadShort(), reader.ReadShort());

        if (id == Method.ConnectionClose)
        {
            ushort code = reader.ReadShort();
            string text = reader.ReadShortStr();
            Log($"client closed connection: {code} {text}");
            ReleaseResources();
            SendMethod(0, Method.ConnectionCloseOk);
            return false;
        }

        switch (_state)
        {
            case State.AwaitingStartOk when id == Method.ConnectionStartOk:
                HandleStartOk(ref reader);
                return true;

            case State.AwaitingTuneOk when id == Method.ConnectionTuneOk:
                HandleTuneOk(ref reader);
                return true;

            case State.AwaitingOpen when id == Method.ConnectionOpen:
                VirtualHost = reader.ReadShortStr();
                _state = State.Open;
                SendMethod(0, Method.ConnectionOpenOk, w => w.WriteShortStr(""));
                Log($"opened (user '{UserName}', vhost '{VirtualHost}', frame-max {_outgoingFrameMax}, heartbeat {HeartbeatSeconds}s)");
                return true;

            case State.Open:
                throw new ConnectionException(ReplyCode.NotImplemented,
                    $"{Method.Name(id)} is not supported by Jackalope", id);

            default:
                throw new ConnectionException(ReplyCode.CommandInvalid,
                    $"unexpected {Method.Name(id)} during connection handshake", id);
        }
    }

    private void SendConnectionStart()
    {
        var capabilities = new Dictionary<string, object?>
        {
            ["publisher_confirms"] = true,
            ["exchange_exchange_bindings"] = false,
            ["basic.nack"] = true,
            ["consumer_cancel_notify"] = true,
            ["connection.blocked"] = false,
            ["consumer_priorities"] = false,
            ["authentication_failure_close"] = true,
            ["per_consumer_qos"] = true,
            ["direct_reply_to"] = false,
        };
        var serverProperties = new Dictionary<string, object?>
        {
            ["product"] = "Jackalope",
            ["version"] = typeof(AmqpConnection).Assembly.GetName().Version?.ToString(3) ?? "0.0.0",
            ["platform"] = $".NET {Environment.Version}",
            ["information"] = "Jackalope mock RabbitMQ broker (AMQP 0-9-1 subset) for functional tests",
            ["cluster_name"] = "jackalope",
            ["capabilities"] = capabilities,
        };

        SendMethod(0, Method.ConnectionStart, w =>
        {
            w.WriteOctet(0); // version-major
            w.WriteOctet(9); // version-minor
            w.WriteTable(serverProperties);
            w.WriteLongStr("PLAIN");
            w.WriteLongStr("en_US");
        });
    }

    private void HandleStartOk(ref AmqpReader reader)
    {
        reader.ReadTable(); // client-properties
        string mechanism = reader.ReadShortStr();
        byte[] response = reader.ReadLongStr();
        reader.ReadShortStr(); // locale

        if (mechanism != "PLAIN")
        {
            throw new ConnectionException(ReplyCode.AccessRefused,
                $"unsupported authentication mechanism '{mechanism}'; Jackalope supports PLAIN only", Method.ConnectionStartOk);
        }

        // PLAIN response: [authzid] NUL authcid NUL password
        string[] parts = Encoding.UTF8.GetString(response).Split('\0');
        string username = parts.Length >= 2 ? parts[^2] : "";
        string password = parts.Length >= 2 ? parts[^1] : "";
        if (!_broker.Authenticate(username, password))
        {
            throw new ConnectionException(ReplyCode.AccessRefused,
                "Login was refused using authentication mechanism PLAIN. For details see the broker logfile.",
                Method.ConnectionStartOk);
        }

        UserName = username;
        _state = State.AwaitingTuneOk;
        SendMethod(0, Method.ConnectionTune, w =>
        {
            w.WriteShort(_options.ChannelMax);
            w.WriteLong((uint)_options.FrameMax);
            w.WriteShort(_options.HeartbeatSeconds);
        });
    }

    private void HandleTuneOk(ref AmqpReader reader)
    {
        ushort channelMax = reader.ReadShort();
        uint frameMax = reader.ReadLong();
        ushort heartbeat = reader.ReadShort();

        if (frameMax != 0 && frameMax < AmqpConstants.MinFrameMax)
        {
            throw new ConnectionException(ReplyCode.NotAllowed,
                $"frame_max={frameMax} < {AmqpConstants.MinFrameMax} min size", Method.ConnectionTuneOk);
        }

        // 0 means "no limit" from the client's side, so the server's proposal applies.
        _channelMax = channelMax == 0 ? _options.ChannelMax : Math.Min(channelMax, _options.ChannelMax);
        _outgoingFrameMax = frameMax == 0 ? _options.FrameMax : (int)Math.Min(frameMax, (uint)_options.FrameMax);
        _incomingFrameMax = _outgoingFrameMax;
        HeartbeatSeconds = heartbeat;
        _state = State.AwaitingOpen;
    }

    /// <summary>Starts a server-initiated connection close (hard error or shutdown). Returns true to keep reading for close-ok.</summary>
    public bool BeginServerClose(AmqpException error)
    {
        if (_state == State.Closing)
        {
            return true;
        }

        Log($"closing connection: {error.Message}");
        _state = State.Closing;
        ReleaseResources();
        SendMethod(0, Method.ConnectionClose, w =>
        {
            w.WriteShort(error.ReplyCode);
            w.WriteShortStr(Truncate(error.Message));
            w.WriteShort((ushort)(error.MethodId >> 16));
            w.WriteShort((ushort)(error.MethodId & 0xFFFF));
        });

        _ = Task.Delay(CloseOkTimeout).ContinueWith(_ => Abort(), TaskScheduler.Default);
        return true;
    }

    public static string Truncate(string text)
    {
        if (Encoding.UTF8.GetByteCount(text) <= 255)
        {
            return text;
        }

        while (Encoding.UTF8.GetByteCount(text) > 252)
        {
            text = text[..^1];
        }

        return text + "...";
    }

    /// <summary>Releases channels (consumers, unacked messages). Idempotent; caller holds the broker lock.</summary>
    private void ReleaseResources()
    {
        foreach (var channel in _channels.Values)
        {
            channel.Release();
        }

        _channels.Clear();
    }

    private void Cleanup()
    {
        if (_cleanedUp)
        {
            return;
        }

        _cleanedUp = true;
        ReleaseResources();
        _broker.DeleteExclusiveQueuesOwnedBy(this);
    }
}
