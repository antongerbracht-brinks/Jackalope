using System.Buffers.Binary;
using System.Diagnostics;
using System.Net.Sockets;
using Jackalope.Protocol;

namespace Jackalope.Tests;

/// <summary>
/// A deliberately minimal hand-rolled AMQP client, used ONLY for wire-level behaviours a well-behaved real client
/// cannot be made to exhibit (e.g. going silent to provoke a heartbeat timeout, or sending malformed frames).
/// All functional compatibility tests use the real RabbitMQ.Client.
/// </summary>
internal sealed class RawAmqpClient : IAsyncDisposable
{
    private readonly TcpClient _tcp = new() { NoDelay = true };
    private NetworkStream _stream = null!;

    public sealed record Frame(byte Type, ushort Channel, byte[] Payload)
    {
        public uint MethodId => Type == AmqpConstants.FrameMethod
            ? Method.Id(BinaryPrimitives.ReadUInt16BigEndian(Payload), BinaryPrimitives.ReadUInt16BigEndian(Payload.AsSpan(2)))
            : 0;
    }

    public static async Task<RawAmqpClient> ConnectAsync(int port)
    {
        var client = new RawAmqpClient();
        await client._tcp.ConnectAsync("127.0.0.1", port);
        client._stream = client._tcp.GetStream();
        return client;
    }

    public Task SendAsync(byte[] bytes) => _stream.WriteAsync(bytes).AsTask();

    public Task SendMethodAsync(ushort channel, uint methodId, Action<AmqpWriter>? args = null) =>
        SendAsync(AmqpWriter.MethodFrame(channel, methodId, args));

    public async Task<byte[]> ReadBytesAsync(int count, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(5));
        var buffer = new byte[count];
        await _stream.ReadExactlyAsync(buffer, cts.Token);
        return buffer;
    }

    public async Task<Frame> ReadFrameAsync(TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(5));
        var header = new byte[7];
        await _stream.ReadExactlyAsync(header, cts.Token);
        uint size = BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(3));
        var payload = new byte[size + 1];
        await _stream.ReadExactlyAsync(payload, cts.Token);
        Assert.Equal(AmqpConstants.FrameEnd, payload[^1]);
        return new Frame(header[0], BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(1)), payload[..^1]);
    }

    /// <summary>Reads frames, skipping heartbeats, until a method frame arrives.</summary>
    public async Task<Frame> ReadMethodAsync(TimeSpan? timeout = null)
    {
        while (true)
        {
            var frame = await ReadFrameAsync(timeout);
            if (frame.Type != AmqpConstants.FrameHeartbeat)
            {
                return frame;
            }
        }
    }

    /// <summary>Performs protocol header + start/tune/open with the given heartbeat.</summary>
    public async Task HandshakeAsync(ushort heartbeat)
    {
        await SendAsync(AmqpConstants.ProtocolHeader);
        Assert.Equal(Method.ConnectionStart, (await ReadMethodAsync()).MethodId);

        await SendMethodAsync(0, Method.ConnectionStartOk, w =>
        {
            w.WriteTable(new Dictionary<string, object?> { ["product"] = "raw-test-client" });
            w.WriteShortStr("PLAIN");
            w.WriteLongStr("\0guest\0guest");
            w.WriteShortStr("en_US");
        });
        Assert.Equal(Method.ConnectionTune, (await ReadMethodAsync()).MethodId);

        await SendMethodAsync(0, Method.ConnectionTuneOk, w =>
        {
            w.WriteShort(2047);
            w.WriteLong(131072);
            w.WriteShort(heartbeat);
        });
        await SendMethodAsync(0, Method.ConnectionOpen, w =>
        {
            w.WriteShortStr("/");
            w.WriteShortStr("");
            w.WriteOctet(0);
        });
        Assert.Equal(Method.ConnectionOpenOk, (await ReadMethodAsync()).MethodId);
    }

    /// <summary>Waits until the server closes the TCP connection; returns how long that took.</summary>
    public async Task<TimeSpan> WaitForServerDisconnectAsync(TimeSpan timeout)
    {
        var stopwatch = Stopwatch.StartNew();
        using var cts = new CancellationTokenSource(timeout);
        var buffer = new byte[4096];
        try
        {
            while (await _stream.ReadAsync(buffer, cts.Token) > 0)
            {
            }
        }
        catch (IOException)
        {
            // Reset by peer also counts as disconnected.
        }

        return stopwatch.Elapsed;
    }

    public ValueTask DisposeAsync()
    {
        _tcp.Dispose();
        return ValueTask.CompletedTask;
    }
}
