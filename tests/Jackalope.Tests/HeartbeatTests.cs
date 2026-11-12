using Jackalope.Protocol;
using static Jackalope.Tests.TestHelpers;

namespace Jackalope.Tests;

/// <summary>Stage 3: heartbeat negotiation, sending, and timeout detection.</summary>
public class HeartbeatTests
{
    [Fact]
    public async Task Real_client_with_short_heartbeat_stays_connected_while_idle()
    {
        // RabbitMQ.Client itself treats missing server heartbeats as a dead connection, so surviving a long idle
        // period with a 1s heartbeat proves the broker is sending them.
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker, f => f.RequestedHeartbeat = TimeSpan.FromSeconds(1));
        Assert.Equal(TimeSpan.FromSeconds(1), connection.Heartbeat);

        await Task.Delay(TimeSpan.FromSeconds(5));

        Assert.True(connection.IsOpen, $"connection closed: {connection.CloseReason}");
        await using var channel = await connection.CreateChannelAsync();
        await channel.QueueDeclareAsync("q", false, false, false);
        await channel.PublishAsync("", "q", "still alive");
        var consumer = await TestConsumer.StartAsync(channel, "q", autoAck: true);
        Assert.Equal("still alive", (await consumer.NextAsync()).Text);
    }

    [Fact]
    public async Task Server_proposed_heartbeat_is_negotiated_down_to_by_the_client()
    {
        await using var broker = await StartBrokerAsync(new JackalopeBrokerOptions { HeartbeatSeconds = 1 });
        await using var connection = await ConnectAsync(broker); // client default requests 60s
        Assert.Equal(TimeSpan.FromSeconds(1), connection.Heartbeat);

        await Task.Delay(TimeSpan.FromSeconds(4));
        Assert.True(connection.IsOpen, $"connection closed: {connection.CloseReason}");
    }

    [Fact]
    public async Task Server_sends_heartbeat_frames_at_half_the_interval()
    {
        await using var broker = await StartBrokerAsync();
        await using var raw = await RawAmqpClient.ConnectAsync(broker.Port);
        await raw.HandshakeAsync(heartbeat: 2);

        // Keep ourselves alive while counting what arrives.
        int heartbeats = 0;
        var until = DateTime.UtcNow.AddSeconds(3.5);
        while (DateTime.UtcNow < until)
        {
            var frame = await raw.ReadFrameAsync(TimeSpan.FromSeconds(3));
            Assert.Equal(AmqpConstants.FrameHeartbeat, frame.Type);
            Assert.Equal(0, frame.Channel);
            Assert.Empty(frame.Payload);
            heartbeats++;
            await raw.SendAsync(AmqpConstants.HeartbeatFrame);
        }

        Assert.InRange(heartbeats, 3, 5); // one per second for ~3.5s
    }

    [Fact]
    public async Task Server_drops_a_client_that_stops_sending_heartbeats()
    {
        await using var broker = await StartBrokerAsync();
        await using var raw = await RawAmqpClient.ConnectAsync(broker.Port);
        await raw.HandshakeAsync(heartbeat: 1);
        await EventuallyAsync(() => broker.ConnectionCount == 1, "connected");

        // Say nothing: after 2 missed intervals (2s) the broker must give up on us.
        var elapsed = await raw.WaitForServerDisconnectAsync(TimeSpan.FromSeconds(10));
        Assert.InRange(elapsed.TotalSeconds, 1.5, 4.5);
        await EventuallyAsync(() => broker.ConnectionCount == 0, "server should release the connection");
    }

    [Fact]
    public async Task Client_heartbeats_keep_an_otherwise_idle_connection_alive()
    {
        await using var broker = await StartBrokerAsync();
        await using var raw = await RawAmqpClient.ConnectAsync(broker.Port);
        await raw.HandshakeAsync(heartbeat: 1);

        for (int i = 0; i < 8; i++)
        {
            await raw.SendAsync(AmqpConstants.HeartbeatFrame);
            await Task.Delay(500);
        }

        Assert.Equal(1, broker.ConnectionCount);
    }

    [Fact]
    public async Task Heartbeat_zero_disables_heartbeats()
    {
        await using var broker = await StartBrokerAsync();
        await using var raw = await RawAmqpClient.ConnectAsync(broker.Port);
        await raw.HandshakeAsync(heartbeat: 0);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => raw.ReadFrameAsync(TimeSpan.FromSeconds(2.5)));
        Assert.Equal(1, broker.ConnectionCount);
    }

    [Fact]
    public async Task Heartbeat_on_non_zero_channel_is_a_frame_error()
    {
        await using var broker = await StartBrokerAsync();
        await using var raw = await RawAmqpClient.ConnectAsync(broker.Port);
        await raw.HandshakeAsync(heartbeat: 0);

        await raw.SendAsync([AmqpConstants.FrameHeartbeat, 0, 1, 0, 0, 0, 0, AmqpConstants.FrameEnd]);
        var close = await raw.ReadMethodAsync();
        Assert.Equal(Method.ConnectionClose, close.MethodId);
        var reader = new AmqpReader(close.Payload.AsSpan(4));
        Assert.Equal(ReplyCode.FrameError, reader.ReadShort());
    }

    [Fact]
    public async Task Wrong_protocol_header_gets_the_supported_header_back()
    {
        await using var broker = await StartBrokerAsync();
        await using var raw = await RawAmqpClient.ConnectAsync(broker.Port);
        await raw.SendAsync("AMQP\0\u0001\u0001\u0000"u8.ToArray()); // AMQP 1.0

        var reply = await raw.ReadBytesAsync(8);
        Assert.Equal(AmqpConstants.ProtocolHeader, reply);
    }
}
