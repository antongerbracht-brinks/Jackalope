using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using static Jackalope.Tests.TestHelpers;

namespace Jackalope.Tests;

/// <summary>Stage 1: connection lifecycle and direct-exchange publish/consume/ack through the real RabbitMQ.Client.</summary>
public class DirectExchangeTests
{
    [Fact]
    public async Task Connects_and_disconnects_cleanly()
    {
        await using var broker = await StartBrokerAsync();
        var connection = await ConnectAsync(broker);

        Assert.True(connection.IsOpen);
        Assert.Equal(broker.Port, connection.Endpoint.Port);
        Assert.Equal("Jackalope"u8.ToArray(), (byte[])connection.ServerProperties!["product"]!);
        await EventuallyAsync(() => broker.ConnectionCount == 1, "server should see the connection");

        await connection.CloseAsync();
        Assert.False(connection.IsOpen);
        Assert.Equal(200, connection.CloseReason!.ReplyCode);
        await EventuallyAsync(() => broker.ConnectionCount == 0, "server should drop the connection");
        await connection.DisposeAsync();
    }

    [Fact]
    public async Task Accepts_any_credentials_when_none_are_configured()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker, f =>
        {
            f.UserName = "someone";
            f.Password = "anything";
        });
        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task Rejects_wrong_credentials_when_configured()
    {
        await using var broker = await StartBrokerAsync(new JackalopeBrokerOptions { Username = "app", Password = "s3cret" });

        await using (var ok = await ConnectAsync(broker))
        {
            Assert.True(ok.IsOpen);
        }

        var ex = await Assert.ThrowsAsync<BrokerUnreachableException>(() => ConnectAsync(broker, f => f.Password = "wrong"));
        Assert.IsType<AuthenticationFailureException>(ex.InnerException);
    }

    [Fact]
    public async Task Direct_exchange_round_trips_body_and_properties_then_ack_removes_message()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync("orders", ExchangeType.Direct);
        var declared = await channel.QueueDeclareAsync("orders.created", durable: true, exclusive: false, autoDelete: false);
        Assert.Equal("orders.created", declared.QueueName);
        await channel.QueueBindAsync("orders.created", "orders", "created");

        var consumer = await TestConsumer.StartAsync(channel, "orders.created");
        Assert.StartsWith("amq.ctag-", consumer.Tag);

        var properties = new BasicProperties
        {
            ContentType = "application/json",
            ContentEncoding = "utf-8",
            DeliveryMode = DeliveryModes.Persistent,
            Priority = 5,
            CorrelationId = "corr-1",
            ReplyTo = "replies",
            Expiration = "60000",
            MessageId = "msg-1",
            Timestamp = new AmqpTimestamp(1_700_000_000),
            Type = "OrderCreated",
            UserId = "guest",
            AppId = "tests",
            Headers = new Dictionary<string, object?>
            {
                ["x-tenant"] = "acme",
                ["x-attempt"] = 3,
                ["x-big"] = 1234567890123L,
                ["x-flag"] = true,
                ["x-nested"] = new Dictionary<string, object?> { ["k"] = "v" },
                ["x-list"] = new List<object?> { 1, "two" },
            },
        };
        const string payload = "{\"id\":42,\"text\":\"héllo wörld\"}";
        await channel.PublishAsync("orders", "created", payload, properties);

        var message = await consumer.NextAsync();
        Assert.Equal(payload, message.Text);
        Assert.Equal("orders", message.Exchange);
        Assert.Equal("created", message.RoutingKey);
        Assert.False(message.Redelivered);
        Assert.Equal(consumer.Tag, message.ConsumerTag);

        var p = message.Properties;
        Assert.Equal("application/json", p.ContentType);
        Assert.Equal("utf-8", p.ContentEncoding);
        Assert.Equal(DeliveryModes.Persistent, p.DeliveryMode);
        Assert.Equal(5, p.Priority);
        Assert.Equal("corr-1", p.CorrelationId);
        Assert.Equal("replies", p.ReplyTo);
        Assert.Equal("60000", p.Expiration);
        Assert.Equal("msg-1", p.MessageId);
        Assert.Equal(1_700_000_000, p.Timestamp.UnixTime);
        Assert.Equal("OrderCreated", p.Type);
        Assert.Equal("guest", p.UserId);
        Assert.Equal("tests", p.AppId);
        Assert.Equal("acme"u8.ToArray(), (byte[])p.Headers!["x-tenant"]!);
        Assert.Equal(3, p.Headers["x-attempt"]);
        Assert.Equal(1234567890123L, p.Headers["x-big"]);
        Assert.Equal(true, p.Headers["x-flag"]);
        Assert.Equal("v"u8.ToArray(), (byte[])((IDictionary<string, object?>)p.Headers["x-nested"]!)["k"]!);

        // Unacked: still held by the broker, not ready.
        Assert.Equal(0, broker.GetQueue("orders.created")!.MessageCount);

        await channel.BasicAckAsync(message.DeliveryTag, multiple: false);
        await consumer.AssertNothingAsync();

        await channel.BasicCancelAsync(consumer.Tag);
        var passive = await channel.QueueDeclarePassiveAsync("orders.created");
        Assert.Equal(0u, passive.MessageCount);
        Assert.Equal(0u, passive.ConsumerCount);
    }

    [Fact]
    public async Task Direct_exchange_routes_only_exact_routing_key_matches()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync("ex", ExchangeType.Direct);
        await channel.QueueDeclareAsync("q", false, false, false);
        await channel.QueueBindAsync("q", "ex", "blue");

        await channel.PublishAsync("ex", "red", "nope");
        await channel.PublishAsync("ex", "Blue", "nope (case-sensitive)");
        await channel.PublishAsync("ex", "blue", "yes");

        var consumer = await TestConsumer.StartAsync(channel, "q", autoAck: true);
        Assert.Equal("yes", (await consumer.NextAsync()).Text);
        await consumer.AssertNothingAsync();
    }

    [Fact]
    public async Task Default_exchange_routes_by_queue_name_and_messages_wait_for_a_consumer()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync("work", false, false, false);
        for (int i = 0; i < 3; i++)
        {
            await channel.PublishAsync("", "work", $"m{i}");
        }

        Assert.Equal(3u, (await channel.QueueDeclarePassiveAsync("work")).MessageCount);

        var consumer = await TestConsumer.StartAsync(channel, "work");
        var received = await consumer.TakeAsync(3);
        Assert.Equal(["m0", "m1", "m2"], received.Select(r => r.Text));
        Assert.Equal([1ul, 2ul, 3ul], received.Select(r => r.DeliveryTag));

        await channel.BasicAckAsync(3, multiple: true);
        await EventuallyAsync(() => broker.GetQueue("work")!.MessageCount == 0, "all acked");
    }

    [Fact]
    public async Task Queue_declare_accepts_and_ignores_broker_specific_arguments()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();

        var arguments = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum",
            ["x-message-ttl"] = 60000,
            ["x-dead-letter-exchange"] = "dlx",
            ["x-max-length"] = 1000L,
            ["x-single-active-consumer"] = true,
            ["x-custom"] = new Dictionary<string, object?> { ["a"] = 1.5 },
        };
        var ok = await channel.QueueDeclareAsync("quorum-q", durable: true, exclusive: false, autoDelete: false, arguments);
        Assert.Equal("quorum-q", ok.QueueName);

        // The arguments arrived intact (decoded by Jackalope's field-table codec).
        var info = broker.GetQueue("quorum-q")!;
        Assert.Equal("quorum"u8.ToArray(), (byte[])info.Arguments["x-queue-type"]!);
        Assert.Equal(60000, info.Arguments["x-message-ttl"]);
        Assert.Equal(1000L, info.Arguments["x-max-length"]);
        Assert.Equal(true, info.Arguments["x-single-active-consumer"]);

        // And the queue still works.
        await channel.PublishAsync("", "quorum-q", "hi");
        var consumer = await TestConsumer.StartAsync(channel, "quorum-q", autoAck: true);
        Assert.Equal("hi", (await consumer.NextAsync()).Text);
    }

    [Fact]
    public async Task Server_named_queue_is_generated_and_usable()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();

        var ok = await channel.QueueDeclareAsync(); // "", exclusive, auto-delete
        Assert.StartsWith("amq.gen-", ok.QueueName);
        await channel.QueueBindAsync(ok.QueueName, "amq.direct", "k");
        await channel.PublishAsync("amq.direct", "k", "via amq.direct");

        var consumer = await TestConsumer.StartAsync(channel, ok.QueueName, autoAck: true);
        Assert.Equal("via amq.direct", (await consumer.NextAsync()).Text);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(131_072 - 8)]
    [InlineData(131_072 - 7)]
    [InlineData(1_000_000)]
    [InlineData(5 * 1024 * 1024 + 3)]
    public async Task Bodies_of_any_size_are_split_and_reassembled_across_frames(int size)
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();
        await channel.QueueDeclareAsync("big", false, false, false);

        var body = new byte[size];
        new Random(size).NextBytes(body);
        await channel.BasicPublishAsync("", "big", body);

        var consumer = await TestConsumer.StartAsync(channel, "big", autoAck: true);
        var message = await consumer.NextAsync(TimeSpan.FromSeconds(15));
        Assert.Equal(body, message.Body);
    }

    [Fact]
    public async Task Small_negotiated_frame_max_is_respected_both_ways()
    {
        await using var broker = await StartBrokerAsync(new JackalopeBrokerOptions { FrameMax = 4096 });
        await using var connection = await ConnectAsync(broker);
        Assert.Equal(4096u, connection.FrameMax);
        await using var channel = await connection.CreateChannelAsync();
        await channel.QueueDeclareAsync("q", false, false, false);

        var body = new byte[50_000];
        new Random(1).NextBytes(body);
        await channel.BasicPublishAsync("", "q", body);

        var consumer = await TestConsumer.StartAsync(channel, "q", autoAck: true);
        Assert.Equal(body, (await consumer.NextAsync()).Body);
    }

    [Fact]
    public async Task Passive_declare_of_missing_queue_fails_with_404_and_closes_only_the_channel()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        var channel = await connection.CreateChannelAsync();

        var ex = await Assert.ThrowsAsync<OperationInterruptedException>(() => channel.QueueDeclarePassiveAsync("missing"));
        Assert.Equal(404, ex.ShutdownReason!.ReplyCode);
        Assert.Contains("no queue 'missing'", ex.ShutdownReason.ReplyText);
        Assert.False(channel.IsOpen);
        Assert.True(connection.IsOpen);

        await using var another = await connection.CreateChannelAsync();
        await another.QueueDeclareAsync("now-it-exists", false, false, false);
    }

    [Fact]
    public async Task Publishing_to_missing_exchange_closes_channel_with_404()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        var channel = await connection.CreateChannelAsync();

        await channel.PublishAsync("does-not-exist", "rk", "x");
        await EventuallyAsync(() => !channel.IsOpen, "channel should be closed by the broker");
        Assert.Equal(404, channel.CloseReason!.ReplyCode);
        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task Redeclaring_exchange_with_different_type_is_a_precondition_failure()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();
        await channel.ExchangeDeclareAsync("x", ExchangeType.Direct, durable: true);

        // Identical redeclare is fine.
        await channel.ExchangeDeclareAsync("x", ExchangeType.Direct, durable: true);

        var ex = await Assert.ThrowsAsync<OperationInterruptedException>(
            () => channel.ExchangeDeclareAsync("x", ExchangeType.Direct, durable: false));
        Assert.Equal(406, ex.ShutdownReason!.ReplyCode);
    }

    [Fact]
    public async Task Double_ack_closes_channel_with_unknown_delivery_tag()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        var channel = await connection.CreateChannelAsync();
        await channel.QueueDeclareAsync("q", false, false, false);
        await channel.PublishAsync("", "q", "x");
        var consumer = await TestConsumer.StartAsync(channel, "q");
        var message = await consumer.NextAsync();

        await channel.BasicAckAsync(message.DeliveryTag, false);
        await channel.BasicAckAsync(message.DeliveryTag, false);

        await EventuallyAsync(() => !channel.IsOpen, "channel should be closed");
        Assert.Equal(406, channel.CloseReason!.ReplyCode);
        Assert.Contains("unknown delivery tag 1", channel.CloseReason.ReplyText);
    }

    [Fact]
    public async Task Unsupported_methods_fail_loudly_with_540_not_implemented()
    {
        await using var broker = await StartBrokerAsync();
        var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();
        await channel.QueueDeclareAsync("q", false, false, false);

        await Assert.ThrowsAnyAsync<Exception>(() => channel.QueueDeleteAsync("q"));
        await EventuallyAsync(() => !connection.IsOpen, "connection should be closed");
        Assert.Equal(540, connection.CloseReason!.ReplyCode);
        Assert.Contains("queue.delete", connection.CloseReason.ReplyText);
    }

    [Fact]
    public async Task Stopping_the_broker_closes_clients_with_connection_forced()
    {
        var broker = await StartBrokerAsync();
        var connection = await ConnectAsync(broker);

        await broker.DisposeAsync();

        await EventuallyAsync(() => !connection.IsOpen, "client should observe shutdown");
        Assert.Equal(320, connection.CloseReason!.ReplyCode);
    }
}
