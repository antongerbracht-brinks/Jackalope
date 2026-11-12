using System.Collections.Concurrent;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using static Jackalope.Tests.TestHelpers;

namespace Jackalope.Tests;

/// <summary>Stage 5: channel multiplexing, isolation between channels, and cross-channel/connection semantics.</summary>
public class MultiChannelTests
{
    [Fact]
    public async Task Many_channels_on_one_connection_publish_and_consume_concurrently()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);

        const int channels = 16, messagesPerChannel = 200;
        var payload = new byte[150_000]; // > frame-max, so content spans several body frames per message
        new Random(7).NextBytes(payload);

        await Parallel.ForEachAsync(Enumerable.Range(0, channels), async (i, _) =>
        {
            await using var channel = await connection.CreateChannelAsync();
            string queue = $"q{i}";
            await channel.QueueDeclareAsync(queue, false, false, false);
            var consumer = await TestConsumer.StartAsync(channel, queue);

            for (int m = 0; m < messagesPerChannel; m++)
            {
                var props = new BasicProperties { MessageId = $"{i}:{m}" };
                await channel.BasicPublishAsync("", queue, false, props, m % 20 == 0 ? payload : Utf8($"{i}:{m}"));
            }

            var received = await consumer.TakeAsync(messagesPerChannel, TimeSpan.FromSeconds(30));
            for (int m = 0; m < messagesPerChannel; m++)
            {
                Assert.Equal($"{i}:{m}", received[m].Properties.MessageId);
                Assert.Equal((ulong)(m + 1), received[m].DeliveryTag); // delivery tags are per channel
                if (m % 20 == 0)
                {
                    Assert.Equal(payload, received[m].Body);
                }
            }

            await channel.BasicAckAsync(received[^1].DeliveryTag, multiple: true);
        });

        await EventuallyAsync(() => broker.GetQueues().All(q => q.MessageCount == 0), "all queues drained");
    }

    [Fact]
    public async Task An_error_on_one_channel_does_not_affect_the_others()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var healthy = await connection.CreateChannelAsync();
        var doomed = await connection.CreateChannelAsync();

        await healthy.QueueDeclareAsync("q", false, false, false);
        var consumer = await TestConsumer.StartAsync(healthy, "q", autoAck: true);

        await Assert.ThrowsAsync<OperationInterruptedException>(() => doomed.QueueBindAsync("q", "no-such-exchange", "k"));
        Assert.False(doomed.IsOpen);
        Assert.Equal(404, doomed.CloseReason!.ReplyCode);

        Assert.True(healthy.IsOpen);
        await healthy.PublishAsync("", "q", "unaffected");
        Assert.Equal("unaffected", (await consumer.NextAsync()).Text);
    }

    [Fact]
    public async Task Consumers_on_different_channels_share_a_queue_round_robin()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var a = await connection.CreateChannelAsync();
        await using var b = await connection.CreateChannelAsync();
        await a.QueueDeclareAsync("shared", false, false, false);

        var consumerA = await TestConsumer.StartAsync(a, "shared", autoAck: true);
        var consumerB = await TestConsumer.StartAsync(b, "shared", autoAck: true);
        for (int i = 0; i < 10; i++)
        {
            await a.PublishAsync("", "shared", i.ToString());
        }

        var fromA = await consumerA.TakeAsync(5);
        var fromB = await consumerB.TakeAsync(5);
        Assert.Equal(["0", "2", "4", "6", "8"], fromA.Select(m => m.Text));
        Assert.Equal(["1", "3", "5", "7", "9"], fromB.Select(m => m.Text));
    }

    [Fact]
    public async Task Closing_a_channel_requeues_its_unacked_messages_to_other_consumers()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        var first = await connection.CreateChannelAsync();
        await using var second = await connection.CreateChannelAsync();
        await first.QueueDeclareAsync("q", false, false, false);
        await first.BasicQosAsync(0, 1, false);
        await first.PublishAsync("", "q", "work item");

        var firstConsumer = await TestConsumer.StartAsync(first, "q");
        Assert.False((await firstConsumer.NextAsync()).Redelivered);

        var secondConsumer = await TestConsumer.StartAsync(second, "q");
        await secondConsumer.AssertNothingAsync(); // still held unacked by the first channel

        await first.CloseAsync();
        var redelivered = await secondConsumer.NextAsync();
        Assert.Equal("work item", redelivered.Text);
        Assert.True(redelivered.Redelivered);
    }

    [Fact]
    public async Task Closing_a_connection_requeues_unacked_messages_for_other_connections()
    {
        await using var broker = await StartBrokerAsync();
        await using var survivor = await ConnectAsync(broker);
        await using var survivorChannel = await survivor.CreateChannelAsync();
        await survivorChannel.QueueDeclareAsync("q", false, false, false);
        await survivorChannel.PublishAsync("", "q", "in flight");

        var crashing = await ConnectAsync(broker);
        var crashingChannel = await crashing.CreateChannelAsync();
        var crashingConsumer = await TestConsumer.StartAsync(crashingChannel, "q");
        await crashingConsumer.NextAsync();
        await crashing.CloseAsync();

        var consumer = await TestConsumer.StartAsync(survivorChannel, "q");
        var message = await consumer.NextAsync();
        Assert.Equal("in flight", message.Text);
        Assert.True(message.Redelivered);
    }

    [Fact]
    public async Task Cancelled_consumer_stops_receiving_but_can_still_ack_outstanding_deliveries()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();
        await channel.QueueDeclareAsync("q", false, false, false);
        await channel.PublishAsync("", "q", "first");

        var consumer = await TestConsumer.StartAsync(channel, "q");
        var first = await consumer.NextAsync();
        await channel.BasicCancelAsync(consumer.Tag);

        await channel.PublishAsync("", "q", "second");
        await consumer.AssertNothingAsync();
        await channel.BasicAckAsync(first.DeliveryTag, false);
        Assert.True(channel.IsOpen);
        await EventuallyAsync(() => broker.GetQueue("q")!.MessageCount == 1, "second message waits in the queue");
    }

    [Fact]
    public async Task Exclusive_queue_is_locked_to_its_connection_and_deleted_when_it_closes()
    {
        await using var broker = await StartBrokerAsync();
        var owner = await ConnectAsync(broker);
        await using var ownerChannel = await owner.CreateChannelAsync();
        await ownerChannel.QueueDeclareAsync("mine", durable: false, exclusive: true, autoDelete: false);

        await using var other = await ConnectAsync(broker);
        var otherChannel = await other.CreateChannelAsync();
        var ex = await Assert.ThrowsAsync<OperationInterruptedException>(() => TestConsumer.StartAsync(otherChannel, "mine"));
        Assert.Equal(405, ex.ShutdownReason!.ReplyCode);

        await owner.CloseAsync();
        await EventuallyAsync(() => broker.GetQueue("mine") is null, "exclusive queue deleted with its connection");
    }

    [Fact]
    public async Task Auto_delete_queue_is_deleted_when_its_last_consumer_cancels()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();
        await channel.QueueDeclareAsync("temp", durable: false, exclusive: false, autoDelete: true);
        Assert.NotNull(broker.GetQueue("temp")); // no consumer yet: survives

        var consumer = await TestConsumer.StartAsync(channel, "temp");
        await channel.BasicCancelAsync(consumer.Tag);
        Assert.Null(broker.GetQueue("temp"));
    }

    [Fact]
    public async Task Exclusive_consumer_blocks_other_consumers()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var a = await connection.CreateChannelAsync();
        var b = await connection.CreateChannelAsync();
        await a.QueueDeclareAsync("q", false, false, false);
        await TestConsumer.StartAsync(a, "q", exclusive: true);

        var ex = await Assert.ThrowsAsync<OperationInterruptedException>(() => TestConsumer.StartAsync(b, "q"));
        Assert.Equal(403, ex.ShutdownReason!.ReplyCode);
    }

    [Fact]
    public async Task Publisher_confirms_complete_each_publish()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true));
        await channel.QueueDeclareAsync("confirmed", false, false, false);

        for (int i = 0; i < 50; i++)
        {
            // With tracking enabled, this await only completes once the broker's basic.ack arrives.
            await channel.PublishAsync("", "confirmed", $"m{i}");
        }

        Assert.Equal(50, broker.GetQueue("confirmed")!.MessageCount);
        Assert.Equal(51ul, await channel.GetNextPublishSequenceNumberAsync());
    }

    [Fact]
    public async Task Publisher_confirms_report_mandatory_returns()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true));

        var ex = await Assert.ThrowsAsync<PublishReturnException>(
            () => channel.PublishAsync("", "no-such-queue", "x", mandatory: true).AsTask());
        Assert.Equal(312, ex.ReplyCode);
    }

    [Fact]
    public async Task Many_connections_each_with_several_channels_work_in_parallel()
    {
        await using var broker = await StartBrokerAsync();
        var errors = new ConcurrentBag<Exception>();

        await Parallel.ForEachAsync(Enumerable.Range(0, 8), async (c, _) =>
        {
            try
            {
                await using var connection = await ConnectAsync(broker);
                await Task.WhenAll(Enumerable.Range(0, 4).Select(async ch =>
                {
                    await using var channel = await connection.CreateChannelAsync();
                    var queue = (await channel.QueueDeclareAsync()).QueueName;
                    await channel.QueueBindAsync(queue, "amq.topic", $"conn.{c}.chan.{ch}");
                    var consumer = await TestConsumer.StartAsync(channel, queue, autoAck: true);
                    for (int m = 0; m < 25; m++)
                    {
                        await channel.PublishAsync("amq.topic", $"conn.{c}.chan.{ch}", $"{c}/{ch}/{m}");
                    }

                    var got = await consumer.TakeAsync(25, TimeSpan.FromSeconds(15));
                    Assert.Equal(Enumerable.Range(0, 25).Select(m => $"{c}/{ch}/{m}"), got.Select(r => r.Text));
                }));
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }
        });

        Assert.Empty(errors);
    }
}
