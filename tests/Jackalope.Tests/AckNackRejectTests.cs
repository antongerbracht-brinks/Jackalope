using RabbitMQ.Client;
using static Jackalope.Tests.TestHelpers;

namespace Jackalope.Tests;

/// <summary>Stage 4: negative acknowledgements, requeue semantics and prefetch.</summary>
public class AckNackRejectTests
{
    private static async Task<(JackalopeBroker Broker, IConnection Connection, IChannel Channel)> SetupAsync(params string[] messages)
    {
        var broker = await StartBrokerAsync();
        var connection = await ConnectAsync(broker);
        var channel = await connection.CreateChannelAsync();
        await channel.QueueDeclareAsync("q", false, false, false);
        foreach (string message in messages)
        {
            await channel.PublishAsync("", "q", message);
        }

        return (broker, connection, channel);
    }

    [Fact]
    public async Task Nack_with_requeue_redelivers_with_redelivered_flag()
    {
        var (broker, connection, channel) = await SetupAsync("retry me");
        await using var _ = broker;
        await using var __ = connection;

        var consumer = await TestConsumer.StartAsync(channel, "q");
        var first = await consumer.NextAsync();
        Assert.False(first.Redelivered);

        await channel.BasicNackAsync(first.DeliveryTag, multiple: false, requeue: true);
        var second = await consumer.NextAsync();
        Assert.Equal("retry me", second.Text);
        Assert.True(second.Redelivered);
        Assert.NotEqual(first.DeliveryTag, second.DeliveryTag);

        await channel.BasicAckAsync(second.DeliveryTag, false);
        await consumer.AssertNothingAsync();
        Assert.Equal(0, broker.GetQueue("q")!.MessageCount);
    }

    [Fact]
    public async Task Nack_without_requeue_discards_the_message()
    {
        var (broker, connection, channel) = await SetupAsync("poison");
        await using var _ = broker;
        await using var __ = connection;

        var consumer = await TestConsumer.StartAsync(channel, "q");
        var message = await consumer.NextAsync();
        await channel.BasicNackAsync(message.DeliveryTag, multiple: false, requeue: false);

        await consumer.AssertNothingAsync();
        await channel.BasicCancelAsync(consumer.Tag);
        Assert.Equal(0u, (await channel.QueueDeclarePassiveAsync("q")).MessageCount);
    }

    [Fact]
    public async Task Reject_with_and_without_requeue()
    {
        var (broker, connection, channel) = await SetupAsync("a", "b");
        await using var _ = broker;
        await using var __ = connection;

        var consumer = await TestConsumer.StartAsync(channel, "q");
        var a = await consumer.NextAsync();
        var b = await consumer.NextAsync();

        await channel.BasicRejectAsync(a.DeliveryTag, requeue: true);
        await channel.BasicRejectAsync(b.DeliveryTag, requeue: false);

        var again = await consumer.NextAsync();
        Assert.Equal("a", again.Text);
        Assert.True(again.Redelivered);
        await consumer.AssertNothingAsync();
    }

    [Fact]
    public async Task Multiple_nack_requeues_all_outstanding_up_to_tag_in_original_order()
    {
        var (broker, connection, channel) = await SetupAsync("1", "2", "3", "4");
        await using var _ = broker;
        await using var __ = connection;

        var consumer = await TestConsumer.StartAsync(channel, "q");
        var firstPass = await consumer.TakeAsync(4);

        // Nack 1..3 in one go; 4 stays outstanding.
        await channel.BasicNackAsync(firstPass[2].DeliveryTag, multiple: true, requeue: true);
        var secondPass = await consumer.TakeAsync(3);
        Assert.Equal(["1", "2", "3"], secondPass.Select(m => m.Text));
        Assert.All(secondPass, m => Assert.True(m.Redelivered));

        await channel.BasicAckAsync(0, multiple: true); // ack everything outstanding
        await consumer.AssertNothingAsync();
        await EventuallyAsync(() => broker.GetQueue("q")!.MessageCount == 0, "queue drained");
    }

    [Fact]
    public async Task Multiple_ack_acknowledges_everything_up_to_and_including_tag()
    {
        var (broker, connection, channel) = await SetupAsync("1", "2", "3");
        await using var _ = broker;
        await using var __ = connection;

        var consumer = await TestConsumer.StartAsync(channel, "q");
        var messages = await consumer.TakeAsync(3);
        await channel.BasicAckAsync(messages[1].DeliveryTag, multiple: true);

        // Closing the channel requeues only what is still unacked: message 3.
        await channel.CloseAsync();
        await EventuallyAsync(() => broker.GetQueue("q")!.MessageCount == 1, "only the unacked message is requeued");
    }

    [Fact]
    public async Task Prefetch_count_limits_unacked_deliveries_per_consumer()
    {
        var (broker, connection, channel) = await SetupAsync("1", "2", "3", "4", "5");
        await using var _ = broker;
        await using var __ = connection;

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 2, global: false);
        var consumer = await TestConsumer.StartAsync(channel, "q");

        var batch = await consumer.TakeAsync(2);
        await consumer.AssertNothingAsync();
        Assert.Equal(3, broker.GetQueue("q")!.MessageCount);

        await channel.BasicAckAsync(batch[0].DeliveryTag, false);
        Assert.Equal("3", (await consumer.NextAsync()).Text);
        await consumer.AssertNothingAsync();

        await channel.BasicAckAsync(0, multiple: true);
        Assert.Equal(["4", "5"], (await consumer.TakeAsync(2)).Select(m => m.Text));
    }

    [Fact]
    public async Task Global_prefetch_limits_unacked_deliveries_across_the_channel()
    {
        var (broker, connection, channel) = await SetupAsync();
        await using var _ = broker;
        await using var __ = connection;
        await channel.QueueDeclareAsync("q2", false, false, false);
        for (int i = 0; i < 3; i++)
        {
            await channel.PublishAsync("", "q", $"q{i}");
            await channel.PublishAsync("", "q2", $"q2-{i}");
        }

        await channel.BasicQosAsync(0, 3, global: true);
        var c1 = await TestConsumer.StartAsync(channel, "q");
        var c2 = await TestConsumer.StartAsync(channel, "q2");

        await EventuallyAsync(() => c1.BufferedCount + c2.BufferedCount == 3, "three deliveries across the channel");
        await Task.Delay(200);
        Assert.Equal(3, c1.BufferedCount + c2.BufferedCount);
    }

    [Fact]
    public async Task Auto_ack_consumer_is_not_limited_by_prefetch_and_needs_no_acks()
    {
        var (broker, connection, channel) = await SetupAsync("1", "2", "3");
        await using var _ = broker;
        await using var __ = connection;
        await channel.BasicQosAsync(0, 1, false);

        var consumer = await TestConsumer.StartAsync(channel, "q", autoAck: true);
        Assert.Equal(["1", "2", "3"], (await consumer.TakeAsync(3)).Select(m => m.Text));
        await channel.CloseAsync();
        Assert.Equal(0, broker.GetQueue("q")!.MessageCount);
    }

    [Fact]
    public async Task Async_consumer_acking_inside_the_handler_processes_a_stream()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();
        await channel.QueueDeclareAsync("stream", false, false, false);
        await channel.BasicQosAsync(0, 10, false);

        const int count = 500;
        var seen = new List<int>();
        var done = new TaskCompletionSource();
        var consumer = new RabbitMQ.Client.Events.AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            int value = int.Parse(System.Text.Encoding.UTF8.GetString(ea.Body.Span));
            // Every 10th message fails once and is retried via nack/requeue.
            if (value % 10 == 0 && !ea.Redelivered)
            {
                await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: true);
                return;
            }

            lock (seen)
            {
                seen.Add(value);
                if (seen.Count == count)
                {
                    done.TrySetResult();
                }
            }

            await channel.BasicAckAsync(ea.DeliveryTag, false);
        };
        await channel.BasicConsumeAsync("stream", autoAck: false, consumer);

        for (int i = 0; i < count; i++)
        {
            await channel.PublishAsync("", "stream", i.ToString());
        }

        await done.Task.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.Equal(Enumerable.Range(0, count), seen.Order());
        await EventuallyAsync(() => broker.GetQueue("stream")!.MessageCount == 0, "all settled");
    }
}
