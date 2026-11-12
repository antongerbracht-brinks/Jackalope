using Jackalope.Broker;
using RabbitMQ.Client;
using static Jackalope.Tests.TestHelpers;

namespace Jackalope.Tests;

/// <summary>Stage 2: topic wildcard routing and fanout broadcast.</summary>
public class TopicAndFanoutTests
{
    [Theory]
    [InlineData("a.b.c", "a.b.c", true)]
    [InlineData("a.b.c", "a.b", false)]
    [InlineData("a.*.c", "a.b.c", true)]
    [InlineData("a.*.c", "a.c", false)]
    [InlineData("a.*.c", "a.b.b.c", false)]
    [InlineData("*", "a", true)]
    [InlineData("*", "a.b", false)]
    [InlineData("*", "", false)]
    [InlineData("#", "", true)]
    [InlineData("#", "a", true)]
    [InlineData("#", "a.b.c", true)]
    [InlineData("a.#", "a", true)]
    [InlineData("a.#", "a.b.c", true)]
    [InlineData("a.#", "b.a", false)]
    [InlineData("#.c", "c", true)]
    [InlineData("#.c", "a.b.c", true)]
    [InlineData("#.c", "a.b.c.d", false)]
    [InlineData("a.#.c", "a.c", true)]
    [InlineData("a.#.c", "a.b.x.c", true)]
    [InlineData("a.#.c", "a.b.x.d", false)]
    [InlineData("#.#", "a.b", true)]
    [InlineData("*.#", "", false)]
    [InlineData("*.#", "a", true)]
    [InlineData("#.*", "a.b.c", true)]
    [InlineData("*.*.#.z", "a.b.z", true)]
    [InlineData("*.*.#.z", "a.z", false)]
    [InlineData("a.*", "a.", true)] // "a." is two words: "a" and ""
    [InlineData("a..b", "a..b", true)]
    [InlineData("stock.usd.*", "stock.usd.nyse", true)]
    [InlineData("stock.usd.*", "stock.eur.nyse", false)]
    [InlineData("A.b", "a.b", false)]
    public void Topic_pattern_matching(string pattern, string routingKey, bool expected) =>
        Assert.Equal(expected, TopicMatcher.IsMatch(pattern, routingKey));

    [Fact]
    public async Task Topic_exchange_routes_by_wildcards_end_to_end()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync("events", ExchangeType.Topic);
        await channel.QueueDeclareAsync("all", false, false, false);
        await channel.QueueDeclareAsync("orders-any-region", false, false, false);
        await channel.QueueDeclareAsync("created-anything", false, false, false);
        await channel.QueueDeclareAsync("exact", false, false, false);
        await channel.QueueBindAsync("all", "events", "#");
        await channel.QueueBindAsync("orders-any-region", "events", "orders.*.created");
        await channel.QueueBindAsync("created-anything", "events", "#.created");
        await channel.QueueBindAsync("exact", "events", "orders.eu.created");

        await channel.PublishAsync("events", "orders.eu.created", "1");
        await channel.PublishAsync("events", "orders.us.created", "2");
        await channel.PublishAsync("events", "orders.eu.shipped", "3");
        await channel.PublishAsync("events", "customers.created", "4");
        await channel.PublishAsync("events", "orders.eu.west.created", "5");

        async Task<string[]> Drain(string queue, int expected)
        {
            var consumer = await TestConsumer.StartAsync(channel, queue, autoAck: true);
            var messages = await consumer.TakeAsync(expected);
            await consumer.AssertNothingAsync(TimeSpan.FromMilliseconds(150));
            return messages.Select(m => m.Text).ToArray();
        }

        Assert.Equal(["1", "2", "3", "4", "5"], await Drain("all", 5));
        Assert.Equal(["1", "2"], await Drain("orders-any-region", 2));
        Assert.Equal(["1", "2", "4", "5"], await Drain("created-anything", 4));
        Assert.Equal(["1"], await Drain("exact", 1));
    }

    [Fact]
    public async Task Message_matching_several_bindings_of_one_queue_is_delivered_once()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync("t", ExchangeType.Topic);
        await channel.QueueDeclareAsync("q", false, false, false);
        await channel.QueueBindAsync("q", "t", "a.*");
        await channel.QueueBindAsync("q", "t", "#");
        await channel.QueueBindAsync("q", "t", "a.b");

        await channel.PublishAsync("t", "a.b", "once");
        var consumer = await TestConsumer.StartAsync(channel, "q", autoAck: true);
        Assert.Equal("once", (await consumer.NextAsync()).Text);
        await consumer.AssertNothingAsync();
    }

    [Fact]
    public async Task Fanout_exchange_broadcasts_to_every_bound_queue_ignoring_routing_key()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync("broadcast", ExchangeType.Fanout);
        var consumers = new List<TestConsumer>();
        for (int i = 0; i < 3; i++)
        {
            var queue = (await channel.QueueDeclareAsync()).QueueName;
            await channel.QueueBindAsync(queue, "broadcast", routingKey: i == 0 ? "" : $"ignored-{i}");
            consumers.Add(await TestConsumer.StartAsync(channel, queue, autoAck: true));
        }

        await channel.PublishAsync("broadcast", "whatever", "hello all");

        foreach (var consumer in consumers)
        {
            var message = await consumer.NextAsync();
            Assert.Equal("hello all", message.Text);
            Assert.Equal("broadcast", message.Exchange);
            Assert.Equal("whatever", message.RoutingKey);
        }
    }

    [Fact]
    public async Task Built_in_amq_topic_and_amq_fanout_exchanges_exist()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclarePassiveAsync("amq.topic");
        await channel.ExchangeDeclarePassiveAsync("amq.fanout");
        await channel.ExchangeDeclarePassiveAsync("amq.direct");

        var q = (await channel.QueueDeclareAsync()).QueueName;
        await channel.QueueBindAsync(q, "amq.topic", "x.#");
        await channel.PublishAsync("amq.topic", "x.y.z", "via amq.topic");
        var consumer = await TestConsumer.StartAsync(channel, q, autoAck: true);
        Assert.Equal("via amq.topic", (await consumer.NextAsync()).Text);
    }

    [Fact]
    public async Task Unbind_stops_routing_to_the_queue()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync("t", ExchangeType.Topic);
        await channel.QueueDeclareAsync("q", false, false, false);
        await channel.QueueBindAsync("q", "t", "a.#");
        var consumer = await TestConsumer.StartAsync(channel, "q", autoAck: true);

        await channel.PublishAsync("t", "a.1", "before");
        Assert.Equal("before", (await consumer.NextAsync()).Text);

        await channel.QueueUnbindAsync("q", "t", "a.#");
        Assert.Empty(broker.GetExchanges().Single(e => e.Name == "t").Bindings);
        await channel.PublishAsync("t", "a.2", "after");
        await consumer.AssertNothingAsync();

        // Unbinding something that is not bound is a no-op, as in RabbitMQ.
        await channel.QueueUnbindAsync("q", "t", "never-bound");
        Assert.True(channel.IsOpen);
    }

    [Fact]
    public async Task Mandatory_unroutable_message_is_returned()
    {
        await using var broker = await StartBrokerAsync();
        await using var connection = await ConnectAsync(broker);
        await using var channel = await connection.CreateChannelAsync();
        await channel.ExchangeDeclareAsync("t", ExchangeType.Topic);

        var returned = new TaskCompletionSource<(ushort Code, string RoutingKey, string Body)>();
        channel.BasicReturnAsync += (_, ea) =>
        {
            returned.TrySetResult((ea.ReplyCode, ea.RoutingKey, System.Text.Encoding.UTF8.GetString(ea.Body.Span)));
            return Task.CompletedTask;
        };

        await channel.PublishAsync("t", "nobody.listens", "lost", mandatory: true);
        var result = await returned.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal((312, "nobody.listens", "lost"), result);
    }
}
