using System.Text;
using System.Threading.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Jackalope.Tests;

/// <summary>A delivery copied out of the client's event args (the body buffer is only valid inside the handler).</summary>
public sealed record Received(
    string ConsumerTag,
    ulong DeliveryTag,
    bool Redelivered,
    string Exchange,
    string RoutingKey,
    IReadOnlyBasicProperties Properties,
    byte[] Body)
{
    public string Text => Encoding.UTF8.GetString(Body);
}

/// <summary>Wraps an <see cref="AsyncEventingBasicConsumer"/> and buffers deliveries for assertions.</summary>
public sealed class TestConsumer
{
    private readonly Channel<Received> _deliveries = System.Threading.Channels.Channel.CreateUnbounded<Received>();

    public TestConsumer(IChannel channel)
    {
        Channel = channel;
        Consumer = new AsyncEventingBasicConsumer(channel);
        Consumer.ReceivedAsync += (_, ea) =>
        {
            _deliveries.Writer.TryWrite(new Received(ea.ConsumerTag, ea.DeliveryTag, ea.Redelivered, ea.Exchange,
                ea.RoutingKey, ea.BasicProperties, ea.Body.ToArray()));
            return Task.CompletedTask;
        };
    }

    public IChannel Channel { get; }

    public AsyncEventingBasicConsumer Consumer { get; }

    public string Tag { get; private set; } = "";

    public static async Task<TestConsumer> StartAsync(IChannel channel, string queue, bool autoAck = false, bool exclusive = false)
    {
        var consumer = new TestConsumer(channel);
        consumer.Tag = await channel.BasicConsumeAsync(queue, autoAck, consumerTag: "", noLocal: false, exclusive: exclusive,
            arguments: null, consumer: consumer.Consumer);
        return consumer;
    }

    public async Task<Received> NextAsync(TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(5));
        try
        {
            return await _deliveries.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException("No message was delivered within the timeout.");
        }
    }

    public async Task<List<Received>> TakeAsync(int count, TimeSpan? timeout = null)
    {
        var list = new List<Received>();
        for (int i = 0; i < count; i++)
        {
            list.Add(await NextAsync(timeout));
        }

        return list;
    }

    /// <summary>Asserts nothing arrives within <paramref name="window"/>.</summary>
    public async Task AssertNothingAsync(TimeSpan? window = null)
    {
        await Task.Delay(window ?? TimeSpan.FromMilliseconds(300));
        Assert.False(_deliveries.Reader.TryRead(out var unexpected),
            unexpected is null ? "" : $"Unexpected delivery: {unexpected.RoutingKey} '{unexpected.Text}'");
    }

    public int BufferedCount => _deliveries.Reader.Count;
}

public static class TestHelpers
{
    public static async Task<JackalopeBroker> StartBrokerAsync(JackalopeBrokerOptions? options = null)
    {
        var broker = new JackalopeBroker(options ?? new JackalopeBrokerOptions());
        await broker.StartAsync();
        return broker;
    }

    public static ConnectionFactory Factory(IJackalopeInstance broker, Action<ConnectionFactory>? configure = null)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(broker.GetConnectionString()),
            AutomaticRecoveryEnabled = false,
            TopologyRecoveryEnabled = false,
            RequestedConnectionTimeout = TimeSpan.FromSeconds(10),
        };
        configure?.Invoke(factory);
        return factory;
    }

    public static Task<IConnection> ConnectAsync(IJackalopeInstance broker, Action<ConnectionFactory>? configure = null) =>
        Factory(broker, configure).CreateConnectionAsync();

    public static byte[] Utf8(string text) => Encoding.UTF8.GetBytes(text);

    public static ValueTask PublishAsync(this IChannel channel, string exchange, string routingKey, string body,
        BasicProperties? properties = null, bool mandatory = false) =>
        channel.BasicPublishAsync(exchange, routingKey, mandatory, properties ?? new BasicProperties(), Utf8(body));

    /// <summary>Polls until <paramref name="condition"/> holds (server-side state changes are asynchronous to the client).</summary>
    public static async Task EventuallyAsync(Func<bool> condition, string because, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail($"Condition not met within timeout: {because}");
            }

            await Task.Delay(20);
        }
    }
}
