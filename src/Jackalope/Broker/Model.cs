using Jackalope.Transport;

namespace Jackalope.Broker;

/// <summary>An immutable published message. The content header is kept as the raw wire payload and forwarded verbatim.</summary>
internal sealed class Message(string exchange, string routingKey, byte[] headerPayload, byte[] body)
{
    public string Exchange { get; } = exchange;
    public string RoutingKey { get; } = routingKey;
    public byte[] HeaderPayload { get; } = headerPayload;
    public byte[] Body { get; } = body;
}

/// <summary>A message sitting in one queue. The same <see cref="Message"/> may be referenced by several queues.</summary>
internal sealed class QueueEntry(Message message, bool redelivered)
{
    public Message Message { get; } = message;
    public bool Redelivered { get; } = redelivered;
}

internal enum ExchangeKind
{
    Direct,
    Fanout,
    Topic,
}

internal sealed record Binding(BrokerQueue Queue, string RoutingKey);

internal sealed class BrokerExchange(string name, ExchangeKind kind, string typeName, bool durable, bool autoDelete, bool isInternal, IReadOnlyDictionary<string, object?> arguments)
{
    public string Name { get; } = name;
    public ExchangeKind Kind { get; } = kind;
    public string TypeName { get; } = typeName;
    public bool Durable { get; } = durable;
    public bool AutoDelete { get; } = autoDelete;
    public bool Internal { get; } = isInternal;
    public IReadOnlyDictionary<string, object?> Arguments { get; } = arguments;
    public List<Binding> Bindings { get; } = [];

    /// <summary>Returns the distinct queues a message with <paramref name="routingKey"/> routes to.</summary>
    public IEnumerable<BrokerQueue> Route(string routingKey)
    {
        var seen = new HashSet<BrokerQueue>();
        foreach (var binding in Bindings)
        {
            bool match = Kind switch
            {
                ExchangeKind.Direct => binding.RoutingKey == routingKey,
                _ => false,
            };

            if (match && seen.Add(binding.Queue))
            {
                yield return binding.Queue;
            }
        }
    }
}

internal sealed class BrokerQueue(string name, bool durable, bool exclusive, bool autoDelete, IReadOnlyDictionary<string, object?> arguments, object? owner)
{
    private int _nextConsumer;

    public string Name { get; } = name;
    public bool Durable { get; } = durable;
    public bool Exclusive { get; } = exclusive;
    public bool AutoDelete { get; } = autoDelete;
    public IReadOnlyDictionary<string, object?> Arguments { get; } = arguments;

    /// <summary>The owning connection for exclusive queues; null otherwise.</summary>
    public object? Owner { get; } = owner;

    public LinkedList<QueueEntry> Ready { get; } = new();
    public List<Consumer> Consumers { get; } = [];
    public bool HadConsumer { get; set; }
    public bool IsDeleted { get; set; }

    /// <summary>Pushes ready messages to consumers that have capacity, round-robin. Caller holds the broker lock.</summary>
    public void Dispatch()
    {
        while (!IsDeleted && Ready.First is { } node)
        {
            var consumer = NextConsumerWithCapacity();
            if (consumer is null)
            {
                return;
            }

            Ready.RemoveFirst();
            consumer.Channel.Deliver(consumer, node.Value);
        }
    }

    private Consumer? NextConsumerWithCapacity()
    {
        int count = Consumers.Count;
        for (int i = 0; i < count; i++)
        {
            int index = (_nextConsumer + i) % count;
            var candidate = Consumers[index];
            if (candidate.CanAccept)
            {
                _nextConsumer = (index + 1) % count;
                return candidate;
            }
        }

        return null;
    }
}

internal sealed class Consumer(string tag, ServerChannel channel, BrokerQueue queue, bool noAck, bool exclusive, ushort prefetchCount)
{
    public string Tag { get; } = tag;
    public ServerChannel Channel { get; } = channel;
    public BrokerQueue Queue { get; } = queue;
    public bool NoAck { get; } = noAck;
    public bool Exclusive { get; } = exclusive;
    public ushort PrefetchCount { get; } = prefetchCount;
    public int Unacked { get; set; }

    public bool CanAccept =>
        !Channel.IsClosing &&
        (NoAck || ((PrefetchCount == 0 || Unacked < PrefetchCount) && Channel.HasChannelPrefetchCapacity));
}

internal sealed record UnackedDelivery(ulong DeliveryTag, Consumer Consumer, BrokerQueue Queue, Message Message);
