using System.Security.Cryptography;
using Jackalope.Protocol;
using Jackalope.Transport;

namespace Jackalope.Broker;

/// <summary>
/// All exchange/queue/binding state for one broker instance. Every member must be called while holding
/// <see cref="Sync"/>; the broker deliberately uses one coarse lock per instance because it is a test double,
/// where simplicity and determinism matter far more than throughput.
/// </summary>
internal sealed class BrokerState
{
    private static readonly IReadOnlyDictionary<string, object?> NoArguments = new Dictionary<string, object?>();

    public BrokerState(JackalopeBrokerOptions options)
    {
        Options = options;
        AddBuiltIn("", ExchangeKind.Direct, "direct");
        AddBuiltIn("amq.direct", ExchangeKind.Direct, "direct");
        AddBuiltIn("amq.fanout", ExchangeKind.Fanout, "fanout");
        AddBuiltIn("amq.topic", ExchangeKind.Topic, "topic");
    }

    public object Sync { get; } = new();

    public JackalopeBrokerOptions Options { get; }

    public Dictionary<string, BrokerExchange> Exchanges { get; } = new(StringComparer.Ordinal);

    public Dictionary<string, BrokerQueue> Queues { get; } = new(StringComparer.Ordinal);

    private void AddBuiltIn(string name, ExchangeKind kind, string typeName) =>
        Exchanges[name] = new BrokerExchange(name, kind, typeName, durable: true, autoDelete: false, isInternal: false, NoArguments);

    public static string GenerateName(string prefix) =>
        prefix + Convert.ToBase64String(RandomNumberGenerator.GetBytes(16)).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    public bool Authenticate(string username, string password) =>
        (Options.Username is null || Options.Username == username) &&
        (Options.Password is null || Options.Password == password);

    // ---- exchanges ----------------------------------------------------------------------------

    public void DeclareExchange(string name, string type, bool passive, bool durable, bool autoDelete, bool isInternal,
        Dictionary<string, object?> arguments, string vhost)
    {
        const uint method = Method.ExchangeDeclare;
        if (passive)
        {
            if (!Exchanges.ContainsKey(name))
            {
                throw new ChannelException(ReplyCode.NotFound, $"no exchange '{name}' in vhost '{vhost}'", method);
            }

            return;
        }

        if (name.Length == 0)
        {
            throw new ChannelException(ReplyCode.AccessRefused, "operation not permitted on the default exchange", method);
        }

        if (name.StartsWith("amq.", StringComparison.Ordinal))
        {
            throw new ChannelException(ReplyCode.AccessRefused,
                $"exchange name '{name}' contains reserved prefix 'amq.*'", method);
        }

        var kind = ParseExchangeType(type);

        if (Exchanges.TryGetValue(name, out var existing))
        {
            void Check(string arg, object received, object current)
            {
                if (!received.Equals(current))
                {
                    throw new ChannelException(ReplyCode.PreconditionFailed,
                        $"inequivalent arg '{arg}' for exchange '{name}' in vhost '{vhost}': received '{Format(received)}' but current is '{Format(current)}'",
                        method);
                }
            }

            Check("type", type, existing.TypeName);
            Check("durable", durable, existing.Durable);
            Check("auto_delete", autoDelete, existing.AutoDelete);
            Check("internal", isInternal, existing.Internal);
            return;
        }

        Exchanges[name] = new BrokerExchange(name, kind, type, durable, autoDelete, isInternal, arguments);
    }

    private static ExchangeKind ParseExchangeType(string type) => type switch
    {
        "direct" => ExchangeKind.Direct,
        "fanout" => ExchangeKind.Fanout,
        "topic" => ExchangeKind.Topic,
        "headers" => throw new ConnectionException(ReplyCode.NotImplemented,
            $"exchange type '{type}' is not supported by Jackalope", Method.ExchangeDeclare),
        _ => throw new ConnectionException(ReplyCode.CommandInvalid,
            $"unknown exchange type '{type}'", Method.ExchangeDeclare),
    };

    private static string Format(object value) => value is bool b ? (b ? "true" : "false") : value.ToString() ?? "";

    private BrokerExchange GetExchange(string name, string vhost, uint method) =>
        Exchanges.TryGetValue(name, out var exchange)
            ? exchange
            : throw new ChannelException(ReplyCode.NotFound, $"no exchange '{name}' in vhost '{vhost}'", method);

    public BrokerExchange? FindExchange(string name) => Exchanges.GetValueOrDefault(name);

    private void MaybeAutoDeleteExchange(BrokerExchange exchange)
    {
        if (exchange.AutoDelete && exchange.Bindings.Count == 0)
        {
            Exchanges.Remove(exchange.Name);
        }
    }

    // ---- queues -------------------------------------------------------------------------------

    public BrokerQueue DeclareQueue(string name, bool passive, bool durable, bool exclusive, bool autoDelete,
        Dictionary<string, object?> arguments, AmqpConnection connection)
    {
        const uint method = Method.QueueDeclare;
        string vhost = connection.VirtualHost;

        if (passive)
        {
            var found = GetQueue(name, vhost, method);
            CheckExclusiveAccess(found, connection, method);
            return found;
        }

        if (name.Length == 0)
        {
            name = GenerateName("amq.gen-");
        }
        else if (name.StartsWith("amq.", StringComparison.Ordinal))
        {
            throw new ChannelException(ReplyCode.AccessRefused,
                $"queue name '{name}' contains reserved prefix 'amq.*'", method);
        }

        if (Queues.TryGetValue(name, out var existing))
        {
            CheckExclusiveAccess(existing, connection, method);
            if (existing.Exclusive != exclusive)
            {
                throw ResourceLocked(name, vhost, method);
            }

            if (existing.Durable != durable)
            {
                throw new ChannelException(ReplyCode.PreconditionFailed,
                    $"inequivalent arg 'durable' for queue '{name}' in vhost '{vhost}': received '{Format(durable)}' but current is '{Format(existing.Durable)}'",
                    method);
            }

            if (existing.AutoDelete != autoDelete)
            {
                throw new ChannelException(ReplyCode.PreconditionFailed,
                    $"inequivalent arg 'auto_delete' for queue '{name}' in vhost '{vhost}': received '{Format(autoDelete)}' but current is '{Format(existing.AutoDelete)}'",
                    method);
            }

            return existing;
        }

        // Unrecognised x-arguments (x-queue-type, x-message-ttl, ...) are stored for introspection but not honoured.
        var queue = new BrokerQueue(name, durable, exclusive, autoDelete, arguments, exclusive ? connection : null);
        Queues[name] = queue;
        return queue;
    }

    public BrokerQueue GetQueue(string name, string vhost, uint method) =>
        Queues.TryGetValue(name, out var queue)
            ? queue
            : throw new ChannelException(ReplyCode.NotFound, $"no queue '{name}' in vhost '{vhost}'", method);

    public static void CheckExclusiveAccess(BrokerQueue queue, AmqpConnection connection, uint method)
    {
        if (queue.Exclusive && !ReferenceEquals(queue.Owner, connection))
        {
            throw ResourceLocked(queue.Name, connection.VirtualHost, method);
        }
    }

    private static ChannelException ResourceLocked(string name, string vhost, uint method) =>
        new(ReplyCode.ResourceLocked,
            $"cannot obtain exclusive access to locked queue '{name}' in vhost '{vhost}'. It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.",
            method);

    public void DeleteQueue(BrokerQueue queue)
    {
        if (queue.IsDeleted)
        {
            return;
        }

        queue.IsDeleted = true;
        Queues.Remove(queue.Name);
        queue.Ready.Clear();

        foreach (var exchange in Exchanges.Values.ToList())
        {
            if (exchange.Bindings.RemoveAll(b => b.Queue == queue) > 0)
            {
                MaybeAutoDeleteExchange(exchange);
            }
        }

        // Consumers on other channels learn about the deletion via a server-sent basic.cancel.
        foreach (var consumer in queue.Consumers.ToList())
        {
            consumer.Channel.CancelConsumerFromServer(consumer);
        }

        queue.Consumers.Clear();
    }

    public void DeleteExclusiveQueuesOwnedBy(AmqpConnection connection)
    {
        foreach (var queue in Queues.Values.Where(q => ReferenceEquals(q.Owner, connection)).ToList())
        {
            DeleteQueue(queue);
        }
    }

    // ---- bindings -----------------------------------------------------------------------------

    public void Bind(string queueName, string exchangeName, string routingKey, AmqpConnection connection)
    {
        const uint method = Method.QueueBind;
        string vhost = connection.VirtualHost;
        if (exchangeName.Length == 0)
        {
            throw new ChannelException(ReplyCode.AccessRefused, "operation not permitted on the default exchange", method);
        }

        var queue = GetQueue(queueName, vhost, method);
        var exchange = GetExchange(exchangeName, vhost, method);
        CheckExclusiveAccess(queue, connection, method);

        if (!exchange.Bindings.Any(b => b.Queue == queue && b.RoutingKey == routingKey))
        {
            exchange.Bindings.Add(new Binding(queue, routingKey));
        }
    }

    public void Unbind(string queueName, string exchangeName, string routingKey, AmqpConnection connection)
    {
        const uint method = Method.QueueUnbind;
        string vhost = connection.VirtualHost;
        if (exchangeName.Length == 0)
        {
            throw new ChannelException(ReplyCode.AccessRefused, "operation not permitted on the default exchange", method);
        }

        var queue = GetQueue(queueName, vhost, method);
        var exchange = GetExchange(exchangeName, vhost, method);
        CheckExclusiveAccess(queue, connection, method);

        // Unbinding a binding that does not exist is a no-op, as in RabbitMQ.
        if (exchange.Bindings.RemoveAll(b => b.Queue == queue && b.RoutingKey == routingKey) > 0)
        {
            MaybeAutoDeleteExchange(exchange);
        }
    }

    // ---- publishing & consumers ---------------------------------------------------------------

    /// <summary>Routes <paramref name="message"/> and enqueues it on every matching queue. Returns the number of queues.</summary>
    public int Publish(Message message)
    {
        if (!Exchanges.TryGetValue(message.Exchange, out var exchange))
        {
            return 0;
        }

        IEnumerable<BrokerQueue> targets = exchange.Name.Length == 0
            ? (Queues.TryGetValue(message.RoutingKey, out var direct) ? [direct] : [])
            : exchange.Route(message.RoutingKey).ToList();

        int routed = 0;
        foreach (var queue in targets)
        {
            queue.Ready.AddLast(new QueueEntry(message, redelivered: false));
            queue.Dispatch();
            routed++;
        }

        return routed;
    }

    /// <summary>Detaches a consumer from its queue, deleting the queue if it is auto-delete and now unused.</summary>
    public void RemoveConsumer(Consumer consumer)
    {
        var queue = consumer.Queue;
        queue.Consumers.Remove(consumer);
        if (queue.AutoDelete && queue.HadConsumer && queue.Consumers.Count == 0)
        {
            DeleteQueue(queue);
        }
    }
}
