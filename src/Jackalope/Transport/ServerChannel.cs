using System.Buffers.Binary;
using Jackalope.Broker;
using Jackalope.Protocol;

namespace Jackalope.Transport;

/// <summary>One AMQP channel multiplexed over an <see cref="AmqpConnection"/>. All members run under the broker lock.</summary>
internal sealed class ServerChannel(AmqpConnection connection, BrokerState broker, ushort number)
{
    private readonly Dictionary<string, Consumer> _consumers = new(StringComparer.Ordinal);
    private readonly SortedDictionary<ulong, UnackedDelivery> _unacked = new();
    private ulong _lastDeliveryTag;
    private ushort _consumerPrefetch;
    private ushort _channelPrefetch;
    private bool _confirmMode;
    private ulong _publishSequence;
    private string? _lastDeclaredQueue;
    private PendingPublish? _pending;
    private bool _released;

    public ushort Number { get; } = number;

    /// <summary>True once the server has sent channel.close and is waiting for close-ok.</summary>
    public bool IsClosing { get; private set; }

    public bool HasChannelPrefetchCapacity => _channelPrefetch == 0 || _unacked.Count < _channelPrefetch;

    private string VHost => connection.VirtualHost;

    private sealed class PendingPublish(string exchange, string routingKey, bool mandatory)
    {
        public string Exchange { get; } = exchange;
        public string RoutingKey { get; } = routingKey;
        public bool Mandatory { get; } = mandatory;
        public byte[]? Header { get; set; }
        public byte[] Body { get; set; } = [];
        public int Received { get; set; }
    }

    public void HandleFrame(byte type, ReadOnlySpan<byte> payload)
    {
        if (IsClosing)
        {
            HandleFrameWhileClosing(type, payload);
            return;
        }

        try
        {
            switch (type)
            {
                case AmqpConstants.FrameMethod:
                    if (_pending is not null)
                    {
                        throw new ConnectionException(ReplyCode.UnexpectedFrame,
                            $"expected content {(_pending.Header is null ? "header" : "body")} for basic.publish on channel {Number}, got a method frame");
                    }

                    HandleMethod(payload);
                    break;

                case AmqpConstants.FrameHeader:
                    if (_pending is null || _pending.Header is not null)
                    {
                        throw new ConnectionException(ReplyCode.UnexpectedFrame, $"unexpected content header frame on channel {Number}");
                    }

                    HandleContentHeader(payload);
                    break;

                case AmqpConstants.FrameBody:
                    if (_pending?.Header is null)
                    {
                        throw new ConnectionException(ReplyCode.UnexpectedFrame, $"unexpected content body frame on channel {Number}");
                    }

                    HandleContentBody(payload);
                    break;

                default:
                    throw new ConnectionException(ReplyCode.FrameError, $"unknown frame type {type}");
            }
        }
        catch (ChannelException ex)
        {
            CloseFromServer(ex);
        }
    }

    private void HandleFrameWhileClosing(byte type, ReadOnlySpan<byte> payload)
    {
        if (type != AmqpConstants.FrameMethod || payload.Length < 4)
        {
            return;
        }

        uint id = Method.Id(BinaryPrimitives.ReadUInt16BigEndian(payload), BinaryPrimitives.ReadUInt16BigEndian(payload[2..]));
        if (id == Method.ChannelClose)
        {
            connection.SendMethod(Number, Method.ChannelCloseOk);
            connection.RemoveChannel(Number);
        }
        else if (id == Method.ChannelCloseOk)
        {
            connection.RemoveChannel(Number);
        }
    }

    private void CloseFromServer(ChannelException error)
    {
        broker.Options.Log($"[conn {connection.Id}] closing channel {Number}: {error.Message}");
        Release();
        IsClosing = true;
        connection.SendMethod(Number, Method.ChannelClose, w =>
        {
            w.WriteShort(error.ReplyCode);
            w.WriteShortStr(AmqpConnection.Truncate(error.Message));
            w.WriteShort((ushort)(error.MethodId >> 16));
            w.WriteShort((ushort)(error.MethodId & 0xFFFF));
        });
    }

    /// <summary>
    /// Frees everything the channel holds: cancels its consumers and requeues its unacknowledged deliveries
    /// (marked redelivered), exactly as RabbitMQ does when a channel or connection closes. Idempotent.
    /// </summary>
    public void Release()
    {
        if (_released)
        {
            return;
        }

        _released = true;
        _pending = null;

        // Remove consumers first so requeued messages are not handed straight back to this channel.
        foreach (var consumer in _consumers.Values)
        {
            broker.RemoveConsumer(consumer);
        }

        _consumers.Clear();

        var affected = Requeue(_unacked.Values.ToList());
        _unacked.Clear();
        foreach (var queue in affected)
        {
            queue.Dispatch();
        }
    }

    // ---- methods ------------------------------------------------------------------------------

    private void HandleMethod(ReadOnlySpan<byte> payload)
    {
        var reader = new AmqpReader(payload);
        uint id = Method.Id(reader.ReadShort(), reader.ReadShort());

        switch (id)
        {
            case Method.ChannelOpen:
                throw new ConnectionException(ReplyCode.ChannelError, $"second 'channel.open' seen on channel {Number}", id);
            case Method.ChannelClose:
                Release();
                connection.SendMethod(Number, Method.ChannelCloseOk);
                connection.RemoveChannel(Number);
                break;
            case Method.ChannelCloseOk:
                break; // Not expecting one; ignore.
            case Method.ExchangeDeclare:
                ExchangeDeclare(ref reader);
                break;
            case Method.QueueDeclare:
                QueueDeclare(ref reader);
                break;
            case Method.QueueBind:
                QueueBind(ref reader);
                break;
            case Method.QueueUnbind:
                QueueUnbind(ref reader);
                break;
            case Method.BasicQos:
                BasicQos(ref reader);
                break;
            case Method.BasicConsume:
                BasicConsume(ref reader);
                break;
            case Method.BasicCancel:
                BasicCancel(ref reader);
                break;
            case Method.BasicPublish:
                BasicPublish(ref reader);
                break;
            case Method.BasicAck:
                BasicAck(ref reader);
                break;
            case Method.ConfirmSelect:
                ConfirmSelect(ref reader);
                break;
            default:
                throw new ConnectionException(ReplyCode.NotImplemented,
                    $"{Method.Name(id)} is not supported by Jackalope (see README: non-goals)", id);
        }
    }

    private void ExchangeDeclare(ref AmqpReader reader)
    {
        reader.ReadShort(); // reserved
        string name = reader.ReadShortStr();
        string type = reader.ReadShortStr();
        byte bits = reader.ReadOctet();
        var arguments = reader.ReadTable();
        bool passive = (bits & 1) != 0, durable = (bits & 2) != 0, autoDelete = (bits & 4) != 0;
        bool isInternal = (bits & 8) != 0, noWait = (bits & 16) != 0;

        broker.DeclareExchange(name, type, passive, durable, autoDelete, isInternal, arguments, VHost);
        if (!noWait)
        {
            connection.SendMethod(Number, Method.ExchangeDeclareOk);
        }
    }

    private void QueueDeclare(ref AmqpReader reader)
    {
        reader.ReadShort(); // reserved
        string name = reader.ReadShortStr();
        byte bits = reader.ReadOctet();
        var arguments = reader.ReadTable();
        bool passive = (bits & 1) != 0, durable = (bits & 2) != 0, exclusive = (bits & 4) != 0;
        bool autoDelete = (bits & 8) != 0, noWait = (bits & 16) != 0;

        var queue = broker.DeclareQueue(name, passive, durable, exclusive, autoDelete, arguments, connection);
        _lastDeclaredQueue = queue.Name;
        if (!noWait)
        {
            connection.SendMethod(Number, Method.QueueDeclareOk, w =>
            {
                w.WriteShortStr(queue.Name);
                w.WriteLong((uint)queue.Ready.Count);
                w.WriteLong((uint)queue.Consumers.Count);
            });
        }
    }

    /// <summary>AMQP lets an empty queue name mean "the last queue declared on this channel".</summary>
    private string ResolveQueueName(string name, uint method) =>
        name.Length > 0
            ? name
            : _lastDeclaredQueue ?? throw new ChannelException(ReplyCode.NotFound, "no previously declared queue", method);

    private void QueueBind(ref AmqpReader reader)
    {
        reader.ReadShort(); // reserved
        string queue = reader.ReadShortStr();
        string exchange = reader.ReadShortStr();
        string routingKey = reader.ReadShortStr();
        bool noWait = (reader.ReadOctet() & 1) != 0;
        reader.ReadTable(); // arguments: only meaningful for headers exchanges, which are unsupported

        if (queue.Length == 0 && routingKey.Length == 0)
        {
            routingKey = ResolveQueueName(queue, Method.QueueBind);
        }

        broker.Bind(ResolveQueueName(queue, Method.QueueBind), exchange, routingKey, connection);
        if (!noWait)
        {
            connection.SendMethod(Number, Method.QueueBindOk);
        }
    }

    private void QueueUnbind(ref AmqpReader reader)
    {
        reader.ReadShort(); // reserved
        string queue = reader.ReadShortStr();
        string exchange = reader.ReadShortStr();
        string routingKey = reader.ReadShortStr();
        reader.ReadTable(); // arguments

        broker.Unbind(ResolveQueueName(queue, Method.QueueUnbind), exchange, routingKey, connection);
        connection.SendMethod(Number, Method.QueueUnbindOk);
    }

    private void BasicQos(ref AmqpReader reader)
    {
        uint prefetchSize = reader.ReadLong();
        ushort prefetchCount = reader.ReadShort();
        bool global = (reader.ReadOctet() & 1) != 0;

        if (prefetchSize != 0)
        {
            throw new ConnectionException(ReplyCode.NotImplemented, $"prefetch_size!=0 ({prefetchSize})", Method.BasicQos);
        }

        // RabbitMQ semantics (per_consumer_qos): global=false applies to consumers created afterwards on this
        // channel; global=true limits unacked deliveries across the whole channel.
        if (global)
        {
            _channelPrefetch = prefetchCount;
        }
        else
        {
            _consumerPrefetch = prefetchCount;
        }

        connection.SendMethod(Number, Method.BasicQosOk);
        DispatchConsumedQueues();
    }

    private void BasicConsume(ref AmqpReader reader)
    {
        const uint method = Method.BasicConsume;
        reader.ReadShort(); // reserved
        string queueName = reader.ReadShortStr();
        string tag = reader.ReadShortStr();
        byte bits = reader.ReadOctet();
        reader.ReadTable(); // arguments (x-priority etc. are not honoured)
        bool noAck = (bits & 2) != 0, exclusive = (bits & 4) != 0, noWait = (bits & 8) != 0;

        var queue = broker.GetQueue(ResolveQueueName(queueName, method), VHost, method);
        BrokerState.CheckExclusiveAccess(queue, connection, method);

        if (tag.Length == 0)
        {
            tag = BrokerState.GenerateName("amq.ctag-");
        }
        else if (_consumers.ContainsKey(tag))
        {
            throw new ConnectionException(ReplyCode.NotAllowed, $"attempt to reuse consumer tag '{tag}'", method);
        }

        if ((exclusive && queue.Consumers.Count > 0) || queue.Consumers.Any(c => c.Exclusive))
        {
            throw new ChannelException(ReplyCode.AccessRefused, $"queue '{queue.Name}' in vhost '{VHost}' in exclusive use", method);
        }

        var consumer = new Consumer(tag, this, queue, noAck, exclusive, _consumerPrefetch);
        _consumers[tag] = consumer;
        queue.Consumers.Add(consumer);
        queue.HadConsumer = true;

        // consume-ok must reach the client before the first basic.deliver for this tag.
        if (!noWait)
        {
            connection.SendMethod(Number, Method.BasicConsumeOk, w => w.WriteShortStr(tag));
        }

        queue.Dispatch();
    }

    private void BasicCancel(ref AmqpReader reader)
    {
        string tag = reader.ReadShortStr();
        bool noWait = (reader.ReadOctet() & 1) != 0;

        // Cancelling an unknown tag is not an error. Unacked deliveries stay on the channel and can still be acked.
        if (_consumers.Remove(tag, out var consumer))
        {
            broker.RemoveConsumer(consumer);
        }

        if (!noWait)
        {
            connection.SendMethod(Number, Method.BasicCancelOk, w => w.WriteShortStr(tag));
        }
    }

    /// <summary>Called when the queue a consumer is attached to is deleted: notify the client (consumer_cancel_notify).</summary>
    public void CancelConsumerFromServer(Consumer consumer)
    {
        if (_consumers.Remove(consumer.Tag) && !IsClosing)
        {
            connection.SendMethod(Number, Method.BasicCancel, w =>
            {
                w.WriteShortStr(consumer.Tag);
                w.WriteBits(true); // no-wait
            });
        }
    }

    private void ConfirmSelect(ref AmqpReader reader)
    {
        bool noWait = (reader.ReadOctet() & 1) != 0;
        _confirmMode = true;
        if (!noWait)
        {
            connection.SendMethod(Number, Method.ConfirmSelectOk);
        }
    }

    // ---- publishing ---------------------------------------------------------------------------

    private void BasicPublish(ref AmqpReader reader)
    {
        const uint method = Method.BasicPublish;
        reader.ReadShort(); // reserved
        string exchangeName = reader.ReadShortStr();
        string routingKey = reader.ReadShortStr();
        byte bits = reader.ReadOctet();
        bool mandatory = (bits & 1) != 0, immediate = (bits & 2) != 0;

        if (immediate)
        {
            throw new ConnectionException(ReplyCode.NotImplemented, "immediate=true", method);
        }

        // Content frames for this publish follow regardless; they are consumed (or discarded if the channel closes).
        _pending = new PendingPublish(exchangeName, routingKey, mandatory);

        var exchange = broker.FindExchange(exchangeName)
            ?? throw new ChannelException(ReplyCode.NotFound, $"no exchange '{exchangeName}' in vhost '{VHost}'", method);
        if (exchange.Internal)
        {
            throw new ChannelException(ReplyCode.AccessRefused,
                $"cannot publish to internal exchange '{exchangeName}' in vhost '{VHost}'", method);
        }
    }

    private void HandleContentHeader(ReadOnlySpan<byte> payload)
    {
        var pending = _pending!;
        if (payload.Length < 14)
        {
            throw new ConnectionException(ReplyCode.FrameError, "content header frame too short");
        }

        ushort classId = BinaryPrimitives.ReadUInt16BigEndian(payload);
        if (classId != 60)
        {
            throw new ConnectionException(ReplyCode.FrameError, $"content header class {classId} does not match basic.publish");
        }

        ulong bodySize = BinaryPrimitives.ReadUInt64BigEndian(payload[4..]);
        if (bodySize > (ulong)broker.Options.MaxMessageSize)
        {
            throw new ChannelException(ReplyCode.PreconditionFailed,
                $"message size {bodySize} is larger than configured max size {broker.Options.MaxMessageSize}", Method.BasicPublish);
        }

        pending.Header = payload.ToArray();
        pending.Body = new byte[bodySize];
        if (bodySize == 0)
        {
            CompletePublish();
        }
    }

    private void HandleContentBody(ReadOnlySpan<byte> payload)
    {
        var pending = _pending!;
        if (pending.Received + payload.Length > pending.Body.Length)
        {
            throw new ConnectionException(ReplyCode.FrameError,
                $"content body exceeds declared size {pending.Body.Length}");
        }

        payload.CopyTo(pending.Body.AsSpan(pending.Received));
        pending.Received += payload.Length;
        if (pending.Received == pending.Body.Length)
        {
            CompletePublish();
        }
    }

    private void CompletePublish()
    {
        var pending = _pending!;
        _pending = null;

        var message = new Message(pending.Exchange, pending.RoutingKey, pending.Header!, pending.Body);
        int routed = broker.Publish(message);

        if (routed == 0 && pending.Mandatory)
        {
            var writer = new AmqpWriter(64 + message.HeaderPayload.Length + message.Body.Length);
            int position = writer.BeginMethod(Number, Method.BasicReturn);
            writer.WriteShort(ReplyCode.NoRoute);
            writer.WriteShortStr("NO_ROUTE");
            writer.WriteShortStr(message.Exchange);
            writer.WriteShortStr(message.RoutingKey);
            writer.EndFrame(position);
            writer.WriteContent(Number, message.HeaderPayload, message.Body, connection.MaxBodyFramePayload);
            connection.Send(writer.ToArray());
        }

        // Publisher confirms: routing is synchronous and in-memory, so every message is confirmed immediately.
        if (_confirmMode)
        {
            ulong sequence = ++_publishSequence;
            connection.SendMethod(Number, Method.BasicAck, w =>
            {
                w.WriteLongLong(sequence);
                w.WriteBits(false); // multiple
            });
        }
    }

    // ---- delivery & acknowledgement ------------------------------------------------------------

    /// <summary>Sends basic.deliver + content to the client for <paramref name="consumer"/>.</summary>
    public void Deliver(Consumer consumer, QueueEntry entry)
    {
        var message = entry.Message;
        ulong tag = ++_lastDeliveryTag;
        if (!consumer.NoAck)
        {
            _unacked.Add(tag, new UnackedDelivery(tag, consumer, consumer.Queue, message));
            consumer.Unacked++;
        }

        var writer = new AmqpWriter(128 + message.HeaderPayload.Length + message.Body.Length);
        int position = writer.BeginMethod(Number, Method.BasicDeliver);
        writer.WriteShortStr(consumer.Tag);
        writer.WriteLongLong(tag);
        writer.WriteBits(entry.Redelivered);
        writer.WriteShortStr(message.Exchange);
        writer.WriteShortStr(message.RoutingKey);
        writer.EndFrame(position);
        writer.WriteContent(Number, message.HeaderPayload, message.Body, connection.MaxBodyFramePayload);
        connection.Send(writer.ToArray());
    }

    private void BasicAck(ref AmqpReader reader)
    {
        ulong deliveryTag = reader.ReadLongLong();
        bool multiple = (reader.ReadOctet() & 1) != 0;
        TakeUnacked(deliveryTag, multiple, Method.BasicAck);
        DispatchConsumedQueues();
    }

    /// <summary>Removes and returns the deliveries an ack/nack/reject refers to, in ascending tag order.</summary>
    private List<UnackedDelivery> TakeUnacked(ulong deliveryTag, bool multiple, uint method)
    {
        List<UnackedDelivery> taken;
        if (multiple)
        {
            taken = _unacked.Values.Where(u => deliveryTag == 0 || u.DeliveryTag <= deliveryTag).ToList();
            if (taken.Count == 0 && deliveryTag != 0)
            {
                throw new ChannelException(ReplyCode.PreconditionFailed, $"unknown delivery tag {deliveryTag}", method);
            }
        }
        else
        {
            if (!_unacked.TryGetValue(deliveryTag, out var single))
            {
                throw new ChannelException(ReplyCode.PreconditionFailed, $"unknown delivery tag {deliveryTag}", method);
            }

            taken = [single];
        }

        foreach (var delivery in taken)
        {
            _unacked.Remove(delivery.DeliveryTag);
            delivery.Consumer.Unacked--;
        }

        return taken;
    }

    /// <summary>Puts deliveries back at the head of their queues (preserving order) flagged as redelivered.</summary>
    private static HashSet<BrokerQueue> Requeue(List<UnackedDelivery> deliveries)
    {
        var affected = new HashSet<BrokerQueue>();
        for (int i = deliveries.Count - 1; i >= 0; i--)
        {
            var delivery = deliveries[i];
            if (!delivery.Queue.IsDeleted)
            {
                delivery.Queue.Ready.AddFirst(new QueueEntry(delivery.Message, redelivered: true));
                affected.Add(delivery.Queue);
            }
        }

        return affected;
    }

    /// <summary>Re-runs dispatch for queues this channel consumes from, after capacity may have been freed.</summary>
    private void DispatchConsumedQueues()
    {
        foreach (var queue in _consumers.Values.Select(c => c.Queue).Distinct().ToList())
        {
            queue.Dispatch();
        }
    }
}
