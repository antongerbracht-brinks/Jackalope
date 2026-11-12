using System.Buffers.Binary;
using System.Text;

namespace Jackalope.Protocol;

/// <summary>Sequential big-endian reader for AMQP 0-9-1 primitive types and field tables.</summary>
internal ref struct AmqpReader
{
    private readonly ReadOnlySpan<byte> _data;
    private int _position;

    public AmqpReader(ReadOnlySpan<byte> data)
    {
        _data = data;
        _position = 0;
    }

    public int Position => _position;

    public int Remaining => _data.Length - _position;

    private ReadOnlySpan<byte> Take(int count)
    {
        if (count < 0 || count > Remaining)
        {
            throw new AmqpSyntaxException($"unexpected end of data (needed {count} bytes, {Remaining} remaining)");
        }

        var slice = _data.Slice(_position, count);
        _position += count;
        return slice;
    }

    public byte ReadOctet() => Take(1)[0];

    public ushort ReadShort() => BinaryPrimitives.ReadUInt16BigEndian(Take(2));

    public uint ReadLong() => BinaryPrimitives.ReadUInt32BigEndian(Take(4));

    public ulong ReadLongLong() => BinaryPrimitives.ReadUInt64BigEndian(Take(8));

    public string ReadShortStr() => Encoding.UTF8.GetString(Take(ReadOctet()));

    public byte[] ReadLongStr()
    {
        uint length = ReadLong();
        if (length > int.MaxValue)
        {
            throw new AmqpSyntaxException($"long string length {length} is too large");
        }

        return Take((int)length).ToArray();
    }

    public Dictionary<string, object?> ReadTable()
    {
        uint length = ReadLong();
        if (length > Remaining)
        {
            throw new AmqpSyntaxException($"field table length {length} exceeds remaining data {Remaining}");
        }

        int end = _position + (int)length;
        var table = new Dictionary<string, object?>(StringComparer.Ordinal);
        while (_position < end)
        {
            string key = ReadShortStr();
            table[key] = ReadFieldValue();
        }

        if (_position != end)
        {
            throw new AmqpSyntaxException("field table overran its declared length");
        }

        return table;
    }

    private List<object?> ReadArray()
    {
        uint length = ReadLong();
        if (length > Remaining)
        {
            throw new AmqpSyntaxException($"field array length {length} exceeds remaining data {Remaining}");
        }

        int end = _position + (int)length;
        var list = new List<object?>();
        while (_position < end)
        {
            list.Add(ReadFieldValue());
        }

        if (_position != end)
        {
            throw new AmqpSyntaxException("field array overran its declared length");
        }

        return list;
    }

    /// <summary>
    /// Reads a typed field value. Type tags follow the RabbitMQ dialect of 0-9-1 (the one RabbitMQ.Client
    /// speaks): 's' is a signed 16-bit int, 'u' unsigned 16-bit, 'I'/'i' signed/unsigned 32-bit, 'l' signed 64-bit.
    /// Long strings ('S') and byte arrays ('x') decode to <c>byte[]</c>, as RabbitMQ.Client does.
    /// </summary>
    public object? ReadFieldValue()
    {
        char tag = (char)ReadOctet();
        switch (tag)
        {
            case 't': return ReadOctet() != 0;
            case 'b': return (sbyte)ReadOctet();
            case 'B': return ReadOctet();
            case 's': return (short)ReadShort();
            case 'u': return ReadShort();
            case 'I': return (int)ReadLong();
            case 'i': return ReadLong();
            case 'l': return (long)ReadLongLong();
            case 'f': return BitConverter.Int32BitsToSingle((int)ReadLong());
            case 'd': return BitConverter.Int64BitsToDouble((long)ReadLongLong());
            case 'D':
            {
                byte scale = ReadOctet();
                long unscaled = (int)ReadLong();
                if (scale > 28)
                {
                    throw new AmqpSyntaxException($"decimal scale {scale} is out of range");
                }

                return new decimal((int)(uint)Math.Abs(unscaled), 0, 0, unscaled < 0, scale);
            }
            case 'S': return ReadLongStr();
            case 'x': return ReadLongStr();
            case 'A': return ReadArray();
            case 'T': return new AmqpTimestamp((long)ReadLongLong());
            case 'F': return ReadTable();
            case 'V': return null;
            default: throw new AmqpSyntaxException($"unknown field table type tag '{tag}' (0x{(byte)tag:X2})");
        }
    }
}
