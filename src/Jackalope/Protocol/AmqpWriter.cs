using System.Buffers.Binary;
using System.Collections;
using System.Text;

namespace Jackalope.Protocol;

/// <summary>A unix timestamp (seconds) carried as an AMQP 'T' field value.</summary>
public readonly record struct AmqpTimestamp(long UnixTimeSeconds);

/// <summary>Growable big-endian writer for AMQP 0-9-1 primitives, field tables and whole frames.</summary>
internal sealed class AmqpWriter
{
    private byte[] _buffer;
    private int _length;

    public AmqpWriter(int capacity = 256)
    {
        _buffer = new byte[Math.Max(capacity, 16)];
    }

    public int Length => _length;

    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _length);

    public byte[] ToArray() => WrittenSpan.ToArray();

    private Span<byte> Reserve(int count)
    {
        if (_length + count > _buffer.Length)
        {
            Array.Resize(ref _buffer, Math.Max(_buffer.Length * 2, _length + count));
        }

        var span = _buffer.AsSpan(_length, count);
        _length += count;
        return span;
    }

    private void PatchLong(int position, uint value) =>
        BinaryPrimitives.WriteUInt32BigEndian(_buffer.AsSpan(position, 4), value);

    public void WriteOctet(byte value) => Reserve(1)[0] = value;

    public void WriteShort(ushort value) => BinaryPrimitives.WriteUInt16BigEndian(Reserve(2), value);

    public void WriteLong(uint value) => BinaryPrimitives.WriteUInt32BigEndian(Reserve(4), value);

    public void WriteLongLong(ulong value) => BinaryPrimitives.WriteUInt64BigEndian(Reserve(8), value);

    public void WriteBytes(ReadOnlySpan<byte> value) => value.CopyTo(Reserve(value.Length));

    public void WriteBits(params bool[] bits)
    {
        byte value = 0;
        for (int i = 0; i < bits.Length; i++)
        {
            if (bits[i])
            {
                value |= (byte)(1 << i);
            }
        }

        WriteOctet(value);
    }

    public void WriteShortStr(string value)
    {
        int count = Encoding.UTF8.GetByteCount(value);
        if (count > byte.MaxValue)
        {
            throw new ArgumentException($"short string exceeds 255 bytes: '{value[..Math.Min(value.Length, 40)]}...'");
        }

        WriteOctet((byte)count);
        Encoding.UTF8.GetBytes(value, Reserve(count));
    }

    public void WriteLongStr(ReadOnlySpan<byte> value)
    {
        WriteLong((uint)value.Length);
        WriteBytes(value);
    }

    public void WriteLongStr(string value) => WriteLongStr(Encoding.UTF8.GetBytes(value));

    public void WriteTable(IEnumerable<KeyValuePair<string, object?>>? table)
    {
        int lengthPosition = _length;
        WriteLong(0);
        if (table is not null)
        {
            foreach (var (key, value) in table)
            {
                WriteShortStr(key);
                WriteFieldValue(value);
            }
        }

        PatchLong(lengthPosition, (uint)(_length - lengthPosition - 4));
    }

    public void WriteFieldValue(object? value)
    {
        switch (value)
        {
            case null: WriteOctet((byte)'V'); break;
            case string s: WriteOctet((byte)'S'); WriteLongStr(s); break;
            case byte[] bytes: WriteOctet((byte)'S'); WriteLongStr(bytes); break;
            case bool b: WriteOctet((byte)'t'); WriteOctet(b ? (byte)1 : (byte)0); break;
            case sbyte sb: WriteOctet((byte)'b'); WriteOctet((byte)sb); break;
            case byte ub: WriteOctet((byte)'B'); WriteOctet(ub); break;
            case short i16: WriteOctet((byte)'s'); WriteShort((ushort)i16); break;
            case ushort u16: WriteOctet((byte)'u'); WriteShort(u16); break;
            case int i32: WriteOctet((byte)'I'); WriteLong((uint)i32); break;
            case uint u32: WriteOctet((byte)'i'); WriteLong(u32); break;
            case long i64: WriteOctet((byte)'l'); WriteLongLong((ulong)i64); break;
            case ulong u64: WriteOctet((byte)'l'); WriteLongLong(checked((ulong)(long)u64)); break;
            case float f: WriteOctet((byte)'f'); WriteLong((uint)BitConverter.SingleToInt32Bits(f)); break;
            case double d: WriteOctet((byte)'d'); WriteLongLong((ulong)BitConverter.DoubleToInt64Bits(d)); break;
            case decimal m: WriteDecimal(m); break;
            case AmqpTimestamp ts: WriteOctet((byte)'T'); WriteLongLong((ulong)ts.UnixTimeSeconds); break;
            case IDictionary<string, object?> table: WriteOctet((byte)'F'); WriteTable(table); break;
            case IEnumerable list:
            {
                WriteOctet((byte)'A');
                int lengthPosition = _length;
                WriteLong(0);
                foreach (object? item in list)
                {
                    WriteFieldValue(item);
                }

                PatchLong(lengthPosition, (uint)(_length - lengthPosition - 4));
                break;
            }
            default:
                throw new ArgumentException($"type {value.GetType()} cannot be encoded as an AMQP field value");
        }
    }

    private void WriteDecimal(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        byte scale = (byte)((bits[3] >> 16) & 0xFF);
        bool negative = bits[3] < 0;
        if (bits[1] != 0 || bits[2] != 0 || (uint)bits[0] > int.MaxValue)
        {
            throw new ArgumentException($"decimal {value} does not fit in an AMQP decimal (32-bit unscaled value)");
        }

        WriteOctet((byte)'D');
        WriteOctet(scale);
        WriteLong((uint)(negative ? -bits[0] : bits[0]));
    }

    // ---- framing ------------------------------------------------------------------------------

    /// <summary>Writes a frame header with a placeholder size. Returns the position to pass to <see cref="EndFrame"/>.</summary>
    public int BeginFrame(byte frameType, ushort channel)
    {
        WriteOctet(frameType);
        WriteShort(channel);
        int sizePosition = _length;
        WriteLong(0);
        return sizePosition;
    }

    public void EndFrame(int sizePosition)
    {
        PatchLong(sizePosition, (uint)(_length - sizePosition - 4));
        WriteOctet(AmqpConstants.FrameEnd);
    }

    public int BeginMethod(ushort channel, uint methodId)
    {
        int sizePosition = BeginFrame(AmqpConstants.FrameMethod, channel);
        WriteShort((ushort)(methodId >> 16));
        WriteShort((ushort)(methodId & 0xFFFF));
        return sizePosition;
    }

    /// <summary>Writes a content header frame (raw payload as received from the publisher) followed by body frames.</summary>
    public void WriteContent(ushort channel, byte[] headerPayload, ReadOnlySpan<byte> body, int maxBodyFramePayload)
    {
        int position = BeginFrame(AmqpConstants.FrameHeader, channel);
        WriteBytes(headerPayload);
        EndFrame(position);

        for (int offset = 0; offset < body.Length; offset += maxBodyFramePayload)
        {
            int count = Math.Min(maxBodyFramePayload, body.Length - offset);
            position = BeginFrame(AmqpConstants.FrameBody, channel);
            WriteBytes(body.Slice(offset, count));
            EndFrame(position);
        }
    }

    /// <summary>Builds a complete method frame with no arguments, or with arguments written by <paramref name="writeArgs"/>.</summary>
    public static byte[] MethodFrame(ushort channel, uint methodId, Action<AmqpWriter>? writeArgs = null)
    {
        var writer = new AmqpWriter(64);
        int position = writer.BeginMethod(channel, methodId);
        writeArgs?.Invoke(writer);
        writer.EndFrame(position);
        return writer.ToArray();
    }
}
