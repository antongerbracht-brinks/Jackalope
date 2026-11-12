using Jackalope.Protocol;

namespace Jackalope.Tests;

/// <summary>Unit tests for the field-table codec and framing primitives.</summary>
public class FieldTableCodecTests
{
    private static Dictionary<string, object?> RoundTrip(Dictionary<string, object?> table)
    {
        var writer = new AmqpWriter();
        writer.WriteTable(table);
        var reader = new AmqpReader(writer.WrittenSpan);
        var result = reader.ReadTable();
        Assert.Equal(0, reader.Remaining);
        return result;
    }

    [Fact]
    public void All_supported_field_types_round_trip()
    {
        var table = RoundTrip(new Dictionary<string, object?>
        {
            ["bool"] = true,
            ["sbyte"] = (sbyte)-5,
            ["byte"] = (byte)250,
            ["short"] = (short)-1234,
            ["ushort"] = (ushort)60000,
            ["int"] = -123456,
            ["uint"] = 4000000000u,
            ["long"] = long.MinValue,
            ["float"] = 1.5f,
            ["double"] = Math.PI,
            ["decimal"] = -123.45m,
            ["string"] = "héllo",
            ["bytes"] = new byte[] { 0, 1, 2, 255 },
            ["timestamp"] = new AmqpTimestamp(1_700_000_000),
            ["void"] = null,
            ["table"] = new Dictionary<string, object?> { ["inner"] = 1 },
            ["array"] = new List<object?> { 1, "two", null, new Dictionary<string, object?>() },
        });

        Assert.Equal(true, table["bool"]);
        Assert.Equal((sbyte)-5, table["sbyte"]);
        Assert.Equal((byte)250, table["byte"]);
        Assert.Equal((short)-1234, table["short"]);
        Assert.Equal((ushort)60000, table["ushort"]);
        Assert.Equal(-123456, table["int"]);
        Assert.Equal(4000000000u, table["uint"]);
        Assert.Equal(long.MinValue, table["long"]);
        Assert.Equal(1.5f, table["float"]);
        Assert.Equal(Math.PI, table["double"]);
        Assert.Equal(-123.45m, table["decimal"]);
        Assert.Equal("héllo"u8.ToArray(), table["string"]);
        Assert.Equal(new byte[] { 0, 1, 2, 255 }, table["bytes"]);
        Assert.Equal(new AmqpTimestamp(1_700_000_000), table["timestamp"]);
        Assert.Null(table["void"]);
        Assert.Equal(1, ((Dictionary<string, object?>)table["table"]!)["inner"]);
        var array = (List<object?>)table["array"]!;
        Assert.Equal(4, array.Count);
        Assert.Equal("two"u8.ToArray(), array[1]);
        Assert.Null(array[2]);
    }

    [Fact]
    public void Empty_and_null_tables_encode_as_zero_length()
    {
        var writer = new AmqpWriter();
        writer.WriteTable(null);
        Assert.Equal(new byte[4], writer.ToArray());
        Assert.Empty(RoundTrip([]));
    }

    [Fact]
    public void Encoding_uses_rabbitmq_type_tags()
    {
        var writer = new AmqpWriter();
        writer.WriteFieldValue((short)1);
        writer.WriteFieldValue(1);
        writer.WriteFieldValue(1L);
        writer.WriteFieldValue("s");
        Assert.Equal((byte)'s', writer.WrittenSpan[0]);
        Assert.Equal((byte)'I', writer.WrittenSpan[3]);
        Assert.Equal((byte)'l', writer.WrittenSpan[8]);
        Assert.Equal((byte)'S', writer.WrittenSpan[17]);
    }

    [Fact]
    public void Truncated_or_unknown_data_is_a_syntax_error()
    {
        Assert.Throws<AmqpSyntaxException>(() => new AmqpReader([0, 0, 0, 10, 1]).ReadTable());
        Assert.Throws<AmqpSyntaxException>(() => new AmqpReader([0, 0, 0, 3, 1, (byte)'k', (byte)'Z']).ReadTable());
    }

    [Fact]
    public void Frames_carry_type_channel_size_and_frame_end()
    {
        byte[] frame = AmqpWriter.MethodFrame(7, Method.BasicQosOk);
        Assert.Equal(new byte[] { 1, 0, 7, 0, 0, 0, 4, 0, 60, 0, 11, 0xCE }, frame);
    }

    [Fact]
    public void Content_is_split_into_body_frames_of_at_most_the_frame_payload()
    {
        var writer = new AmqpWriter();
        byte[] header = new byte[14];
        writer.WriteContent(1, header, new byte[10], maxBodyFramePayload: 4);
        // header frame (8 + 14) + body frames 4,4,2 (8 + n each)
        Assert.Equal(22 + 12 + 12 + 10, writer.Length);
    }

    [Fact]
    public void Short_strings_longer_than_255_bytes_are_rejected()
    {
        var writer = new AmqpWriter();
        Assert.Throws<ArgumentException>(() => writer.WriteShortStr(new string('x', 256)));
    }
}
