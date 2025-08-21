using System.Net;

namespace ClickHouse.BinaryIO.Test;

public class ClickHouseBinaryWriterTests : ClickHouseDatabaseTestBase
{
	[Fact]
	public async Task WritesWellFormedData()
	{
		using var content = new TestHttpContent();
		using var response = await ExecuteClickHouseQueryAsync("INSERT INTO test FORMAT RowBinary", content);
		Assert.Equal(HttpStatusCode.OK, response.StatusCode);

		// Re-use assertions from the reader test as its the same data
		var readerTests = new ClickHouseBinaryReaderTests();
		await readerTests.ReadsColumnMetadataAndRows();
	}

	private sealed class TestHttpContent : HttpContent
	{
		protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
		{
			// Same data as in ClickHouseBinaryReaderTests
			var options = new ClickHouseBinaryWriterOptions() { Buffer = new byte[256] };
			await using var writer = new ClickHouseBinaryWriter(stream, options);
			writer.WriteUInt8(1);
			writer.WriteInt8(2);
			writer.WriteUInt16(3);
			writer.WriteInt16(4);
			writer.WriteUInt32(5);
			writer.WriteInt32(6);
			writer.WriteUInt64(7);
			writer.WriteInt64(8);
			writer.WriteFloat32(1.5f);
			writer.WriteFloat64(2.87);
			writer.WriteString("test_a");
			writer.WriteFixedString("US");
			writer.WriteDate(new(2025, 1, 1));
			writer.WriteDate32(new(2200, 1, 1));
			writer.WriteDateTime(new(2025, 1, 1, 8, 0, 0, DateTimeKind.Utc));
			writer.WriteDateTime64(new(2025, 1, 1, 10, 0, 0, 500, DateTimeKind.Utc));
			writer.WriteTime(new TimeSpan(0, 100, 23, 44));
			writer.WriteTime64(new(0, 1, 5, 10, 812));
			writer.WriteUuid(new("61f0c404-5cb3-11e7-907b-a6006ad3dba0"));
			writer.WriteIPv4(IPAddress.Parse("116.106.34.242"));
			writer.WriteIPv6(IPAddress.Parse("2a02:e980:1e::1"));
			writer.WriteArrayLength(3);
			writer.WriteString("a");
			writer.WriteString("b");
			writer.WriteString("c");
			writer.WriteBoolean(true);
			writer.WriteNull(true);
			writer.WriteArrayLength(2);
			writer.WriteArrayLength(2);
			writer.WriteInt8(8);
			writer.WriteInt8(10);
			writer.WriteArrayLength(2);
			writer.WriteInt8(12);
			writer.WriteInt8(14);
			writer.WriteInt32(1);
			writer.WriteInt32(2);
			writer.WriteString("tuple_a");
			await writer.FlushAsync();
			writer.WriteUInt8(2);
			writer.WriteInt8(3);
			writer.WriteUInt16(4);
			writer.WriteInt16(5);
			writer.WriteUInt32(6);
			writer.WriteInt32(7);
			writer.WriteUInt64(8);
			writer.WriteInt64(9);
			writer.WriteFloat32(1.75f);
			writer.WriteFloat64(2.91);
			writer.WriteString("test_b");
			writer.WriteFixedString("CA");
			writer.WriteDate(new(2025, 2, 1));
			writer.WriteDate32(new(2200, 2, 1));
			writer.WriteDateTime(new(2025, 2, 1, 9, 0, 0, DateTimeKind.Utc));
			writer.WriteDateTime64(new(2025, 2, 1, 11, 0, 0, 750, DateTimeKind.Utc));
			writer.WriteTime(new TimeSpan(0, 8, 3, 15));
			writer.WriteTime64(new(0, 0, 0, 23, 400));
			writer.WriteUuid(new("36a0b67c-b74a-4640-803b-e44bb4547e3c"));
			writer.WriteIPv4(IPAddress.Parse("183.247.232.58"));
			writer.WriteIPv6(IPAddress.Parse("2a02:aa08:e000:3100::2"));
			writer.WriteArrayLength(2);
			writer.WriteString("d");
			writer.WriteString("e");
			writer.WriteBoolean(false);
			writer.WriteNull(false);
			writer.WriteInt8(127);
			writer.WriteArrayLength(2);
			writer.WriteArrayLength(2);
			writer.WriteInt8(5);
			writer.WriteInt8(7);
			writer.WriteArrayLength(2);
			writer.WriteInt8(9);
			writer.WriteInt8(11);
			writer.WriteInt32(3);
			writer.WriteInt32(5);
			writer.WriteString("tuple_b");
		}

		protected override bool TryComputeLength(out long length)
		{
			length = 0;
			return false;
		}
	}
}
