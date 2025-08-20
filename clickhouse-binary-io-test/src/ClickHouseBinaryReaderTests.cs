using System.Net;

namespace ClickHouse.BinaryIO.Test;

public class ClickHouseBinaryReaderTests : ClickHouseDatabaseTestBase
{
	[Fact]
	public async Task ReadsColumnMetadataAndRows()
	{
		using var response = await ExecuteClickHouseQueryAsync("SELECT * FROM test ORDER BY u8 FORMAT RowBinaryWithNamesAndTypes");
		var stream = await response.Content.ReadAsStreamAsync();
		var options = new ClickHouseBinaryReaderOptions() { Buffer = new byte[256] };
		using var reader = new ClickHouseBinaryReader(stream, options);

		var columns = await reader.ReadColumnsAsync();
		Assert.Equal(26, columns.Length);
		Assert.Equal("u8", columns[0].Name);
		Assert.Equal(ClickHouseTypeName.UInt8, columns[0].Type.Name);
		Assert.Equal("s8", columns[1].Name);
		Assert.Equal(ClickHouseTypeName.Int8, columns[1].Type.Name);
		Assert.Equal("u16", columns[2].Name);
		Assert.Equal(ClickHouseTypeName.UInt16, columns[2].Type.Name);
		Assert.Equal("s16", columns[3].Name);
		Assert.Equal(ClickHouseTypeName.Int16, columns[3].Type.Name);
		Assert.Equal("u32", columns[4].Name);
		Assert.Equal(ClickHouseTypeName.UInt32, columns[4].Type.Name);
		Assert.Equal("s32", columns[5].Name);
		Assert.Equal(ClickHouseTypeName.Int32, columns[5].Type.Name);
		Assert.Equal("u64", columns[6].Name);
		Assert.Equal(ClickHouseTypeName.UInt64, columns[6].Type.Name);
		Assert.Equal("s64", columns[7].Name);
		Assert.Equal(ClickHouseTypeName.Int64, columns[7].Type.Name);
		Assert.Equal("f32", columns[8].Name);
		Assert.Equal(ClickHouseTypeName.Float32, columns[8].Type.Name);
		Assert.Equal("f64", columns[9].Name);
		Assert.Equal(ClickHouseTypeName.Float64, columns[9].Type.Name);
		Assert.Equal("str", columns[10].Name);
		Assert.Equal(ClickHouseTypeName.String, columns[10].Type.Name);
		Assert.Equal("fstr", columns[11].Name);
		Assert.Equal(ClickHouseTypeName.FixedString, columns[11].Type.Name);
		Assert.Equal("date", columns[12].Name);
		Assert.Equal(ClickHouseTypeName.Date, columns[12].Type.Name);
		Assert.Equal("date32", columns[13].Name);
		Assert.Equal(ClickHouseTypeName.Date32, columns[13].Type.Name);
		Assert.Equal("dt", columns[14].Name);
		Assert.Equal(ClickHouseTypeName.DateTime, columns[14].Type.Name);
		Assert.Equal("dt64", columns[15].Name);
		Assert.Equal(ClickHouseTypeName.DateTime64, columns[15].Type.Name);
		Assert.Equal("time", columns[16].Name);
		Assert.Equal(ClickHouseTypeName.Time, columns[16].Type.Name);
		Assert.Equal("time64", columns[17].Name);
		Assert.Equal(ClickHouseTypeName.Time64, columns[17].Type.Name);
		Assert.Equal("uuid", columns[18].Name);
		Assert.Equal(ClickHouseTypeName.UUID, columns[18].Type.Name);
		Assert.Equal("ipv4", columns[19].Name);
		Assert.Equal(ClickHouseTypeName.IPv4, columns[19].Type.Name);
		Assert.Equal("ipv6", columns[20].Name);
		Assert.Equal(ClickHouseTypeName.IPv6, columns[20].Type.Name);
		Assert.Equal("arr", columns[21].Name);
		Assert.Equal(ClickHouseTypeName.Array, columns[21].Type.Name);
		Assert.Equal("bool", columns[22].Name);
		Assert.Equal(ClickHouseTypeName.Bool, columns[22].Type.Name);
		Assert.Equal("nullable", columns[23].Name);
		Assert.Equal(ClickHouseTypeName.Nullable, columns[23].Type.Name);
		Assert.Equal("narr", columns[24].Name);
		Assert.Equal(ClickHouseTypeName.Array, columns[24].Type.Name);
		Assert.Equal("tuple", columns[25].Name);
		Assert.Equal(ClickHouseTypeName.Tuple, columns[25].Type.Name);

		var rows = new List<Row>();
		while (!await reader.IsCompleteAsync())
		{
			var row = new Row();
			row.U8 = reader.ReadUInt8();
			row.S8 = reader.ReadInt8();
			row.U16 = reader.ReadUInt16();
			row.S16 = reader.ReadInt16();
			row.U32 = reader.ReadUInt32();
			row.S32 = reader.ReadInt32();
			row.U64 = reader.ReadUInt64();
			row.S64 = reader.ReadInt64();
			row.F32 = reader.ReadFloat32();
			row.F64 = reader.ReadFloat64();
			row.Str = reader.ReadString();
			row.FStr = reader.ReadFixedString(2);
			row.Date = reader.ReadDate();
			row.Date32 = reader.ReadDate32();
			row.Dt = reader.ReadDateTime();
			row.Dt64 = reader.ReadDateTime64();
			row.Time = reader.ReadTime();
			row.Time64 = reader.ReadTime64();
			row.Uuid = reader.ReadUuid();
			row.IPv4 = reader.ReadIPv4();
			row.IPv6 = reader.ReadIPv6();
			var arr = new string[reader.ReadArrayLength()];
			for (int i = 0; i < arr.Length; i++)
			{
				arr[i] = reader.ReadString();
			}
			row.Arr = arr;
			row.Bool = reader.ReadBoolean();
			row.Nullable = reader.IsNull() ? null : reader.ReadInt8();
			var narr = new sbyte[reader.ReadArrayLength()][];
			for (int i = 0; i < narr.Length; i++)
			{
				narr[i] = new sbyte[reader.ReadArrayLength()];
				for (int j = 0; j < narr[i].Length; j++)
				{
					narr[i][j] = reader.ReadInt8();
				}
			}
			row.Narr = narr;
			row.Tuple = (reader.ReadInt32(), (reader.ReadInt32(), reader.ReadString()));
			rows.Add(row);
		}
		// Row 1
		Assert.Equal(1, rows[0].U8);
		Assert.Equal(2, rows[0].S8);
		Assert.Equal(3, rows[0].U16);
		Assert.Equal(4, rows[0].S16);
		Assert.Equal(5u, rows[0].U32);
		Assert.Equal(6, rows[0].S32);
		Assert.Equal(7ul, rows[0].U64);
		Assert.Equal(8, rows[0].S64);
		Assert.InRange(rows[0].F32, 1.4999, 1.5001);
		Assert.InRange(rows[0].F64, 2.8699, 2.8701);
		Assert.Equal("test_a", rows[0].Str);
		Assert.Equal("US", rows[0].FStr);
		Assert.Equal(new(2025, 1, 1), rows[0].Date);
		Assert.Equal(new(2200, 1, 1), rows[0].Date32);
		Assert.Equal(new(2025, 1, 1, 8, 0, 0), rows[0].Dt);
		Assert.Equal(new(2025, 1, 1, 10, 0, 0, 500), rows[0].Dt64);
		Assert.Equal(new(100, 23, 44), rows[0].Time);
		Assert.Equal(new(0, 1, 5, 10, 812), rows[0].Time64);
		Assert.Equal(new("61f0c404-5cb3-11e7-907b-a6006ad3dba0"), rows[0].Uuid);
		Assert.Equal("116.106.34.242", rows[0].IPv4.ToString());
		Assert.Equal("2a02:e980:1e::1", rows[0].IPv6.ToString());
		Assert.Equivalent(new[] { "a", "b", "c" }, rows[0].Arr);
		Assert.True(rows[0].Bool);
		Assert.Null(rows[0].Nullable);
		Assert.Equivalent(new[] { new sbyte[] { 8, 10 }, [12, 14] }, rows[0].Narr);
		Assert.Equal((1, (2, "tuple_a")), rows[0].Tuple);
		// Row 2
		Assert.Equal(2, rows[1].U8);
		Assert.Equal(3, rows[1].S8);
		Assert.Equal(4, rows[1].U16);
		Assert.Equal(5, rows[1].S16);
		Assert.Equal(6u, rows[1].U32);
		Assert.Equal(7, rows[1].S32);
		Assert.Equal(8ul, rows[1].U64);
		Assert.Equal(9, rows[1].S64);
		Assert.InRange(rows[1].F32, 1.7499, 1.7501);
		Assert.InRange(rows[1].F64, 2.9099, 2.9101);
		Assert.Equal("test_b", rows[1].Str);
		Assert.Equal("CA", rows[1].FStr);
		Assert.Equal(new(2025, 2, 1), rows[1].Date);
		Assert.Equal(new(2200, 2, 1), rows[1].Date32);
		Assert.Equal(new(2025, 2, 1, 9, 0, 0), rows[1].Dt);
		Assert.Equal(new(2025, 2, 1, 11, 0, 0, 750), rows[1].Dt64);
		Assert.Equal(new(8, 3, 15), rows[1].Time);
		Assert.Equal(new(0, 0, 0, 23, 400), rows[1].Time64);
		Assert.Equal(new("36a0b67c-b74a-4640-803b-e44bb4547e3c"), rows[1].Uuid);
		Assert.Equal("183.247.232.58", rows[1].IPv4.ToString());
		Assert.Equal("2a02:aa08:e000:3100::2", rows[1].IPv6.ToString());
		Assert.Equivalent(new[] { "d", "e" }, rows[1].Arr);
		Assert.False(rows[1].Bool);
		Assert.Equal((sbyte?)127, rows[1].Nullable);
		Assert.Equivalent(new[] { new sbyte[] { 5, 7 }, [9, 11] }, rows[1].Narr);
		Assert.Equal((3, (5, "tuple_b")), rows[1].Tuple);
	}

	public override async Task InitializeAsync()
	{
		await base.InitializeAsync();

		using (await ExecuteClickHouseQueryAsync(@"
			INSERT INTO test VALUES (
				1, 2, 3, 4, 5, 6, 7, 8,
				1.5, 2.87,
				'test_a', 'US',
				'2025-01-01', '2200-01-01', '2025-01-01 08:00:00',  '2025-01-01 10:00:00.500', '100:23:44', '01:05:10.812',
				'61f0c404-5cb3-11e7-907b-a6006ad3dba0', '116.106.34.242', '2a02:e980:1e::1',
				['a', 'b', 'c'], true, NULL,
				[[8, 10], [12, 14]], (1, (2, 'tuple_a'))
			), (
				2, 3, 4, 5, 6, 7, 8, 9,
				1.75, 2.91,
				'test_b', 'CA',
				'2025-02-01', '2200-02-01', '2025-02-01 09:00:00',  '2025-02-01 11:00:00.750', '08:03:15', '00:00:23.400',
				'36a0b67c-b74a-4640-803b-e44bb4547e3c', '183.247.232.58', '2a02:aa08:e000:3100::2',
				['d', 'e'], false, 127,
				[[5, 7], [9, 11]], (3, (5, 'tuple_b'))
			)"))
		{
		}
	}

	private record struct Row
	{
		public byte U8 { get; set; }
		public sbyte S8 { get; set; }
		public ushort U16 { get; set; }
		public short S16 { get; set; }
		public uint U32 { get; set; }
		public int S32 { get; set; }
		public ulong U64 { get; set; }
		public long S64 { get; set; }
		public float F32 { get; set; }
		public double F64 { get; set; }
		public string Str { get; set; }
		public string FStr { get; set; }
		public DateOnly Date { get; set; }
		public DateOnly Date32 { get; set; }
		public DateTime Dt { get; set; }
		public DateTime Dt64 { get; set; }
		public TimeSpan Time { get; set; }
		public TimeSpan Time64 { get; set; }
		public Guid Uuid { get; set; }
		public IPAddress IPv4 { get; set; }
		public IPAddress IPv6 { get; set; }
		public string[] Arr { get; set; }
		public bool Bool { get; set; }
		public sbyte? Nullable { get; set; }
		public sbyte[][] Narr { get; set; }
		public (int, (int, string)) Tuple { get; set; }
	}
}
