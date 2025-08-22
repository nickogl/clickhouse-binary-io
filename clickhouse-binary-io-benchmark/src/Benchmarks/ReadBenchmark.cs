using BenchmarkDotNet.Attributes;
using ClickHouse.Client.ADO;

namespace ClickHouse.BinaryIO.Benchmark.Benchmarks;

[MemoryDiagnoser(false)]
[MaxIterationCount(16)]
[HideColumns("Error", "StdDev", "RatioSD", "Median")]
public class ReadBenchmark
{
	private readonly ClickHouseConnection _connection = new(Database.ConnectionString);

	[Params(1_000_000, 5_000_000, 10_000_000)]
	public int RowCount { get; set; }

	[Benchmark(Baseline = true)]
	public async Task<ulong> Read_ClickHouseClient()
	{
		// NOTE: This does not use gzip compression due to the ADO parameter in the
		// connection string, ClickHouse.Client respects this setting for the ADO
		// interface. As such, we are also not compressing for the BinaryIO benchmark.
		await using var cmd = _connection.CreateCommand();
		cmd.CommandText = "SELECT * FROM test.benchmark";
		await using var reader = await cmd.ExecuteReaderAsync();
		ulong rows = 0;
		while (await reader.ReadAsync())
		{
			_ = reader.GetFieldValue<DateTime>(0); // ClickHouse.Client cannot handle DateOnly in the ADO reader impl
			_ = reader.GetFieldValue<string>(1);
			_ = reader.GetFieldValue<ushort>(2);
			_ = reader.GetFieldValue<byte>(3);
			_ = reader.GetFieldValue<byte>(4);
			_ = reader.GetFieldValue<byte>(5);
			_ = reader.GetFieldValue<ulong>(6);
			rows++;
		}
		return rows;
	}

	[Benchmark]
	public async Task<ulong> Read_ClickHouseBinaryIO()
	{
		using var response = await Database.ExecuteQueryAsync("SELECT * FROM test.benchmark FORMAT RowBinary");
		var stream = await response.Content.ReadAsStreamAsync();
		using var reader = new ClickHouseBinaryReader(stream, new());
		var lastString = new char[128];
		ulong rows = 0;
		while (!await reader.IsCompleteAsync())
		{
			_ = reader.ReadDate();
			_ = reader.ReadString(lastString);
			_ = reader.ReadUInt16();
			_ = reader.ReadUInt8();
			_ = reader.ReadUInt8();
			_ = reader.ReadUInt8();
			_ = reader.ReadUInt64();
			rows++;
		}
		return rows;
	}

	[GlobalSetup]
	public async Task Setup()
	{
		// Reuse the write benchmark to insert the rows we will later read
		var writeBenchmark = new WriteBenchmark() { RowCount = RowCount };
		await writeBenchmark.Setup();
		await writeBenchmark.Write_ClickHouseBinaryIO();
	}

	[GlobalCleanup]
	public Task Cleanup()
	{
		return new WriteBenchmark().Cleanup();
	}
}
