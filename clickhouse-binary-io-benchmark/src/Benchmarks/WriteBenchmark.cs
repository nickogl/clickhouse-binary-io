using BenchmarkDotNet.Attributes;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using System.Data;
using System.IO.Compression;
using System.Net;

namespace ClickHouse.BinaryIO.Benchmark.Benchmarks;

[MemoryDiagnoser(false)]
[MaxIterationCount(16)]
[HideColumns("Error", "StdDev", "RatioSD", "Median")]
public class WriteBenchmark
{
	private readonly static int _concurrency = Environment.ProcessorCount;

	private Row[] _rows = null!;
	private IEnumerable<object[]> _rowsAsObjectArrays = null!;
	private ClickHouseConnection _connection = null!;

	[Params(100_000, 1_000_000, 10_000_000)]
	public int RowCount { get; set; }

	[Benchmark(Baseline = true)]
	public async Task Write_ClickHouseClient()
	{
		// Technically we would need to include converting the rows into an IEnumerable<object[]>,
		// but let's assume someone actually stores this bloat in memory in favor of ClickHouse.Client
		using var bulkCopy = new ClickHouseBulkCopy(_connection)
		{
			DestinationTableName = "test.benchmark",
			ColumnNames = ["day", "page_url", "country_id", "browser_id", "device_id", "os_id", "visits"],
			BatchSize = _rows.Length / _concurrency,
			MaxDegreeOfParallelism = _concurrency,
		};
		await bulkCopy.InitAsync();
		await bulkCopy.WriteToServerAsync(_rowsAsObjectArrays);
	}

	[Benchmark]
	public async Task Write_ClickHouseBinaryIO()
	{
		var batchSize = _rows.Length / _concurrency;
		var tasks = new Task[_concurrency];
		for (int i = 0; i < _concurrency; i++)
		{
			var rowsHttpContent = new RowsHttpContent(_rows, i * batchSize, Math.Min(batchSize, _rows.Length - i * batchSize));
			tasks[i] = Database.ExecuteQueryAsync("INSERT INTO test.benchmark FORMAT RowBinary", rowsHttpContent);
		}
		await Task.WhenAll(tasks);
	}

	[GlobalSetup]
	public async Task Setup()
	{
		using (await Database.ExecuteQueryAsync(@"
			CREATE TABLE IF NOT EXISTS benchmark (
				day Date,
				page_url String,
				country_id UInt16,
				browser_id UInt8,
				device_id UInt8,
				os_id UInt8,
				visits UInt64
			)
			ENGINE = MergeTree
			PARTITION BY day
			ORDER BY (day, page_url, country_id, browser_id, device_id, os_id)"))
		{
		}

		_rows = new Row[RowCount];
		string[] urls = ["/url1", "/url2", "/url3", "/url4", "/url5"];
		for (int i = 0; i < _rows.Length; i++)
		{
			_rows[i] = new Row()
			{
				Day = new(2025, 8, 21),
				PageUrl = Random.Shared.GetItems(urls, 1)[0],
				CountryId = (ushort)Random.Shared.Next(1, 6),
				BrowserId = (byte)Random.Shared.Next(1, 6),
				DeviceId = (byte)Random.Shared.Next(1, 6),
				OsId = (byte)Random.Shared.Next(1, 6),
				Visits = (ulong)Random.Shared.NextInt64(1, 1_000)
			};
		}
		_rowsAsObjectArrays = [.. _rows.Select(row => new object[] { row.Day, row.PageUrl, row.CountryId, row.BrowserId, row.DeviceId, row.OsId, row.Visits })];

		_connection = new ClickHouseConnection(Database.ConnectionString);
	}

	[GlobalCleanup]
	public async Task Cleanup()
	{
		_rows = [];
		_rowsAsObjectArrays = [];
		_connection?.Dispose();

		using (await Database.ExecuteQueryAsync("DROP TABLE benchmark"))
		{
		}
	}

	[IterationCleanup]
	public void TruncateTable()
	{
		using (Database.ExecuteQueryAsync("TRUNCATE TABLE benchmark").GetAwaiter().GetResult())
		{
		}
	}

	private readonly struct Row
	{
		public required DateOnly Day { get; init; }
		public required string PageUrl { get; init; }
		public required ushort CountryId { get; init; }
		public required byte BrowserId { get; init; }
		public required byte DeviceId { get; init; }
		public required byte OsId { get; init; }
		public required ulong Visits { get; init; }
	}

	private sealed class RowsHttpContent : HttpContent
	{
		private readonly Row[] _rows;
		private readonly int _offset;
		private readonly int _length;

		public RowsHttpContent(Row[] rows, int offset, int length)
		{
			_rows = rows;
			_offset = offset;
			_length = length;

			// ClickHouse.Client's ClickHouseBulkCopy always compresses with GZip, so do the same to have a fair comparison
			Headers.Add("Content-Encoding", "gzip");
		}

		protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
		{
			return SerializeToStreamAsync(stream, context, default);
		}

		protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
		{
			await using var gzipStream = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true);
			await using var writer = new ClickHouseBinaryWriter(gzipStream, new());
			for (int j = 0; j < _length; j++)
			{
				var row = _rows[_offset + j];
				writer.WriteDate(row.Day);
				writer.WriteString(row.PageUrl);
				writer.WriteUInt16(row.CountryId);
				writer.WriteUInt8(row.BrowserId);
				writer.WriteUInt8(row.DeviceId);
				writer.WriteUInt8(row.OsId);
				writer.WriteUInt64(row.Visits);

				if ((j % 40_000) == 0)
				{
					await writer.FlushAsync(cancellationToken);
				}
			}
		}

		protected override bool TryComputeLength(out long length)
		{
			length = 0;
			return false;
		}
	}
}
