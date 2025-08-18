
using Microsoft.AspNetCore.Http;
using System.Net.Http.Headers;

namespace ClickHouse.BinaryIO.Test;

[Collection("Database")]
public abstract class ClickHouseDatabaseTestBase : IDisposable, IAsyncLifetime
{
	public static string ClickHouseScheme { get; } = Environment.GetEnvironmentVariable("CLICKHOUSE_SCHEME") ?? "http";
	public static string ClickHouseHost { get; } = Environment.GetEnvironmentVariable("CLICKHOUSE_HOST") ?? "127.0.0.1";
	public static short ClickHousePort { get; } = short.Parse(Environment.GetEnvironmentVariable("CLICKHOUSE_PORT") ?? "8123");
	public static string ClickHouseUser { get; } = Environment.GetEnvironmentVariable("CLICKHOUSE_USER") ?? "test";
	public static string ClickHousePassword { get; } = Environment.GetEnvironmentVariable("CLICKHOUSE_PASSWORD") ?? "test";
	public static string ClickHouseDatabase { get; } = Environment.GetEnvironmentVariable("CLICKHOUSE_DB") ?? "test";

	private readonly HttpClient _httpClient = new();

	protected Task<HttpResponseMessage> ExecuteClickHouseQueryAsync(string sql)
	{
		return ExecuteClickHouseQueryAsync(sql, httpContent: null);
	}

	protected async Task<HttpResponseMessage> ExecuteClickHouseQueryAsync(string sql, HttpContent? httpContent)
	{
		var requestQuery = new QueryString()
			.Add("user", ClickHouseUser)
			.Add("password", ClickHousePassword)
			.Add("database", ClickHouseDatabase)
			.Add("query", sql);
		var requestUri = new Uri($"{ClickHouseScheme}://{ClickHouseHost}:{ClickHousePort}{requestQuery}");
		using var request = new HttpRequestMessage(HttpMethod.Post, requestUri) { Content = httpContent };
		request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));

		var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
		if (response.StatusCode != System.Net.HttpStatusCode.OK)
		{
			throw new HttpRequestException(await response.Content.ReadAsStringAsync(), null, response.StatusCode);
		}
		return response;
	}

	public virtual void Dispose()
	{
		_httpClient.Dispose();

		GC.SuppressFinalize(this);
	}

	public virtual async Task InitializeAsync()
	{
		using (await ExecuteClickHouseQueryAsync(@"
			CREATE TABLE IF NOT EXISTS test (
				u8 UInt8, s8 Int8, u16 UInt16, s16 Int16, u32 UInt32, s32 Int32, u64 UInt64, s64 Int64,
				f32 Float32, f64 Float64,
				str String, fstr FixedString(2),
				date Date, date32 Date32, dt DateTime, dt64 DateTime64, time Time, time64 Time64,
				uuid UUID, ipv4 IPv4, ipv6 IPv6,
				arr Array(String), bool Bool, nullable Nullable(Int8),
				narr Array(Array(Int8)), tuple Tuple(a Int32, b Tuple(c Int32, d String))
			)
			ENGINE = MergeTree
			ORDER BY (u8)
			SETTINGS enable_time_time64_type = 1"))
		{
		}

		using (await ExecuteClickHouseQueryAsync("TRUNCATE test"))
		{
		}
	}

	public virtual async Task DisposeAsync()
	{
		using (await ExecuteClickHouseQueryAsync("DROP TABLE test"))
		{
		}
	}
}
