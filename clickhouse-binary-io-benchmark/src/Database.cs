using Microsoft.AspNetCore.Http;
using System.Net.Http.Headers;

namespace ClickHouse.BinaryIO.Benchmark;

public static class Database
{
	public static string Scheme { get; } = Environment.GetEnvironmentVariable("CLICKHOUSE_SCHEME") ?? "http";
	public static string Host { get; } = Environment.GetEnvironmentVariable("CLICKHOUSE_HOST") ?? "127.0.0.1";
	public static short Port { get; } = short.Parse(Environment.GetEnvironmentVariable("CLICKHOUSE_PORT") ?? "8123");
	public static string User { get; } = Environment.GetEnvironmentVariable("CLICKHOUSE_USER") ?? "test";
	public static string Password { get; } = Environment.GetEnvironmentVariable("CLICKHOUSE_PASSWORD") ?? "test";
	public static string DatabaseName { get; } = Environment.GetEnvironmentVariable("CLICKHOUSE_DB") ?? "test";
	public static string ConnectionString { get; } = $"Host={Host};Port={Port};Username={User};Password={Password};Database={DatabaseName};Compression=false";

	public static Task<HttpResponseMessage> ExecuteQueryAsync(string sql)
	{
		return ExecuteQueryAsync(sql, httpContent: null);
	}

	public static async Task<HttpResponseMessage> ExecuteQueryAsync(string sql, HttpContent? httpContent)
	{
		var requestQuery = new QueryString()
			.Add("user", User)
			.Add("password", Password)
			.Add("database", DatabaseName)
			.Add("query", sql);
		var requestUri = new Uri($"{Scheme}://{Host}:{Port}{requestQuery}");
		using var request = new HttpRequestMessage(HttpMethod.Post, requestUri) { Content = httpContent };
		request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));

		using var httpClient = new HttpClient();
		var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
		if (response.StatusCode != System.Net.HttpStatusCode.OK)
		{
			throw new HttpRequestException(await response.Content.ReadAsStringAsync(), null, response.StatusCode);
		}
		return response;
	}
}
