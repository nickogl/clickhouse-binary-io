# ClickHouse Binary IO

A fast, zero-dependency, near zero-allocation library for reading and writing large volumes of ClickHouse data in .NET.

## Usage

### Writing data

<details>
<summary>Expand code</summary>

```csharp
class RowHttpContent(Row[] rows) : HttpContent
{
  protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
  {
    return SerializeToStreamAsync(stream, context, default);
  }

  protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
  {
    await using var writer = new ClickHouseBinaryWriter(stream, new ClickHouseBinaryWriterOptions());
    for (int i = 0; i < rows.Length; i++)
    {
      var row = rows[i];
      writer.WriteDate(row.Day);
      writer.WriteString(row.PageUrl);
      writer.WriteUInt16(row.CountryId);
      writer.WriteUInt8(row.BrowserId);
      writer.WriteUInt8(row.DeviceId);
      writer.WriteUInt8(row.OsId);
      writer.WriteUInt64(row.Visits);

      // Flush every once in a while to avoid blocking writes to the underlying stream once the buffer is full
      if ((i % 50_000) != 0)
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

var sql = "INSERT INTO xyz FORMAT RowBinary";
var queryParams = new QueryString().Add("user", "...").Add("password", "...").Add("database", "...").Add("query", sql);

using var request = new HttpRequestMessage(HttpMethod.Post, $"http://localhost:8123{queryParams}");
request.Content = new RowHttpContent(rows);
request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));

using var httpClient = new HttpClient();
await httpClient.SendAsync(request, cancellationToken);
```

</details>

### Reading data

<details>
<summary>Expand code</summary>

```csharp
var sql = "SELECT * FROM xyz FORMAT RowBinary";
var queryParams = new QueryString().Add("user", "...").Add("password", "...").Add("database", "...").Add("query", sql);

using var request = new HttpRequestMessage(HttpMethod.Post, $"http://localhost:8123{queryParams}");
request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));

using var httpClient = new HttpClient();
using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
var stream = await response.Content.ReadAsStreamAsync(cancellationToken);

using var reader = new ClickHouseBinaryReader(stream, new ClickHouseBinaryReaderOptions());
var rows = new List<Row>();
while (!await reader.IsCompleteAsync(cancellationToken))
{
  rows.Add(new Row()
  {
    Day = reader.ReadDate(),
    PageUrl = reader.ReadString(),
    CountryId = reader.ReadUInt16(),
    BrowserId = reader.ReadUInt8(),
    DeviceId = reader.ReadUInt8(),
    OsId = reader.ReadUInt8(),
    Visits = reader.ReadUInt64(),
  });
}
```

It's important to use `HttpCompletionOption.ResponseHeadersRead` to stream the response rather than buffer it as a whole in-memory.

</details>

## Benchmarks

The baseline for the benchmarks is the ClickHouse.Client library. It does not offer functionality to query data with the
RowBinary format. It does offer inserting data with the RowBinary format through the ClickHouseBulkCopy class. However,
it forces you to prepare your data as an `IEnumerable<object[]>`, causing a bunch of boxing and allocations as a result,
leading to performance degradation.

All benchmarks were performed on an unspecified cloud service provider who uses AMD EPYC CPUs, against ClickHouse v25.6, on a 10 GBit/s network.

### Writing

Please note that this benchmark excludes both the computing and memory overhead to convert the row data to an
`IEnumerable<object[]>` for ClickHouse.Client's ClickHouseBulkCopy (i.e. the best case scenario albeit unrealistic).

| Method                   | RowCount | Mean      | Ratio | Allocated   | Alloc Ratio |
|------------------------- |--------- |----------:|------:|------------:|------------:|
| Write_ClickHouseClient   | 100000   |  50.15 ms |  1.03 | 9343.83 KB  |        1.00 |
| Write_ClickHouseBinaryIO | 100000   |  37.26 ms |  0.76 |  812.48 KB* |        0.09 |
|                          |          |           |       |             |             |
| Write_ClickHouseClient   | 1000000  | 109.61 ms |  1.16 | 9535.29 KB  |        1.00 |
| Write_ClickHouseBinaryIO | 1000000  |  53.37 ms |  0.56 |  813.07 KB* |        0.09 |
|                          |          |           |       |             |             |
| Write_ClickHouseClient   | 10000000 | 556.08 ms |  1.00 | 9304.19 KB  |        1.00 |
| Write_ClickHouseBinaryIO | 10000000 | 399.63 ms |  0.72 |  812.83 KB* |        0.09 |

\* The only allocations here are an instance of the class and resources used by HttpClient for 8 concurrent inserts

### Reading

| Method                  | RowCount | Mean        | Ratio | Allocated      | Alloc Ratio |
|------------------------ |--------- |------------:|------:|---------------:|------------:|
| Read_ClickHouseClient   | 1000000  |   327.99 ms |  1.00 |  172400.84 KB* |       1.000 |
| Read_ClickHouseBinaryIO | 1000000  |    37.62 ms |  0.11 |       25.3 KB  |       0.000 |
|                         |          |             |       |                |             |
| Read_ClickHouseClient   | 5000000  | 1,653.15 ms |  1.00 |  859901.87 KB* |       1.000 |
| Read_ClickHouseBinaryIO | 5000000  |   150.85 ms |  0.09 |      25.31 KB  |       0.000 |
|                         |          |             |       |                |             |
| Read_ClickHouseClient   | 10000000 | 3,488.92 ms |  1.00 | 1719275.91 KB* |       1.000 |
| Read_ClickHouseBinaryIO | 10000000 |   298.46 ms |  0.09 |      25.31 KB  |       0.000 |

\* A good chunk of the allocations here are strings, which are unavoidable with ClickHouse.Client

## Diagnostics

For reading data, this library provides diagnostics in debug mode if you request the output format `RowBinaryWithNamesAndTypes`.
It will assert that you read the correct type for all columns, also for complex nested types.

```csharp
// Assume a table: u32 UInt32, fstr FixedString(2), arr Array(Tuple(Int8, Int16))
var sql = "SELECT * FROM xyz FORMAT RowBinaryWithNamesAndTypes";
...
using var reader = new ClickHouseBinaryReader(stream, new ClickHouseBinaryReaderOptions());
var columns = await reader.ReadColumnsAsync(cancellationToken);
while (!await reader.IsCompleteAsync(cancellationToken))
{
  _ = reader.ReadUInt64(); // error, expected to read UInt32
  _ = reader.ReadFixedString(8); // error, expected to read fixed string with length 2, not 8
  var arrayLength = reader.ReadArrayLength();
  for (int i = 0; i < arrayLength; i++)
  {
    _ = reader.ReadInt8();
    _ = reader.ReadInt32(); // error, expected to read nested type Int16
  }
}
```

## Avoiding allocations

There are a few methods in `ClickHouseBinaryReader` that allocate, but this library provides alternatives to avoid
those allocations if necessary. The diagnostics define exceptions to allow for these alternatives.

### ReadString

Use `ReadString` overloads that take a `Span<char>`.
```csharp
var stringBuffer = new char[128];
while (!await reader.IsCompleteAsync(cancellationToken))
{
  reader.ReadString(stringBuffer);
  // process stringBuffer
}
```

### ReadFixedString

Same as `ReadString`.

### ReadIPv4

Use `ReadBytes(4)` and process the octets manually. Note that ClickHouse transmits the octets in reverse order.
```csharp
record struct IPv4(byte First, byte Second, byte Third, byte Fourth);

var ips = new List<IPv4>();
while (!await reader.IsCompleteAsync(cancellationToken))
{
  var view = reader.ReadBytes(4);
  ips.Add(new(view[3], view[2], view[1], view[0]));
}
```

### ReadIPv6

Use `ReadBytes(16)` and process the octet pairs manually.

```csharp
record struct IPv6(ushort First, ..., ushort Eigth);

var ips = new List<IPv6>();
while (!await reader.IsCompleteAsync(cancellationToken))
{
  var view = reader.ReadBytes(16);
  ips.Add(new(BinaryPrimitives.ReadUInt16LittleEndian(view[0..2]), ..., BinaryPrimitives.ReadUInt16LittleEndian(view[14..16])));
}
```

## Special usage

### Array

Array data contains a length, immediately following by `length` items:

```csharp
// Assume a table: column Array(UInt8)
while (!await reader.IsCompleteAsync(cancellationToken))
{
  int arrayLength = reader.ReadArrayLength();
  for (int i = 0; i < arrayLength; i++)
  {
    _ = reader.ReadUInt8();
  }
}
```

### Nullable

Nullable data contains a boolean, which if `false`, is immediately followed by the non-null value.

```csharp
// Assume a table: column Nullable(String)
while (!await reader.IsCompleteAsync(cancellationToken))
{
  string? value = reader.IsNull() ? null : reader.ReadString();
}
```

### Tuple

Tuples are simply flattened in the data stream, as if they were separate columns:

```csharp
// Assume a table: column Array(Tuple(String, Int32))
while (!await reader.IsCompleteAsync(cancellationToken))
{
  int arrayLength = reader.ReadArrayLength();
  for (int i = 0; i < arrayLength; i++)
  {
    _ = reader.ReadString();
    _ = reader.ReadInt32();
  }
}
```

## Supported types

Every type is supported (also complex nested types), except the following: BFloat16, Decimal, Enum, Map, Variant, Geo-related types, JSON

Feel free to create a PR! :)

## Running tests

```sh
./run-test-db.sh &
dotnet test -m:1
```