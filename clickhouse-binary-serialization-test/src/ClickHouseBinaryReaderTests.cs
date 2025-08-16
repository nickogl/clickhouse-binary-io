namespace ClickHouse.IO.Test;

public class ClickHouseBinaryReaderTests : ClickHouseTestBase
{
	public override async Task InitializeAsync()
	{
		await base.InitializeAsync();

		using (await ExecuteClickHouseQueryAsync(@"
			INSERT INTO test VALUES (
				1, 2, 3, 4, 5, 6, 7, 8,
				1.5, 2.87,
				'test_a', 'US',
				'2025-01-01', '2300-01-01', 1000000, '2025-01-01 08:00:00',  '2025-01-01 10:00:00.500',
				'61f0c404-5cb3-11e7-907b-a6006ad3dba0', '116.106.34.242', '2a02:e980:1e::1',
				['a', 'b', 'c'], true, NULL
			), (
				2, 3, 4, 5, 6, 7, 8, 9,
				1.75, 2.91,
				'test_b', 'CA',
				'2025-02-01', '2300-02-01', 2000000, '2025-02-01 09:00:00',  '2025-02-01 11:00:00.750',
				'36a0b67c-b74a-4640-803b-e44bb4547e3c', '183.247.232.58', '2a02:aa08:e000:3100::2',
				['d', 'e'], false, 127
			)"))
		{
		}
	}
}
