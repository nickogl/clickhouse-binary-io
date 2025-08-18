namespace ClickHouse.BinaryIO;

/// <summary>
/// Represents a ClickHouse column in a result set.
/// </summary>
public record struct ClickHouseColumn
{
	/// <summary>Name of the column.</summary>
	public string Name { get; set; }

	/// <summary>Type of the column.</summary>
	public ClickHouseType Type { get; set; }
}
