#pragma warning disable CS1591

namespace ClickHouse.IO;

/// <summary>
/// ClickHouse type names (without nesting and formats) supported by this library.
/// </summary>
public enum ClickHouseTypeName
{
	// Flat
	UInt8,
	Int8,
	UInt16,
	Int16,
	UInt32,
	Int32,
	UInt64,
	Int64,
	Float32,
	Float64,
	String,
	FixedString,
	Date,
	Date32,
	Time,
	DateTime,
	DateTime64,
	UUID,
	IPv4,
	IPv6,
	Bool,

	// Nested
	Array,
	Nullable,
	Tuple,
}
