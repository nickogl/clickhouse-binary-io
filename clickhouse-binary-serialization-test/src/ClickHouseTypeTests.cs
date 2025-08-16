namespace ClickHouse.IO.Test;

public class ClickHouseTypeTests
{
	[Theory]
	[InlineData("UInt8")]
	[InlineData("Int8")]
	[InlineData("UInt16")]
	[InlineData("Int16")]
	[InlineData("UInt32")]
	[InlineData("Int32")]
	[InlineData("UInt64")]
	[InlineData("Int64")]
	[InlineData("Float32")]
	[InlineData("Float64")]
	[InlineData("String")]
	[InlineData("Date")]
	[InlineData("Date32")]
	[InlineData("Time")]
	[InlineData("DateTime")]
	[InlineData("UUID")]
	[InlineData("IPv4")]
	[InlineData("IPv6")]
	[InlineData("Bool")]
	public void ParsesSimpleTypes(string name)
	{
		var type = ClickHouseType.Parse(name);
		Assert.Equal(name, type.Name.ToString());
		Assert.Empty(type.NestedTypes);
		Assert.Null(type.PrecisionOrLength);
	}

	[Fact]
	public void ParsesFixedString()
	{
		var type = ClickHouseType.Parse("FixedString(32)");
		Assert.Equal(ClickHouseTypeName.FixedString, type.Name);
		Assert.Equal(32, type.PrecisionOrLength);
		Assert.Empty(type.NestedTypes);
	}

	[Theory]
	[InlineData("DateTime64", 3)]
	[InlineData("DateTime64(4)", 4)]
	[InlineData("DateTime64(5, 'UTC')", 5)]
	public void ParsesDateTime64(string name, int expectedPrecision)
	{
		var type = ClickHouseType.Parse(name);
		Assert.Equal(ClickHouseTypeName.DateTime64, type.Name);
		Assert.Equal(expectedPrecision, type.PrecisionOrLength);
		Assert.Empty(type.NestedTypes);
	}

	[Fact]
	public void ParsesNullable()
	{
		var type = ClickHouseType.Parse("Nullable(Int32)");
		Assert.Equal(ClickHouseTypeName.Nullable, type.Name);
		Assert.Null(type.PrecisionOrLength);
		Assert.Single(type.NestedTypes);
		Assert.Equal(ClickHouseTypeName.Int32, type.NestedTypes[0].Name);
		Assert.Null(type.NestedTypes[0].PrecisionOrLength);
		Assert.Empty(type.NestedTypes[0].NestedTypes);
	}

	[Fact]
	public void ParsesArrays()
	{
		var type = ClickHouseType.Parse("Array(Bool)");
		Assert.Equal(ClickHouseTypeName.Array, type.Name);
		Assert.Null(type.PrecisionOrLength);
		Assert.Single(type.NestedTypes);
		Assert.Equal(ClickHouseTypeName.Bool, type.NestedTypes[0].Name);
		Assert.Null(type.NestedTypes[0].PrecisionOrLength);
		Assert.Empty(type.NestedTypes[0].NestedTypes);
	}

	[Fact]
	public void ParsesTuples()
	{
		var type = ClickHouseType.Parse("Tuple(a Int16, b String)");
		Assert.Equal(ClickHouseTypeName.Tuple, type.Name);
		Assert.Null(type.PrecisionOrLength);
		Assert.Equal(2, type.NestedTypes.Count);
		Assert.Equal(ClickHouseTypeName.Int16, type.NestedTypes[0].Name);
		Assert.Null(type.NestedTypes[0].PrecisionOrLength);
		Assert.Empty(type.NestedTypes[0].NestedTypes);
		Assert.Equal(ClickHouseTypeName.String, type.NestedTypes[1].Name);
		Assert.Null(type.NestedTypes[1].PrecisionOrLength);
		Assert.Empty(type.NestedTypes[1].NestedTypes);
	}

	[Fact]
	public void ParsesComplexNestedType()
	{
		var type = ClickHouseType.Parse("Array(Tuple(a Nullable(String), b UInt64, c Array(Tuple(d DateTime, e Float32))))");
		Assert.Equal(ClickHouseTypeName.Array, type.Name);
		Assert.Null(type.PrecisionOrLength);
		Assert.Single(type.NestedTypes);
		Assert.Equal(ClickHouseTypeName.Tuple, type.NestedTypes[0].Name);
		Assert.Null(type.NestedTypes[0].PrecisionOrLength);
		Assert.Equal(3, type.NestedTypes[0].NestedTypes.Count);
		Assert.Equal(ClickHouseTypeName.Nullable, type.NestedTypes[0].NestedTypes[0].Name);
		Assert.Null(type.NestedTypes[0].NestedTypes[0].PrecisionOrLength);
		Assert.Single(type.NestedTypes[0].NestedTypes[0].NestedTypes);
		Assert.Equal(ClickHouseTypeName.String, type.NestedTypes[0].NestedTypes[0].NestedTypes[0].Name);
		Assert.Null(type.NestedTypes[0].NestedTypes[0].NestedTypes[0].PrecisionOrLength);
		Assert.Empty(type.NestedTypes[0].NestedTypes[0].NestedTypes[0].NestedTypes);
		Assert.Equal(ClickHouseTypeName.UInt64, type.NestedTypes[0].NestedTypes[1].Name);
		Assert.Null(type.NestedTypes[0].NestedTypes[1].PrecisionOrLength);
		Assert.Empty(type.NestedTypes[0].NestedTypes[1].NestedTypes);
		Assert.Equal(ClickHouseTypeName.Array, type.NestedTypes[0].NestedTypes[2].Name);
		Assert.Null(type.NestedTypes[0].NestedTypes[2].PrecisionOrLength);
		Assert.Single(type.NestedTypes[0].NestedTypes[2].NestedTypes);
		Assert.Equal(ClickHouseTypeName.Tuple, type.NestedTypes[0].NestedTypes[2].NestedTypes[0].Name);
		Assert.Null(type.NestedTypes[0].NestedTypes[2].NestedTypes[0].PrecisionOrLength);
		Assert.Equal(2, type.NestedTypes[0].NestedTypes[2].NestedTypes[0].NestedTypes.Count);
		Assert.Equal(ClickHouseTypeName.DateTime, type.NestedTypes[0].NestedTypes[2].NestedTypes[0].NestedTypes[0].Name);
		Assert.Null(type.NestedTypes[0].NestedTypes[2].NestedTypes[0].NestedTypes[0].PrecisionOrLength);
		Assert.Empty(type.NestedTypes[0].NestedTypes[2].NestedTypes[0].NestedTypes[0].NestedTypes);
		Assert.Equal(ClickHouseTypeName.Float32, type.NestedTypes[0].NestedTypes[2].NestedTypes[0].NestedTypes[1].Name);
		Assert.Null(type.NestedTypes[0].NestedTypes[2].NestedTypes[0].NestedTypes[1].PrecisionOrLength);
		Assert.Empty(type.NestedTypes[0].NestedTypes[2].NestedTypes[0].NestedTypes[1].NestedTypes);
	}

	[Theory]
	[InlineData("JSON")]
	[InlineData("Variant")]
	[InlineData("Map")]
	[InlineData("BFloat16")]
	[InlineData("Array")]
	[InlineData("Array()")]
	[InlineData("Array(Int8")]
	[InlineData("Array(Int8, String)")]
	[InlineData("Nullable")]
	[InlineData("Nullable()")]
	[InlineData("Nullable(Bool")]
	[InlineData("Nullable(String, UInt32)")]
	[InlineData("Tuple")]
	[InlineData("Tuple()")]
	[InlineData("Tuple(DateTime)")]
	[InlineData("Tuple(field Date")]
	[InlineData("Tuple(field Date,)")]
	[InlineData("Tuple(field UInt16(String))")]
	public void RejectsInvalidOrUnsupportedTypes(string name)
	{
		Assert.Throws<ClickHouseTypeParseException>(() => ClickHouseType.Parse(name));
	}
}
