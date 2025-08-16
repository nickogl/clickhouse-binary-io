using System.Diagnostics;
using System.Globalization;

namespace ClickHouse.IO;

/// <summary>
/// Represents a ClickHouse data type with potential nested data types.
/// </summary>
public sealed class ClickHouseType
{
	/// <summary>Underlying ClickHouse data type name.</summary>
	public required ClickHouseTypeName Name { get; init; }

	/// <summary>Nested types, e.g. Int8 in an Array(Int8).</summary>
	public required IReadOnlyList<ClickHouseType> NestedTypes { get; init; }

	/// <summary>Precision or length specifier, e.g. 2 in a FixedString(2).</summary>
	/// <remarks>This is <c>null</c> if the type does not support it.</remarks>
	public int? PrecisionOrLength { get; init; }

	/// <summary>
	/// Parse a ClickHouse type by name. The type may be a nested type.
	/// </summary>
	/// <param name="name">Name of the type. E.g. Array(Tuple(Int8, FixedString(2)))</param>
	/// <exception cref="ArgumentException">Unsupported or wrongly formatted type.</exception>
	/// <returns>The parsed type plus nested types if applicable.</returns>
	public static ClickHouseType Parse(ReadOnlySpan<char> name)
	{
		var result = ParseInternal(ref name);
		Debug.Assert(name.Length == 0);
		return result;
	}

	private static ClickHouseType ParseInternal(ref ReadOnlySpan<char> name)
	{
		// Quickly check for a simple type and return a precomputed instance.
		var nextTokenIndex = name.IndexOfAny(')', ',', ' ');
		var maybeSimpleType = nextTokenIndex == -1 ? name : name[..nextTokenIndex];
#if NET9_0_OR_GREATER
		if (_simpleTypesBySpan.TryGetValue(maybeSimpleType, out var type))
		{
			name = name[maybeSimpleType.Length..];
			return type;
		}
#else
		if (_simpleTypes.TryGetValue(new string(maybeSimpleType), out var type))
		{
			name = name[maybeSimpleType.Length..];
			return type;
		}
#endif

		// Check if the type is one of those with a precision or length specifier.
		if (name.StartsWith(FixedStringName, StringComparison.Ordinal))
		{
			name = name[FixedStringName.Length..];
			return new ClickHouseType()
			{
				Name = ClickHouseTypeName.FixedString,
				NestedTypes = [],
				PrecisionOrLength = ParsePrecisionOrLength(ref name, FixedStringName),
			};
		}
		if (name.StartsWith(DateTime64Name, StringComparison.Ordinal))
		{
			name = name[DateTime64Name.Length..];
			return new ClickHouseType()
			{
				Name = ClickHouseTypeName.DateTime64,
				NestedTypes = [],
				PrecisionOrLength = ParsePrecisionOrLength(ref name, DateTime64Name, defaultValue: 3),
			};
		}

		// Check if the type is a complex nested type.
		if (name.StartsWith(NullableName, StringComparison.Ordinal))
		{
			name = name[NullableName.Length..];
			return new ClickHouseType()
			{
				Name = ClickHouseTypeName.Nullable,
				NestedTypes = [ParseSingleNestedType(ref name, NullableName)],
			};
		}
		if (name.StartsWith(ArrayName, StringComparison.Ordinal))
		{
			name = name[ArrayName.Length..];
			return new ClickHouseType()
			{
				Name = ClickHouseTypeName.Array,
				NestedTypes = [ParseSingleNestedType(ref name, ArrayName)],
			};
		}
		if (name.StartsWith(TupleName, StringComparison.Ordinal))
		{
			name = name[TupleName.Length..];
			return new ClickHouseType()
			{
				Name = ClickHouseTypeName.Tuple,
				NestedTypes = ParseNestedTypes(ref name, TupleName),
			};
		}

		throw new ArgumentException($"Unsupported type '{name}'");
	}

	private static int ParsePrecisionOrLength(ref ReadOnlySpan<char> name, ReadOnlySpan<char> type, int? defaultValue = null)
	{
		if (name.Length == 0 || name[0] != '(')
		{
			if (defaultValue is not null)
			{
				return defaultValue.Value;
			}
			throw new ArgumentException($"Type '{type}' requires a format, but none was provided", nameof(name));
		}

		// Support (<arg>) and (<arg1>, <arg2>, ...)
		var endIndex = name.IndexOfAny(')', ',');
		if (endIndex == -1)
		{
			throw new ArgumentException($"Type '{type}' has invalid format '{name}': Could not find closing parenthesis", nameof(name));
		}
		var formatNumber = name[1..endIndex];
		if (!int.TryParse(formatNumber, NumberStyles.AllowLeadingWhite | NumberStyles.AllowTrailingWhite, null, out var result))
		{
			throw new ArgumentException($"Type '{type}' has invalid format '{name}': Could not convert to an integer", nameof(name));
		}

		// Skip remaining format arguments if needed
		var isEnd = name[endIndex++] == ')';
		var originalName = name;
		name = name[endIndex..];
		if (!isEnd)
		{
			endIndex = name.IndexOf(')');
			if (endIndex++ == -1)
			{
				throw new ArgumentException($"Type '{type}' has invalid format '{originalName}': Missing closing paranthesis", nameof(name));
			}
			name = name[endIndex..];
		}
		return result;
	}

	private static ClickHouseType ParseSingleNestedType(ref ReadOnlySpan<char> name, ReadOnlySpan<char> parentType)
	{
		if (name.Length == 0 || name[0] != '(')
		{
			throw new ArgumentException($"Type '{parentType}' requires a single nested type, but none was provided", nameof(name));
		}
		var originalName = name;
		name = name[1..];

		var result = ParseInternal(ref name);
		if (name[0] != ')')
		{
			throw new ArgumentException($"Type '{parentType}' has invalid nested type '{originalName}': Missing closing paranthesis", nameof(name));
		}
		name = name[1..];
		return result;
	}

	private static List<ClickHouseType> ParseNestedTypes(ref ReadOnlySpan<char> name, ReadOnlySpan<char> parentType)
	{
		if (name.Length == 0 || name[0] != '(')
		{
			throw new ArgumentException($"Type '{parentType}' requires nested types, but none were provided", nameof(name));
		}
		var originalName = name;
		name = name[1..];

		var result = new List<ClickHouseType>(capacity: 2);
		while (true)
		{
			if (!SkipFieldName(ref name))
			{
				throw new ArgumentException($"Type '{parentType}' has invalid nested types '{originalName}': Every nested type must be preceded by its field name", nameof(name));
			}

			result.Add(ParseInternal(ref name));
			var separatorOrEndIndex = name.IndexOfAny(',', ')');
			if (separatorOrEndIndex == -1)
			{
				throw new ArgumentException($"Type '{parentType}' has invalid nested types '{originalName}': Missing closing paranthesis", nameof(name));
			}
			if (name[separatorOrEndIndex++] == ')')
			{
				name = name[separatorOrEndIndex..];
				return result;
			}
			name = name[separatorOrEndIndex..];
		}
	}

	private static bool SkipFieldName(ref ReadOnlySpan<char> name)
	{
		bool encounteredFieldName = false;
		while (name.Length > 0)
		{
			var ch = name[0];
			name = name[1..];
			if (ch == ' ' || ch == '\t' || ch == '\n')
			{
				if (encounteredFieldName)
				{
					return true;
				}
			}
			else if (ch != ',' && ch != '(' && ch != ')')
			{
				encounteredFieldName = true;
			}
		}
		return false;
	}

	private ClickHouseType()
	{
	}

	private const string FixedStringName = "FixedString";
	private const string DateTime64Name = "DateTime64";
	private const string NullableName = "Nullable";
	private const string ArrayName = "Array";
	private const string TupleName = "Tuple";

	private readonly static Dictionary<string, ClickHouseType> _simpleTypes = new(StringComparer.Ordinal)
	{
		[ClickHouseTypeName.UInt8.ToString()] = new() { Name = ClickHouseTypeName.UInt8, NestedTypes = [] },
		[ClickHouseTypeName.Int8.ToString()] = new() { Name = ClickHouseTypeName.Int8, NestedTypes = [] },
		[ClickHouseTypeName.UInt16.ToString()] = new() { Name = ClickHouseTypeName.UInt16, NestedTypes = [] },
		[ClickHouseTypeName.Int16.ToString()] = new() { Name = ClickHouseTypeName.Int16, NestedTypes = [] },
		[ClickHouseTypeName.UInt32.ToString()] = new() { Name = ClickHouseTypeName.UInt32, NestedTypes = [] },
		[ClickHouseTypeName.Int32.ToString()] = new() { Name = ClickHouseTypeName.Int32, NestedTypes = [] },
		[ClickHouseTypeName.UInt64.ToString()] = new() { Name = ClickHouseTypeName.UInt64, NestedTypes = [] },
		[ClickHouseTypeName.Int64.ToString()] = new() { Name = ClickHouseTypeName.Int64, NestedTypes = [] },
		[ClickHouseTypeName.Float32.ToString()] = new() { Name = ClickHouseTypeName.Float32, NestedTypes = [] },
		[ClickHouseTypeName.Float64.ToString()] = new() { Name = ClickHouseTypeName.Float64, NestedTypes = [] },
		[ClickHouseTypeName.String.ToString()] = new() { Name = ClickHouseTypeName.String, NestedTypes = [] },
		[ClickHouseTypeName.Date.ToString()] = new() { Name = ClickHouseTypeName.Date, NestedTypes = [] },
		[ClickHouseTypeName.Date32.ToString()] = new() { Name = ClickHouseTypeName.Date32, NestedTypes = [] },
		[ClickHouseTypeName.Time.ToString()] = new() { Name = ClickHouseTypeName.Time, NestedTypes = [] },
		[ClickHouseTypeName.DateTime.ToString()] = new() { Name = ClickHouseTypeName.DateTime, NestedTypes = [] },
		[ClickHouseTypeName.UUID.ToString()] = new() { Name = ClickHouseTypeName.UUID, NestedTypes = [] },
		[ClickHouseTypeName.IPv4.ToString()] = new() { Name = ClickHouseTypeName.IPv4, NestedTypes = [] },
		[ClickHouseTypeName.IPv6.ToString()] = new() { Name = ClickHouseTypeName.IPv6, NestedTypes = [] },
		[ClickHouseTypeName.Bool.ToString()] = new() { Name = ClickHouseTypeName.Bool, NestedTypes = [] },
	};
#if NET9_0_OR_GREATER
	private readonly static Dictionary<string, ClickHouseType>.AlternateLookup<ReadOnlySpan<char>> _simpleTypesBySpan
		= _simpleTypes.GetAlternateLookup<ReadOnlySpan<char>>();
#endif
}
