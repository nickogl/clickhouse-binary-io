using System.Diagnostics;
using System.Globalization;

namespace ClickHouse.BinaryIO;

/// <summary>
/// Represents a ClickHouse data type with potential nested data types.
/// </summary>
public sealed class ClickHouseType
{
	private IReadOnlyList<ClickHouseType> _nestedTypes = null!;

	/// <summary>Underlying ClickHouse data type name.</summary>
	public required ClickHouseTypeName Name { get; init; }

	/// <summary>Nested types, e.g. Int8 in an Array(Int8).</summary>
	public required IReadOnlyList<ClickHouseType> NestedTypes
	{
		get => _nestedTypes;
		init
		{
			_nestedTypes = value;
			foreach (var type in _nestedTypes)
			{
				type.Parent = this;
			}
		}
	}

	/// <summary>Precision or length specifier, e.g. 2 in a FixedString(2).</summary>
	/// <remarks>Null if the type does not support it.</remarks>
	public int? PrecisionOrLength { get; init; }

	/// <summary>In tuples, the name of the field associated with this type.</summary>
	public string? FieldName { get; set; }

	/// <summary>The parent of this type.</summary>
	/// <remarks>Null if this type is not a nested type.</remarks>
	public ClickHouseType? Parent { get; set; }

	/// <summary>Get the root type of this type.</summary>
	/// <remarks>This is the type itself if it is not a nested type.</remarks>
	public ClickHouseType Root
	{
		get
		{
			var root = this;
			while (root.Parent is not null)
			{
				root = root.Parent;
			}
			return root;
		}
	}

	/// <summary>
	/// Converts this type back to its ClickHouse string representation.
	/// </summary>
	public override string ToString()
	{
		Span<char> result = stackalloc char[1024];
		int written = 0;
		ToStringInternal(result, ref written);
		return new string(result[..written]);
	}

	/// <summary>
	/// Parse a ClickHouse type by name. The type may be a nested type.
	/// </summary>
	/// <param name="name">Name of the type. E.g. Array(Tuple(Int8, FixedString(2)))</param>
	/// <exception cref="ClickHouseTypeParseException">Unsupported or wrongly formatted type.</exception>
	/// <returns>The parsed type plus nested types if applicable.</returns>
	public static ClickHouseType Parse(ReadOnlySpan<char> name)
	{
		var result = ParseInternal(ref name);
		Debug.Assert(name.IsEmpty);
		return result;
	}

	private static ClickHouseType ParseInternal(ref ReadOnlySpan<char> name)
	{
		// Quickly check for a simple type and return a precomputed instance
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

		// Check if the type is one of those with a precision or length specifier
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
		if (name.StartsWith(Time64Name, StringComparison.Ordinal))
		{
			name = name[Time64Name.Length..];
			return new ClickHouseType()
			{
				Name = ClickHouseTypeName.Time64,
				NestedTypes = [],
				PrecisionOrLength = ParsePrecisionOrLength(ref name, Time64Name, defaultValue: 3),
			};
		}

		// Check if the type is a complex nested type
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

		throw new ClickHouseTypeParseException($"Unsupported type '{name}'");
	}

	private static int ParsePrecisionOrLength(ref ReadOnlySpan<char> name, ReadOnlySpan<char> type, int? defaultValue = null)
	{
		if (name.IsEmpty || name[0] != '(')
		{
			if (defaultValue is not null)
			{
				return defaultValue.Value;
			}
			throw new ClickHouseTypeParseException($"Type '{type}' requires a format, but none was provided");
		}

		// Support (<arg>) and (<arg1>, <arg2>, ...)
		var endIndex = name.IndexOfAny(')', ',');
		if (endIndex == -1)
		{
			throw new ClickHouseTypeParseException($"Type '{type}' has invalid format '{name}': Could not find closing parenthesis");
		}
		var formatNumber = name[1..endIndex];
		if (!int.TryParse(formatNumber, NumberStyles.AllowLeadingWhite | NumberStyles.AllowTrailingWhite, null, out var result))
		{
			throw new ClickHouseTypeParseException($"Type '{type}' has invalid format '{name}': Could not convert to an integer");
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
				throw new ClickHouseTypeParseException($"Type '{type}' has invalid format '{originalName}': Missing closing paranthesis");
			}
			name = name[endIndex..];
		}
		return result;
	}

	private static ClickHouseType ParseSingleNestedType(ref ReadOnlySpan<char> name, ReadOnlySpan<char> parentType)
	{
		if (name.IsEmpty || name[0] != '(')
		{
			throw new ClickHouseTypeParseException($"Type '{parentType}' requires a single nested type, but none was provided");
		}
		var originalName = name;
		name = name[1..];

		var result = ParseInternal(ref name);
		if (name.IsEmpty || name[0] != ')')
		{
			throw new ClickHouseTypeParseException($"Type '{parentType}' has invalid nested type '{originalName}': Missing closing paranthesis");
		}
		name = name[1..];
		return result;
	}

	private static List<ClickHouseType> ParseNestedTypes(ref ReadOnlySpan<char> name, ReadOnlySpan<char> parentType)
	{
		if (name.IsEmpty || name[0] != '(')
		{
			throw new ClickHouseTypeParseException($"Type '{parentType}' requires nested types, but none were provided");
		}
		var originalName = name;
		name = name[1..];

		var result = new List<ClickHouseType>(capacity: 2);
		while (true)
		{
			var fieldName = ParseFieldName(ref name);
			if (fieldName.IsEmpty)
			{
				throw new ClickHouseTypeParseException($"Type '{parentType}' has invalid nested types '{originalName}': Every nested type must be preceded by its field name");
			}
			var type = ParseInternal(ref name);
			type.FieldName = new string(fieldName);
			result.Add(type);

			var separatorOrEndIndex = name.IndexOfAny(',', ')');
			if (separatorOrEndIndex == -1)
			{
				throw new ClickHouseTypeParseException($"Type '{parentType}' has invalid nested types '{originalName}': Missing closing paranthesis");
			}
			if (name[separatorOrEndIndex++] == ')')
			{
				name = name[separatorOrEndIndex..];
				return result;
			}
			name = name[separatorOrEndIndex..];
		}
	}

	private static ReadOnlySpan<char> ParseFieldName(ref ReadOnlySpan<char> name)
	{
		// Supports "<field><whitespace><type>" and "<whitespace><field><whitespace><type>"
		var originalName = name;
		int fieldNameStart = -1;
		for (int i = 0; i < name.Length; i++)
		{
			var ch = name[i];
			if (ch == ' ' || ch == '\t' || ch == '\n')
			{
				if (fieldNameStart != -1)
				{
					name = name[(i + 1)..];
					return originalName[fieldNameStart..i];
				}
			}
			else if (ch != ',' && ch != '(' && ch != ')')
			{
				fieldNameStart = i;
			}
		}
		return [];
	}

	private void ToStringInternal(Span<char> result, ref int written)
	{
		if (FieldName is not null)
		{
			FieldName.CopyTo(result[written..]);
			written += FieldName.Length;
			result[written++] = ' ';
		}

		var typeAsStr = Name.ToString();
		typeAsStr.CopyTo(result[written..]);
		written += typeAsStr.Length;

		if (PrecisionOrLength is not null)
		{
			result[written++] = '(';
			PrecisionOrLength.Value.TryFormat(result[written..], out int lengthCharCount);
			written += lengthCharCount;
			result[written++] = ')';
		}
		else if (_nestedTypes.Count > 0)
		{
			result[written++] = '(';
			for (int i = 0; i < _nestedTypes.Count; i++)
			{
				_nestedTypes[i].ToStringInternal(result, ref written);
				if (i != _nestedTypes.Count - 1)
				{
					result[written++] = ',';
					result[written++] = ' ';
				}
			}
			result[written++] = ')';
		}
	}

	internal ClickHouseType()
	{
	}

	private const string FixedStringName = "FixedString";
	private const string DateTime64Name = "DateTime64";
	private const string Time64Name = "Time64";
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
