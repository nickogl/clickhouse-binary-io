using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace ClickHouse.BinaryIO;

/// <summary>
/// A buffered, forward-only binary reader for result sets received from ClickHouse.
/// </summary>
/// <remarks>
/// <para>
/// As of now, only the query output formats <c>RowBinary</c> and
/// <c>RowBinaryWithNamesAndTypes</c> are supported.
/// </para>
/// <para>
/// If the query output format is <c>RowBinaryWithNamesAndTypes</c>, columns must
/// be read with <see cref="ReadColumnsAsync"/> before anything else. It is recommended
/// to request this output format as it will aid with debugging.
/// </para>
/// <para>
/// Data must be read in order of the queried columns, using the Read methods that
/// match each column's type. Rows should be read in a loop as long as <see cref="IsCompleteAsync"/>
/// returns <c>false</c>.
/// </para>
/// </remarks>
public sealed class ClickHouseBinaryReader : IDisposable
{
	private readonly static DateOnly DateReferencePoint = DateOnly.FromDateTime(DateTime.UnixEpoch);

	private ClickHouseBinaryReaderOptions _options;
	private Stream _stream;
	private byte[] _buffer;
	private int _available;
	private int _position;
	private int _lastRowPosition;
#if DEBUG
	// Members for diagnostics in debug mode, supports infinitely nested types
	private ClickHouseColumn[] _columns;
	private Stack<(string Name, ClickHouseType Type)> _remainingColumns;
#endif

#pragma warning disable CS8618
	/// <summary>
	/// Create a binary reader that operates on a null stream.
	/// </summary>
	public ClickHouseBinaryReader()
	{
		Reset();
	}

	/// <summary>
	/// Create a binary reader that operates on the provided <paramref name="stream"/>.
	/// </summary>
	/// <param name="stream">Stream to read result set from.</param>
	/// <param name="options">Options to fine-tune reading behavior.</param>
	public ClickHouseBinaryReader(Stream stream, ClickHouseBinaryReaderOptions options)
	{
		Reset(stream, options);
	}
#pragma warning restore CS8618

	/// <inheritdoc/>
	public void Dispose()
	{
		Reset();
	}

	/// <summary>
	/// Reset this instance with a new stream and options.
	/// </summary>
	/// <param name="stream">Stream to read result set from.</param>
	/// <param name="options">Options to fine-tune reading behavior.</param>
	public void Reset(Stream stream, ClickHouseBinaryReaderOptions options)
	{
		if (options.Buffer is null)
		{
			ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.BufferSize, nameof(options));
		}
		if (!stream.CanRead)
		{
			throw new ArgumentException("Stream must be readable", nameof(stream));
		}

		Reset();

		_stream = stream;
		_options = options;
		_buffer = options.Buffer ?? options.BufferPool.Rent(options.BufferSize);
	}

	/// <summary>
	/// Reset this instance for later re-use.
	/// </summary>
	public void Reset()
	{
		if (_options.Buffer is null && _buffer?.Length > 0)
		{
			_options.BufferPool.Return(_buffer);
		}

		_stream = Stream.Null;
		_buffer = [];
		_available = 0;
		_position = 0;
		_lastRowPosition = 0;
#if DEBUG
		_columns = [];
		_remainingColumns = [];
#endif
	}

	/// <summary>
	/// Read and parse the columns returned in the result set. This needs to be done
	/// before reading any rows for output format <c>RowBinaryWithNamesAndTypes</c>.
	/// </summary>
	/// <returns>Parsed column names and their types.</returns>
	public async Task<ClickHouseColumn[]> ReadColumnsAsync(CancellationToken cancellationToken = default)
	{
		// This should fully read the columns into the buffer in the majority of cases
		_available += await _stream.ReadAsync(_buffer.AsMemory(_available), cancellationToken);

		int columnCount = ReadVarint();
		Debug.Assert(columnCount > 0 && columnCount <= 1000, "Unexpected column count. Did you request the output format RowBinaryWithNamesAndTypes?");

		var columns = new ClickHouseColumn[columnCount];
		ReadColumnNames(columns);
		ReadColumnTypes(columns);
#if DEBUG
		_columns = columns;
#endif
		return columns;
	}

	private void ReadColumnNames(ClickHouseColumn[] columns)
	{
		for (int columnIndex = 0; columnIndex < columns.Length; columnIndex++)
		{
			columns[columnIndex].Name = ReadString();
		}
	}

	private void ReadColumnTypes(ClickHouseColumn[] columns)
	{
		// Below code does not allocate anything if all column types are flat
		Span<char> type = stackalloc char[256];
		for (int columnIndex = 0; columnIndex < columns.Length; columnIndex++)
		{
			int typeLength = ReadString(type);
			columns[columnIndex].Type = ClickHouseType.Parse(type[..typeLength]);
		}
	}

	/// <summary>
	/// Determine whether or not all data has been read from the result set.
	/// </summary>
	/// <remarks>This should be called in a loop for every row.</remarks>
	public ValueTask<bool> IsCompleteAsync(CancellationToken cancellationToken = default)
	{
		// For performance reasons, we avoid using ValueTask/Task for ReadXXX() as
		// these are potentially called hundreds of millions of times when processing
		// large volumes of data. Which means we have to perform a blocking read on
		// the underlying stream should we run out of data while reading columns.
		//
		// To avoid the blocking read to happen too many times (or never at all, if
		// the size of the rows are fixed), we check if the remaining buffered data
		// suffices for the next row based on the size of the previous row and perform
		// a non-blocking read should it not fit.
		var lastRowSize = _position - _lastRowPosition;
		_lastRowPosition = _position;
		if (_available - _position >= lastRowSize)
		{
			return ValueTask.FromResult(false);
		}

		return CheckEndOfStreamAsync(lastRowSize, cancellationToken);
	}

	/// <summary>
	/// Read a signed 8-bit value from the result set.
	/// </summary>
	public sbyte ReadInt8()
	{
		AssertColumnType(ClickHouseTypeName.Int8);

		return (sbyte)ReadUInt8();
	}

	/// <summary>
	/// Read an unsigned 8-bit value from the result set.
	/// </summary>
	public byte ReadUInt8()
	{
		AssertColumnType(ClickHouseTypeName.UInt8);

		return ReadByteWithoutDiagnostics();
	}

	/// <summary>
	/// Read a boolean value from the result set.
	/// </summary>
	public bool ReadBoolean()
	{
		AssertColumnType(ClickHouseTypeName.Bool);

		// This works because .NET internally stores booleans as one byte that is either 0 or 1.
		// By doing this, we avoid branching, which is important when reading millions of rows.
		var u8 = ReadUInt8();
		return Unsafe.As<byte, bool>(ref u8);
	}

	/// <summary>
	/// Read a signed 16-bit value from the result set.
	/// </summary>
	public short ReadInt16()
	{
		AssertColumnType(ClickHouseTypeName.Int16);

		return ReadNumber<short>();
	}

	/// <summary>
	/// Read an unsigned 16-bit value from the result set.
	/// </summary>
	public ushort ReadUInt16()
	{
		AssertColumnType(ClickHouseTypeName.UInt16);

		return ReadNumber<ushort>();
	}

	/// <summary>
	/// Read a signed 32-bit value from the result set.
	/// </summary>
	public int ReadInt32()
	{
		AssertColumnType(ClickHouseTypeName.Int32);

		return ReadNumber<int>();
	}

	/// <summary>
	/// Read an usigned 32-bit value from the result set.
	/// </summary>
	public uint ReadUInt32()
	{
		AssertColumnType(ClickHouseTypeName.UInt32);

		return ReadNumber<uint>();
	}

	/// <summary>
	/// Read a signed 64-bit value from the result set.
	/// </summary>
	public long ReadInt64()
	{
		AssertColumnType(ClickHouseTypeName.Int64);

		return ReadNumber<long>();
	}

	/// <summary>
	/// Read an unsigned 64-bit value from the result set.
	/// </summary>
	public ulong ReadUInt64()
	{
		AssertColumnType(ClickHouseTypeName.UInt64);

		return ReadNumber<ulong>();
	}

	/// <summary>
	/// Read a floating-point number with single precision from the result set.
	/// </summary>
	public float ReadFloat32()
	{
		AssertColumnType(ClickHouseTypeName.Float32);

		return ReadNumber<float>();
	}

	/// <summary>
	/// Read a floating-point number with double precision from the result set.
	/// </summary>
	public double ReadFloat64()
	{
		AssertColumnType(ClickHouseTypeName.Float64);

		return ReadNumber<double>();
	}

	/// <summary>
	/// Read a variable-length string from the result set.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The string is decoded with UTF-8 without BOM. Use <see cref="ReadString(Encoding)"/>
	/// to decode the string with a different encoding.
	/// </para>
	/// <para>
	/// This allocates a string. Use <see cref="ReadString(Span{char})"/> if this is not desired.
	/// </para>
	/// </remarks>
	public string ReadString()
	{
		return ReadString(Encoding.Default);
	}

	/// <summary>
	/// Read a variable-length string from the result set.
	/// </summary>
	/// <param name="encoding">
	/// Encoding used to decode the string. This should be the same encoding that was
	/// orginally used for writing the string. Defaults to UTF8 without BOM.
	/// </param>
	/// <remarks>
	/// This allocates a string. Use <see cref="ReadString(Span{char}, Encoding)"/> if this is not desired.
	/// </remarks>
	public string ReadString(Encoding encoding)
	{
		var length = ReadVarint();
		AssertColumnType(ClickHouseTypeName.String, variableLength: length);

		return ReadFixedString(length, encoding);
	}

	/// <summary>
	/// Read a variable-length string from the result set into the given span.
	/// </summary>
	/// <param name="destination">Span to write decoded characters into.</param>
	/// <remarks>
	/// The string is decoded with UTF-8 without BOM. Use <see cref="ReadString(Span{char}, Encoding)"/>
	/// to decode the string with a different encoding.
	/// </remarks>
	/// <returns>The amount of characters written into <paramref name="destination"/>.</returns>
	public int ReadString(Span<char> destination)
	{
		return ReadString(destination, Encoding.Default);
	}

	/// <summary>
	/// Read a variable-length string from the result set into the given span.
	/// </summary>
	/// <param name="destination">Span to write decoded characters into.</param>
	/// <param name="encoding">
	/// Encoding used to decode the string. This should be the same encoding that was
	/// orginally used for writing the string. Defaults to UTF8 without BOM.
	/// </param>
	/// <returns>The amount of characters written into <paramref name="destination"/>.</returns>
	public int ReadString(Span<char> destination, Encoding encoding)
	{
		var length = ReadVarint();
		AssertColumnType(ClickHouseTypeName.String, variableLength: length);

		return ReadFixedString(length, destination, encoding);
	}

	/// <summary>
	/// Read a fixed-length string from the result set.
	/// </summary>
	/// <param name="byteLength">Length of the string in bytes.</param>
	/// <remarks>
	/// <para>
	/// The string is decoded with ASCII. Use <see cref="ReadFixedString(int, Encoding)"/>
	/// to decode the string with a different encoding.
	/// </para>
	/// <para>
	/// This allocates a string. Use <see cref="ReadFixedString(int, Span{char})"/> if this is not desired.
	/// </para>
	/// </remarks>
	public string ReadFixedString(int byteLength)
	{
		return ReadFixedString(byteLength, Encoding.ASCII);
	}

	/// <summary>
	/// Read a fixed-length string from the result set.
	/// </summary>
	/// <param name="byteLength">Length of the string in bytes.</param>
	/// <param name="encoding">
	/// Encoding to use to decode the string. This should be the same encoding that
	/// was orginally used for writing the string into the column.
	/// </param>
	/// <remarks>
	/// <para>
	/// ClickHouse does not have the concept of an encoding, therefore the length of
	/// a FixedString is always the length in bytes, not characters or code points.
	/// Make sure to use a fixed-width encoding (e.g. ASCII) for FixedString.
	/// </para>
	/// <para>
	/// This allocates a string. Use <see cref="ReadFixedString(int, Span{char}, Encoding)"/> if this is not desired.
	/// </para>
	/// </remarks>
	public string ReadFixedString(int byteLength, Encoding encoding)
	{
		AssertColumnType(ClickHouseTypeName.FixedString, byteLength);
		EnsureAvailable(byteLength);

		var str = encoding.GetString(GetRemainingSpanUnsafe(byteLength));
		_position += byteLength;
		return str;
	}

	/// <summary>
	/// Read a fixed-length string from the result set.
	/// </summary>
	/// <param name="byteLength">Length of the string in bytes.</param>
	/// <param name="destination">Span to write decoded characters into.</param>
	/// <remarks>
	/// The string is decoded with ASCII. Use <see cref="ReadFixedString(int, Encoding)"/>
	/// to decode the string with a different encoding.
	/// </remarks>
	/// <returns>The amount of characters written into <paramref name="destination"/>.</returns>
	public int ReadFixedString(int byteLength, Span<char> destination)
	{
		return ReadFixedString(byteLength, destination, Encoding.ASCII);
	}

	/// <summary>
	/// Read a fixed-length string from the result set.
	/// </summary>
	/// <param name="byteLength">Length of the string in bytes.</param>
	/// <param name="destination">Span to write decoded characters into.</param>
	/// <param name="encoding">
	/// Encoding to use to decode the string. This should be the same encoding that
	/// was orginally used for writing the string into the column.
	/// </param>
	/// <remarks>
	/// ClickHouse does not have the concept of an encoding, therefore the length of
	/// a FixedString is always the length in bytes, not characters or code points.
	/// Make sure to use a fixed-width encoding (e.g. ASCII) for FixedString.
	/// </remarks>
	/// <returns>The amount of characters written into <paramref name="destination"/>.</returns>
	public int ReadFixedString(int byteLength, Span<char> destination, Encoding encoding)
	{
		AssertColumnType(ClickHouseTypeName.FixedString, byteLength);
		EnsureAvailable(byteLength);

		var written = encoding.GetChars(GetRemainingSpanUnsafe(byteLength), destination);
		_position += byteLength;
		return written;
	}

	/// <summary>
	/// Read a date from the result set.
	/// </summary>
	public DateOnly ReadDate()
	{
		AssertColumnType(ClickHouseTypeName.Date);

		var daysSinceEpoch = ReadUInt16();
		return DateReferencePoint.AddDays(daysSinceEpoch);
	}

	/// <summary>
	/// Read a date with a higher range than Date from the result set.
	/// </summary>
	public DateOnly ReadDate32()
	{
		AssertColumnType(ClickHouseTypeName.Date32);

		// This can be negative for representing dates prior to 1970-01-01
		var daysSinceEpoch = ReadInt32();
		return DateReferencePoint.AddDays(daysSinceEpoch);
	}

	/// <summary>
	/// Read a date and time from the result set.
	/// </summary>
	public DateTime ReadDateTime()
	{
		AssertColumnType(ClickHouseTypeName.DateTime);

		var offset = DateTimeOffset.FromUnixTimeSeconds(ReadUInt32());
		return offset.DateTime;
	}

	/// <summary>
	/// Read a date and time with sub-second precision from the result set.
	/// </summary>
	/// <param name="precision">
	/// Precision parameter used when creating the table.
	/// Defaults to 3 in ClickHouse when none specified.
	/// </param>
	/// <remarks>
	/// ClickHouse supports nanosecond resolution while <see cref="DateTime"/>'s
	/// resolution is only 100 nanoseconds. So if a <paramref name="precision"/>
	/// of 8 or 9 is used, some information will be lost. If this is unacceptable,
	/// consider using <see cref="ReadInt64"/> and decode the value according to
	/// the documentation: https://clickhouse.com/docs/sql-reference/data-types/datetime64
	/// </remarks>
	public DateTime ReadDateTime64(int precision = 3)
	{
		AssertColumnType(ClickHouseTypeName.DateTime64, precision);

		var ticks = ReadInt64();
		return DateTime.UnixEpoch.AddTicks(precision switch
		{
			0 => ticks * 10_000_000,
			1 => ticks * 1_000_000,
			2 => ticks * 100_000,
			3 => ticks * 10_000,
			4 => ticks * 1_000,
			5 => ticks * 100,
			6 => ticks * 10,
			7 => ticks,
			8 => (long)(ticks * 0.1),
			9 => (long)(ticks * 0.01),
			_ => throw new NotSupportedException($"Precision {precision} for DateTime64 not supported by ClickHouse"),
		});
	}

	/// <summary>
	/// Read a time with a resolution of 1 second from the result set.
	/// </summary>
	public TimeSpan ReadTime()
	{
		AssertColumnType(ClickHouseTypeName.Time);

		int seconds = ReadInt32();
		return TimeSpan.FromSeconds(seconds);
	}

	/// <summary>
	/// Read a time with sub-second resolution from the result set.
	/// </summary>
	/// <param name="precision">
	/// Precision parameter used when creating the table.
	/// Defaults to 3 in ClickHouse when none specified.
	/// </param>
	/// <remarks>
	/// ClickHouse supports nanosecond resolution while <see cref="TimeSpan"/>'s
	/// resolution is only 100 nanoseconds. So if a <paramref name="precision"/>
	/// of 8 or 9 is used, some information will be lost. If this is unacceptable,
	/// consider using <see cref="ReadInt64"/> and decode the value according to
	/// the documentation: https://clickhouse.com/docs/sql-reference/data-types/time64
	/// </remarks>
	public TimeSpan ReadTime64(int precision = 3)
	{
		AssertColumnType(ClickHouseTypeName.Time64, precision);

		var ticks = ReadInt64();
		return TimeSpan.FromTicks(precision switch
		{
			0 => ticks * 10_000_000,
			1 => ticks * 1_000_000,
			2 => ticks * 100_000,
			3 => ticks * 10_000,
			4 => ticks * 1_000,
			5 => ticks * 100,
			6 => ticks * 10,
			7 => ticks,
			8 => (long)(ticks * 0.1),
			9 => (long)(ticks * 0.01),
			_ => throw new NotSupportedException($"Precision {precision} for Time64 not supported by ClickHouse"),
		});
	}

	/// <summary>
	/// Read a UUID from the result set.
	/// </summary>
	public Guid ReadUuid()
	{
		AssertColumnType(ClickHouseTypeName.UUID);

		var c = ReadInt16();
		var b = ReadInt16();
		var a = ReadInt32();
		var d = ReadBytes(8);
		return new Guid(a, b, c, d[7], d[6], d[5], d[4], d[3], d[2], d[1], d[0]);
	}

	/// <summary>
	/// Read an IPv4 address from the result set.
	/// </summary>
	/// <remarks>
	/// This allocates an instance of <see cref="IPAddress"/> on the heap. If this is
	/// not desired, use <c>ReadBytes(4)</c> and process the octets manually. The
	/// octets are in reverse order.
	/// </remarks>
	public IPAddress ReadIPv4()
	{
		AssertColumnType(ClickHouseTypeName.IPv4);

		var leValue = ReadUInt32();
		var beValue = BinaryPrimitives.ReverseEndianness(leValue);
		var octets = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref beValue, 1));
		return new IPAddress(octets);
	}

	/// <summary>
	/// Read an IPv6 address from the result set.
	/// </summary>
	/// <remarks>
	/// This allocates an instance of <see cref="IPAddress"/> on the heap. If this is
	/// not desired, use <c>ReadBytes(16)</c> or <c>ReadUInt16()</c> eight times.
	/// </remarks>
	public IPAddress ReadIPv6()
	{
		AssertColumnType(ClickHouseTypeName.IPv6);

		return new IPAddress(ReadBytes(16));
	}

	/// <summary>
	/// Determine whether the next column value is null.
	/// </summary>
	/// <remarks>
	/// Example usage for a Nullable(Int32) column:
	/// <c>var value = reader.IsNull() ? null : reader.ReadInt32()</c>
	/// </remarks>
	/// <returns>True if the next column value is null.</returns>
	public bool IsNull()
	{
		var isNull = ReadByteWithoutDiagnostics();
		AssertColumnType(ClickHouseTypeName.Nullable, variableLength: isNull);
		return Unsafe.As<byte, bool>(ref isNull);
	}

	/// <summary>
	/// Read the length of the next array column in the result set.
	/// </summary>
	/// <remarks>
	/// Each array item shall be read right after this call. All fields of the items
	/// inside the array must be consumed.
	/// </remarks>
	public int ReadArrayLength()
	{
		var length = ReadVarint();
		AssertColumnType(ClickHouseTypeName.Array, variableLength: length);
		return length;
	}

	/// <summary>
	/// Read <paramref name="count"/> bytes from the result set and return a reference.
	/// </summary>
	/// <remarks>
	/// No diagnostics provided for raw byte reads. Use at your own risk.
	/// </remarks>
	/// <param name="count">Amount of bytes to read.</param>
	/// <returns>
	/// A view of the bytes. They should be immediately processed/copied as the
	/// reference to them might be invalid after the next read.
	/// </returns>
	public ReadOnlySpan<byte> ReadBytes(int count)
	{
		AssertColumnType(null);
		EnsureAvailable(count);

		var view = GetRemainingSpanUnsafe(count);
		_position += count;
		return view;
	}

	private int ReadVarint()
	{
		// Despite the return value being a signed 32-bit integer, this decodes an
		// unsigned LEB128. For our use case here it will never exceed int.MaxValue
		// anyway, but makes it more convenient to use as we don't have to cast everywhere.
		uint result = 0;
		int shift = 0;
		byte current;
		do
		{
			current = ReadByteWithoutDiagnostics();
			result |= (current & 0x7fu) << shift;
			shift += 7;
		} while ((current & 0x80u) != 0);

		Debug.Assert(result <= int.MaxValue);
		return (int)result;
	}

	private async Task ReadAtLeastAsync(int count)
	{
		int totalRead = 0;
		while (totalRead < count)
		{
			ResetBufferIfFull();

			int read = await _stream.ReadAsync(_buffer.AsMemory(_available));
			if (read == 0)
			{
				ThrowEndOfStream(count, totalRead);
			}
			totalRead += read;
			_available += read;
		}
	}

	private void EnsureAvailable(int count)
	{
		if (_position + count > _available)
		{
			// Perform a blocking read in the uncommon case. While ugly, depending on the data in the
			// result set this might never even happen for the reasons described in IsCompleteAsync().
			ReadAtLeastAsync(count).GetAwaiter().GetResult();
		}
	}

	private T ReadNumber<T>() where T : unmanaged
	{
		if (_position + Unsafe.SizeOf<T>() > _available)
		{
			// See: EnsureAvailable()
			ReadAtLeastAsync(Unsafe.SizeOf<T>()).GetAwaiter().GetResult();

			// Aid the JIT in generating code that only jumps in the uncommon case
			return ReadNumberUnsafe<T>();
		}

		return ReadNumberUnsafe<T>();
	}

	private T ReadNumberUnsafe<T>() where T : unmanaged
	{
		Debug.Assert(BitConverter.IsLittleEndian);
		Debug.Assert(_position + Unsafe.SizeOf<T>() <= _available);

		// Completely elide bounds checks that we'd otherwise have to pay for in
		// Array.AsSpan() and BinaryPrimitives.ReadXXXXLittleEndian()
		var span = GetRemainingSpanUnsafe(Unsafe.SizeOf<T>());
		var result = Unsafe.ReadUnaligned<T>(ref MemoryMarshal.GetReference(span));
		_position += Unsafe.SizeOf<T>();
		return result;
	}

	private byte ReadByteWithoutDiagnostics()
	{
		EnsureAvailable(1);

		var span = GetRemainingSpanUnsafe(1);
		var u8 = Unsafe.ReadUnaligned<byte>(ref MemoryMarshal.GetReference(span));
		_position++;
		return u8;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void ThrowEndOfStream(int count, int totalRead)
	{
		var messageBuilder = new StringBuilder();
#if DEBUG
		if (_remainingColumns.Count > 0)
		{
			var nextColumn = _remainingColumns.Peek();
			messageBuilder.AppendLine($"Column: {nextColumn.Name} {nextColumn.Type.Root}");
			if (nextColumn.Type.Root != nextColumn.Type)
			{
				messageBuilder.AppendLine($"(Currently reading nested type {nextColumn.Type})");
			}
			messageBuilder.AppendLine(new string('-', 80));
		}
#endif
		messageBuilder.AppendLine($"Required {count} bytes, but could only read {totalRead}. Did you forget to consume a column?");
		messageBuilder.AppendLine("Make sure to cast results of sum() to a fixed type or use sumOverflow() to have deterministic column sizes.");
		throw new EndOfStreamException(messageBuilder.ToString());
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ReadOnlySpan<byte> GetRemainingSpanUnsafe(int length)
	{
		Debug.Assert(_position + length <= _available);

		// Elide bounds checks, only call this after EnsureAvailable()
		return MemoryMarshal.CreateReadOnlySpan(
			ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_buffer), _position),
			length);
	}

	private async ValueTask<bool> CheckEndOfStreamAsync(int lastRowSize, CancellationToken cancellationToken)
	{
		int totalRead = 0;
		while (totalRead < lastRowSize)
		{
			ResetBufferIfFull();

			int read = await _stream.ReadAsync(_buffer.AsMemory(_available), cancellationToken);
			if (read == 0)
			{
				// It is only the end of the stream if we already consumed all available bytes
				return _position >= _available;
			}
			_available += read;
			totalRead += read;
		}

		return false;
	}

	private void ResetBufferIfFull()
	{
		Debug.Assert(_available - _position < _buffer.Length, "Buffer size should be at least the size of a single row");

		if (_available == _buffer.Length)
		{
			// Our buffer is full, move the remaining unread data to the beginning of the
			// buffer and continue reading from the stream. This is necessary so that we do
			// not cut the data of a single column in half.
			int leftover = _available - _position;
			_buffer.AsSpan(_position, leftover).CopyTo(_buffer.AsSpan(0, leftover));
			_available = leftover;
			_position = 0;
		}
	}

#if DEBUG
	private void AssertColumnType(ClickHouseTypeName? type, int? precisionOrLength = null, int? variableLength = null)
	{
		if (_columns.Length == 0)
		{
			// Cannot provide diagnostics if requested output format was not RowBinaryWithNamesAndTypes
			return;
		}

		if (_remainingColumns.Count == 0)
		{
			// Columns exhausted, this is a new row
			for (int i = _columns.Length - 1; i >= 0; i--)
			{
				_remainingColumns.Push((_columns[i].Name, _columns[i].Type));
			}
		}

		var nextColumn = _remainingColumns.Pop();
		if (type is null)
		{
			// Raw bytes were requested, impossible to provide diagnostics
			return;
		}
		if (nextColumn.Type.Name == ClickHouseTypeName.Tuple)
		{
			// Tuples are provided as a flat structure, i.e. they are treated as if they
			// were multiple consecutive columns. Simply replace the tuple with all of
			// its nested types.
			foreach (var nestedType in nextColumn.Type.NestedTypes.Reverse())
			{
				_remainingColumns.Push((nextColumn.Name, nestedType));
			}
			nextColumn = _remainingColumns.Pop();
		}

		Debug.Assert(nextColumn.Type.PrecisionOrLength == precisionOrLength, @$"
Column: {nextColumn.Name} {nextColumn.Type.Root}
{new string('-', 80)}
Expected precision or length {nextColumn.Type.PrecisionOrLength} for type {nextColumn.Type},
but provided precision or length {precisionOrLength?.ToString() ?? "<null>"} instead.
Make sure you use the same precision or length that you specified when creating the table.");

		// We allow sidestepping some type-specific methods to avoid allocations or inaccuracies
		bool isException = (type == ClickHouseTypeName.UInt32 && nextColumn.Type.Name == ClickHouseTypeName.IPv4)
			|| (type == ClickHouseTypeName.Int64 && nextColumn.Type.Name == ClickHouseTypeName.DateTime64)
			|| (type == ClickHouseTypeName.Int64 && nextColumn.Type.Name == ClickHouseTypeName.Time64);
		Debug.Assert(isException || nextColumn.Type.Name == type, @$"
Column: {nextColumn.Name} {nextColumn.Type.Root}
{new string('-', 80)}
Expected to read data of type {nextColumn.Type.Name} but attempted to read data of type {type}.
Did you forget to consume a column prior to this one?");

		// Some read methods build upon other read methods, so add the individual components here
		if (type == ClickHouseTypeName.Int8)
		{
			PushSimple(ClickHouseTypeName.UInt8);
		}
		else if (type == ClickHouseTypeName.Bool)
		{
			PushSimple(ClickHouseTypeName.UInt8);
		}
		else if (type == ClickHouseTypeName.Date)
		{
			PushSimple(ClickHouseTypeName.UInt16);
		}
		else if (type == ClickHouseTypeName.Date32)
		{
			PushSimple(ClickHouseTypeName.Int32);
		}
		else if (type == ClickHouseTypeName.DateTime)
		{
			PushSimple(ClickHouseTypeName.UInt32);
		}
		else if (type == ClickHouseTypeName.DateTime64)
		{
			PushSimple(ClickHouseTypeName.Int64);
		}
		else if (type == ClickHouseTypeName.Time)
		{
			PushSimple(ClickHouseTypeName.Int32);
		}
		else if (type == ClickHouseTypeName.Time64)
		{
			PushSimple(ClickHouseTypeName.Int64);
		}
		else if (type == ClickHouseTypeName.IPv4)
		{
			PushSimple(ClickHouseTypeName.UInt32);
		}
		else if (type == ClickHouseTypeName.IPv6)
		{
			PushSimple(ClickHouseTypeName.Array);
		}
		else if (type == ClickHouseTypeName.UUID)
		{
			PushSimple(ClickHouseTypeName.Int16, ClickHouseTypeName.Int16, ClickHouseTypeName.Int32, ClickHouseTypeName.Array);
		}
		else if (type == ClickHouseTypeName.String)
		{
			// NOTE: We reuse the fixed string method for variable-length strings
			Debug.Assert(variableLength is not null);
			_remainingColumns.Push((nextColumn.Name, new ClickHouseType()
			{
				Name = ClickHouseTypeName.FixedString,
				PrecisionOrLength = variableLength,
				NestedTypes = [],
			}));
		}
		else if (type == ClickHouseTypeName.Nullable && variableLength == 0)
		{
			// If the first byte of a nullable is 0, the value is not null and must be read
			_remainingColumns.Push((nextColumn.Name, nextColumn.Type.NestedTypes[0]));
		}
		else if (type == ClickHouseTypeName.Array)
		{
			// For arrays, simply expect the nested type as many times as the array is long
			Debug.Assert(variableLength is not null);
			for (int i = 0; i < variableLength; i++)
			{
				_remainingColumns.Push((nextColumn.Name, nextColumn.Type.NestedTypes[0]));
			}
		}

		void PushSimple(params ClickHouseTypeName[] types)
		{
			foreach (var type in types.Reverse())
			{
				_remainingColumns.Push((nextColumn.Name, new ClickHouseType() { Name = type, NestedTypes = [] }));
			}
		}
	}
#else
	private void AssertColumnType(ClickHouseTypeName? type, int? precisionOrLength = null, int? variableLength = null)
	{
	}
#endif
}
