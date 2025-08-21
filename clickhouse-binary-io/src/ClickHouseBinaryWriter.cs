using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace ClickHouse.BinaryIO;

/// <summary>
/// A buffered binary writer for writing row data to ClickHouse.
/// </summary>
/// <remarks>
/// <para>
/// As of now, only the query input format <c>RowBinary</c> is supported. ClickHouse
/// will respond with a HTTP 400 should the input data be malformed.
/// </para>
/// <para>
/// Data must be written in order of the columns specified in the query, using
/// the Write methods that match each column's type. Once in every while,
/// <see cref="FlushAsync"/> should be used to write the underlying buffer to the
/// stream in a non-blocking manner. If this is not done, it will perform blocking
/// writes sometimes during execution of the WriteXYZ() methods.
/// </para>
/// </remarks>
public sealed class ClickHouseBinaryWriter : IDisposable, IAsyncDisposable
{
	private readonly static DateOnly UnixEpochDate = DateOnly.FromDateTime(DateTime.UnixEpoch);
	private readonly static DateOnly DateMinValue = UnixEpochDate;
	private readonly static DateOnly DateMaxValue = new(2149, 6, 6);
	private readonly static DateOnly Date32MinValue = new(1900, 1, 1);
	private readonly static DateOnly Date32MaxValue = new(2299, 12, 31);
	private readonly static DateTime DateTimeMinValue = DateTime.UnixEpoch;
	private readonly static DateTime DateTimeMaxValue = new(2106, 2, 7, 6, 28, 15);
	private readonly static DateTime DateTime64MinValue = new(1900, 1, 1);
	private readonly static DateTime DateTime64MaxValue = new(2299, 12, 31, 23, 59, 59, 999);
	private readonly static TimeSpan TimeMinValue = new(-999, 59, 59);
	private readonly static TimeSpan TimeMaxValue = new(999, 59, 59);
	private readonly static TimeSpan Time64MinValue = new(-999, 59, 59);
	private readonly static TimeSpan Time64MaxValue = new(999, 59, 59);

	private ClickHouseBinaryWriterOptions _options;
	private Stream _stream;
	private byte[] _buffer;
	private int _position;
	private int _lastBatchPosition;

#pragma warning disable CS8618
	/// <summary>
	/// Create a binary writer that operates on a null stream.
	/// </summary>
	public ClickHouseBinaryWriter()
	{
		Reset();
	}

	/// <summary>
	/// Create a binary writer that operates on the provided <paramref name="stream"/>.
	/// </summary>
	/// <param name="stream">Stream to write RowBinary data to.</param>
	/// <param name="options">Options to fine-tune writing behavior.</param>
	public ClickHouseBinaryWriter(Stream stream, ClickHouseBinaryWriterOptions options)
	{
		Reset(stream, options);
	}
#pragma warning restore CS8618

	/// <inheritdoc/>
	public void Dispose()
	{
		if (_position > 0)
		{
			FlushInternalAsync().GetAwaiter().GetResult();
		}

		Reset();
	}

	/// <inheritdoc/>
	public async ValueTask DisposeAsync()
	{
		if (_position > 0)
		{
			await FlushInternalAsync(default);
		}

		Reset();
	}

	/// <summary>
	/// Reset this instance with a new stream and options.
	/// </summary>
	/// <param name="stream">Stream to write RowBinary data to.</param>
	/// <param name="options">Options to fine-tune writing behavior.</param>
	public void Reset(Stream stream, ClickHouseBinaryWriterOptions options)
	{
		if (options.Buffer is null)
		{
			ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.BufferSize, nameof(options));
		}
		if (!stream.CanWrite)
		{
			throw new ArgumentException("Stream must be writable", nameof(stream));
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
		_position = 0;
		_lastBatchPosition = 0;
	}

	/// <summary>
	/// Write the buffered data to the underlying stream.
	/// </summary>
	public ValueTask FlushAsync(CancellationToken cancellationToken = default)
	{
		// For performance reasons, we avoid using ValueTask/Task for WriteXXX() as
		// these are potentially called hundreds of millions of times when processing
		// large volumes of data. Which means we have to perform a blocking write on
		// the underlying stream should we run out of data while writing columns.
		//
		// To avoid the blocking write to happen too many times (or never at all, if
		// the size of the rows are fixed), we check if the remaining buffer
		// suffices for the next batch based on the size of the previous one and perform
		// a non-blocking write should it not fit.
		var lastBatchSize = _position - _lastBatchPosition;
		_lastBatchPosition = _position;
		if (_buffer.Length - _position >= lastBatchSize)
		{
			return ValueTask.CompletedTask;
		}

		return FlushInternalAsync(cancellationToken);
	}

	/// <summary>
	/// Write a signed 8-bit value to the stream.
	/// </summary>
	/// <param name="value">Signed 8-bit value to write.</param>
	public void WriteInt8(sbyte value)
	{
		WriteUInt8((byte)value);
	}

	/// <summary>
	/// Write an unsigned 8-bit value to the stream.
	/// </summary>
	/// <param name="value">Unsigned 8-bit value to write.</param>
	public void WriteUInt8(byte value)
	{
		EnsureAvailable(1);

		WriteByteUnsafe(value);
	}

	/// <summary>
	/// Write a boolean value to the stream.
	/// </summary>
	/// <param name="value">Boolean value to write.</param>
	public void WriteBoolean(bool value)
	{
		// This works because .NET internally stores booleans as one byte that can either be 0 or 1.
		// By doing this, we avoid branching, which is important when writing millions of rows.
		WriteUInt8(Unsafe.As<bool, byte>(ref value));
	}

	/// <summary>
	/// Write a signed 16-bit value to the stream.
	/// </summary>
	/// <param name="value">Signed 16-bit value to write.</param>
	public void WriteInt16(short value)
	{
		WriteNumber(value);
	}

	/// <summary>
	/// Write an unsigned 16-bit value to the stream.
	/// </summary>
	/// <param name="value">Signed 16-bit value to write.</param>
	public void WriteUInt16(ushort value)
	{
		WriteNumber(value);
	}

	/// <summary>
	/// Write a signed 32-bit value to the stream.
	/// </summary>
	/// <param name="value">Signed 32-bit value to write.</param>
	public void WriteInt32(int value)
	{
		WriteNumber(value);
	}

	/// <summary>
	/// Write an unsigned 32-bit value to the stream.
	/// </summary>
	/// <param name="value">Unsigned 32-bit value to write.</param>
	public void WriteUInt32(uint value)
	{
		WriteNumber(value);
	}

	/// <summary>
	/// Write a signed 64-bit value to the stream.
	/// </summary>
	/// <param name="value">Signed 64-bit value to write.</param>
	public void WriteInt64(long value)
	{
		WriteNumber(value);
	}

	/// <summary>
	/// Write an unsigned 64-bit value to the stream.
	/// </summary>
	/// <param name="value">Unsigned 64-bit value to write.</param>
	public void WriteUInt64(ulong value)
	{
		WriteNumber(value);
	}

	/// <summary>
	/// Write a floating-point number with single precision to the stream.
	/// </summary>
	/// <param name="value">Single-precision floating-point number to write.</param>
	public void WriteFloat32(float value)
	{
		WriteNumber(value);
	}

	/// <summary>
	/// Write a floating-point number with double precision to the stream.
	/// </summary>
	/// <param name="value">Double-precision floating-point number to write.</param>
	public void WriteFloat64(double value)
	{
		WriteNumber(value);
	}

	/// <summary>
	/// Write a variable-length string to the stream.
	/// </summary>
	/// <param name="value">String to write.</param>
	/// <remarks>
	/// The string is encoded with UTF-8 without BOM. Use <see cref="WriteString(ReadOnlySpan{char}, Encoding)"/>
	/// to encode the string with a different encoding.
	/// </remarks>
	public void WriteString(ReadOnlySpan<char> value)
	{
		WriteString(value, Encoding.Default);
	}

	/// <summary>
	/// Write a variable-length string to the stream.
	/// </summary>
	/// <param name="value">String to write.</param>
	/// <param name="encoding">
	/// Encoding used to decode the string. This should be the same encoding that
	/// will be used for reading the string. Defaults to UTF8 without BOM.
	/// </param>
	public void WriteString(ReadOnlySpan<char> value, Encoding encoding)
	{
		var byteLength = encoding.GetByteCount(value);
		_position += WriteVarint(byteLength);
		EnsureAvailable(byteLength);

		_position += encoding.GetBytes(value, GetRemainingSpanUnsafe(byteLength));
	}

	/// <summary>
	/// Write a fixed-length string to the stream.
	/// </summary>
	/// <param name="value">String to write. Byte length should be equal to N in ClickHouse type FixedString(N).</param>
	/// <remarks>
	/// The string is encoded with ASCII. Use <see cref="WriteFixedString(ReadOnlySpan{char}, Encoding)"/>
	/// to encode the string with a different encoding.
	/// </remarks>
	public void WriteFixedString(ReadOnlySpan<char> value)
	{
		WriteFixedString(value, Encoding.ASCII);
	}

	/// <summary>
	/// Write a fixed-length string to the stream.
	/// </summary>
	/// <param name="value">String to write. Byte length should be equal to N in ClickHouse type FixedString(N).</param>
	/// <param name="encoding">
	/// Encoding used to decode the string. This should be the same encoding that
	/// will be used for reading the string.
	/// </param>
	/// <remarks>
	/// ClickHouse does not have the concept of an encoding, therefore the length of
	/// a FixedString is always the length in bytes, not characters or code points.
	/// Make sure to use a fixed-width encoding (e.g. ASCII) for FixedString.
	/// </remarks>
	public void WriteFixedString(ReadOnlySpan<char> value, Encoding encoding)
	{
		var byteLength = encoding.GetByteCount(value);
		EnsureAvailable(byteLength);

		_position += encoding.GetBytes(value, GetRemainingSpanUnsafe(byteLength));
	}

	/// <summary>
	/// Write a date to the stream.
	/// </summary>
	/// <param name="value">Date to write.</param>
	public void WriteDate(DateOnly value)
	{
		Debug.Assert(value >= DateMinValue && value <= DateMaxValue, "Exceeded supported range for Date in ClickHouse");

		var daysSinceEpoch = value.DayNumber - UnixEpochDate.DayNumber;
		WriteUInt16((ushort)daysSinceEpoch);
	}

	/// <summary>
	/// Write a date with a higher range than Date to the stream.
	/// </summary>
	/// <param name="value">Date to write.</param>
	public void WriteDate32(DateOnly value)
	{
		Debug.Assert(value >= Date32MinValue && value <= Date32MaxValue, "Exceeded supported range for Date32 in ClickHouse");

		var daysSinceEpoch = value.DayNumber - UnixEpochDate.DayNumber;
		WriteInt32(daysSinceEpoch);
	}

	/// <summary>
	/// Write a date and time to the stream.
	/// </summary>
	/// <param name="value">Date and time to write.</param>
	/// <remarks>
	/// The time zone of the value should match the column's.
	/// </remarks>
	public void WriteDateTime(DateTime value)
	{
		Debug.Assert(value >= DateTimeMinValue && value <= DateTimeMaxValue, "Exceeded supported range for DateTime in ClickHouse");

		var secondsSinceEpoch = new DateTimeOffset(value).ToUnixTimeSeconds();
		WriteUInt32((uint)secondsSinceEpoch);
	}

	/// <summary>
	/// Write a date and time with sub-second precision to the stream.
	/// </summary>
	/// <param name="value">Date and time to write.</param>
	/// <param name="precision">
	/// Precision parameter used when creating the table.
	/// Defaults to 3 in ClickHouse when none specified.
	/// </param>
	/// <remarks>
	/// <para>
	/// The time zone of the value should match the column's.
	/// </para>
	/// <para>
	/// ClickHouse supports nanosecond resolution while <see cref="DateTime"/>'s
	/// resolution is only 100 nanoseconds. So if a <paramref name="precision"/>
	/// of 8 or 9 is used, some information will be lost. If this is unacceptable,
	/// consider using <see cref="WriteInt64"/> and encode the value according to
	/// the documentation: https://clickhouse.com/docs/sql-reference/data-types/datetime64
	/// </para>
	/// </remarks>
	public void WriteDateTime64(DateTime value, int precision = 3)
	{
		Debug.Assert(value >= DateTime64MinValue && value <= DateTime64MaxValue, "Exceeded supported range for DateTime64 in ClickHouse");

		var ticks = value.Ticks - DateTime.UnixEpoch.Ticks;
		WriteInt64(precision switch
		{
			0 => (long)(ticks * 0.0000001),
			1 => (long)(ticks * 0.000001),
			2 => (long)(ticks * 0.00001),
			3 => (long)(ticks * 0.0001),
			4 => (long)(ticks * 0.001),
			5 => (long)(ticks * 0.01),
			6 => (long)(ticks * 0.1),
			7 => ticks,
			8 => ticks * 10,
			9 => ticks * 100,
			_ => throw new NotSupportedException($"Precision {precision} for DateTime64 not supported by ClickHouse"),
		});
	}

	/// <summary>
	/// Write a time to the stream.
	/// </summary>
	/// <param name="value">Time to write.</param>
	public void WriteTime(TimeSpan value)
	{
		Debug.Assert(value >= TimeMinValue && value <= TimeMaxValue, "Exceeded supported range for Time in ClickHouse");

		WriteInt32((int)value.TotalSeconds);
	}

	/// <summary>
	/// Write a time with sub-second precision to the stream.
	/// </summary>
	/// <param name="value">Time to write.</param>
	/// <param name="precision">
	/// Precision parameter used when creating the table.
	/// Defaults to 3 in ClickHouse when none specified.
	/// </param>
	/// <remarks>
	/// ClickHouse supports nanosecond resolution while <see cref="TimeSpan"/>'s
	/// resolution is only 100 nanoseconds. So if a <paramref name="precision"/>
	/// of 8 or 9 is used, some information will be lost. If this is unacceptable,
	/// consider using <see cref="WriteInt64"/> and encode the value according to
	/// the documentation: https://clickhouse.com/docs/sql-reference/data-types/time64
	/// </remarks>
	public void WriteTime64(TimeSpan value, int precision = 3)
	{
		Debug.Assert(value >= Time64MinValue && value <= Time64MaxValue, "Exceeded supported range for Time64 in ClickHouse");

		WriteInt64(precision switch
		{
			0 => (long)(value.Ticks * 0.0000001),
			1 => (long)(value.Ticks * 0.000001),
			2 => (long)(value.Ticks * 0.00001),
			3 => (long)(value.Ticks * 0.0001),
			4 => (long)(value.Ticks * 0.001),
			5 => (long)(value.Ticks * 0.01),
			6 => (long)(value.Ticks * 0.1),
			7 => value.Ticks,
			8 => value.Ticks * 10,
			9 => value.Ticks * 100,
			_ => throw new NotSupportedException($"Precision {precision} for Time64 not supported by ClickHouse"),
		});
	}

	/// <summary>
	/// Write a UUID to the stream.
	/// </summary>
	/// <param name="value">UUID to write.</param>
	public void WriteUuid(Guid value)
	{
		EnsureAvailable(16);

		// Elide a bunch of bounds checks
		ref var source = ref MemoryMarshal.GetReference(MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref value, 1)));
		ref var destination = ref MemoryMarshal.GetReference(GetRemainingSpanUnsafe(16));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 0), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 6)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 1), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 7)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 2), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 4)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 3), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 5)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 4), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 0)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 5), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 1)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 6), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 2)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 7), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 3)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 8), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 15)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 9), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 14)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 10), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 13)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 11), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 12)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 12), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 11)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 13), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 10)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 14), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 9)));
		Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 15), Unsafe.ReadUnaligned<byte>(ref Unsafe.Add(ref source, 8)));

		_position += 16;
	}

	/// <summary>
	/// Write an IPv4 address to the stream.
	/// </summary>
	/// <param name="value">IPv4 address to write.</param>
	/// <remarks>
	/// This requires allocation of an instance of <see cref="IPAddress"/>. If this
	/// is not desired, use <see cref="WriteBytes"/> to write the octets manually.
	/// The octets must be written in reverse order.
	/// </remarks>
	public void WriteIPv4(IPAddress value)
	{
		Debug.Assert(value.AddressFamily == AddressFamily.InterNetwork);

		Span<byte> source = stackalloc byte[4];
		value.TryWriteBytes(source, out _);
		var beValue = Unsafe.ReadUnaligned<uint>(ref MemoryMarshal.GetReference(source));
		var leValue = BinaryPrimitives.ReverseEndianness(beValue);
		WriteUInt32(leValue);
	}

	/// <summary>
	/// Write an IPv6 address to the stream.
	/// </summary>
	/// <param name="value">IPv6 address to write.</param>
	/// <remarks>
	/// This requires allocation of an instance of <see cref="IPAddress"/>. If this
	/// is not desired, use <see cref="WriteBytes"/> to write the 16 bytes manually.
	/// </remarks>
	public void WriteIPv6(IPAddress value)
	{
		Debug.Assert(value.AddressFamily == AddressFamily.InterNetworkV6);

		var destination = GetBufferView(16);
		value.TryWriteBytes(destination, out _);
	}

	/// <summary>
	/// Write a marker that indicates whether or not the next column value is null.
	/// </summary>
	/// <param name="value">Whether or not the next column value is null.</param>
	/// <remarks>
	/// Example usage for a Nullable(Int32) column:
	/// <example>
	/// <code>
	/// int? value = ...;
	/// if (value is null)
	/// {
	/// 	writer.WriteNull(true);
	/// }
	/// else
	/// {
	/// 	writer.WriteNull(false);
	/// 	writer.WriteInt32(value.Value);
	/// }
	/// </code>
	/// </example>
	/// </remarks>
	public void WriteNull(bool value)
	{
		WriteBoolean(value);
	}

	/// <summary>
	/// Write the length of the next array column to the stream.
	/// </summary>
	/// <remarks>
	/// Each array item shall be written right after this call.
	/// </remarks>
	public void WriteArrayLength(int length)
	{
		_position += WriteVarint(length);
	}

	/// <summary>
	/// Write raw bytes to the stream.
	/// </summary>
	/// <param name="value">Raw bytes to write.</param>
	/// <remarks>
	/// Use <see cref="GetBufferView"/> to write directly into the buffer.
	/// </remarks>
	public void WriteBytes(ReadOnlySpan<byte> value)
	{
		ref var source = ref MemoryMarshal.GetReference(value);
		ref var destination = ref MemoryMarshal.GetReference(GetBufferView(value.Length));
		Unsafe.CopyBlockUnaligned(ref destination, ref source, (uint)value.Length);
	}

	/// <summary>
	/// Get a view of <paramref name="count"/> raw bytes in the buffer to write to.
	/// </summary>
	/// <remarks>
	/// Use this over <see cref="WriteBytes"/> if it allows you to avoid allocations.
	/// </remarks>
	public Span<byte> GetBufferView(int count)
	{
		EnsureAvailable(count);

		var view = GetRemainingSpanUnsafe(count);
		_position += count;
		return view;
	}

	private void WriteByteUnsafe(byte value)
	{
		var span = GetRemainingSpanUnsafe(1);
		Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(span), value);
		_position++;
	}

	private int WriteVarint(int value)
	{
		// Despite us writing an unsigned LEB128, we are working with signed 32-bit
		// numbers for convenience. No length will ever exceed int.MaxValue anyway.
		Debug.Assert(value >= 0);

		// While a LEB128 here can range anywhere between 1 and 4 bytes, we ensure the
		// maximum are available so we do not have to perform a check every iteration.
		EnsureAvailable(sizeof(uint));

		int written = 1;
		while (value > 0x7f)
		{
			WriteByteUnsafe((byte)(value | ~0x7f));
			value >>= 7;
			written++;
		}
		WriteByteUnsafe((byte)value);
		return written;
	}

	private void EnsureAvailable(int count)
	{
		if (_position + count > _buffer.Length)
		{
			// Perform a blocking write in the uncommon case. While ugly, depending on the row
			// data this might never even happen for the reasons described in FlushAsync().
			FlushInternalAsync().GetAwaiter().GetResult();
		}
	}

	private void WriteNumber<T>(T value) where T : unmanaged
	{
		if (_position + Unsafe.SizeOf<T>() > _buffer.Length)
		{
			// See: EnsureAvailable()
			FlushInternalAsync().GetAwaiter().GetResult();

			// Aid the JIT in generating code that only jumps in the uncommon case
			WriteNumberUnsafe(value);
			return;
		}

		WriteNumberUnsafe(value);
	}

	private void WriteNumberUnsafe<T>(T value) where T : unmanaged
	{
		Debug.Assert(BitConverter.IsLittleEndian);

		// Completely elides bounds checks that we'd otherwise have to pay for
		// Array.AsSpan() and BinaryPrimitives.WriteXXXXLittleEndian().
		var span = GetRemainingSpanUnsafe(Unsafe.SizeOf<T>());
		Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(span), value);
		_position += Unsafe.SizeOf<T>();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private Span<byte> GetRemainingSpanUnsafe(int length)
	{
		Debug.Assert(_position + length <= _buffer.Length);

		// Elide bounds checks, only call this after EnsureAvailable()
		return MemoryMarshal.CreateSpan(
			ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_buffer), _position),
			length);
	}

	private async Task FlushInternalAsync()
	{
		await _stream.WriteAsync(_buffer.AsMemory(0, _position));

		_position = 0;
	}

	private async ValueTask FlushInternalAsync(CancellationToken cancellationToken)
	{
		await _stream.WriteAsync(_buffer.AsMemory(0, _position), cancellationToken);

		_position = 0;
	}
}
