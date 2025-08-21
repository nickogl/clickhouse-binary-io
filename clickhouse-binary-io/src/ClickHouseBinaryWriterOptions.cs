using System.Buffers;

namespace ClickHouse.BinaryIO;

/// <summary>
/// Configure instances of <see cref="ClickHouseBinaryWriter"/>.
/// </summary>
public readonly record struct ClickHouseBinaryWriterOptions
{
	/// <summary>Size of the buffer to use when writing the data. Defaults to 1 MB.</summary>
	/// <remarks>Memory is pooled from the provided <see cref="BufferPool"/>.</remarks>
	public int BufferSize { get; init; }

	/// <summary>Pool to use for pooling memory for the buffer. Defaults to <see cref="ArrayPool{T}.Shared"/>.</summary>
	public ArrayPool<byte> BufferPool { get; init; }

	/// <summary>If non-null, uses this buffer instead of pooling memory. Defaults to null.</summary>
	public byte[]? Buffer { get; init; }

	/// <summary>
	/// Create a new instance of <see cref="ClickHouseBinaryWriterOptions"/> with sensible defaults.
	/// </summary>
	public ClickHouseBinaryWriterOptions()
	{
		BufferSize = 1024 * 1024 * 1;
		BufferPool = ArrayPool<byte>.Shared;
		Buffer = null;
	}
}
