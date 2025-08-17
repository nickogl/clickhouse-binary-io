namespace ClickHouse.BinaryIO;

/// <summary>
/// Thrown when parsing a ClickHouse type from a string failed.
/// </summary>
public sealed class ClickHouseTypeParseException : Exception
{
	/// <summary>
	/// Create a new parse exception.
	/// </summary>
	/// <param name="message">Reason for the failure.</param>
	public ClickHouseTypeParseException(string? message) : base(message)
	{
	}
}
