using System.Diagnostics.CodeAnalysis;
using Amazon.S3.Model;
using Amazon.Sdk.Fork;

namespace Amazon.Sdk.S3.Transfer;

/// <summary>
/// Encapsulates the information needed to provide
/// transfer progress to subscribers of the Put Object
/// Event.
/// </summary>
[SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
[SuppressMessage("ReSharper", "UnusedMember.Global")]
[AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityUploadRequest.cs", "Amazon.S3.Transfer")]
public class UploadProgressArgs : TransferProgressArgs
{
    /// <summary>
    /// The constructor takes the number of
    /// currently transferred bytes and the
    /// total number of bytes to be transferred
    /// </summary>
    /// <param name="incrementTransferred">The how many bytes were transferred since last event.</param>
    /// <param name="transferred">The number of bytes transferred</param>
    /// <param name="total">The total number of bytes to be transferred</param>
    public UploadProgressArgs(long incrementTransferred, long transferred, long total)
        : base(incrementTransferred, transferred, total)
    {
    }

    /// <summary>
    /// The constructor takes the number of
    /// currently transferred bytes and the
    /// total number of bytes to be transferred
    /// </summary>
    /// <param name="incrementTransferred">The how many bytes were transferred since last event.</param>
    /// <param name="transferred">The number of bytes transferred</param>
    /// <param name="total">The total number of bytes to be transferred</param>        
    /// <param name="filePath">The file being uploaded</param>
    public UploadProgressArgs(long incrementTransferred, long transferred, long total, string filePath)
        : this(incrementTransferred, transferred, total, 0, filePath)
    {
    }

    /// <summary>
    /// The constructor takes the number of
    /// currently transferred bytes and the
    /// total number of bytes to be transferred
    /// </summary>
    /// <param name="incrementTransferred">The how many bytes were transferred since last event.</param>
    /// <param name="transferred">The number of bytes transferred</param>
    /// <param name="total">The total number of bytes to be transferred</param>
    /// <param name="compensationForRetry">A compensation for any upstream aggregators of this event to correct the totalTransferred count,
    /// in case the underlying request is retried.</param>
    /// <param name="filePath">The file being uploaded</param>
    internal UploadProgressArgs(long incrementTransferred, long transferred, long total, long compensationForRetry, string? filePath)
        : base(incrementTransferred, transferred, total)
    {
        FilePath = filePath;
        CompensationForRetry = compensationForRetry;
    }

    /// <summary>
    /// Gets the FilePath.
    /// </summary>
    public string? FilePath { get; private set; }

    internal long CompensationForRetry { get; set; }
}