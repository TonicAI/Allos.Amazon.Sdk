using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.S3.Model;

namespace Allos.Amazon.Sdk.S3.Transfer;

/// <summary>
/// Encapsulates the information needed to provide
/// transfer progress to subscribers of the Put Object
/// Event.
/// </summary>
[SuppressMessage("ReSharper", "MemberCanBeProtected.Global")]
[SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
[SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
[SuppressMessage("ReSharper", "UnusedMember.Global")]
[SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
[DebuggerDisplay("{DebuggerDisplay}")]
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
    /// <param name="total">The total number of bytes to be transferred, if available</param>
    public UploadProgressArgs(ulong incrementTransferred, ulong transferred, ulong? total)
        : base(
            Convert.ToInt64(incrementTransferred), 
            Convert.ToInt64(transferred),
            total.HasValue ? 
                Convert.ToInt64(total.Value) : 
                Constants.UnknownContentLengthSentinel //supports the original behavior of shadowed properties
            )
    {
        IncrementTransferred = incrementTransferred;
        TransferredBytes = transferred;
        TotalBytes = total;
    }

    /// <summary>
    /// The constructor takes the number of
    /// currently transferred bytes and the
    /// total number of bytes to be transferred
    /// </summary>
    /// <param name="incrementTransferred">The how many bytes were transferred since last event.</param>
    /// <param name="transferred">The number of bytes transferred</param>
    /// <param name="total">The total number of bytes to be transferred, if available</param>        
    /// <param name="filePath">The file being uploaded</param>
    public UploadProgressArgs(ulong incrementTransferred, ulong transferred, ulong? total, string filePath)
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
    /// <param name="total">The total number of bytes to be transferred, if available</param>
    /// <param name="compensationForRetry">
    /// A compensation for any upstream aggregators of this event to correct the totalTransferred count,
    /// in case the underlying request is retried.
    /// </param>
    /// <param name="filePath">The file being uploaded</param>
    internal UploadProgressArgs(
        ulong incrementTransferred, 
        ulong transferred, 
        ulong? total, 
        ulong compensationForRetry, 
        string? filePath
        )
        : base(
            Convert.ToInt64(incrementTransferred), 
            Convert.ToInt64(transferred), 
            total.HasValue ? 
                Convert.ToInt64(total.Value) : 
                Constants.UnknownContentLengthSentinel //supports the original behavior of shadowed properties
            )
    {
        IncrementTransferred = incrementTransferred;
        TransferredBytes = transferred;
        FilePath = filePath;
        CompensationForRetry = compensationForRetry;
        TotalBytes = total;
    }
    
    /// <param name="argsWithoutCompensation">The event args prior to compensation for retry</param>
    /// <param name="compensationForRetry">
    /// A compensation for any upstream aggregators of this event to correct the totalTransferred count,
    /// in case the underlying request is retried.
    /// </param>
    internal UploadProgressArgs(UploadProgressArgs argsWithoutCompensation, ulong compensationForRetry)
        : this(
            argsWithoutCompensation.IncrementTransferred,
            argsWithoutCompensation.TransferredBytes,
            argsWithoutCompensation.TotalBytes,
            compensationForRetry,
            argsWithoutCompensation.FilePath)
    {
    }
    
    /// <summary>
    /// Gets the percentage of transfer completed, if possible
    /// </summary>
    public new virtual uint? PercentDone
    {
        get 
        {
            if (TotalBytes.HasValue)
            {
                if (TotalBytes == 0)
                {
                    return 100;
                }
                return (uint)((TransferredBytes * 100) / TotalBytes.Value);
            }

            return null;
        }
    }
    
    /// <summary>
    /// Gets the number of bytes transferred since last event
    /// </summary>
    public virtual ulong IncrementTransferred { get; }
    
    /// <inheritdoc cref="TransferProgressArgs.TransferredBytes"/>
    public new virtual ulong TransferredBytes { get; }
    
    /// <summary>
    /// Gets the total number of bytes to be transferred, if available
    /// </summary>
    public new virtual ulong? TotalBytes { get; }

    /// <summary>
    /// Gets the FilePath.
    /// </summary>
    public virtual string? FilePath { get; }
    
    /// <summary>
    /// A compensation for any upstream aggregators of this event to correct the totalTransferred count,
    /// in case the underlying request is retried.
    /// </summary>
    public virtual ulong CompensationForRetry { get; }

    /// <summary>
    /// Returns a string representation of this object
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        if (TotalBytes.HasValue)
        {
            return $"{PercentDone}% completed, {TransferredBytes} bytes transferred of {TotalBytes} total bytes";
        }
        return $"{TransferredBytes} bytes transferred";
    }
    
    internal virtual string DebuggerDisplay => ToString();
}