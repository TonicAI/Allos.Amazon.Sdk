using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;

namespace Allos.Amazon.Sdk.S3.Transfer;

/// <summary>
/// Contains a single TransferUtilityUploadRequest corresponding
/// to a single file about to be uploaded, allowing changes to
/// the request before it is executed.
/// </summary>
[SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
[SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
[DebuggerDisplay("{DebuggerDisplay}")]
[AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityUploadDirectoryRequest.cs", "Amazon.S3.Transfer")]
public class UploadDirectoryFileRequestArgs : EventArgs
{
    /// <summary>
    /// Constructs a new UploadDirectoryFileRequestArgs instance.
    /// </summary>
    /// <param name="request">Request being processed.</param>
    public UploadDirectoryFileRequestArgs(UploadRequest request)
    {
        UploadRequest = request;
    }

    /// <summary>
    /// Gets and sets the UploadRequest property.
    /// </summary>
    public UploadRequest UploadRequest { get; set; }

    internal virtual string DebuggerDisplay => ToString() ?? GetType().Name;
}