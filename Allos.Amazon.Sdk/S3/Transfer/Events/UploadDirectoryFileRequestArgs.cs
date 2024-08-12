using System.Diagnostics.CodeAnalysis;
using Amazon.Sdk.Fork;

namespace Amazon.Sdk.S3.Transfer;

/// <summary>
/// Contains a single TransferUtilityUploadRequest corresponding
/// to a single file about to be uploaded, allowing changes to
/// the request before it is executed.
/// </summary>
[SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
[AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityUploadDirectoryRequest.cs", "Amazon.S3.Transfer")]
public class UploadDirectoryFileRequestArgs : EventArgs
{
    /// <summary>
    /// Constructs a new UploadDirectoryFileRequestArgs instance.
    /// </summary>
    /// <param name="request">Request being processed.</param>
    public UploadDirectoryFileRequestArgs(TransferUtilityUploadRequest request)
    {
        UploadRequest = request;
    }

    /// <summary>
    /// Gets and sets the UploadRequest property.
    /// </summary>
    public TransferUtilityUploadRequest UploadRequest { get; set; }
}