using Amazon.Sdk.Fork;

namespace Amazon.Sdk.S3.Transfer
{
    /// <summary>
    /// Contains all the parameters
    /// that can be set when making a request with the 
    /// <c>TransferUtility</c> method.
    /// </summary>
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityOpenStreamRequest.cs", "Amazon.S3.Transfer")]
    public class TransferUtilityOpenStreamRequest : BaseDownloadRequest
    {
    }
}
