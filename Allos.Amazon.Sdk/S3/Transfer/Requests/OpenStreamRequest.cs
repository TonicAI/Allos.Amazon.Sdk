using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;

namespace Allos.Amazon.Sdk.S3.Transfer
{
    /// <summary>
    /// Contains all the parameters
    /// that can be set when making a request with the 
    /// <see cref="AsyncTransferUtility"/> method.
    /// </summary>
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [SuppressMessage("ReSharper", "RedundantTypeDeclarationBody")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityOpenStreamRequest.cs", "Amazon.S3.Transfer")]
    public class OpenStreamRequest : BaseDownloadRequest
    {
    }
}
