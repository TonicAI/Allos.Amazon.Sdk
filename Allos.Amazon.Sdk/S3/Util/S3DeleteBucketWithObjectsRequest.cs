using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.S3;

namespace Allos.Amazon.Sdk.S3.Util
{
    /// <summary>
    /// Internal class used to pass the parameters for DeleteS3BucketWithObjects operation.
    /// </summary>
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Util/S3DeleteBucketWithObjectsRequest.cs", "Amazon.S3.Util")]
    internal class S3DeleteBucketWithObjectsRequest
    {
        /// <summary>
        /// Name of the bucket to be deleted.
        /// </summary>
        public string? BucketName { get; init; }
        
        /// <summary>
        /// Gets whether the bucket name is set.
        /// </summary>
        /// <returns>
        /// 	A value of <c>true</c> if the bucket name is set.
        ///    Returns <c>false</c> if otherwise.
        /// </returns>
        [MemberNotNullWhen(true, nameof(BucketName))]
        internal bool IsSetBucketName() => !string.IsNullOrWhiteSpace(BucketName);

        /// <summary>
        /// The Amazon S3 Client to use for S3 specific operations.
        /// </summary>
        public IAmazonS3? S3Client { get; init; }

        /// <summary>
        /// Options to control the behavior of the delete operation.
        /// </summary>
        public S3DeleteBucketWithObjectsOptions? DeleteOptions { get; init; }

        /// <summary>
        /// The callback which is used to send updates about the delete operation.
        /// </summary>
        public Action<S3DeleteBucketWithObjectsUpdate>? UpdateCallback { get; init; }
    }
}
