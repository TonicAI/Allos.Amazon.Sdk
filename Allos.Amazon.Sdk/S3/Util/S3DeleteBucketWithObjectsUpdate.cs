using System.Diagnostics.CodeAnalysis;
using Amazon.S3.Model;
using Amazon.Sdk.Fork;

namespace Amazon.Sdk.S3.Util
{
    /// <summary>
    /// Contains updates from DeleteS3BucketWithObjects operation.
    /// </summary>
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Util/S3DeleteBucketWithObjectsUpdate.cs", "Amazon.S3.Util")]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
    public class S3DeleteBucketWithObjectsUpdate
    {
        /// <summary>
        /// The list of objects which were successfully deleted.
        /// </summary>
        public IList<DeletedObject> DeletedObjects { get; set; } = new List<DeletedObject>();

        /// <summary>
        /// The list of objects for which the delete operation failed.
        /// </summary>
        public IList<DeleteError> DeleteErrors { get; set; } = new List<DeleteError>();
    }
}
