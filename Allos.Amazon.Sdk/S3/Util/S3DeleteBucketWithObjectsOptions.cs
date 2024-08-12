using Amazon.Sdk.Fork;

namespace Amazon.Sdk.S3.Util
{
    /// <summary>
    /// Options which control the behaviour of the DeleteS3BucketWithObjects operation.
    /// </summary>
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Util/S3DeleteBucketWithObjectsOptions.cs", "Amazon.S3.Util")]
    public class S3DeleteBucketWithObjectsOptions
    {
        /// <summary>
        /// Gets or sets a value which indicates whether the 
        /// operation should be aborted if an error is encountered during execution.
        /// </summary>
        public bool ContinueOnError { get; init; }

        /// <summary>
        /// Gets or sets a value which indicated whether verbose results shoule be returned to the
        /// <see cref="Action&lt;S3DeleteBucketWithObjectsUpdate&gt;" /> callback.
        /// If quiet mode is true the callback will receive only keys where the delete operation encountered an error.
        /// If quiet mode is false the callback will receive keys for both successful and unsuccessful delete operations.
        /// </summary>
        public bool QuietMode { get; init; }
    }


}
