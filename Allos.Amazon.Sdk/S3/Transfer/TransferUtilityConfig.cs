using System.Diagnostics.CodeAnalysis;
using Amazon.Sdk.Fork;

namespace Amazon.Sdk.S3.Transfer
{
    /// <summary>
    /// 	<para>
    /// 	Provides configuration options for how <see cref="AsyncTransferUtility"/> processes requests.
    /// 	</para>
    /// 	<para>
    /// 	The best configuration settings depend on network
    /// 	configuration, latency and bandwidth. 
    /// 	The default configuration settings are suitable
    /// 	for most applications, but this class enables developers to experiment with
    /// 	different configurations and tune transfer manager performance.
    /// 	</para>
    /// </summary>
    [SuppressMessage("ReSharper", "PropertyCanBeMadeInitOnly.Global")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityConfig.cs", "Amazon.S3.Transfer")]
    public class TransferUtilityConfig
    {
        private int _concurrentServiceRequests;

        /// <summary>
        /// Default constructor.
        /// </summary>
        public TransferUtilityConfig()
        {
            ConcurrentServiceRequests = 10;
        }

        /// <summary>
        /// Gets or sets the minimum size required (in bytes) to enable multi-part upload. The default is 16 MB.
        /// If the file size is greater than or equal to MinSizeBeforePartUpload, multi-part upload will be used.
        /// </summary>
        public long MinSizeBeforePartUpload { get; set; } = 16 * (long)Math.Pow(2, 20);

        /// <summary>
        /// This property determines how many active threads
        /// or the number of concurrent asynchronous web requests 
        /// will be used to upload/download the file .
        /// The default value is 10.
        /// </summary>
        /// <remarks>
        /// 	A value less than or equal to 0 will be silently ignored.
        /// </remarks>
        public int ConcurrentServiceRequests
        {
            get => _concurrentServiceRequests;
            set
            {
                if (value < 1)
                    value = 1;

                _concurrentServiceRequests = value;
            }
        }
    }
}
