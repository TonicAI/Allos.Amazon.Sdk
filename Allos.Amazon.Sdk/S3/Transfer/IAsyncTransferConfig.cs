using System.Diagnostics.CodeAnalysis;

namespace Allos.Amazon.Sdk.S3.Transfer;

/// <summary>
/// 	<para>
/// 	Provides configuration options for how <see cref="IAsyncTransferUtility"/> processes requests.
/// 	</para>
/// 	<para>
/// 	The best configuration settings depend on network
/// 	configuration, latency and bandwidth. 
/// 	The default configuration settings are suitable
/// 	for most applications, but this class enables developers to experiment with
/// 	different configurations and tune transfer manager performance.
/// 	</para>
/// </summary>
[SuppressMessage("ReSharper", "UnusedMember.Global")]
public interface IAsyncTransferConfig
{
    /// <summary>
    /// Gets or sets the minimum size required (in bytes) to enable multipart upload. The default is 16 MB.
    /// If the file size is greater than or equal to MinSizeBeforePartUpload, multipart upload will be used.
    /// </summary>
    ulong MinSizeBeforePartUpload { get; set; }
    
    /// <summary>
    /// This property determines how many active threads
    /// or the number of concurrent asynchronous web requests 
    /// will be used to upload/download the file .
    /// The default value is 10.
    /// </summary>
    /// <remarks>
    /// 	A value less than or equal to 0 will be silently ignored.
    /// </remarks>
    uint ConcurrentServiceRequests { get; set; }
    
    /// <summary>
    /// The timeout for finalizing a multipart upload in milliseconds
    /// </summary>
    /// <remarks>The default is 5000</remarks>
    uint MultipartUploadFinalizeTimeout { get; set; }
    
    /// <summary>
    /// Get or set the factory for creating <see cref="UploadProgressArgs"/> instances.
    /// </summary>
    IUploadProgressArgsFactory UploadProgressArgsFactory { get; set; }
}