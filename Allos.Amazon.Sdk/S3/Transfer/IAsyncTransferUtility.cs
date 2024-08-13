using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.S3;

namespace Allos.Amazon.Sdk.S3.Transfer
{
    /// <summary>
    /// 	<para>
    /// 	Provides a high level utility for managing transfers to and from Amazon S3.
    /// 	</para>
    /// 	<para>
    /// 	<see cref="AsyncTransferUtility"/> provides a simple API for 
    /// 	uploading content to and downloading content
    /// 	from Amazon S3. It makes extensive use of Amazon S3 multipart uploads to
    /// 	achieve enhanced throughput, performance, and reliability. 
    /// 	</para>
    /// 	<para>
    /// 	When uploading large files by specifying file paths instead of a stream, 
    /// 	<see cref="AsyncTransferUtility"/> uses multiple threads to upload
    /// 	multiple parts of a single upload at once. When dealing with large content
    /// 	sizes and high bandwidth, this can increase throughput significantly.
    /// 	</para>
    /// </summary>
    /// <remarks>
    /// 	<para>
    /// 	Transfers are stored in memory. If the application is restarted, 
    /// 	previous transfers are no longer accessible. In this situation, if necessary, 
    /// 	you should clean up any multipart uploads that are incomplete.
    /// 	</para>
    /// </remarks>
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    [SuppressMessage("ReSharper", "UnusedMemberInSuper.Global")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/ITransferUtility.cs", "Amazon.S3.Transfer")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/_async/ITransferUtility.async.cs", "Amazon.S3.Transfer")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/_bcl45%2Bnetstandard/ITransferUtility.async.cs", "Amazon.S3.Transfer")]
    public interface IAsyncTransferUtility : IDisposable
    {
        /// <summary>
        /// 	Uploads the specified file.  
        /// 	The object key is derived from the file's name.
        /// 	Multiple threads are used to read the file and perform multiple uploads in parallel.  
        /// 	For large uploads, the file will be divided and uploaded in parts using 
        /// 	Amazon S3's multipart API.  The parts will be reassembled as one object in
        /// 	Amazon S3.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If you are uploading large files, TransferUtility will use multipart upload to fulfill the request. 
        /// If a multipart upload is interrupted, TransferUtility will attempt to abort the multipart upload. 
        /// Under certain circumstances (network outage, power failure, etc.), TransferUtility will not be able 
        /// to abort the multipart upload. In this case, in order to stop getting charged for the storage of uploaded parts,
        /// you should manually invoke TransferUtility.AbortMultipartUploadsAsync() to abort the incomplete multipart uploads.
        /// </para>
        /// </remarks>
        /// <param name="filePath">
        /// 	The file path of the file to upload.
        /// </param>
        /// <param name="bucketName">
        /// 	The target Amazon S3 bucket, that is, the name of the bucket to upload the file to.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task UploadAsync(string filePath, string bucketName, CancellationToken cancellationToken = default);

        /// <summary>
        /// 	Uploads the specified file.  
        /// 	Multiple threads are used to read the file and perform multiple uploads in parallel.  
        /// 	For large uploads, the file will be divided and uploaded in parts using 
        /// 	Amazon S3's multipart API.  The parts will be reassembled as one object in
        /// 	Amazon S3.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If you are uploading large files, TransferUtility will use multipart upload to fulfill the request. 
        /// If a multipart upload is interrupted, TransferUtility will attempt to abort the multipart upload. 
        /// Under certain circumstances (network outage, power failure, etc.), TransferUtility will not be able 
        /// to abort the multipart upload. In this case, in order to stop getting charged for the storage of uploaded parts,
        /// you should manually invoke TransferUtility.AbortMultipartUploadsAsync() to abort the incomplete multipart uploads.
        /// </para>
        /// </remarks>
        /// <param name="filePath">
        /// 	The file path of the file to upload.
        /// </param>
        /// <param name="bucketName">
        /// 	The target Amazon S3 bucket, that is, the name of the bucket to upload the file to.
        /// </param>
        /// <param name="key">
        /// 	The key under which the Amazon S3 object is stored.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task UploadAsync(string filePath, string bucketName, string key, CancellationToken cancellationToken = default);

        /// <summary>
        /// 	Uploads the contents of the specified stream.  
        /// 	For large uploads, the file will be divided and uploaded in parts using 
        /// 	Amazon S3's multipart API.  The parts will be reassembled as one object in
        /// 	Amazon S3.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If you are uploading large files, TransferUtility will use multipart upload to fulfill the request. 
        /// If a multipart upload is interrupted, TransferUtility will attempt to abort the multipart upload. 
        /// Under certain circumstances (network outage, power failure, etc.), TransferUtility will not be able 
        /// to abort the multipart upload. In this case, in order to stop getting charged for the storage of uploaded parts,
        /// you should manually invoke TransferUtility.AbortMultipartUploadsAsync() to abort the incomplete multipart uploads.
        /// </para>
        /// </remarks>
        /// <param name="stream">
        /// 	The stream to read to obtain the content to upload.
        /// </param>
        /// <param name="bucketName">
        /// 	The target Amazon S3 bucket, that is, the name of the bucket to upload the stream to.
        /// </param>
        /// <param name="key">
        /// 	The key under which the Amazon S3 object is stored.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task UploadAsync(Stream stream, string bucketName, string key, CancellationToken cancellationToken = default);

        /// <summary>
        /// 	Uploads the file or stream specified by the request.  
        /// 	To track the progress of the upload,
        /// 	add an event listener to the request's <c>UploadProgressEvent</c>.
        /// 	For large uploads, the file will be divided and uploaded in parts using 
        /// 	Amazon S3's multipart API.  The parts will be reassembled as one object in
        /// 	Amazon S3.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If you are uploading large files, TransferUtility will use multipart upload to fulfill the request. 
        /// If a multipart upload is interrupted, TransferUtility will attempt to abort the multipart upload. 
        /// Under certain circumstances (network outage, power failure, etc.), TransferUtility will not be able 
        /// to abort the multipart upload. In this case, in order to stop getting charged for the storage of uploaded parts,
        /// you should manually invoke TransferUtility.AbortMultipartUploadsAsync() to abort the incomplete multipart uploads.
        /// </para>
        /// </remarks>
        /// <param name="request">
        /// 	Contains all the parameters required to upload to Amazon S3.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task UploadAsync(UploadRequest request, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// 	Aborts the multipart uploads that were initiated before the specified date.
        /// </summary>
        /// <param name="bucketName">
        /// 	The name of the bucket containing multipart uploads.
        /// </param>
        /// <param name="initiatedDate">
        /// 	The date before which the multipart uploads were initiated.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task AbortMultipartUploadsAsync(string bucketName, DateTime initiatedDate, CancellationToken cancellationToken = default);

        /// <summary>
        /// 	Downloads the content from Amazon S3 and writes it to the specified file.    
        /// 	If the key is not specified in the request parameter,
        /// 	the file name will used as the key name.
        /// </summary>
        /// <param name="request">
        /// 	Contains all the parameters required to download an Amazon S3 object.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task DownloadAsync(DownloadRequest request, CancellationToken cancellationToken = default);

        /// <summary>
        /// 	Returns a stream from which the caller can read the content from the specified
        /// 	Amazon S3  bucket and key.
        /// 	The caller of this method is responsible for closing the stream.
        /// </summary>
        /// <param name="bucketName">
        /// 	The name of the bucket.
        /// </param>
        /// <param name="key">
        /// 	The object key.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task<Stream> OpenStreamAsync(string bucketName, string key, CancellationToken cancellationToken = default);

        /// <summary>
        /// 	Returns a stream to read the contents from Amazon S3 as 
        /// 	specified by the <see cref="OpenStreamRequest"/>.
        /// 	The caller of this method is responsible for closing the stream.
        /// </summary>
        /// <param name="request">
        /// 	Contains all the parameters required for the OpenStream operation.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task<Stream> OpenStreamAsync(OpenStreamRequest request, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// 	Uploads files from a specified directory.  
        /// 	The object key is derived from the file names
        /// 	inside the directory.
        /// 	For large uploads, the file will be divided and uploaded in parts using 
        /// 	Amazon S3's multipart API.  The parts will be reassembled as one object in
        /// 	Amazon S3.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If you are uploading large files, TransferUtility will use multipart upload to fulfill the request. 
        /// If a multipart upload is interrupted, TransferUtility will attempt to abort the multipart upload. 
        /// Under certain circumstances (network outage, power failure, etc.), TransferUtility will not be able 
        /// to abort the multipart upload. In this case, in order to stop getting charged for the storage of uploaded parts,
        /// you should manually invoke TransferUtility.AbortMultipartUploads() to abort the incomplete multipart uploads.
        /// </para>
        /// </remarks>
        /// <param name="directory">
        /// 	The source directory, that is, the directory containing the files to upload.
        /// </param>
        /// <param name="bucketName">
        /// 	The target Amazon S3 bucket, that is, the name of the bucket to upload the files to.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task UploadDirectoryAsync(string directory, string bucketName, CancellationToken cancellationToken = default);

        /// <summary>
        /// 	Uploads files from a specified directory.  
        /// 	The object key is derived from the file names
        /// 	inside the directory.
        /// 	For large uploads, the file will be divided and uploaded in parts using 
        /// 	Amazon S3's multipart API.  The parts will be reassembled as one object in
        /// 	Amazon S3.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If you are uploading large files, TransferUtility will use multipart upload to fulfill the request. 
        /// If a multipart upload is interrupted, TransferUtility will attempt to abort the multipart upload. 
        /// Under certain circumstances (network outage, power failure, etc.), TransferUtility will not be able 
        /// to abort the multipart upload. In this case, in order to stop getting charged for the storage of uploaded parts,
        /// you should manually invoke TransferUtility.AbortMultipartUploads() to abort the incomplete multipart uploads.
        /// </para>
        /// </remarks>
        /// <param name="directory">
        /// 	The source directory, that is, the directory containing the files to upload.
        /// </param>
        /// <param name="bucketName">
        /// 	The target Amazon S3 bucket, that is, the name of the bucket to upload the files to.
        /// </param>
        /// <param name="searchPattern">
        /// 	A pattern used to identify the files from the source directory to upload.
        /// </param>                                                                 
        /// <param name="searchOption">
        /// 	A search option that specifies whether to recursively search for files to upload
        /// 	in subdirectories.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task UploadDirectoryAsync(string directory, string bucketName, string searchPattern, SearchOption searchOption, CancellationToken cancellationToken = default);

        /// <summary>
        /// 	Uploads files from a specified directory.  
        /// 	The object key is derived from the file names
        /// 	inside the directory.
        /// 	For large uploads, the file will be divided and uploaded in parts using 
        /// 	Amazon S3's multipart API.  The parts will be reassembled as one object in
        /// 	Amazon S3.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If you are uploading large files, TransferUtility will use multipart upload to fulfill the request. 
        /// If a multipart upload is interrupted, TransferUtility will attempt to abort the multipart upload. 
        /// Under certain circumstances (network outage, power failure, etc.), TransferUtility will not be able 
        /// to abort the multipart upload. In this case, in order to stop getting charged for the storage of uploaded parts,
        /// you should manually invoke TransferUtility.AbortMultipartUploads() to abort the incomplete multipart uploads.
        /// </para>
        /// </remarks>
        /// <param name="request">
        /// 	The request that contains all the parameters required to upload a directory.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task UploadDirectoryAsync(UploadDirectoryRequest request, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// 	Downloads the objects in Amazon S3 that have a key that starts with the value 
        /// 	specified by <c>s3Directory</c>.
        /// </summary>
        /// <param name="bucketName">
        /// 	The name of the bucket containing the Amazon S3 objects to download.
        /// </param>
        /// <param name="s3Directory">
        /// 	The directory in Amazon S3 to download.
        /// </param>
        /// <param name="localDirectory">
        /// 	The local directory to download the objects to.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task DownloadDirectoryAsync(string bucketName, string s3Directory, string localDirectory, CancellationToken cancellationToken = default);

        /// <summary>
        /// 	Downloads the objects in Amazon S3 that have a key that starts with the value 
        /// 	specified by the <c>S3Directory</c>
        /// 	property of the passed in <see cref="DownloadDirectoryRequest"/> object.
        /// </summary>
        /// <param name="request">
        /// 	Contains all the parameters required to download objects from Amazon S3 
        /// 	into a local directory.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task DownloadDirectoryAsync(DownloadDirectoryRequest request, CancellationToken cancellationToken = default);

        /// <summary>
        /// 	Downloads the content from Amazon S3 and writes it to the specified file.    
        /// </summary>
        /// <param name="filePath">
        /// 	The file path where the content from Amazon S3 will be written to.
        /// </param>
        /// <param name="bucketName">
        /// 	The name of the bucket containing the Amazon S3 object to download.
        /// </param>
        /// <param name="key">
        /// 	The key under which the Amazon S3 object is stored.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        Task DownloadAsync(string filePath, string bucketName, string key, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// 	Gets the Amazon S3 client used for making calls into Amazon S3.
        /// </summary>
        /// <value>
        /// 	The Amazon S3 client used for making calls into Amazon S3.
        /// </value>
        IAmazonS3 S3Client { get; }
    }
}
