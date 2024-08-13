using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Amazon.S3;
using Amazon.Sdk.Fork;
using Amazon.Sdk.S3.Transfer.Internal;
using Serilog;

namespace Amazon.Sdk.S3.Transfer
{
    /// <summary>
    /// 	<para>
    /// 	Provides a high level utility for managing transfers to and from Amazon S3.
    /// 	</para>
    /// 	<para>
    /// 	<c>TransferUtility</c> provides a simple API for 
    /// 	uploading content to and downloading content
    /// 	from Amazon S3. It makes extensive use of Amazon S3 multipart uploads to
    /// 	achieve enhanced throughput, performance, and reliability. 
    /// 	</para>
    /// 	<para>
    /// 	When uploading large files by specifying file paths instead of a stream, 
    /// 	<c>TransferUtility</c> uses multiple threads to upload
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
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtility.cs", "Amazon.S3.Transfer")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/_async/TransferUtility.async.cs", "Amazon.S3.Transfer")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/_bcl45%2Bnetstandard/TransferUtility.async.cs", "Amazon.S3.Transfer")]
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    public class AsyncTransferUtility : IAsyncTransferUtility
    {
        private readonly TransferUtilityConfig _config;
        private readonly bool _shouldDispose;
        private bool _isDisposed;
        private readonly HashSet<string> _blockedServiceNames = new()
        {
            "s3-object-lambda"
        };

        [SuppressMessage("ReSharper", "UnusedMember.Local")] 
        private static ILogger Logger => TonicLogger.ForContext<AsyncTransferUtility>();
        
        /// <summary>
        /// 	Constructs a new <see cref="AsyncTransferUtility"/> class.
        /// </summary>
        /// <param name="awsAccessKeyId">
        /// 	The AWS Access Key ID.
        /// </param>
        /// <param name="awsSecretAccessKey">
        /// 	The AWS Secret Access Key.
        /// </param>
        /// <remarks>
        /// <para>
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a paramater.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility(string awsAccessKeyId, string awsSecretAccessKey)
            : this(new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey))
        {
            _shouldDispose = true;
        }

        /// <summary>
        /// 	Constructs a new <see cref="AsyncTransferUtility"/> class.
        /// </summary>
        /// <param name="awsAccessKeyId">
        /// 	The AWS Access Key ID.
        /// </param>
        /// <param name="awsSecretAccessKey">
        /// 	The AWS Secret Access Key.
        /// </param>
        /// <param name="region">
        ///     The region to configure the transfer utility for.
        /// </param>
        /// <remarks>
        /// <para>
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a paramater.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility(string awsAccessKeyId, string awsSecretAccessKey, RegionEndpoint region)
            : this(new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, region))
        {
            _shouldDispose = true;
        }

        /// <summary>
        /// 	Constructs a new instance of the <see cref="AsyncTransferUtility"/> class.
        /// </summary>
        /// <param name="awsAccessKeyId">
        /// 	The AWS Access Key ID.
        /// </param>
        /// <param name="awsSecretAccessKey">
        /// 	The AWS Secret Access Key.
        /// </param>
        /// <param name="config">
        /// 	Specifies advanced settings.
        /// </param>
        /// <remarks>
        /// <para>
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a paramater.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility(string awsAccessKeyId, string awsSecretAccessKey, TransferUtilityConfig config)
            : this(new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey), config)
        {
            _shouldDispose = true;
        }

        /// <summary>
        /// 	Constructs a new instance of the <see cref="AsyncTransferUtility"/> class.
        /// </summary>
        /// <param name="awsAccessKeyId">
        /// 	The AWS Access Key ID.
        /// </param>
        /// <param name="awsSecretAccessKey">
        /// 	The AWS Secret Access Key.
        /// </param>
        /// <param name="region">
        ///     The region to configure the transfer utility for.
        /// </param>
        /// <param name="config">
        /// 	Specifies advanced settings.
        /// </param>
        /// <remarks>
        /// <para>
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a paramater.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility(string awsAccessKeyId, string awsSecretAccessKey, RegionEndpoint region, TransferUtilityConfig config)
            : this(new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, region), config)
        {
            _shouldDispose = true;
        }

        /// <summary>
        /// 	Constructs a new instance of the <see cref="AsyncTransferUtility"/> class.
        /// </summary>
        /// <param name="s3Client">
        /// 	The Amazon S3 client.
        /// </param>
        /// <remarks>
        /// <para>
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a paramater.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility(IAmazonS3 s3Client)
            : this(s3Client, new())
        {
        }

        /// <summary>
        /// 	Initializes a new instance of the <see cref="AsyncTransferUtility"/> class.
        /// </summary>
        /// <param name="s3Client">
        /// 	The Amazon S3 client.
        /// </param>
        /// <param name="config">
        /// 	Specifies advanced configuration settings for <see cref="AsyncTransferUtility"/>.
        /// </param>
        /// <remarks>
        /// <para>
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a paramater.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility(IAmazonS3 s3Client, TransferUtilityConfig config)
        {
            S3Client = s3Client;
            _config = config;
        }

        /// <summary>
        /// 	Constructs a new <see cref="AsyncTransferUtility"/> class.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a paramater.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility()
            : this(new AmazonS3Client())
        {
            _shouldDispose = true;
        }

        /// <summary>
        /// 	Constructs a new <see cref="AsyncTransferUtility"/> class.
        /// </summary>
        /// <param name="region">
        ///     The region to configure the transfer utility for.
        /// </param>
        /// <remarks>
        /// <para>
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a paramater.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility(RegionEndpoint region)
            : this(new AmazonS3Client(region))
        {
            _shouldDispose = true;
        }

        /// <summary>
        /// 	Constructs a new <see cref="AsyncTransferUtility"/> class.
        /// </summary>
        /// <param name="config">
        /// 	Specifies advanced configuration settings for <see cref="AsyncTransferUtility"/>.
        /// </param>
        /// <remarks>
        /// </remarks>
        public AsyncTransferUtility(TransferUtilityConfig config)
            : this(new AmazonS3Client(), config)
        {
            _shouldDispose = true;
            _config = config;
        }
        
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
        /// <para>
        /// For nonseekable streams or streams with an unknown length, TransferUtility will use multipart upload and buffer up to a part size in memory
        /// until the final part is reached and complete the upload. The buffer for the multipart upload is controlled by S3Constants.MinPartSize
        /// and the default value is 5 megabytes. You can also adjust the read buffer size(i.e.how many bytes to read before writing to the part buffer)
        /// via the BufferSize property on the ClientConfig.The default value for this is 8192 bytes.
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
        public Task UploadAsync(string filePath, string bucketName, CancellationToken cancellationToken = default)
        {
            var request = ConstructUploadRequest(filePath, bucketName);
            return UploadAsync(request, cancellationToken);
        }

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
        /// <para>
        /// For nonseekable streams or streams with an unknown length, TransferUtility will use multipart upload and buffer up to a part size in memory
        /// until the final part is reached and complete the upload. The buffer for the multipart upload is controlled by S3Constants.MinPartSize
        /// and the default value is 5 megabytes. You can also adjust the read buffer size(i.e.how many bytes to read before writing to the part buffer)
        /// via the BufferSize property on the ClientConfig.The default value for this is 8192 bytes.
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
        public Task UploadAsync(string filePath, string bucketName, string key, CancellationToken cancellationToken = default)
        {
            var request = ConstructUploadRequest(filePath, bucketName,key);
            return UploadAsync(request, cancellationToken);            
        }

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
        /// <para>
        /// For nonseekable streams or streams with an unknown length, TransferUtility will use multipart upload and buffer up to a part size in memory
        /// until the final part is reached and complete the upload. The buffer for the multipart upload is controlled by S3Constants.MinPartSize
        /// and the default value is 5 megabytes. You can also adjust the read buffer size(i.e.how many bytes to read before writing to the part buffer)
        /// via the BufferSize property on the ClientConfig.The default value for this is 8192 bytes.
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
        public Task UploadAsync(Stream stream, string bucketName, string key, CancellationToken cancellationToken = default)
        {
            var request = ConstructUploadRequest(stream, bucketName, key);
            return UploadAsync(request, cancellationToken);                    
        }

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
        /// <para>
        /// For nonseekable streams or streams with an unknown length, TransferUtility will use multipart upload and buffer up to a part size in memory 
        /// until the final part is reached and complete the upload. The part size buffer for the multipart upload is controlled by the partSize
        /// specified on the TransferUtilityUploadRequest, and if none is specified it defaults to S3Constants.MinPartSize (5 megabytes).
        /// You can also adjust the read buffer size (i.e. how many bytes to read before adding it to the 
        /// part buffer) via the BufferSize property on the ClientConfig. The default value for this is 8192 bytes.
        /// </para>
        /// </remarks>
        /// <param name="request">
        /// 	Contains all the parameters required to upload to Amazon S3.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        public Task UploadAsync(TransferUtilityUploadRequest request, CancellationToken cancellationToken = default)
        {
            if (!request.IsSetBucketName())
            {
                ArgumentNullException.ThrowIfNull(request.BucketName);
            }
                
            CheckForBlockedArn(request.BucketName);
            var command = GetUploadCommand(request, null);
            return command.ExecuteAsync(cancellationToken);
        }

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
        public Task AbortMultipartUploadsAsync(string bucketName, DateTime initiatedDate, CancellationToken cancellationToken = default)
        {
            CheckForBlockedArn(bucketName);
            var command = new AbortMultipartUploadsCommand(S3Client, bucketName, initiatedDate, _config);
            return command.ExecuteAsync(cancellationToken);
        }

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
        public Task DownloadAsync(TransferUtilityDownloadRequest request, CancellationToken cancellationToken = default)
        {
            if (!request.IsSetBucketName())
            {
                ArgumentNullException.ThrowIfNull(request.BucketName);   
            }
            
            CheckForBlockedArn(request.BucketName);
            var command = new DownloadCommand(S3Client, request);
            return command.ExecuteAsync(cancellationToken);
        }
        
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
        public Task<Stream> OpenStreamAsync(string bucketName, string key, CancellationToken cancellationToken = default)
        {
            TransferUtilityOpenStreamRequest request = new()
            {
                BucketName = bucketName,
                Key = key
            };
            return OpenStreamAsync(request, cancellationToken);
        }

        /// <summary>
        /// 	Returns a stream to read the contents from Amazon S3 as 
        /// 	specified by the <c>TransferUtilityOpenStreamRequest</c>.
        /// 	The caller of this method is responsible for closing the stream.
        /// </summary>
        /// <param name="request">
        /// 	Contains all the parameters required for the OpenStream operation.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        public async Task<Stream> OpenStreamAsync(TransferUtilityOpenStreamRequest request, CancellationToken cancellationToken = default)
        {
            if (!request.IsSetBucketName())
            {
                ArgumentNullException.ThrowIfNull(request.BucketName);   
            }
            
            CheckForBlockedArn(request.BucketName);
            OpenStreamCommand command = new(S3Client, request);
            await command.ExecuteAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
            
            ArgumentNullException.ThrowIfNull(command.ResponseStream);
            return command.ResponseStream;
        }

        internal BaseCommand GetUploadCommand(TransferUtilityUploadRequest request, SemaphoreSlim? asyncThrottler)
        {
            Validate(request);
            if (IsMultipartUpload(request))
            {
                var command = new MultipartUploadCommand(S3Client, _config, request);
                command.AsyncThrottler = asyncThrottler;
                return command;
            }
            else
            {
                var command = new SimpleUploadCommand(S3Client, request);
                command.AsyncThrottler = asyncThrottler;
                return command;
            }
        }
        
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
        public Task UploadDirectoryAsync(string directory, string bucketName, CancellationToken cancellationToken = default)
        {
            var request = ConstructUploadDirectoryRequest(directory, bucketName);
            return UploadDirectoryAsync(request, cancellationToken);
        }

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
        public Task UploadDirectoryAsync(string directory, string bucketName, string searchPattern, SearchOption searchOption, CancellationToken cancellationToken = default)
        {
            var request = ConstructUploadDirectoryRequest(directory, bucketName, searchPattern, searchOption);
            return UploadDirectoryAsync(request, cancellationToken);
        }

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
        public Task UploadDirectoryAsync(TransferUtilityUploadDirectoryRequest request, CancellationToken cancellationToken = default)
        {
            if (!request.IsSetBucketName())
            {
                ArgumentNullException.ThrowIfNull(request.BucketName);   
            }
            CheckForBlockedArn(request.BucketName);
            Validate(request);
            UploadDirectoryCommand command = new(this, _config, request);
            command.UploadFilesConcurrently = request.UploadFilesConcurrently;
            return command.ExecuteAsync(cancellationToken);
        }

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
        public Task DownloadDirectoryAsync(string bucketName, string s3Directory, string localDirectory, CancellationToken cancellationToken = default)
        {
            var request = ConstructDownloadDirectoryRequest(bucketName, s3Directory, localDirectory);
            return DownloadDirectoryAsync(request, cancellationToken);
        }

        /// <summary>
        /// 	Downloads the objects in Amazon S3 that have a key that starts with the value 
        /// 	specified by the <c>S3Directory</c>
        /// 	property of the passed in <c>TransferUtilityDownloadDirectoryRequest</c> object.
        /// </summary>
        /// <param name="request">
        /// 	Contains all the parameters required to download objects from Amazon S3 
        /// 	into a local directory.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        public Task DownloadDirectoryAsync(TransferUtilityDownloadDirectoryRequest request, CancellationToken cancellationToken = default)
        {
            if (!request.IsSetBucketName())
            {
                ArgumentNullException.ThrowIfNull(request.BucketName);   
            }
            CheckForBlockedArn(request.BucketName);
            var command = new DownloadDirectoryCommand(S3Client, request, _config);
            command.DownloadFilesConcurrently = request.DownloadFilesConcurrently;
            return command.ExecuteAsync(cancellationToken);
        }

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
        public Task DownloadAsync(string filePath, string bucketName, string key, CancellationToken cancellationToken = default)
        {
            var request = ConstructDownloadRequest(filePath, bucketName, key);
            return DownloadAsync(request, cancellationToken);
        }

        /// <summary>
        /// 	Gets the Amazon S3 client used for making calls into Amazon S3.
        /// </summary>
        /// <value>
        /// 	The Amazon S3 client used for making calls into Amazon S3.
        /// </value>
        public IAmazonS3 S3Client { get; private set; }

        /// <summary>
        /// Implements the Dispose pattern
        /// </summary>
        /// <param name="disposing">Whether this object is being disposed via a call to Dispose
        /// or garbage collected.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing && _shouldDispose)
                {
                    S3Client.Dispose();
                    S3Client = null!;
                }
                _isDisposed = true;
            }
        }

        /// <summary>
        /// Disposes of all managed and unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void CheckForBlockedArn(string bucketName, [CallerMemberName] string? command = null)
        {
            ArgumentNullException.ThrowIfNull(command);
            
            if (Arn.IsArn(bucketName))
            {
                Arn s3Arn = Arn.Parse(bucketName);
                if (_blockedServiceNames.Contains(s3Arn.Service))
                {
                    if (s3Arn.IsService("s3-object-lambda"))
                        throw new AmazonS3Exception($"`{command}` does not support S3 Object Lambda resources");
                }
            }
        }

        private static TransferUtilityUploadRequest ConstructUploadRequest(string filePath, string bucketName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException($"The file `{nameof(filePath)}` does not exist", filePath);
            }
            return new()
            {
                BucketName = bucketName,
                FilePath = filePath
            };
        }

        private static TransferUtilityUploadRequest ConstructUploadRequest(string filePath, string bucketName, string key)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException($"The file `{nameof(filePath)}` does not exist", filePath);
            }
            return new()
            {
                BucketName = bucketName,
                Key = key,
                FilePath = filePath
            };
        }

        private static TransferUtilityUploadRequest ConstructUploadRequest(Stream stream, string bucketName, string key)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentException.ThrowIfNullOrWhiteSpace(key);

            return new()
            {
                BucketName = bucketName,
                Key = key,
                InputStream = stream
            };
        }

        internal BaseCommand GetUploadCommand(TransferUtilityUploadRequest request)
        {
            Validate(request);

            if (IsMultipartUpload(request))
            {
                return new MultipartUploadCommand(S3Client, _config, request);
            }

            return new SimpleUploadCommand(S3Client, request);
        }

        private bool IsMultipartUpload(TransferUtilityUploadRequest request)
        {
            if (request.ContentLength.HasValue)
            {
                return request.ContentLength.Value >= (ulong) _config.MinSizeBeforePartUpload;
            }
            //If the length is null that means when we tried to get the ContentLength, we caught a NotSupportedException,
            //or it means the length is unknown. In this case we do a MultiPartUpload. If we are uploading
            //a nonseekable stream and the ContentLength is more than zero, we also do a multipart upload.
            return true;
        }

        private static void Validate(TransferUtilityUploadRequest request)
        {
            ArgumentNullException.ThrowIfNull(request);

            if (!request.IsSetBucketName())
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(request.BucketName);
            }
            if (!request.IsSetFilePath() &&
                !request.IsSetInputStream())
            {
                throw new ArgumentException(
                    $"Please specify either a `{nameof(request.FilePath)}` or provide a `{nameof(request.InputStream)}` to {nameof(HttpMethod.Put).ToUpperInvariant()} an object into Amazon S3");
            }
            if (!request.IsSetKey())
            {
                if (request.IsSetFilePath())
                {
                    request.Key = Path.GetFileName(request.FilePath);
                }
                else
                {
                    throw new ArgumentException(
                        $"The `{nameof(request.Key)}` property must be specified when using a `{nameof(Stream)}` to upload into Amazon S3");
                }
            }
            if (request.IsSetFilePath())
            {
                var fileExists = File.Exists(request.FilePath);
                if (!fileExists)
                {
                    throw new FileNotFoundException($"The file specified by the `{nameof(request.FilePath)}` property does not exist", request.FilePath);
                }
            }
        }

        private static TransferUtilityDownloadRequest ConstructDownloadRequest(string filePath, string bucketName, string key)
        {
            return new()
            {
                BucketName = bucketName,
                Key = key,
                FilePath = filePath
            };
        }

        private static TransferUtilityDownloadDirectoryRequest ConstructDownloadDirectoryRequest(string bucketName, string s3Directory, string localDirectory)
        {
            return new()
            {
                BucketName = bucketName,
                S3Directory = s3Directory,
                LocalDirectory = localDirectory
            };
        }

        private static void Validate(TransferUtilityUploadDirectoryRequest request)
        {
            if (!request.IsSetDirectory())
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(request.Directory);
            }
            if (!request.IsSetBucketName())
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(request.BucketName);
            }
            if (!Directory.Exists(request.Directory))
            {
                throw new DirectoryNotFoundException($"The directory {request.Directory} does not exist");
            }
        }

        private static TransferUtilityUploadDirectoryRequest ConstructUploadDirectoryRequest(string directory, string bucketName)
        {
            return new()
            {
                BucketName = bucketName,
                Directory = directory
            };
        }

        private static TransferUtilityUploadDirectoryRequest ConstructUploadDirectoryRequest(string directory, string bucketName, string searchPattern, SearchOption searchOption)
        {
            return new()
            {
                BucketName = bucketName,
                Directory = directory,
                SearchPattern = searchPattern,
                SearchOption = searchOption
            };
        }
    }
}
