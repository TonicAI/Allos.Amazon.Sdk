using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Allos.Amazon.Sdk.Fork;
using Allos.Amazon.Sdk.S3.Transfer.Internal;
using Amazon;
using Amazon.S3;
using Serilog;

namespace Allos.Amazon.Sdk.S3.Transfer
{
    /// <inheritdoc cref="IAsyncTransferUtility"/>
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "MemberCanBeProtected.Global")]
    [DebuggerDisplay("{DebuggerDisplay}")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtility.cs", "Amazon.S3.Transfer")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/_async/TransferUtility.async.cs", "Amazon.S3.Transfer")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/_bcl45%2Bnetstandard/TransferUtility.async.cs", "Amazon.S3.Transfer")]
    public class AsyncTransferUtility : IAsyncTransferUtility
    {
        protected bool _isDisposed;
        protected readonly AsyncTransferConfig _config;
        protected readonly bool _shouldDispose;
        protected readonly HashSet<string> _blockedServiceNames = new HashSet<string>
        {
            "s3-object-lambda"
        };
        
        private static readonly Lazy<ILogger> _logger =
            new Lazy<ILogger>(() => TonicLogger.ForContext(typeof(AsyncTransferUtility)));
        [SuppressMessage("ReSharper", "UnusedMember.Local")]
        internal virtual ILogger Logger => _logger.Value;

        ILogger IAsyncTransferUtility.Logger => _logger.Value; 
        
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
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a parameter.
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
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a parameter.
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
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a parameter.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility(string awsAccessKeyId, string awsSecretAccessKey, AsyncTransferConfig config)
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
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a parameter.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility(
            string awsAccessKeyId, 
            string awsSecretAccessKey, 
            RegionEndpoint region, 
            AsyncTransferConfig config)
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
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a parameter.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility(IAmazonS3 s3Client)
            : this(s3Client, new AsyncTransferConfig())
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
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a parameter.
        /// Use an instance of <see cref="AmazonS3Client"/> constructed with an <see cref="AmazonS3Config"/> object with the Timeout specified. 
        /// </para>        
        /// </remarks>
        public AsyncTransferUtility(IAmazonS3 s3Client, AsyncTransferConfig config)
        {
            S3Client = s3Client;
            _config = config;
        }

        /// <summary>
        /// 	Constructs a new <see cref="AsyncTransferUtility"/> class.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a parameter.
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
        /// If a Timeout needs to be specified, use the constructor which takes an <see cref="AmazonS3Client"/> as a parameter.
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
        public AsyncTransferUtility(AsyncTransferConfig config)
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
        public virtual Task UploadAsync(string filePath, string bucketName, CancellationToken cancellationToken = default)
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
        public virtual Task UploadAsync(string filePath, string bucketName, string key, CancellationToken cancellationToken = default)
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
        public virtual Task UploadAsync(Stream stream, string bucketName, string key, CancellationToken cancellationToken = default)
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
        public virtual async Task UploadAsync(UploadRequest request, CancellationToken cancellationToken = default)
        {
            if (!request.IsSetBucketName())
            {
                ArgumentNullException.ThrowIfNull(request.BucketName);
            }
                
            CheckForBlockedArn(request.BucketName);
            var command = GetUploadCommand(this, request, null);
            bool isMultiPartUpload = command is MultipartUploadCommand;

            try
            {
                await command.ExecuteAsync(cancellationToken);
                
                if (isMultiPartUpload)
                {
                    var metadata = await S3Client.GetObjectMetadataAsync(
                            new()
                            {
                                BucketName = request.BucketName,
                                Key = request.Key,
                                ServerSideEncryptionCustomerMethod = request.ServerSideEncryptionCustomerMethod,
                                ServerSideEncryptionCustomerProvidedKey = request.ServerSideEncryptionCustomerProvidedKey
                            },
                            cancellationToken)
                        .ConfigureAwait(false);

                    var progressArgs = Config.UploadProgressArgsFactory.Create(
                        command,
                        0,
                        metadata.ContentLength.ToUInt64(),
                        request.ContentLength,
                        0,
                        request.FilePath
                    );

                    request.OnRaiseProgressEvent(progressArgs);
                }
            }
            finally
            {
                if (isMultiPartUpload &&
                    request.InputStream != null && 
                    !request.IsSetFilePath() && 
                    request.AutoCloseStream)
                {
                    request.InputStream.Close();
                }
            }
            
        }

        /// <summary>
        /// 	Aborts the multipart uploads that were initiated before the specified date.
        /// </summary>
        /// <param name="bucketName">
        /// 	The name of the bucket containing multipart uploads.
        /// </param>
        /// <param name="initiatedDateUtc">
        /// 	The date before which the multipart uploads were initiated.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        public virtual Task AbortMultipartUploadsAsync(
            string bucketName, 
            DateTimeOffset initiatedDateUtc, 
            CancellationToken cancellationToken = default)
        {
            CheckForBlockedArn(bucketName);
            AbortMultipartUploadsRequest request = new AbortMultipartUploadsRequest
            {
                BucketName = bucketName,
                InitiateDateUtc = initiatedDateUtc
            };
            var command = new AbortMultipartUploadsCommand(this, request);
            return command.ExecuteAsync(cancellationToken);
        }
        
        public virtual Task DownloadAsync(DownloadRequest request, CancellationToken cancellationToken = default)
        {
            if (!request.IsSetBucketName())
            {
                ArgumentNullException.ThrowIfNull(request.BucketName);   
            }
            
            CheckForBlockedArn(request.BucketName);
            var command = new DownloadCommand(this, request);
            return command.ExecuteAsync(cancellationToken);
        }
        
        public virtual Task<Stream> OpenStreamAsync(string bucketName, string key, CancellationToken cancellationToken = default)
        {
            OpenStreamRequest request = new OpenStreamRequest
            {
                BucketName = bucketName,
                Key = key
            };
            return OpenStreamAsync(request, cancellationToken);
        }
        
        public virtual async Task<Stream> OpenStreamAsync(OpenStreamRequest request, CancellationToken cancellationToken = default)
        {
            if (!request.IsSetBucketName())
            {
                ArgumentNullException.ThrowIfNull(request.BucketName);   
            }
            
            CheckForBlockedArn(request.BucketName);
            OpenStreamCommand command = new OpenStreamCommand(this, request);
            await command.ExecuteAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);
            
            ArgumentNullException.ThrowIfNull(command.ResponseStream);
            return command.ResponseStream;
        }

        internal static BaseCommand GetUploadCommand(IAsyncTransferUtility asyncTransferUtility, UploadRequest request, SemaphoreSlim? asyncThrottler)
        {
            Validate(request);
            if (request.IsMultipartUpload(asyncTransferUtility.Config))
            {
                var command = new MultipartUploadCommand(asyncTransferUtility, request, asyncTransferUtility.Logger);
                command.AsyncThrottler = asyncThrottler;
                return command;
            }
            else
            {
                var command = new SimpleUploadCommand(asyncTransferUtility, request);
                command.AsyncThrottler = asyncThrottler;
                return command;
            }
        }
        
        public virtual Task UploadDirectoryAsync(string directory, string bucketName, CancellationToken cancellationToken = default)
        {
            var request = ConstructUploadDirectoryRequest(directory, bucketName);
            return UploadDirectoryAsync(request, cancellationToken);
        }

        public virtual Task UploadDirectoryAsync(
            string directory, 
            string bucketName, 
            string searchPattern, 
            SearchOption searchOption, 
            CancellationToken cancellationToken = default)
        {
            var request = ConstructUploadDirectoryRequest(directory, bucketName, searchPattern, searchOption);
            return UploadDirectoryAsync(request, cancellationToken);
        }
        
        public virtual Task UploadDirectoryAsync(
            UploadDirectoryRequest request, 
            CancellationToken cancellationToken = default)
        {
            if (!request.IsSetBucketName())
            {
                ArgumentNullException.ThrowIfNull(request.BucketName);   
            }
            CheckForBlockedArn(request.BucketName);
            Validate(request);
            UploadDirectoryCommand command = new UploadDirectoryCommand(this, _config, request);
            command.UploadFilesConcurrently = request.UploadFilesConcurrently;
            return command.ExecuteAsync(cancellationToken);
        }

        public virtual Task DownloadDirectoryAsync(
            string bucketName, 
            string s3Directory, 
            string localDirectory, 
            CancellationToken cancellationToken = default)
        {
            var request = ConstructDownloadDirectoryRequest(bucketName, s3Directory, localDirectory);
            return DownloadDirectoryAsync(request, cancellationToken);
        }

        public virtual Task DownloadDirectoryAsync(
            DownloadDirectoryRequest request, 
            CancellationToken cancellationToken = default)
        {
            if (!request.IsSetBucketName())
            {
                ArgumentNullException.ThrowIfNull(request.BucketName);   
            }
            CheckForBlockedArn(request.BucketName);
            var command = new DownloadDirectoryCommand(this, request, _config);
            command.DownloadFilesConcurrently = request.DownloadFilesConcurrently;
            return command.ExecuteAsync(cancellationToken);
        }

        public virtual Task DownloadAsync(string filePath, string bucketName, string key, CancellationToken cancellationToken = default)
        {
            var request = ConstructDownloadRequest(filePath, bucketName, key);
            return DownloadAsync(request, cancellationToken);
        }
        
        public IAmazonS3 S3Client { get; private set; }

        public IAsyncTransferConfig Config => _config;

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
        
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void CheckForBlockedArn(string bucketName, [CallerMemberName] string? command = null)
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

        protected virtual UploadRequest ConstructUploadRequest(string filePath, string bucketName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException($"The file `{nameof(filePath)}` does not exist", filePath);
            }
            return new UploadRequest
            {
                BucketName = bucketName,
                FilePath = filePath
            };
        }

        protected virtual UploadRequest ConstructUploadRequest(string filePath, string bucketName, string key)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException($"The file `{nameof(filePath)}` does not exist", filePath);
            }
            return new UploadRequest
            {
                BucketName = bucketName,
                Key = key,
                FilePath = filePath
            };
        }

        protected virtual UploadRequest ConstructUploadRequest(Stream stream, string bucketName, string key)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentException.ThrowIfNullOrWhiteSpace(key);

            return new UploadRequest
            {
                BucketName = bucketName,
                Key = key,
                InputStream = stream
            };
        }

        internal virtual BaseCommand GetUploadCommand(UploadRequest request)
        {
            Validate(request);

            if (request.IsMultipartUpload(_config))
            {
                return new MultipartUploadCommand(this, request, Logger);
            }

            return new SimpleUploadCommand(this, request);
        }

        internal static void Validate(UploadRequest request)
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

        protected virtual DownloadRequest ConstructDownloadRequest(string filePath, string bucketName, string key)
        {
            return new DownloadRequest
            {
                BucketName = bucketName,
                Key = key,
                FilePath = filePath
            };
        }

        protected virtual DownloadDirectoryRequest ConstructDownloadDirectoryRequest(string bucketName, string s3Directory, string localDirectory)
        {
            return new DownloadDirectoryRequest
            {
                BucketName = bucketName,
                S3Directory = s3Directory,
                LocalDirectory = localDirectory
            };
        }

        protected virtual void Validate(UploadDirectoryRequest request)
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

        protected virtual UploadDirectoryRequest ConstructUploadDirectoryRequest(string directory, string bucketName)
        {
            return new UploadDirectoryRequest
            {
                BucketName = bucketName,
                Directory = directory
            };
        }

        protected virtual UploadDirectoryRequest ConstructUploadDirectoryRequest(string directory, string bucketName, string searchPattern, SearchOption searchOption)
        {
            return new UploadDirectoryRequest
            {
                BucketName = bucketName,
                Directory = directory,
                SearchPattern = searchPattern,
                SearchOption = searchOption
            };
        }
        
        internal virtual string DebuggerDisplay => ToString() ?? GetType().Name;
    }
}
