using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Allos.Amazon.Sdk.S3.Util;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Internal;
using Amazon.S3.Model;
using Amazon.Util;
using Serilog;

namespace Allos.Amazon.Sdk.S3.Transfer.Internal
{
    /// <summary>
    /// The command to manage an upload using the S3 multipart API.
    /// </summary>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/MultipartUploadCommand.cs", "Amazon.S3.Transfer.Internal")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/_async/MultipartUploadCommand.async.cs", "Amazon.S3.Transfer.Internal")]
    internal class MultipartUploadCommand : BaseCommand
    {
        protected readonly long _partSize;
        protected uint _totalNumberOfParts;
        protected readonly UploadRequest _fileTransporterRequest;
        protected readonly ConcurrentDictionary<uint, Stream> _inputStreams;

        protected List<UploadPartResponse> _uploadResponses;
        protected ulong _totalTransferredBytes;
        protected readonly Queue<UploadPartRequest> _partsToUpload;
        
        protected readonly ulong? _contentLength;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultipartUploadCommand"/> class.
        /// </summary>
        /// <param name="asyncTransferUtility">The <see cref="IAsyncTransferUtility"/> that created this command.</param>
        /// <param name="fileTransporterRequest">The file transporter request.</param>
        /// <param name="logger">Logger</param>
        internal MultipartUploadCommand(
            IAsyncTransferUtility asyncTransferUtility,
            UploadRequest fileTransporterRequest,
            ILogger logger)
            : base(asyncTransferUtility, fileTransporterRequest)
        {
            if (fileTransporterRequest.IsSetFilePath())
            {
                logger.Debug("Beginning upload of file `{FilePath}`", 
                    fileTransporterRequest.FilePath);
            }
            else if (fileTransporterRequest.IsSetInputStream())
            {
                logger.Debug("Beginning upload of `{StreamType}`", 
                    fileTransporterRequest.InputStream.GetType().FullName);
            }
            else
            {
                throw new ArgumentException(
                    $"One of `{nameof(fileTransporterRequest.FilePath)}` or `{nameof(fileTransporterRequest.InputStream)}` must be provided");
            }
            _uploadResponses = new List<UploadPartResponse>();
            _partsToUpload = new Queue<UploadPartRequest>();
            _inputStreams = new ConcurrentDictionary<uint, Stream>();
            _fileTransporterRequest = fileTransporterRequest;
            _contentLength = _fileTransporterRequest.ContentLength;

            _partSize = fileTransporterRequest.IsSetPartSize() ? 
                fileTransporterRequest.PartSize.ToInt64() : 
                CalculatePartSize(_contentLength);

            if (fileTransporterRequest.InputStream != null)
            {
                if (fileTransporterRequest.AutoResetStreamPosition && fileTransporterRequest.InputStream.CanSeek)
                {
                    fileTransporterRequest.InputStream.Seek(0, SeekOrigin.Begin);
                }
            }

            logger.Debug("Upload part size {PartSize}", _partSize);
        }
        
        public virtual SemaphoreSlim? AsyncThrottler { get; set; }

        public override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            if (_fileTransporterRequest.InputStream is { CanSeek: false } ||
                 !_fileTransporterRequest.ContentLength.HasValue)
            {
                await UploadUnseekableStreamAsync(_fileTransporterRequest, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                ArgumentNullException.ThrowIfNull(_fileTransporterRequest.ContentLength);
                
                var initRequest = ConstructInitiateMultipartUploadRequest();
                var initResponse = await S3Client.InitiateMultipartUploadAsync(initRequest, cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);
                
                Logger.Debug("Initiated upload: {UploadId}", 
                    initResponse.UploadId);

                var pendingUploadPartTasks = new List<Task<UploadPartResponse>>();

                SemaphoreSlim? localThrottler = null;
                CancellationTokenSource? internalCts = null;
                try
                {
                    Logger.Debug("Queue up the {Request}s to be executed", 
                        nameof(UploadPartRequest));
                    
                    var contentLengthLong = Convert.ToInt64(_contentLength);
                    
                    long filePosition = 0;
                    
                    for (uint i = 1; filePosition < contentLengthLong; i++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var uploadRequest = ConstructUploadPartRequest(i, filePosition, initResponse);
                        _partsToUpload.Enqueue(uploadRequest);
                        filePosition += _partSize;
                    }

                    _totalNumberOfParts = _partsToUpload.Count.ToUInt32();

                    Logger.Debug("Scheduling the {TotalNumberOfParts} UploadPartRequests in the queue",
                        _totalNumberOfParts);

                    internalCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    var concurrencyLevel = CalculateConcurrentServiceRequests();
                    localThrottler = AsyncThrottler ?? new SemaphoreSlim(concurrencyLevel.ToInt32());

                    foreach (var uploadRequest in _partsToUpload)
                    {
                        await localThrottler.WaitAsync(cancellationToken)
                            .ConfigureAwait(continueOnCapturedContext: false);

                        cancellationToken.ThrowIfCancellationRequested();
                        if (internalCts.IsCancellationRequested)
                        {
                            // Operation cancelled as one of the UploadParts requests failed with an exception,
                            // don't schedule anymore UploadPart tasks.
                            // Don't throw an OperationCanceledException here as we want to process the 
                            // responses and throw the original exception.
                            break;
                        }

                        var task = UploadPartAsync(uploadRequest, internalCts, localThrottler);
                        
                        pendingUploadPartTasks.Add(task);
                    }

                    Logger.Debug("Waiting for upload part requests to complete `{UploadId}`", 
                        initResponse.UploadId);
                    _uploadResponses = await WhenAllOrFirstExceptionAsync(pendingUploadPartTasks, cancellationToken)
                        .ConfigureAwait(continueOnCapturedContext: false);

                    Logger.Debug("Beginning completing multipart `{UploadId}`", 
                        initResponse.UploadId);
                    var compRequest = ConstructCompleteMultipartUploadRequest(initResponse);
                    await S3Client.CompleteMultipartUploadAsync(compRequest, cancellationToken)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    Logger.Debug("Done completing multipart `{UploadId}`", 
                        initResponse.UploadId);

                }
                catch (Exception e)
                {
                    Logger.Error(e, "Exception while uploading `{UploadId}`", 
                        initResponse.UploadId);
                    // Can't do async invocation in the catch block, doing cleanup synchronously.
                    await Cleanup(initResponse.UploadId, pendingUploadPartTasks).ConfigureAwait(false);
                    throw;
                }
                finally
                {
                    if (internalCts != null)
                        internalCts.Dispose();

                    if (localThrottler != null && localThrottler != AsyncThrottler)
                        localThrottler.Dispose();
                    
                    foreach (var inputStream in _inputStreams.Values)
                    {
                        inputStream.Close();
                    }
                    _inputStreams.Clear();
                } 
            }
        }

        protected virtual async Task<UploadPartResponse> UploadPartAsync(
            UploadPartRequest uploadRequest, 
            CancellationTokenSource internalCts, 
            SemaphoreSlim asyncThrottler)
        {
            try
            {
                return await S3Client.UploadPartAsync(uploadRequest, internalCts.Token)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            catch (Exception exception)
            {
                if (!(exception is OperationCanceledException))
                {
                    // Cancel scheduling any more tasks
                    // Cancel other UploadPart requests running in parallel.
                    await internalCts.CancelAsync().ConfigureAwait(false);
                }
                throw;
            }
            finally
            {
                uploadRequest.InputStream.Close();
                asyncThrottler.Release();
            }
        }       

        protected virtual async Task Cleanup(string uploadId, List<Task<UploadPartResponse>> tasks)
        {
            await WaitForAllTasksAsync(tasks).ConfigureAwait(false);;
            await AbortMultipartUploadAsync(uploadId).ConfigureAwait(false);
        }

        protected virtual async Task WaitForAllTasksAsync(List<Task<UploadPartResponse>> tasks)
        {
            try
            {
                // Make sure all tasks complete (to completion/faulted/cancelled).
                await Task.WhenAll(tasks.Cast<Task>())
                    .WaitAsync(TimeSpan.FromMilliseconds(Config.MultipartUploadFinalizeTimeout))
                    .ConfigureAwait(false);
            }
            catch(Exception exception)
            {
                Logger.Information(
                    exception,
                    "A timeout occured while waiting for all upload part request to complete as part of aborting the multipart upload : {ErrorMessage}",
                    exception.Message);
            }
            
        }

        protected virtual async Task AbortMultipartUploadAsync(string uploadId)
        {
            try
            {
                await S3Client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                {
                    BucketName = _fileTransporterRequest.BucketName,
                    Key = _fileTransporterRequest.Key,
                    UploadId = uploadId
                });
            }
            catch (Exception e)
            {
                Logger.Information("Error attempting to abort multipart for key `{ObjectKey}`: {ErrorMessage}", 
                    _fileTransporterRequest.Key, 
                    e.Message);
            }
        }
        protected virtual async Task UploadUnseekableStreamAsync(
            UploadRequest request, 
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request.InputStream);
            
            int readBufferSize = S3Client.Config.BufferSize;

            RequestEventHandler requestEventHandler = (_, args) =>
            {
                if (args is WebServiceRequestEventArgs wsArgs)
                {
                    string currentUserAgent = wsArgs.Headers[AWSSDKUtils.UserAgentHeader];
                    wsArgs.Headers[AWSSDKUtils.UserAgentHeader] =
                        currentUserAgent + " ft/s3-transfer md/UploadNonSeekableStream";
                }
            };

            var initiateRequest = ConstructInitiateMultipartUploadRequest(requestEventHandler);
            var initiateResponse = await S3Client.InitiateMultipartUploadAsync(initiateRequest, cancellationToken)
                .ConfigureAwait(false);

            try
            {
                // if partSize is not specified on the request, the default value is 0
                long minPartSize = request.PartSize != 0 ? request.PartSize.ToInt64() : S3Constants.MinPartSize;
                var uploadPartResponses = new List<UploadPartResponse>();
                var readBuffer = ArrayPool<byte>.Shared.Rent(readBufferSize);
                var partBuffer = ArrayPool<byte>.Shared.Rent((int)minPartSize + (readBufferSize));
                MemoryStream nextUploadBuffer = new MemoryStream(partBuffer);
                await using (var stream = request.InputStream)
                {
                    try
                    {
                        uint partNumber = 1;
                        int readAheadBytesCount;

                        var readBytesCount = await stream.ReadAsync(readBuffer, 0, readBuffer.Length, cancellationToken)
                            .ConfigureAwait(false);

                        do
                        {
                            await nextUploadBuffer.WriteAsync(readBuffer, 0, readBytesCount, cancellationToken)
                                .ConfigureAwait(false);
                            // read the stream ahead and process it in the next iteration.
                            // this is used to set isLastPart when there is no data left in the stream.
                            readAheadBytesCount = await stream.ReadAsync(readBuffer, 0, readBuffer.Length, cancellationToken)
                                .ConfigureAwait(false);

                            if ((nextUploadBuffer.Position > minPartSize || readAheadBytesCount == 0))
                            {
                                if (nextUploadBuffer.Position == 0)
                                {
                                    if (partNumber == 1)
                                    {
                                        // if the input stream is empty then upload empty MemoryStream.
                                        // without doing this the UploadPart call will use the length of the
                                        // nextUploadBuffer as the pastSize. The length will be incorrectly computed
                                        // for the part as (int)minPartSize + (READ_BUFFER_SIZE) as defined above for partBuffer.
                                        await nextUploadBuffer.DisposeAsync().ConfigureAwait(false);
                                        nextUploadBuffer = new MemoryStream();
                                    }
                                }
                                bool isLastPart = readAheadBytesCount == 0;

                                var partSize = nextUploadBuffer.Position;
                                nextUploadBuffer.Position = 0;
                                
                                UploadPartRequest uploadPartRequest = ConstructUploadPartRequestForNonSeekableStream(
                                    nextUploadBuffer, 
                                    partNumber, 
                                    partSize.ToUInt64(), 
                                    isLastPart, 
                                    initiateResponse);

                                var partResponse = await S3Client.UploadPartAsync(uploadPartRequest, cancellationToken).ConfigureAwait(false);
                                Logger.Debug(
                                    "Uploaded part {PartNumber} (PartSize={PartSize} UploadId={UploadId} IsLastPart={IsLastPart})", 
                                    partNumber, 
                                    partSize, 
                                    initiateResponse.UploadId,
                                    isLastPart);
                                uploadPartResponses.Add(partResponse);
                                partNumber++;

                                await nextUploadBuffer.DisposeAsync().ConfigureAwait(false);
                                nextUploadBuffer = new MemoryStream(partBuffer);
                            }
                            readBytesCount = readAheadBytesCount;
                        }
                        while (readAheadBytesCount > 0);
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(partBuffer);
                        ArrayPool<byte>.Shared.Return(readBuffer);
                        await nextUploadBuffer.DisposeAsync().ConfigureAwait(false);
                    }

                    _uploadResponses = uploadPartResponses;
                    CompleteMultipartUploadRequest compRequest = ConstructCompleteMultipartUploadRequest(
                        initiateResponse, 
                        true,
                        requestEventHandler);
                    await S3Client.CompleteMultipartUploadAsync(compRequest, cancellationToken).ConfigureAwait(false);
                    Logger.Debug("Completed multi part upload (PartCount={PartCount}, UploadId={UploadId})",
                        uploadPartResponses.Count, 
                        initiateResponse.UploadId);
                }
            }
            catch (Exception ex)
            {
                await S3Client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    UploadId = initiateResponse.UploadId
                }, cancellationToken).ConfigureAwait(false);
                Logger.Error(ex, "{ErrorMessage}", ex.Message);
                throw;
            }
        }

        protected static long CalculatePartSize(ulong? fileSize)
        {
            if(fileSize == null)
            {
                return S3Constants.MinPartSize;
            }
            double partSize = Math.Ceiling((double)fileSize / S3Constants.MaxNumberOfParts);
            if (partSize < S3Constants.MinPartSize)
            {
                partSize = S3Constants.MinPartSize;
            }

            return (long)partSize;
        }

        protected virtual string? DetermineContentType()
        {
            if (_fileTransporterRequest.IsSetContentType())
                return _fileTransporterRequest.ContentType;

            if (_fileTransporterRequest.IsSetFilePath() ||
                _fileTransporterRequest.IsSetKey())
            {
                // Get the extension of the file from the path.
                // Try the key as well.
                string ext = AWSSDKUtils.GetExtension(_fileTransporterRequest.FilePath);
                if (string.IsNullOrWhiteSpace(ext) &&
                    _fileTransporterRequest.IsSetKey())
                {
                    ext = AWSSDKUtils.GetExtension(_fileTransporterRequest.Key);
                }

                string type = AmazonS3Util.MimeTypeFromExtension(ext);
                return type;
            }
            return null;
        }

        protected virtual uint CalculateConcurrentServiceRequests()
        {
            uint threadCount;
            if (_fileTransporterRequest.IsSetFilePath()
                && !(S3Client is IAmazonS3Encryption))
            {
                threadCount = Config.ConcurrentServiceRequests;
            }
            else
            {
                threadCount = 1; // When using streams or encryption, multiple threads can not be used to read from the same stream.
            }

            if (_totalNumberOfParts < threadCount)
            {
                threadCount = _totalNumberOfParts;
            }
            return threadCount;
        }

        protected virtual CompleteMultipartUploadRequest ConstructCompleteMultipartUploadRequest(
            InitiateMultipartUploadResponse initResponse
            ) => 
            ConstructCompleteMultipartUploadRequest(initResponse, false, null);

        protected virtual CompleteMultipartUploadRequest ConstructCompleteMultipartUploadRequest(
            InitiateMultipartUploadResponse initResponse, 
            bool skipPartValidation, 
            RequestEventHandler? requestEventHandler)
        {
            if (!skipPartValidation)
            {
                if (_uploadResponses.Count != _totalNumberOfParts)
                {
                    throw new InvalidOperationException(
                        $"Cannot complete multipart upload request. The total number of completed parts ({_uploadResponses.Count}) " +
                        $"does not equal the total number of parts created ({_totalNumberOfParts})");
                }
            }

            var compRequest = new CompleteMultipartUploadRequest
            {
                BucketName = _fileTransporterRequest.BucketName,
                Key = _fileTransporterRequest.Key,
                UploadId = initResponse.UploadId
            };

            if(_fileTransporterRequest.ServerSideEncryptionCustomerMethod != null 
                && _fileTransporterRequest.ServerSideEncryptionCustomerMethod != ServerSideEncryptionCustomerMethod.None)
            {
                compRequest.SSECustomerAlgorithm = _fileTransporterRequest.ServerSideEncryptionCustomerMethod.ToString();
            }

            compRequest.AddPartETags(_uploadResponses);

            ((IAmazonWebServiceRequest)compRequest).AddBeforeRequestHandler(requestEventHandler ?? RequestEventHandler);

            return compRequest;
        }

        protected virtual UploadPartRequest ConstructUploadPartRequest(
            uint partNumber, 
            long filePosition, 
            InitiateMultipartUploadResponse initiateResponse)
        {
            ArgumentNullException.ThrowIfNull(_contentLength);

            var contentLengthLong = Convert.ToInt64(_contentLength);

            string partFileName;
            string partFileFauxPath;
            
            if (_fileTransporterRequest.IsSetFilePath())
            {
                var fileInfo = new FileInfo(_fileTransporterRequest.FilePath);
                
                if (!fileInfo.Exists)
                {
                    throw new FileNotFoundException(
                        $"The file `{fileInfo.FullName}` does not exist",
                        nameof(_fileTransporterRequest.FilePath));
                }
                
                partFileName = $"{fileInfo.Name}.part{partNumber}";

                if (fileInfo.Directory != null)
                {
                    partFileFauxPath = Path.Combine(fileInfo.Directory.FullName, partFileName);
                }
                else
                {
                    partFileFauxPath = partFileName;
                }
            }
            else
            {
                partFileName = $"{_fileTransporterRequest.Key}.part{partNumber}";
                partFileFauxPath = partFileName;
            }
            
            UploadPartRequest uploadPartRequest = ConstructGenericUploadPartRequest(initiateResponse);

            uploadPartRequest.PartNumber = Convert.ToInt32(partNumber);
            uploadPartRequest.PartSize = _partSize;

            if ((filePosition + _partSize >= contentLengthLong)
                && S3Client is IAmazonS3Encryption)
            {
                uploadPartRequest.IsLastPart = true;
                uploadPartRequest.PartSize = 0;
            }

            var progressHandler = new ProgressHandler(
                Config,
                this,
                S3Client.Config.ProgressUpdateInterval.ToUInt64(),
                _contentLength,
                partFileFauxPath,
                UploadPartProgressEventCallback,
                Logger
                );
            
            ((IAmazonWebServiceRequest)uploadPartRequest).AddBeforeRequestHandler(RequestEventHandler);

            if (_fileTransporterRequest.IsSetFilePath())
            {
                uploadPartRequest.FilePosition = filePosition;
                uploadPartRequest.FilePath = _fileTransporterRequest.FilePath;
                
                var partInputStream = File.OpenRead(_fileTransporterRequest.FilePath);
                partInputStream.Seek(filePosition, SeekOrigin.Begin);
                _inputStreams.TryAdd(partNumber, partInputStream);
            
                uploadPartRequest.FilePath = null;
                uploadPartRequest.InputStream = partInputStream;
            }
            else
            {
                uploadPartRequest.InputStream = _fileTransporterRequest.InputStream;
            }

            if (uploadPartRequest.InputStream == null)
            {
                ArgumentNullException.ThrowIfNull(uploadPartRequest.InputStream);
            }

            var eventStream = new EventStream(uploadPartRequest.InputStream, logger: Logger);
            
            eventStream.OnRead += progressHandler.OnBytesRead;
            
            uploadPartRequest.InputStream = eventStream;

            return uploadPartRequest;
        }

        protected virtual UploadPartRequest ConstructGenericUploadPartRequest(InitiateMultipartUploadResponse initiateResponse)
        {
            UploadPartRequest uploadPartRequest = new UploadPartRequest
            {
                BucketName = _fileTransporterRequest.BucketName,
                Key = _fileTransporterRequest.Key,
                UploadId = initiateResponse.UploadId,
                ServerSideEncryptionCustomerMethod = _fileTransporterRequest.ServerSideEncryptionCustomerMethod,
                ServerSideEncryptionCustomerProvidedKey = _fileTransporterRequest.ServerSideEncryptionCustomerProvidedKey,
                ServerSideEncryptionCustomerProvidedKeyMD5 = _fileTransporterRequest.ServerSideEncryptionCustomerProvidedKeyMd5,
                DisableDefaultChecksumValidation = _fileTransporterRequest.DisableDefaultChecksumValidation,
                DisablePayloadSigning = _fileTransporterRequest.DisablePayloadSigning,
                ChecksumAlgorithm = _fileTransporterRequest.ChecksumAlgorithm,
                CalculateContentMD5Header = _fileTransporterRequest.CalculateContentMd5Header
            };

            // If the InitiateMultipartUploadResponse indicates that this upload is using KMS, force SigV4 for each UploadPart request
            bool useSigV4 = initiateResponse.ServerSideEncryptionMethod == ServerSideEncryptionMethod.AWSKMS || 
                            initiateResponse.ServerSideEncryptionMethod == ServerSideEncryptionMethod.AWSKMSDSSE;
            if (useSigV4)
                ((IAmazonWebServiceRequest)uploadPartRequest).SignatureVersion = SignatureVersion.SigV4;

            return uploadPartRequest;
        }

        protected virtual UploadPartRequest ConstructUploadPartRequestForNonSeekableStream(
            Stream inputStream, 
            uint partNumber, 
            ulong partSize, 
            bool isLastPart, 
            InitiateMultipartUploadResponse initiateResponse)
        {
            UploadPartRequest uploadPartRequest = ConstructGenericUploadPartRequest(initiateResponse);
            
            uploadPartRequest.InputStream = inputStream;
            uploadPartRequest.PartNumber = partNumber.ToInt32();
            uploadPartRequest.PartSize = partSize.ToInt64();
            uploadPartRequest.IsLastPart = isLastPart;

            var progressHandler = new ProgressHandler(
                Config,
                this,
                S3Client.Config.ProgressUpdateInterval.ToUInt64(),
                _contentLength,
                uploadPartRequest.FilePath, 
                UploadPartProgressEventCallback,
                Logger
                );
                
            var eventStream = new EventStream(uploadPartRequest.InputStream, logger: Logger);
            
            eventStream.OnRead += progressHandler.OnBytesRead;
            
            uploadPartRequest.InputStream = eventStream;
                
            ((IAmazonWebServiceRequest)uploadPartRequest).AddBeforeRequestHandler(RequestEventHandler);

            return uploadPartRequest;
        }

        protected virtual InitiateMultipartUploadRequest ConstructInitiateMultipartUploadRequest() => 
            ConstructInitiateMultipartUploadRequest(null);

        protected virtual InitiateMultipartUploadRequest ConstructInitiateMultipartUploadRequest(RequestEventHandler? requestEventHandler)
        {
            var initRequest = new InitiateMultipartUploadRequest
            {
                BucketName = _fileTransporterRequest.BucketName,
                Key = _fileTransporterRequest.Key,
                CannedACL = _fileTransporterRequest.CannedAcl,
                ContentType = DetermineContentType(),
                StorageClass = _fileTransporterRequest.StorageClass,
                ServerSideEncryptionMethod = _fileTransporterRequest.ServerSideEncryptionMethod,
                ServerSideEncryptionKeyManagementServiceKeyId = _fileTransporterRequest.ServerSideEncryptionKeyManagementServiceKeyId,
                ServerSideEncryptionCustomerMethod = _fileTransporterRequest.ServerSideEncryptionCustomerMethod,
                ServerSideEncryptionCustomerProvidedKey = _fileTransporterRequest.ServerSideEncryptionCustomerProvidedKey,
                ServerSideEncryptionCustomerProvidedKeyMD5 = _fileTransporterRequest.ServerSideEncryptionCustomerProvidedKeyMd5,
                TagSet = _fileTransporterRequest.TagSet,
                ChecksumAlgorithm = _fileTransporterRequest.ChecksumAlgorithm,
                ObjectLockLegalHoldStatus = _fileTransporterRequest.ObjectLockLegalHoldStatus,
                ObjectLockMode = _fileTransporterRequest.ObjectLockMode
            };

            if (_fileTransporterRequest.IsSetObjectLockRetainUntilDate())
                initRequest.ObjectLockRetainUntilDate = _fileTransporterRequest.ObjectLockRetainUntilDate.DateTime;

            ((IAmazonWebServiceRequest)initRequest).AddBeforeRequestHandler(requestEventHandler ?? RequestEventHandler);

            if (_fileTransporterRequest.Metadata is { Count: > 0 })
                initRequest.Metadata.AddRange(_fileTransporterRequest.Metadata);
            if (_fileTransporterRequest.Headers is { Count: > 0 })
                initRequest.Headers.AddRange(_fileTransporterRequest.Headers);

            return initRequest;
        }

        protected virtual void UploadPartProgressEventCallback(object? sender, UploadProgressArgs e)
        {
            ulong transferredBytes = Interlocked.Add(
                ref _totalTransferredBytes, 
                e.IncrementTransferred - e.CompensationForRetry);

            var progressArgs = Config.UploadProgressArgsFactory.Create(
                this,
                e.IncrementTransferred, 
                transferredBytes, 
                _contentLength,
                e.CompensationForRetry, 
                _fileTransporterRequest.FilePath);
            
            _fileTransporterRequest.OnRaiseProgressEvent(progressArgs);
        }
    }

    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    internal class ProgressHandler
    {
        protected IAsyncTransferConfig _config;
        protected ITransferCommand _command;
        protected UploadProgressArgs? _lastProgressArgs;
        protected readonly EventHandler<UploadProgressArgs> _callback;
        protected readonly ulong? _contentLength;
        protected ulong _totalBytesRead;
        protected ulong _totalIncrementTransferred;
        protected readonly ulong _progressUpdateInterval;
        protected readonly string? _filePath;
        protected readonly ILogger _logger;

        public ProgressHandler(
            IAsyncTransferConfig config,
            ITransferCommand command,
            ulong progressUpdateInterval,
            ulong? contentLength,
            string? filePath, 
            EventHandler<UploadProgressArgs> callback,
            ILogger logger
            )
        {
            ArgumentNullException.ThrowIfNull(callback);

            _config = config;
            _command = command;
            _progressUpdateInterval = progressUpdateInterval;
            _contentLength = contentLength;
            _filePath = filePath;
            _callback = callback;
            _logger = logger;
        }

        protected virtual void OnTransferProgress(object? sender, UploadProgressArgs e)
        {
            ulong compensationForRetry = 0U;

            if (_lastProgressArgs != null)
            {
                if (_lastProgressArgs.TransferredBytes >= e.TransferredBytes)
                {
                    // The request was retried
                    compensationForRetry = _lastProgressArgs.TransferredBytes;
                }
            }

            var progressArgs = _config.UploadProgressArgsFactory.Create(e, compensationForRetry);
            
            _callback(this, progressArgs);

            _lastProgressArgs = e;
        }

        public virtual void OnBytesRead(object? sender, EventStream.StreamBytesReadEventArgs args)
        {
            // Invoke the progress callback only if bytes read > 0
            if (args.BytesRead > 0)
            {
                var bytesRead = args.BytesRead.ToUInt32();
                _totalBytesRead += bytesRead;
                _totalIncrementTransferred += bytesRead;
                
                if (_totalIncrementTransferred >= _progressUpdateInterval ||
                    (_contentLength.HasValue && _totalBytesRead == _contentLength.Value))
                {
                    var uploadProgressArgs = _config.UploadProgressArgsFactory.Create(
                        _command,
                        _totalIncrementTransferred,
                        _totalBytesRead,
                        _contentLength,
                        0,
                        _filePath
                    );
                    
                    AWSSDKUtils.InvokeInBackground(
                        OnTransferProgress,
                        uploadProgressArgs,
                        sender);
                    
                    _totalIncrementTransferred = 0;
                }
            }
        }
    }
}
