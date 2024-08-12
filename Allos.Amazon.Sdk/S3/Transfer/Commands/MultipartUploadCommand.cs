using System.Buffers;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Internal;
using Amazon.S3.Model;
using Amazon.Sdk.Fork;
using Amazon.Sdk.S3.Util;
using Amazon.Util;
using Serilog;

namespace Amazon.Sdk.S3.Transfer.Internal
{
    /// <summary>
    /// The command to manage an upload using the S3 multipart API.
    /// </summary>
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/MultipartUploadCommand.cs", "Amazon.S3.Transfer.Internal")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/_async/MultipartUploadCommand.async.cs", "Amazon.S3.Transfer.Internal")]
    internal class MultipartUploadCommand : BaseCommand
    {
        private readonly IAmazonS3 _s3Client;
        private readonly long _partSize;
        private int _totalNumberOfParts;
        private readonly TransferUtilityConfig _config;
        private readonly TransferUtilityUploadRequest _fileTransporterRequest;

        private List<UploadPartResponse> _uploadResponses = new();
        private long _totalTransferredBytes;
        private readonly Queue<UploadPartRequest> _partsToUpload = new();
        
        private readonly long _contentLength;
        private static ILogger Logger => TonicLogger.ForContext<AsyncTransferUtility>();

        /// <summary>
        /// Initializes a new instance of the <see cref="MultipartUploadCommand"/> class.
        /// </summary>
        /// <param name="s3Client">The s3 client.</param>
        /// <param name="config">The config object that has the number of threads to use.</param>
        /// <param name="fileTransporterRequest">The file transporter request.</param>
        internal MultipartUploadCommand(
            IAmazonS3 s3Client, 
            TransferUtilityConfig config, 
            TransferUtilityUploadRequest fileTransporterRequest)
        {
            _config = config;

            if (fileTransporterRequest.IsSetFilePath())
            {
                Logger.Debug("Beginning upload of file `{FilePath}`", 
                    fileTransporterRequest.FilePath);
            }
            else if (fileTransporterRequest.IsSetInputStream())
            {
                Logger.Debug("Beginning upload of `{StreamType}`", 
                    fileTransporterRequest.InputStream.GetType().FullName);
            }
            else
            {
                throw new ArgumentException(
                    $"One of `{nameof(fileTransporterRequest.FilePath)}` or `{nameof(fileTransporterRequest.InputStream)}` must be provided");
            }

            _s3Client = s3Client;
            _fileTransporterRequest = fileTransporterRequest;
            _contentLength = _fileTransporterRequest.ContentLength;

            _partSize = fileTransporterRequest.IsSetPartSize() ? 
                fileTransporterRequest.PartSize : 
                CalculatePartSize(_contentLength);

            if (fileTransporterRequest.InputStream != null)
            {
                if (fileTransporterRequest.AutoResetStreamPosition && fileTransporterRequest.InputStream.CanSeek)
                {
                    fileTransporterRequest.InputStream.Seek(0, SeekOrigin.Begin);
                }
            }

            Logger.Debug("Upload part size {PartSize}", _partSize);
        }
        
        public SemaphoreSlim? AsyncThrottler { get; set; }

        public override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            if ( _fileTransporterRequest.InputStream is { CanSeek: false } || 
                 _fileTransporterRequest.ContentLength == -1)
            {
                await UploadUnseekableStreamAsync(_fileTransporterRequest, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var initRequest = ConstructInitiateMultipartUploadRequest();
                var initResponse = await _s3Client.InitiateMultipartUploadAsync(initRequest, cancellationToken)
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
                    long filePosition = 0;
                    for (int i = 1; filePosition < _contentLength; i++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var uploadRequest = ConstructUploadPartRequest(i, filePosition, initResponse);
                        _partsToUpload.Enqueue(uploadRequest);
                        filePosition += _partSize;
                    }

                    _totalNumberOfParts = _partsToUpload.Count;

                    Logger.Debug("Scheduling the {TotalNumberOfParts} UploadPartRequests in the queue",
                        _totalNumberOfParts);

                    internalCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    var concurrencyLevel = CalculateConcurrentServiceRequests();
                    localThrottler = AsyncThrottler ?? new SemaphoreSlim(concurrencyLevel);

                    foreach (var uploadRequest in _partsToUpload)
                    {
                        await localThrottler.WaitAsync(cancellationToken)
                            .ConfigureAwait(continueOnCapturedContext: false);

                        cancellationToken.ThrowIfCancellationRequested();
                        if (internalCts.IsCancellationRequested)
                        {
                            // Operation cancelled as one of the UploadParts requests failed with an exception,
                            // don't schedule any more UploadPart tasks.
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
                    await _s3Client.CompleteMultipartUploadAsync(compRequest, cancellationToken)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    Logger.Debug("Done completing multipart `{UploadId}`", 
                        initResponse.UploadId);

                }
                catch (Exception e)
                {
                    Logger.Error(e, "Exception while uploading `{UploadId}`", 
                        initResponse.UploadId);
                    // Can't do async invocation in the catch block, doing cleanup synchronously.
                    Cleanup(initResponse.UploadId, pendingUploadPartTasks);
                    throw;
                }
                finally
                {
                    if (internalCts != null)
                        internalCts.Dispose();

                    if (localThrottler != null && localThrottler != AsyncThrottler)
                        localThrottler.Dispose();

                    if (_fileTransporterRequest.InputStream != null && 
                        !_fileTransporterRequest.IsSetFilePath() && 
                        _fileTransporterRequest.AutoCloseStream)
                    {
                        await _fileTransporterRequest.InputStream.DisposeAsync().ConfigureAwait(false);
                    }
                } 
            }
        }

        private async Task<UploadPartResponse> UploadPartAsync(
            UploadPartRequest uploadRequest, 
            CancellationTokenSource internalCts, 
            SemaphoreSlim asyncThrottler)
        {
            try
            {
                return await _s3Client.UploadPartAsync(uploadRequest, internalCts.Token)
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
                asyncThrottler.Release();
            }
        }       

        private void Cleanup(string uploadId, List<Task<UploadPartResponse>> tasks)
        {
            try
            {
                // Make sure all tasks complete (to completion/faulted/cancelled).
                Task.WaitAll(tasks.Cast<Task>().ToArray(), 5000); 
            }
            catch(Exception exception)
            {
                Logger.Information(
                    exception,
                    "A timeout occured while waiting for all upload part request to complete as part of aborting the multipart upload : {ErrorMessage}",
                    exception.Message);
            }
            AbortMultipartUpload(uploadId);
        }

        private void AbortMultipartUpload(string uploadId)
        {
            try
            {
                _s3Client.AbortMultipartUploadAsync(new()
                {
                    BucketName = _fileTransporterRequest.BucketName,
                    Key = _fileTransporterRequest.Key,
                    UploadId = uploadId
                }).Wait();
            }
            catch (Exception e)
            {
                Logger.Information("Error attempting to abort multipart for key `{ObjectKey}`: {ErrorMessage}", 
                    _fileTransporterRequest.Key, 
                    e.Message);
            }
        }
        private async Task UploadUnseekableStreamAsync(
            TransferUtilityUploadRequest request, 
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request.InputStream);
            
            int readBufferSize = _s3Client.Config.BufferSize;

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
            var initiateResponse = await _s3Client.InitiateMultipartUploadAsync(initiateRequest, cancellationToken)
                .ConfigureAwait(false);

            try
            {
                // if partSize is not specified on the request, the default value is 0
                long minPartSize = request.PartSize != 0 ? request.PartSize : S3Constants.MinPartSize;
                var uploadPartResponses = new List<UploadPartResponse>();
                var readBuffer = ArrayPool<byte>.Shared.Rent(readBufferSize);
                var partBuffer = ArrayPool<byte>.Shared.Rent((int)minPartSize + (readBufferSize));
                MemoryStream nextUploadBuffer = new(partBuffer);
                await using (var stream = request.InputStream)
                {
                    try
                    {
                        int partNumber = 1;
                        int readBytesCount, readAheadBytesCount;

                        readBytesCount = await stream.ReadAsync(readBuffer, 0, readBuffer.Length, cancellationToken)
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
                                        nextUploadBuffer = new();
                                    }
                                }
                                bool isLastPart = readAheadBytesCount == 0;

                                var partSize = nextUploadBuffer.Position;
                                nextUploadBuffer.Position = 0;
                                UploadPartRequest uploadPartRequest = ConstructUploadPartRequestForNonSeekableStream(
                                    nextUploadBuffer, 
                                    partNumber, 
                                    partSize, 
                                    isLastPart, 
                                    initiateResponse);

                                var partResponse = await _s3Client.UploadPartAsync(uploadPartRequest, cancellationToken).ConfigureAwait(false);
                                Logger.Debug(
                                    "Uploaded part {PartNumber} (PartSize={PartSize} UploadId={UploadId} IsLastPart={IsLastPart})", 
                                    partNumber, 
                                    partSize, 
                                    initiateResponse.UploadId,
                                    isLastPart);
                                uploadPartResponses.Add(partResponse);
                                partNumber++;

                                await nextUploadBuffer.DisposeAsync().ConfigureAwait(false);
                                nextUploadBuffer = new(partBuffer);
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
                    CompleteMultipartUploadRequest compRequest = ConstructCompleteMultipartUploadRequest(initiateResponse, true, requestEventHandler);
                    await _s3Client.CompleteMultipartUploadAsync(compRequest, cancellationToken).ConfigureAwait(false);
                    Logger.Debug("Completed multi part upload (PartCount={PartCount}, UploadId={UploadId})",
                        uploadPartResponses.Count, 
                        initiateResponse.UploadId);
                }
            }
            catch (Exception ex)
            {
                await _s3Client.AbortMultipartUploadAsync(new()
                {
                    BucketName = request.BucketName,
                    Key = request.Key,
                    UploadId = initiateResponse.UploadId
                }, cancellationToken).ConfigureAwait(false);
                Logger.Error(ex, ex.Message);
                throw;
            }
        }

        private static long CalculatePartSize(long fileSize)
        {
            double partSize = Math.Ceiling((double)fileSize / S3Constants.MaxNumberOfParts);
            if (partSize < S3Constants.MinPartSize)
            {
                partSize = S3Constants.MinPartSize;
            }

            return (long)partSize;
        }

        private string? DetermineContentType()
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

        private int CalculateConcurrentServiceRequests()
        {
            int threadCount;
            if (_fileTransporterRequest.IsSetFilePath()
                && !(_s3Client is IAmazonS3Encryption))
            {
                threadCount = _config.ConcurrentServiceRequests;
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

        private CompleteMultipartUploadRequest ConstructCompleteMultipartUploadRequest(InitiateMultipartUploadResponse initResponse) => 
            ConstructCompleteMultipartUploadRequest(initResponse, false, null);

        private CompleteMultipartUploadRequest ConstructCompleteMultipartUploadRequest(
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

        private UploadPartRequest ConstructUploadPartRequest(
            int partNumber, 
            long filePosition, 
            InitiateMultipartUploadResponse initiateResponse)
        {
            UploadPartRequest uploadPartRequest = ConstructGenericUploadPartRequest(initiateResponse);

            uploadPartRequest.PartNumber = partNumber;
            uploadPartRequest.PartSize = _partSize;

            if ((filePosition + _partSize >= _contentLength)
                && _s3Client is IAmazonS3Encryption)
            {
                uploadPartRequest.IsLastPart = true;
                uploadPartRequest.PartSize = 0;
            }

            var progressHandler = new ProgressHandler(UploadPartProgressEventCallback);
            ((IAmazonWebServiceRequest)uploadPartRequest).StreamUploadProgressCallback += progressHandler.OnTransferProgress;
            ((IAmazonWebServiceRequest)uploadPartRequest).AddBeforeRequestHandler(RequestEventHandler);

            if (_fileTransporterRequest.IsSetFilePath())
            {
                uploadPartRequest.FilePosition = filePosition;
                uploadPartRequest.FilePath = _fileTransporterRequest.FilePath;
            }
            else
            {
                uploadPartRequest.InputStream = _fileTransporterRequest.InputStream;
            }

            return uploadPartRequest;
        }

        private UploadPartRequest ConstructGenericUploadPartRequest(InitiateMultipartUploadResponse initiateResponse)
        {
            UploadPartRequest uploadPartRequest = new()
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

        private UploadPartRequest ConstructUploadPartRequestForNonSeekableStream(
            Stream inputStream, 
            int partNumber, 
            long partSize, 
            bool isLastPart, 
            InitiateMultipartUploadResponse initiateResponse)
        {
            UploadPartRequest uploadPartRequest = ConstructGenericUploadPartRequest(initiateResponse);
            
            uploadPartRequest.InputStream = inputStream;
            uploadPartRequest.PartNumber = partNumber;
            uploadPartRequest.PartSize = partSize;
            uploadPartRequest.IsLastPart = isLastPart;
            // we can only determine the percentage uploaded if content length is known. For an unseekable stream with unknown length we will not
            // report on the transfer progress. The part numbers uploaded can still be looked at through verbose logging.
            if (_fileTransporterRequest.ContentLength != -1)
            {
                var progressHandler = new ProgressHandler(UploadPartProgressEventCallback);
                ((IAmazonWebServiceRequest)uploadPartRequest).StreamUploadProgressCallback += progressHandler.OnTransferProgress;
                ((IAmazonWebServiceRequest)uploadPartRequest).AddBeforeRequestHandler(RequestEventHandler);
            }

            return uploadPartRequest;
        }

        private InitiateMultipartUploadRequest ConstructInitiateMultipartUploadRequest() => 
            ConstructInitiateMultipartUploadRequest(null);

        private InitiateMultipartUploadRequest ConstructInitiateMultipartUploadRequest(RequestEventHandler? requestEventHandler)
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
                initRequest.ObjectLockRetainUntilDate = _fileTransporterRequest.ObjectLockRetainUntilDate;

            ((IAmazonWebServiceRequest)initRequest).AddBeforeRequestHandler(requestEventHandler ?? RequestEventHandler);

            if (_fileTransporterRequest.Metadata is { Count: > 0 })
                initRequest.Metadata.AddRange(_fileTransporterRequest.Metadata);
            if (_fileTransporterRequest.Headers is { Count: > 0 })
                initRequest.Headers.AddRange(_fileTransporterRequest.Headers);

            return initRequest;
        }

        private void UploadPartProgressEventCallback(object? sender, UploadProgressArgs e)
        {
            long transferredBytes = Interlocked.Add(
                ref _totalTransferredBytes, 
                e.IncrementTransferred() - e.CompensationForRetry);

            var progressArgs = new UploadProgressArgs(
                e.IncrementTransferred(), 
                transferredBytes, 
                _contentLength,
                e.CompensationForRetry, 
                _fileTransporterRequest.FilePath);
            _fileTransporterRequest.OnRaiseProgressEvent(progressArgs);
        }
    }

    internal class ProgressHandler
    {
        private StreamTransferProgressArgs? _lastProgressArgs;
        private readonly EventHandler<UploadProgressArgs> _callback;

        public ProgressHandler(EventHandler<UploadProgressArgs> callback)
        {
            ArgumentNullException.ThrowIfNull(callback);

            _callback = callback;
        }

        public void OnTransferProgress(object? sender, StreamTransferProgressArgs e)
        {
            var compensationForRetry = 0L;

            if (_lastProgressArgs != null)
            {
                if (_lastProgressArgs.TransferredBytes >= e.TransferredBytes)
                {
                    // The request was retried
                    compensationForRetry = _lastProgressArgs.TransferredBytes;
                }
            }

            var progressArgs = new UploadProgressArgs(e.IncrementTransferred, e.TransferredBytes, e.TotalBytes,
            compensationForRetry, null);
            _callback(this, progressArgs);

            _lastProgressArgs = e;
        }
    }
}
