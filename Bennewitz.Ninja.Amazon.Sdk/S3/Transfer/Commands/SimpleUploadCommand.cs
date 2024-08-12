using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Sdk.Fork;

namespace Amazon.Sdk.S3.Transfer.Internal
{
    /// <summary>
    /// This command is for doing regular PutObject requests.
    /// </summary>
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/SimpleUploadCommand.cs", "Amazon.S3.Transfer.Internal")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/_async/SimpleUploadCommand.async.cs", "Amazon.S3.Transfer.Internal")]
    internal class SimpleUploadCommand : BaseCommand
    {
        private readonly IAmazonS3 _s3Client;
        private readonly TransferUtilityUploadRequest _fileTransporterRequest;

        internal SimpleUploadCommand(IAmazonS3 s3Client, TransferUtilityUploadRequest fileTransporterRequest)
        {
            _s3Client = s3Client;
            _fileTransporterRequest = fileTransporterRequest;
        }
        
        public SemaphoreSlim? AsyncThrottler { get; set; }

        public override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            try
            {
                if (AsyncThrottler != null)
                {
                    await AsyncThrottler.WaitAsync(cancellationToken)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                var putRequest = ConstructRequest();
                await _s3Client.PutObjectAsync(putRequest, cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            finally
            {
                if (AsyncThrottler != null)
                {
                    AsyncThrottler.Release();
                }
            }
        }

        private PutObjectRequest ConstructRequest()
        {
            PutObjectRequest putRequest = new()
            {
                BucketName = _fileTransporterRequest.BucketName,
                Key = _fileTransporterRequest.Key,
                CannedACL = _fileTransporterRequest.CannedAcl,
                StorageClass = _fileTransporterRequest.StorageClass,
                AutoCloseStream = _fileTransporterRequest.AutoCloseStream,
                AutoResetStreamPosition = _fileTransporterRequest.AutoResetStreamPosition,
                ServerSideEncryptionMethod = _fileTransporterRequest.ServerSideEncryptionMethod,
                ServerSideEncryptionCustomerMethod = _fileTransporterRequest.ServerSideEncryptionCustomerMethod,
                ServerSideEncryptionCustomerProvidedKey = _fileTransporterRequest.ServerSideEncryptionCustomerProvidedKey,
                ServerSideEncryptionCustomerProvidedKeyMD5 = _fileTransporterRequest.ServerSideEncryptionCustomerProvidedKeyMd5,
                ServerSideEncryptionKeyManagementServiceKeyId = _fileTransporterRequest.ServerSideEncryptionKeyManagementServiceKeyId,
                TagSet = _fileTransporterRequest.TagSet,
// if (BCL && !BCL45)
//              Timeout = ClientConfig.GetTimeoutValue(this._config.DefaultTimeout, this._fileTransporterRequest.Timeout),
// endif
                DisableDefaultChecksumValidation = _fileTransporterRequest.DisableDefaultChecksumValidation,
                DisablePayloadSigning = _fileTransporterRequest.DisablePayloadSigning,
                ChecksumAlgorithm = _fileTransporterRequest.ChecksumAlgorithm
            };
            
            putRequest.Headers.AddRange(_fileTransporterRequest.Headers);
            putRequest.Metadata.AddRange(_fileTransporterRequest.Metadata);

            // Avoid setting ContentType to null, as that may clear
            // out an existing value in Headers collection
            if (!string.IsNullOrWhiteSpace(_fileTransporterRequest.ContentType))
                putRequest.ContentType = _fileTransporterRequest.ContentType;

            putRequest.FilePath = _fileTransporterRequest.FilePath;
            var progressHandler = new ProgressHandler(PutObjectProgressEventCallback);
            ((IAmazonWebServiceRequest)putRequest).StreamUploadProgressCallback += progressHandler.OnTransferProgress;
            ((IAmazonWebServiceRequest)putRequest).AddBeforeRequestHandler(RequestEventHandler);

            putRequest.InputStream = _fileTransporterRequest.InputStream;
            putRequest.CalculateContentMD5Header = _fileTransporterRequest.CalculateContentMd5Header;
            putRequest.ObjectLockLegalHoldStatus = _fileTransporterRequest.ObjectLockLegalHoldStatus;
            putRequest.ObjectLockMode = _fileTransporterRequest.ObjectLockMode;

            if (_fileTransporterRequest.IsSetObjectLockRetainUntilDate())
                putRequest.ObjectLockRetainUntilDate = _fileTransporterRequest.ObjectLockRetainUntilDate;

            return putRequest;
        }

        private void PutObjectProgressEventCallback(object? sender, UploadProgressArgs e)
        {
            var progressArgs = new UploadProgressArgs(e.IncrementTransferred(), e.TransferredBytes, e.TotalBytes, 
                e.CompensationForRetry, _fileTransporterRequest.FilePath);
            _fileTransporterRequest.OnRaiseProgressEvent(progressArgs);
        }
    }
}
