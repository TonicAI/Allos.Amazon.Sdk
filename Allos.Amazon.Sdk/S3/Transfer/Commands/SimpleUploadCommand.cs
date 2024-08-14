using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;

namespace Allos.Amazon.Sdk.S3.Transfer.Internal
{
    /// <summary>
    /// This command is for doing regular PutObject requests.
    /// </summary>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/SimpleUploadCommand.cs", "Amazon.S3.Transfer.Internal")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/_async/SimpleUploadCommand.async.cs", "Amazon.S3.Transfer.Internal")]
    internal class SimpleUploadCommand : BaseCommand
    {
        protected readonly IAmazonS3 _s3Client;
        protected readonly UploadRequest _fileTransporterRequest;

        protected FileStream? _inputStream;

        internal SimpleUploadCommand(IAmazonS3 s3Client, UploadRequest fileTransporterRequest)
        {
            _s3Client = s3Client;
            _fileTransporterRequest = fileTransporterRequest;
        }
        
        public virtual SemaphoreSlim? AsyncThrottler { get; set; }

        public override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            PutObjectRequest? putRequest = null;
            try
            {
                if (AsyncThrottler != null)
                {
                    await AsyncThrottler.WaitAsync(cancellationToken)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                putRequest = ConstructRequest();
                await _s3Client.PutObjectAsync(putRequest, cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            finally
            {
                if (AsyncThrottler != null)
                {
                    AsyncThrottler.Release();
                }

                if (putRequest != null &&
                    putRequest.InputStream != null)
                {
                    //a stream was created and swapped in for file path which
                    //needs to be disposed and the file path put back on the request
                    putRequest.InputStream.Close();
                    putRequest.InputStream = null;
                    putRequest.FilePath = _fileTransporterRequest.FilePath;
                }
            }
        }

        protected virtual PutObjectRequest ConstructRequest()
        {
            PutObjectRequest putRequest = new PutObjectRequest
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

            if (_fileTransporterRequest.IsSetFilePath())
            {
                _inputStream = File.OpenRead(_fileTransporterRequest.FilePath);
                
                putRequest.FilePath = null;
                putRequest.InputStream = _inputStream;
            }
            else
            {
                putRequest.InputStream = _fileTransporterRequest.InputStream;
            }
            
            ArgumentNullException.ThrowIfNull(putRequest.InputStream);

            var progressHandler = new ProgressHandler(
                _s3Client.Config.ProgressUpdateInterval.ToUInt64(),
                _fileTransporterRequest.ContentLength,
                putRequest.FilePath, 
                PutObjectProgressEventCallback,
                Logger
                );
                
            var eventStream = new EventStream(putRequest.InputStream, logger: Logger);
            
            eventStream.OnRead += progressHandler.OnBytesRead;
            
            putRequest.InputStream = eventStream;
            
            ((IAmazonWebServiceRequest)putRequest).AddBeforeRequestHandler(RequestEventHandler);

            putRequest.CalculateContentMD5Header = _fileTransporterRequest.CalculateContentMd5Header;
            putRequest.ObjectLockLegalHoldStatus = _fileTransporterRequest.ObjectLockLegalHoldStatus;
            putRequest.ObjectLockMode = _fileTransporterRequest.ObjectLockMode;

            if (_fileTransporterRequest.IsSetObjectLockRetainUntilDate())
                putRequest.ObjectLockRetainUntilDate = _fileTransporterRequest.ObjectLockRetainUntilDate.DateTime;

            return putRequest;
        }

        protected virtual void PutObjectProgressEventCallback(object? sender, UploadProgressArgs e)
        {
            var progressArgs = IUploadProgressArgsFactory.Instance.Create(
                e.IncrementTransferred, 
                e.TransferredBytes, 
                e.TotalBytes, 
                e.CompensationForRetry, 
                _fileTransporterRequest.FilePath
                );
            
            _fileTransporterRequest.OnRaiseProgressEvent(progressArgs);
        }
    }
}
