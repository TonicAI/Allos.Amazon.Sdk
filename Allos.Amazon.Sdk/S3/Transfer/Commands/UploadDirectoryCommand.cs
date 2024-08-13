using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.S3.Internal;

namespace Allos.Amazon.Sdk.S3.Transfer.Internal
{
    /// <summary>
    /// This command files all the files that meets the criteria specified in the TransferUtilityUploadDirectoryRequest request
    /// and uploads them.
    /// </summary>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/UploadDirectoryCommand.cs", "Amazon.S3.Transfer.Internal")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/_bcl45+netstandard/UploadDirectoryCommand.cs", "Amazon.S3.Transfer.Internal")]
    internal class UploadDirectoryCommand : BaseCommand
    {
        protected readonly UploadDirectoryRequest _request;
        protected readonly AsyncTransferUtility _utility;
        protected readonly AsyncTransferConfig _config;

        protected uint _totalNumberOfFiles;
        protected uint _numberOfFilesUploaded;
        protected ulong _totalBytes;
        protected ulong _transferredBytes;        

        internal UploadDirectoryCommand(
            AsyncTransferUtility utility, 
            AsyncTransferConfig config, 
            UploadDirectoryRequest request)
        {
            _utility = utility;
            _request = request;
            _config = config;
        }
        
        public virtual bool UploadFilesConcurrently { get; set; }

        public override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            string prefix = GetKeyPrefix();

            string basePath = _request.Directory == null ? 
                string.Empty : 
                new DirectoryInfo(_request.Directory).FullName;

            string[] filePaths = await GetFiles(
                    basePath, 
                    _request.SearchPattern, 
                    _request.SearchOption, 
                    cancellationToken)
                .ConfigureAwait(continueOnCapturedContext: false);                
            _totalNumberOfFiles = filePaths.Length.ToUInt32();

            SemaphoreSlim? asyncThrottler = null;
            SemaphoreSlim? loopThrottler = null;
            CancellationTokenSource? internalCts = null;
            try
            {
                var pendingTasks = new List<Task>();
                loopThrottler = UploadFilesConcurrently ? 
                    new SemaphoreSlim(_config.ConcurrentServiceRequests.ToInt32()) :
                    new SemaphoreSlim(1);

                asyncThrottler = _utility.S3Client is IAmazonS3Encryption ?
                    // If we are using AmazonS3EncryptionClient, don't set the async throttler.
                    // The loopThrottler will be used to control how many files are uploaded in parallel.
                    // Each upload (multipart) will upload parts serially.
                    null :
                    // Use a throttler which will be shared between simple and multipart uploads
                    // to control concurrent IO.
                    new SemaphoreSlim(_config.ConcurrentServiceRequests.ToInt32());


                internalCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                foreach (string filepath in filePaths)
                {
                    await loopThrottler.WaitAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

                    cancellationToken.ThrowIfCancellationRequested();
                    if (internalCts.IsCancellationRequested)
                    {
                        // Operation cancelled as one of the upload requests failed with an exception,
                        // don't schedule any more upload tasks. 
                        // Don't throw an OperationCanceledException here as we want to process the 
                        // responses and throw the original exception.
                        break;
                    }
                    var uploadRequest = ConstructRequest(basePath, filepath, prefix);
                    var uploadCommand = _utility.GetUploadCommand(uploadRequest, asyncThrottler);

                    var task = ExecuteCommandAsync(uploadCommand, internalCts, loopThrottler);
                    pendingTasks.Add(task);
                }
                await WhenAllOrFirstExceptionAsync(pendingTasks, cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            finally
            {                
                internalCts?.Dispose();
                loopThrottler?.Dispose();
                asyncThrottler?.Dispose();
                
            }
        }

        protected virtual Task<string[]> GetFiles(
            string path, 
            string searchPattern, 
            SearchOption searchOption, 
            CancellationToken cancellationToken)
        {
            return Task.Run(() =>
                { 
                    var filePaths = Directory.GetFiles(path, searchPattern, searchOption);
                    foreach (var filePath in filePaths)
                    {
                        _totalBytes += new FileInfo(filePath).Length.ToUInt64();
                    }
                    return filePaths;
                }, cancellationToken);

        }

        protected virtual UploadRequest ConstructRequest(string basePath, string filepath, string prefix)
        {
            string key = filepath.Substring(basePath.Length);
            key = key.Replace(@"\", "/");
            if (key.StartsWith('/'))
                key = key.Substring(1);
            key = prefix + key;

            var uploadRequest = new UploadRequest
            {
                BucketName = _request.BucketName,
                Key = key,
                FilePath = filepath,
                CannedAcl = _request.CannedAcl,
                Metadata = _request.Metadata,
                ContentType = _request.ContentType,
                StorageClass = _request.StorageClass,
                ServerSideEncryptionMethod = _request.ServerSideEncryptionMethod,
                ServerSideEncryptionKeyManagementServiceKeyId = _request.ServerSideEncryptionKeyManagementServiceKeyId,
                ServerSideEncryptionCustomerMethod = _request.ServerSideEncryptionCustomerMethod,
                ServerSideEncryptionCustomerProvidedKey = _request.ServerSideEncryptionCustomerProvidedKey,
                ServerSideEncryptionCustomerProvidedKeyMd5 = _request.ServerSideEncryptionCustomerProvidedKeyMd5,
                TagSet = _request.TagSet,
                CalculateContentMd5Header = _request.CalculateContentMd5Header,
                ObjectLockLegalHoldStatus = _request.ObjectLockLegalHoldStatus,
                ObjectLockMode = _request.ObjectLockMode,
                DisablePayloadSigning = _request.DisablePayloadSigning,
            };
            
            if (_request.IsSetObjectLockRetainUntilDate())
                uploadRequest.ObjectLockRetainUntilDate = _request.ObjectLockRetainUntilDate.DateTime;

            uploadRequest.UploadProgressEvent += UploadProgressEventCallback;

            // Raise event to allow subscribers to modify request
            _request.RaiseUploadDirectoryFileRequestEvent(uploadRequest);

            return uploadRequest;
        }

        protected virtual string GetKeyPrefix()
        {
            var prefix = string.Empty;
            if (_request.IsSetKeyPrefix())
            {
                prefix = _request.KeyPrefix;
                prefix = prefix.Replace(@"\", "/");
                if (prefix.StartsWith('/'))
                    prefix = prefix.Substring(1);

                if (!prefix.EndsWith('/'))
                {
                    prefix += '/';
                }
            }
            return prefix;
        }

        protected virtual void UploadProgressEventCallback(object? sender, UploadProgressArgs e)
        {
            var totalTransferredBytes = Interlocked.Add(
                ref _transferredBytes, 
                e.IncrementTransferred - e.CompensationForRetry);

            uint numberOfFilesUploaded = _numberOfFilesUploaded;
            if (e.TransferredBytes == e.TotalBytes)
            {
                numberOfFilesUploaded = Interlocked.Increment(ref _numberOfFilesUploaded);
            }

            // If concurrent upload is enabled (i.e. _request.UploadFilesConcurrently),
            // values for current file (including transferred, total bytes, and file path) may not be set.
            UploadDirectoryProgressArgs uploadDirectoryProgressArgs = new UploadDirectoryProgressArgs(
                numberOfFilesUploaded, _totalNumberOfFiles, totalTransferredBytes, _totalBytes, e.FilePath,
                e.TransferredBytes, e.TotalBytes);
            
            _request.OnRaiseProgressEvent(uploadDirectoryProgressArgs);
        }
    }
}
