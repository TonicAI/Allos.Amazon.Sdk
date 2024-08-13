using System.Diagnostics.CodeAnalysis;
using System.Net;
using Allos.Amazon.S3.Model;
using Allos.Amazon.Sdk.Fork;
using Allos.Amazon.Sdk.S3.Util;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Internal;
using Amazon.S3.Model;
using Amazon.Util.Internal;

namespace Allos.Amazon.Sdk.S3.Transfer.Internal
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/DownloadDirectoryCommand.cs", "Amazon.S3.Transfer.Internal")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/_bcl45+netstandard/DownloadDirectoryCommand.cs", "Amazon.S3.Transfer.Internal")]
    internal class DownloadDirectoryCommand : BaseCommand
    {
        protected readonly AsyncTransferConfig _config;
        
        protected readonly IAmazonS3 _s3Client;
        protected readonly DownloadDirectoryRequest _request;
        protected readonly bool _skipEncryptionInstructionFiles;
        protected uint _totalNumberOfFilesToDownload;
        protected uint _numberOfFilesDownloaded;
        protected ulong _totalBytes;
        protected ulong _transferredBytes;
        protected string? _currentFile;

        internal DownloadDirectoryCommand(IAmazonS3 s3Client, DownloadDirectoryRequest request)
        {
            ArgumentNullException.ThrowIfNull(s3Client);

            _s3Client = s3Client;
            _request = request;
            _skipEncryptionInstructionFiles = s3Client is IAmazonS3Encryption;
            _config = new AsyncTransferConfig();
        }
        
        public bool DownloadFilesConcurrently { get; set; }

        internal DownloadDirectoryCommand(
            IAmazonS3 s3Client, 
            DownloadDirectoryRequest request, 
            AsyncTransferConfig config)
            : this(s3Client, request)
        {
            _config = config;
        }

        public override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            //ValidateRequest()
            if (!_request.IsSetBucketName())
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(_request.BucketName);
            }
            if (!_request.IsSetS3Directory())
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(_request.S3Directory);
            }
            if (!_request.IsSetLocalDirectory())
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(_request.LocalDirectory);
            }

            if (File.Exists(_request.S3Directory))
            {
                throw new IOException($"A file `{_request.S3Directory}` already exists with the same name specified by `{nameof(_request.S3Directory)}`");
            }
            //\
            
            EnsureDirectoryExists(new DirectoryInfo(_request.LocalDirectory));

            List<S3Object> objs;
            string listRequestPrefix;
            try
            {
                ListObjectsRequest listRequest = ConstructListObjectRequest();
                listRequestPrefix = listRequest.Prefix;
                objs = await GetS3ObjectsToDownloadAsync(listRequest, cancellationToken).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex)
            {
                if (ex.StatusCode != HttpStatusCode.NotImplemented)
                    throw;

                ListObjectsV2Request listRequestV2 = ConstructListObjectRequestV2();
                listRequestPrefix = listRequestV2.Prefix;
                objs = await GetS3ObjectsToDownloadV2Async(listRequestV2, cancellationToken).ConfigureAwait(false);
            }

            _totalNumberOfFilesToDownload = objs.Count.ToUInt32();

            SemaphoreSlim? asyncThrottler = null;
            CancellationTokenSource? internalCts = null;

            try
            {
                asyncThrottler = DownloadFilesConcurrently ?
                    new SemaphoreSlim(_config.ConcurrentServiceRequests.ToInt32()) :
                    new SemaphoreSlim(1);

                internalCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var pendingTasks = new List<Task>();
                foreach (S3Object s3O in objs)
                {
                    if (s3O.Key.EndsWith('/'))
                        continue;

                    await asyncThrottler.WaitAsync(cancellationToken)
                        .ConfigureAwait(continueOnCapturedContext: false);

                    cancellationToken.ThrowIfCancellationRequested();
                    if (internalCts.IsCancellationRequested)
                    {
                        // Operation cancelled as one of the download requests failed with an exception,
                        // don't schedule any more download tasks.
                        // Don't throw an OperationCanceledException here as we want to process the 
                        // responses and throw the original exception.
                        break;
                    }

                    // Valid for serial uploads when
                    // TransferUtilityDownloadDirectoryRequest.DownloadFilesConcurrently is set to false.
                    int prefixLength = listRequestPrefix.Length;

                    // If DisableSlashCorrection is enabled (i.e. S3Directory is a key prefix) and it doesn't end with '/' then we need the parent directory to properly construct download path.
                    if (_request.DisableSlashCorrection && !listRequestPrefix.EndsWith('/'))
                    {
                        prefixLength = listRequestPrefix.LastIndexOf('/') + 1;
                    }

                    _currentFile = s3O.Key.Substring(prefixLength);

                    var downloadRequest = ConstructTransferUtilityDownloadRequest(s3O, prefixLength);
                    var command = new DownloadCommand(_s3Client, downloadRequest);

                    var task = ExecuteCommandAsync(command, internalCts, asyncThrottler);
                    pendingTasks.Add(task);
                }
                await WhenAllOrFirstExceptionAsync(pendingTasks, cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            finally
            {
                internalCts?.Dispose();
                asyncThrottler?.Dispose();
            }
        }

        protected virtual async Task<List<S3Object>> GetS3ObjectsToDownloadAsync(
            ListObjectsRequest listRequest, 
            CancellationToken cancellationToken)
        {
            List<S3Object> objs = new List<S3Object>();
            do
            {
                ListObjectsResponse listResponse = await _s3Client.ListObjectsAsync(listRequest, cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);

                if (listResponse.S3Objects != null)
                {
                    foreach (S3Object s3O in listResponse.S3Objects)
                    {
                        if (ShouldDownload(s3O))
                        {
                            _totalBytes += s3O.Size.ToUInt64();
                            objs.Add(s3O);
                        }
                    }
                }
                listRequest.Marker = listResponse.NextMarker;
            } while (!string.IsNullOrWhiteSpace(listRequest.Marker));
            return objs;
        }

        protected virtual async Task<List<S3Object>> GetS3ObjectsToDownloadV2Async(ListObjectsV2Request listRequestV2, CancellationToken cancellationToken)
        {
            List<S3Object> objs = new List<S3Object>();
            do
            {
                ListObjectsV2Response listResponse = await _s3Client.ListObjectsV2Async(listRequestV2, cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);

                if (listResponse.S3Objects != null)
                {
                    foreach (S3Object s3O in listResponse.S3Objects)
                    {
                        if (ShouldDownload(s3O))
                        {
                            _totalBytes += s3O.Size.ToUInt64();
                            objs.Add(s3O);
                        }
                    }
                }
                listRequestV2.ContinuationToken = listResponse.NextContinuationToken;
            } while (!string.IsNullOrWhiteSpace(listRequestV2.ContinuationToken));
            return objs;
        }

        protected virtual void DownloadedProgressEventCallback(object? sender, WriteObjectProgressArgs e)
        {
            var transferredBytes = Interlocked.Add(ref _transferredBytes, e.IncrementTransferred());

            uint numberOfFilesDownloaded = _numberOfFilesDownloaded;
            if (e.IsCompleted)
            {
                numberOfFilesDownloaded = Interlocked.Increment(ref _numberOfFilesDownloaded);
            }

            DownloadDirectoryProgressArgs downloadDirectoryProgress;
            if (_request.DownloadFilesConcurrently)
            {
                // If concurrent download is enabled, values for current file, 
                // transferred and total bytes for current file are not set.
                downloadDirectoryProgress = new DownloadDirectoryProgressArgs(numberOfFilesDownloaded, _totalNumberOfFilesToDownload,
                           transferredBytes, _totalBytes,
                           null, 0, 0);
            }
            else
            {
                downloadDirectoryProgress = new DownloadDirectoryProgressArgs(
                    numberOfFilesDownloaded, 
                    _totalNumberOfFilesToDownload,
                    transferredBytes, 
                    _totalBytes,
                    _currentFile, 
                    e.TransferredBytes.ToUInt64(), 
                    e.TotalBytes.ToUInt64()
                    );
            }
            _request.OnRaiseProgressEvent(downloadDirectoryProgress);
        }

        protected virtual void EnsureDirectoryExists(DirectoryInfo directory)
        {
            if (directory.Exists)
                return;

            if (directory.Parent != null)
            {
                EnsureDirectoryExists(directory.Parent);    
            }
            directory.Create();
        }

        protected virtual DownloadRequest ConstructTransferUtilityDownloadRequest(S3Object s3Object, int prefixLength)
        {
            var downloadRequest = new DownloadRequest();
            downloadRequest.BucketName = _request.BucketName;
            downloadRequest.Key = s3Object.Key;
            var file = s3Object.Key.Substring(prefixLength).Replace('/', Path.DirectorySeparatorChar);
            downloadRequest.FilePath = _request.LocalDirectory == null ? file : Path.Combine(_request.LocalDirectory, file);
            downloadRequest.ServerSideEncryptionCustomerMethod = _request.ServerSideEncryptionCustomerMethod;
            downloadRequest.ServerSideEncryptionCustomerProvidedKey = _request.ServerSideEncryptionCustomerProvidedKey;
            downloadRequest.ServerSideEncryptionCustomerProvidedKeyMd5 = _request.ServerSideEncryptionCustomerProvidedKeyMd5;

            //Ensure the target file is a rooted within LocalDirectory. Otherwise error.
            if(!InternalSDKUtils.IsFilePathRootedWithDirectoryPath(downloadRequest.FilePath, _request.LocalDirectory))
            {
                throw new AmazonClientException($"The file `{downloadRequest.FilePath}` is not allowed outside of the target directory `{_request.LocalDirectory}`.");
            }

            downloadRequest.WriteObjectProgressEvent += DownloadedProgressEventCallback;

            return downloadRequest;
        }

        protected virtual ListObjectsV2Request ConstructListObjectRequestV2()
        {
            ListObjectsV2Request listRequestV2 = new ListObjectsV2Request();
            listRequestV2.BucketName = _request.BucketName;
            listRequestV2.Prefix = _request.S3Directory;

            if (listRequestV2.Prefix != null)
            {
                listRequestV2.Prefix = listRequestV2.Prefix.Replace('\\', '/');
                
                if (!_request.DisableSlashCorrection)
                {
                    if (!listRequestV2.Prefix.EndsWith('/'))
                        listRequestV2.Prefix += '/';
                }
                
                if (listRequestV2.Prefix.StartsWith('/'))
                {
                    listRequestV2.Prefix = listRequestV2.Prefix.Length == 1 ? string.Empty : listRequestV2.Prefix.Substring(1);
                }
            }

            return listRequestV2;
        }

        protected virtual ListObjectsRequest ConstructListObjectRequest()
        {
            ListObjectsRequest listRequest = new ListObjectsRequest();
            listRequest.BucketName = _request.BucketName;
            listRequest.Prefix = _request.S3Directory;

            if (listRequest.Prefix != null)
            {
                listRequest.Prefix = listRequest.Prefix.Replace('\\', '/');

                if (!_request.DisableSlashCorrection)
                {
                    if (!listRequest.Prefix.EndsWith('/'))
                        listRequest.Prefix += '/';
                }

                if (listRequest.Prefix.StartsWith('/'))
                {
                    listRequest.Prefix = listRequest.Prefix.Length == 1 ? string.Empty : listRequest.Prefix.Substring(1);
                }
            }

            return listRequest;
        }

        protected virtual bool IsInstructionFile(string key) => 
            (_skipEncryptionInstructionFiles && AmazonS3Util.IsInstructionFile(key));

        protected virtual bool ShouldDownload(S3Object s3O)
        {
            // skip objects based on ModifiedSinceDateUtc
            if (_request.IsSetModifiedSinceDateUtc() && s3O.LastModified.ToUniversalTime() <= _request.ModifiedSinceDateUtc.ToUniversalTime())
                return false;
            // skip objects based on UnmodifiedSinceDateUtc
            if (_request.IsSetUnmodifiedSinceDateUtc() && s3O.LastModified.ToUniversalTime() > _request.UnmodifiedSinceDateUtc.ToUniversalTime())
                return false;
            // skip objects which are instruction files and we're using encryption client
            if (IsInstructionFile(s3O.Key))
                return false;

            return true;
        }
    }
}
