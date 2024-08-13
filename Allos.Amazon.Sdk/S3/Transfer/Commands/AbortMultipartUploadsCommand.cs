using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;

namespace Allos.Amazon.Sdk.S3.Transfer.Internal
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/AbortMultipartUploadsCommand.cs", "Amazon.S3.Transfer.Internal")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/_async/AbortMultipartUploadsCommand.async.cs", "Amazon.S3.Transfer.Internal")]
    internal class AbortMultipartUploadsCommand : BaseCommand
    {
        protected readonly AsyncTransferConfig _config;
        
        protected readonly IAmazonS3 _s3Client;
        protected readonly string _bucketName;
        protected readonly DateTime _initiatedDate;

        internal AbortMultipartUploadsCommand(IAmazonS3 s3Client, string bucketName, DateTime initiateDate)
        {
            _s3Client = s3Client;
            _bucketName = bucketName;
            _initiatedDate = initiateDate;
            _config = new AsyncTransferConfig();
        }
        
        internal AbortMultipartUploadsCommand(
            IAmazonS3 s3Client, 
            string bucketName, 
            DateTime initiateDate, 
            AsyncTransferConfig config)
        {
            _s3Client = s3Client;
            _bucketName = bucketName;
            _initiatedDate = initiateDate;
            _config = config;
        }
        
        public override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(_bucketName);
            
            SemaphoreSlim? asyncThrottler = null;
            CancellationTokenSource? internalCts = null;
            try
            {
                asyncThrottler = new SemaphoreSlim(_config.ConcurrentServiceRequests.ToInt32());
                internalCts = new CancellationTokenSource();
                var internalCancellationToken = internalCts.Token;

                ListMultipartUploadsResponse listResponse = new ListMultipartUploadsResponse();
                var pendingTasks = new List<Task<AbortMultipartUploadResponse>>();
                do
                {
                    ListMultipartUploadsRequest listRequest = ConstructListMultipartUploadsRequest(listResponse);
                    listResponse = await _s3Client.ListMultipartUploadsAsync(listRequest, cancellationToken)
                        .ConfigureAwait(continueOnCapturedContext: false);

                    if (listResponse.MultipartUploads != null)
                    {
                        foreach (MultipartUpload upload in listResponse.MultipartUploads)
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            if (internalCancellationToken.IsCancellationRequested)
                            {
                                // Operation cancelled as one of the AbortMultipartUpload requests failed with an exception,
                                // don't schedule anymore AbortMultipartUpload tasks. 
                                // Don't throw an OperationCanceledException here as we want to process the 
                                // responses and throw the original exception.
                                break;
                            }
                            if (upload.Initiated < _initiatedDate)
                            {
                                await asyncThrottler.WaitAsync(cancellationToken)
                                    .ConfigureAwait(continueOnCapturedContext: false);

                                var abortRequest = ConstructAbortMultipartUploadRequest(upload);
                                var task = AbortAsync(abortRequest, internalCts, cancellationToken, asyncThrottler);
                                pendingTasks.Add(task);
                            }
                        }
                    }
                }
                while (listResponse.IsTruncated);

                await WhenAllOrFirstExceptionAsync(pendingTasks,cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            finally
            {
                if (internalCts != null)
                    internalCts.Dispose();

                if (asyncThrottler!=null)
                    asyncThrottler.Dispose();
            }
        }

        protected virtual async Task<AbortMultipartUploadResponse> AbortAsync(
            AbortMultipartUploadRequest abortRequest, 
            CancellationTokenSource internalCts,
            CancellationToken cancellationToken, 
            SemaphoreSlim asyncThrottler)
        {
            try
            {
                return await _s3Client.AbortMultipartUploadAsync(abortRequest, cancellationToken)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            catch (Exception exception)
            {
                if (!(exception is OperationCanceledException))
                {
                    // Cancel scheduling any more tasks.
                    await internalCts.CancelAsync().ConfigureAwait(false);
                }
                throw;
            }
            finally
            {
                asyncThrottler.Release();
            }
        }

        protected virtual ListMultipartUploadsRequest ConstructListMultipartUploadsRequest(ListMultipartUploadsResponse listResponse)
            {
                ListMultipartUploadsRequest listRequest = new ListMultipartUploadsRequest
                {
                    BucketName = _bucketName,
                    KeyMarker = listResponse.KeyMarker,
                    UploadIdMarker = listResponse.NextUploadIdMarker,
                };
                ((IAmazonWebServiceRequest)listRequest).AddBeforeRequestHandler(RequestEventHandler);
            return listRequest;
        }

        protected virtual AbortMultipartUploadRequest ConstructAbortMultipartUploadRequest(MultipartUpload upload)
                    {
                        var abortRequest = new AbortMultipartUploadRequest
                        {
                            BucketName = _bucketName,
                            Key = upload.Key,
                            UploadId = upload.UploadId,
                        };
                        ((IAmazonWebServiceRequest)abortRequest).AddBeforeRequestHandler(RequestEventHandler);
            return abortRequest;
        }
    }
}
