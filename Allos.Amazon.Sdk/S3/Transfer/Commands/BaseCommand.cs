using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.S3.Model;
using Amazon.Util;
using Serilog;

namespace Allos.Amazon.Sdk.S3.Transfer.Internal
{
    [SuppressMessage("ReSharper", "VirtualMemberNeverOverridden.Global")]
    [DebuggerDisplay("{DebuggerDisplay}")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/BaseCommand.cs", "Amazon.S3.Transfer.Internal")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/_async/BaseCommand.async.cs", "Amazon.S3.Transfer.Internal")]
    internal abstract class BaseCommand
    {
        private static readonly Lazy<ILogger> _logger =
            new Lazy<ILogger>(() => TonicLogger.ForContext(typeof(AsyncTransferUtility)));
        protected virtual ILogger Logger => _logger.Value;
        
        public abstract Task ExecuteAsync(CancellationToken cancellationToken);

        /// <summary>
        ///  Waits for all the tasks to complete or till any task fails or is canceled.
        /// </summary>        
        protected virtual async Task<List<T>> WhenAllOrFirstExceptionAsync<T>(
            List<Task<T>> pendingTasks, 
            CancellationToken cancellationToken)
        {
            int processed = 0;            
            int total = pendingTasks.Count;
            var responses = new List<T>();
            while (processed < total)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var completedTask = await Task.WhenAny(pendingTasks)
                    .ConfigureAwait(continueOnCapturedContext: false);
                
                //If RanToCompletion a response will be returned
                //If Faulted or Canceled an appropriate exception will be thrown  
                var response = await completedTask
                    .ConfigureAwait(continueOnCapturedContext: false);
                responses.Add(response);
                
                pendingTasks.Remove(completedTask);
                processed++;
            }
            
            return responses;
        }

        /// <summary>
        /// Waits for all the tasks to complete or till any task fails or is canceled.
        /// </summary>        
        protected virtual async Task WhenAllOrFirstExceptionAsync(List<Task> pendingTasks, CancellationToken cancellationToken)
        {
            int processed = 0;
            int total = pendingTasks.Count;            
            while (processed < total)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var completedTask = await Task.WhenAny(pendingTasks)
                    .ConfigureAwait(continueOnCapturedContext: false);                
                
                //If RanToCompletion a response will be returned
                //If Faulted or Canceled an appropriate exception will be thrown       
                await completedTask
                    .ConfigureAwait(continueOnCapturedContext: false);                    
                
                pendingTasks.Remove(completedTask);
                processed++;
            }
        }

        protected virtual async Task ExecuteCommandAsync(BaseCommand command, CancellationTokenSource internalCts, SemaphoreSlim throttler)
        {
            try
            {
                await command.ExecuteAsync(internalCts.Token)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            catch (Exception exception)
            {
                if (!(exception is OperationCanceledException))
                {
                    // Cancel scheduling any more tasks.
                    // Cancel other upload requests.
                    await internalCts.CancelAsync().ConfigureAwait(false);
                }
                throw;
            }
            finally
            {
                throttler.Release();
            }
        }
        
        protected virtual GetObjectRequest ConvertToGetObjectRequest(BaseDownloadRequest request)
        {
            GetObjectRequest getRequest = new GetObjectRequest
            {
                BucketName = request.BucketName,
                Key = request.Key,
                VersionId = request.VersionId
            };
            ((IAmazonWebServiceRequest)getRequest).AddBeforeRequestHandler(RequestEventHandler);

            if (request.IsSetModifiedSinceDateUtc())
            {
                getRequest.ModifiedSinceDateUtc = request.ModifiedSinceDateUtc;
            }
            if (request.IsSetUnmodifiedSinceDateUtc())
            {
                getRequest.UnmodifiedSinceDateUtc = request.UnmodifiedSinceDateUtc;
            }

            getRequest.ServerSideEncryptionCustomerMethod = request.ServerSideEncryptionCustomerMethod;
            getRequest.ServerSideEncryptionCustomerProvidedKey = request.ServerSideEncryptionCustomerProvidedKey;
            getRequest.ServerSideEncryptionCustomerProvidedKeyMD5 = request.ServerSideEncryptionCustomerProvidedKeyMd5;
            getRequest.ChecksumMode = request.ChecksumMode;
            getRequest.RequestPayer = request.RequestPayer;

            return getRequest;
        }

        protected virtual void RequestEventHandler(object? sender, RequestEventArgs args)
        {
            if (args is WebServiceRequestEventArgs wsArgs)
            {
                string currentUserAgent = wsArgs.Headers[AWSSDKUtils.UserAgentHeader];
                wsArgs.Headers[AWSSDKUtils.UserAgentHeader] =
                    currentUserAgent + " ft/s3-transfer md/" + GetType().Name;
            }
        }

        internal virtual string DebuggerDisplay => ToString() ?? GetType().Name;
    }
}
