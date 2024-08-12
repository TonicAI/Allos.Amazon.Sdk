using System.Net;
using System.Runtime.ExceptionServices;
using Amazon.Runtime;
using Amazon.Runtime.Internal.Util;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Sdk.Fork;
using Amazon.Sdk.S3.Util;
using Amazon.Util;

namespace Amazon.Sdk.S3.Transfer.Internal
{
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/DownloadCommand.cs", "Amazon.S3.Transfer.Internal")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/_async/DownloadCommand.async.cs", "Amazon.S3.Transfer.Internal")]
    internal class DownloadCommand : BaseCommand
    {
        private static readonly int MaxBackoffInMilliseconds = (int)TimeSpan.FromSeconds(30).TotalMilliseconds;
        
#if !NETSTANDARD
        // Set of web exception status codes to retry on.
        private static readonly ICollection<WebExceptionStatus> WebExceptionStatusesToRetryOn = new HashSet<WebExceptionStatus>
        {
            WebExceptionStatus.ConnectFailure,

            WebExceptionStatus.ConnectionClosed,
            WebExceptionStatus.KeepAliveFailure,
            WebExceptionStatus.NameResolutionFailure,
            WebExceptionStatus.ReceiveFailure
        };
#endif

        private static Logger Logger => Logger.GetLogger(typeof(TransferUtility));

        private readonly IAmazonS3 _s3Client;
        private readonly TransferUtilityDownloadRequest _request;

        internal DownloadCommand(IAmazonS3 s3Client, TransferUtilityDownloadRequest request)
        {
            _s3Client = s3Client;
            _request = request;
        }
        
        public override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            //ValidateRequest()
            if (!_request.IsSetBucketName())
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(_request.BucketName);
            }
            if (!_request.IsSetFilePath())
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(_request.FilePath);
            }
            if (!_request.IsSetKey())
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(_request.Key);
            }
            //\
            GetObjectRequest getRequest = ConvertToGetObjectRequest(_request);

            var maxRetries = _s3Client.Config.MaxErrorRetry;
            var retries = 0;
            bool shouldRetry;
            string? mostRecentETag = null;
            do
            {
                shouldRetry = false;

                if (retries != 0)
                {
                    ByteRange bytesRemaining = ByteRangeRemainingForDownload(_request.FilePath);
                    getRequest.ByteRange = bytesRemaining;
                }

                try
                {
                    using (var response = await _s3Client.GetObjectAsync(getRequest, cancellationToken)
                        .ConfigureAwait(continueOnCapturedContext: false))
                    {
                        if (!string.IsNullOrWhiteSpace(mostRecentETag) && !string.Equals(mostRecentETag, response.ETag))
                        {
                            //if the eTag changed, we need to retry from the start of the file
                            mostRecentETag = response.ETag;
                            getRequest.ByteRange = null;
                            retries = 0;
                            shouldRetry = true;
                            WaitBeforeRetry(retries);
                            continue;
                        }
                        mostRecentETag = response.ETag;

                        if (retries == 0)
                        {
                            /* 
                             * Wipe the local file, if it exists, to handle edge case where:
                             * 
                             * 1. File foo exists
                             * 2. We start trying to download, but unsuccessfully write any data
                             * 3. We retry the download, with retires > 0, thus hitting the else statement below
                             * 4. We will append to file foo, instead of overwriting it
                             * 
                             * We counter it with the call below because it's the same call that would be hit
                             * in WriteResponseStreamToFile. If any exceptions are thrown, they will be the same as before
                             * to avoid any breaking changes to customers who handle that specific exception in a
                             * particular manner.
                             */
#if BCL || !NETSTANDARD
                            if (File.Exists(_request.FilePath))
                            {
                                await using (new FileStream(_request.FilePath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, S3Constants.DefaultBufferSize))
                                {
                                    //Do nothing. Simply using the "using" statement to create and dispose of FileStream temp in the same call.
                                }
                            }
#endif

                            response.WriteObjectProgressEvent += OnWriteObjectProgressEvent;
                            await response.WriteResponseStreamToFileAsync(_request.FilePath, false, cancellationToken)
                                .ConfigureAwait(continueOnCapturedContext: false);
                        }
                        else
                        {
                            response.WriteObjectProgressEvent += OnWriteObjectProgressEvent;
                            await response.WriteResponseStreamToFileAsync(_request.FilePath, true, cancellationToken)
                                .ConfigureAwait(continueOnCapturedContext: false);
                        }
                    }
                }
                catch (Exception exception)
                {
                    retries++;
                    shouldRetry = HandleExceptionForHttpClient(exception, retries, maxRetries);
                    if (!shouldRetry)
                    {
                        if (exception is IOException)
                        {
                            throw;
                        }

                        if (exception.InnerException is IOException)
                        {
                            ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
                        }
                        else if (exception is AmazonServiceException ||
                                 exception is AmazonClientException)
                        {
                            throw;
                        }
                        else
                        {
                            throw new AmazonServiceException(exception);
                        }
                    }
                }
                WaitBeforeRetry(retries);
            } while (shouldRetry);
        }

        private static bool HandleExceptionForHttpClient(Exception exception, int retries, int maxRetries)
        {
            if (AWSHttpClient.IsHttpInnerException(exception))
            {
                var innerHttpException = exception.InnerException;
                if (innerHttpException is IOException
#if !NETSTANDARD
                    || innerHttpException is WebException
#endif
                    )
                {
                    return HandleException(innerHttpException, retries, maxRetries);
                }

                return false;
            }

            return HandleException(exception, retries, maxRetries);
        }

        private void OnWriteObjectProgressEvent(object? sender, WriteObjectProgressArgs e)
        {
            _request.OnRaiseProgressEvent(e);
        }

        private static bool HandleException(Exception exception, int retries, int maxRetries)
        {
            var canRetry = true;
            if (exception is IOException)
            {
#if !NETSTANDARD
                while (exception.InnerException != null)
                {
                    if (exception.InnerException is ThreadAbortException)
                    {
                        Logger.Error(exception, "Encountered a IOException caused by a ThreadAbortException.");
                        return false;
                    }
                    exception = exception.InnerException;
                }
#endif
                if (retries < maxRetries)
                {
                    Logger.InfoFormat("Encountered an IOException. Retrying, retry {0} of {1}.",
                        retries, maxRetries);
                    return true;
                }

                canRetry = false;
            }

#if !NETSTANDARD
            if (exception is WebException webException)
            {
                Logger.Error(exception, "Encountered a WebException ({1}).", webException.GetType().Name, webException.Status);
                if (WebExceptionStatusesToRetryOn.Contains(webException.Status) && retries < maxRetries)
                {

                    Logger.InfoFormat("Encountered a WebException ({0}). Retrying, retry {1} of {2}.",
                        webException.Status, retries, maxRetries);
                    return true;
                }

                canRetry = false;
            }
#endif
            if (!canRetry)
            {
                Logger.Error(exception, "Encountered a {0}. Reached maximum retries {1} of {2}.", exception.GetType().Name, retries, maxRetries);
                return false;
            }

            Logger.Error(exception, "Encountered a non retryable {0}, rethrowing exception.", exception.GetType().Name);
            return false;
        }

        private static void WaitBeforeRetry(int retries)
        {
            int delay = (int)(Math.Pow(4, retries) * 100);
            delay = Math.Min(delay, MaxBackoffInMilliseconds);
            AWSSDKUtils.Sleep(delay);
        }

        /// <summary>
        /// Returns the amount of bytes remaining that need to be pulled down from S3.
        /// </summary>
        /// <param name="filepath">The fully qualified path of the file.</param>
        /// <returns></returns>
        private static ByteRange ByteRangeRemainingForDownload(string filepath)
        {
            /*
             * Initialize the ByteRange as the whole file.
             * long.MaxValue works regardless of the size because
             * S3 will stop sending bits if you specify beyond the
             * size of the file anyways.
             */
            ByteRange byteRange = new(0, long.MaxValue);

            if (File.Exists(filepath))
            {
                FileInfo info = new(filepath);
                byteRange.Start = info.Length;
            }

            return byteRange;
        }
    }
}

