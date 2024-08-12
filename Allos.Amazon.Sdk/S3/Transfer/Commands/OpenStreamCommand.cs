using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Sdk.Fork;

namespace Amazon.Sdk.S3.Transfer.Internal
{
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/OpenStreamCommand.cs", "Amazon.S3.Transfer.Internal")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/_async/OpenStreamCommand.async.cs", "Amazon.S3.Transfer.Internal")]
    internal class OpenStreamCommand : BaseCommand
    {
        private readonly IAmazonS3 _s3Client;
        private readonly TransferUtilityOpenStreamRequest _request;

        internal OpenStreamCommand(IAmazonS3 s3Client, TransferUtilityOpenStreamRequest request)
        {
            _s3Client = s3Client;
            _request = request;
        }
        
        public override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var getRequest = ConstructRequest();
            var response = await _s3Client.GetObjectAsync(getRequest, cancellationToken)
                .ConfigureAwait(continueOnCapturedContext: false);
            ResponseStream = response.ResponseStream;
        }

        private GetObjectRequest ConstructRequest()
        {
            if (!_request.IsSetBucketName())
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(_request.BucketName);
            }
            if (!_request.IsSetKey())
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(_request.Key);
            }

            return ConvertToGetObjectRequest(_request);
        }

        internal Stream? ResponseStream { get; private set; }
    }
}
