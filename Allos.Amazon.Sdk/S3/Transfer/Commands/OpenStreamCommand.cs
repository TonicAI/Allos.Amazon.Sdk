using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.S3;
using Amazon.S3.Model;

namespace Allos.Amazon.Sdk.S3.Transfer.Internal
{
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/OpenStreamCommand.cs", "Amazon.S3.Transfer.Internal")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/Internal/_async/OpenStreamCommand.async.cs", "Amazon.S3.Transfer.Internal")]
    internal class OpenStreamCommand : BaseCommand
    {
        protected readonly OpenStreamRequest _request;

        internal OpenStreamCommand(IAsyncTransferUtility asyncTransferUtility, OpenStreamRequest request)
            : base(asyncTransferUtility, request)
        {
            _request = request;
        }
        
        public override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var getRequest = ConstructRequest();
            var response = await S3Client.GetObjectAsync(getRequest, cancellationToken)
                .ConfigureAwait(continueOnCapturedContext: false);
            ResponseStream = response.ResponseStream;
        }

        protected virtual GetObjectRequest ConstructRequest()
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

        internal virtual Stream? ResponseStream { get; private set; }
    }
}
