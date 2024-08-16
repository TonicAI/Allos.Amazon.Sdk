using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;

namespace Allos.Amazon.Sdk.S3.Transfer
{
    /// <inheritdoc cref="IAsyncTransferConfig"/>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "PropertyCanBeMadeInitOnly.Global")]
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "ConvertConstructorToMemberInitializers")]
    [DebuggerDisplay("{DebuggerDisplay}")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityConfig.cs", "Amazon.S3.Transfer")]
    public class AsyncTransferConfig : IAsyncTransferConfig
    {
        protected uint _concurrentServiceRequests;
        
        public AsyncTransferConfig()
        {
            _concurrentServiceRequests = 10;
        }
        
        public virtual ulong MinSizeBeforePartUpload { get; set; } = (16 * (long)Math.Pow(2, 20)).ToUInt64();
        
        public virtual uint ConcurrentServiceRequests
        {
            get => _concurrentServiceRequests;
            set
            {
                if (value < 1)
                    value = 1;

                _concurrentServiceRequests = value;
            }
        }
        
        public virtual uint MultipartUploadFinalizeTimeout { get; set; } = 5000;
        
        public virtual IUploadProgressArgsFactory UploadProgressArgsFactory { get; set; } 
            = new DefaultUploadProgressArgsFactory();
        
        internal virtual string DebuggerDisplay => ToString() ?? GetType().Name;
    }
}
