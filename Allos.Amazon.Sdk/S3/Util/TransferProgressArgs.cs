using System.Reflection;
using Allos.Amazon.Sdk.Fork;
using Amazon.S3.Model;

namespace Allos.Amazon.S3.Model
{
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Util/TransferProgressArgs.cs", "Amazon.S3.Util")]
    internal static class TransferProgressArgsAdapter
    {
        private static readonly PropertyInfo IncrementTransferredPropertyInfo;
        
        static TransferProgressArgsAdapter()
        {
            var typeOfTransferProgressArgs = typeof(TransferProgressArgs);
            
            var incrementTransferredPropertyInfo =
                typeOfTransferProgressArgs.GetProperty(
                    "IncrementTransferred",
                    BindingFlags.Instance | BindingFlags.NonPublic);
            
            //if this is null, the AWS sdk has changed its `internal` representation of TransferProgressArgs
            ArgumentNullException.ThrowIfNull(incrementTransferredPropertyInfo);

            IncrementTransferredPropertyInfo = incrementTransferredPropertyInfo;
        }

        /// <inheritdoc cref="TransferProgressArgs.IncrementTransferred"/>
        public static ulong IncrementTransferred(this TransferProgressArgs transferProgressArgs) =>
            (ulong?) (long?) IncrementTransferredPropertyInfo.GetValue(transferProgressArgs) ?? default;
    }
}
