using Amazon.Sdk.Fork;

namespace Amazon.Sdk.S3.Util
{
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Util/S3Constants.cs", "Amazon.S3.Util")]
    internal static class S3Constants
    {
        internal static readonly long MinPartSize = 5 * (long)Math.Pow(2, 20);
        internal const int MaxNumberOfParts = 10000;

        internal const int DefaultBufferSize = 8192;

        // Commonly used static strings
        internal const string EncryptionInstructionfileSuffix = "INSTRUCTION_SUFFIX";
        internal const string EncryptionInstructionfileSuffixV2 = ".instruction";

    }
}