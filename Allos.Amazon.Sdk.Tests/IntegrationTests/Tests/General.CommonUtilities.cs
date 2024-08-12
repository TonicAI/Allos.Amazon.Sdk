using System.Reflection;
using Amazon.Runtime;
using Amazon.Sdk.Fork;

namespace AWSSDK_DotNet.IntegrationTests.Tests
{
    [AmazonSdkFork("sdk/test/IntegrationTests/Tests/General.CommonUtilities.cs", "AWSSDK_DotNet.IntegrationTests.Tests")]
    public static class General
    {
        // Reflection helpers
        public static TimeSpan IncorrectPositiveClockSkewOffset = TimeSpan.FromHours(26);
        public static TimeSpan IncorrectNegativeClockSkewOffset = TimeSpan.FromHours(-1);

        public static void SetClockSkewCorrection<T>(T client, TimeSpan value)
            where T : AmazonServiceClient
        {
            var method = typeof(CorrectClockSkew).GetMethod("SetClockCorrectionForEndpoint", BindingFlags.Static | BindingFlags.NonPublic);
            ArgumentNullException.ThrowIfNull(method);
            
#pragma warning disable CS0618
            method.Invoke(null, new object[] { client.Config.DetermineServiceURL(), value });
#pragma warning restore CS0618
        }
    }
}