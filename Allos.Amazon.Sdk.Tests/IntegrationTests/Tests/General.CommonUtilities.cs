using System.Reflection;
using Allos.Amazon.Sdk.Fork;
using Amazon;

namespace Allos.Amazon.Sdk.Tests.IntegrationTests.Tests
{
    [AmazonSdkFork("sdk/test/IntegrationTests/Tests/General.CommonUtilities.cs", "AWSSDK_DotNet.IntegrationTests.Tests")]
    public static class General
    {
        // Reflection helpers
        public static TimeSpan IncorrectPositiveClockSkewOffset = TimeSpan.FromHours(26);
        public static TimeSpan IncorrectNegativeClockSkewOffset = TimeSpan.FromHours(-1);

        public static void SetClockSkewCorrection(TimeSpan value)
        {
            var property = typeof(AWSConfigs).GetProperty("ClockOffset", BindingFlags.Static | BindingFlags.Public);
            property?.SetValue(null, value);
        }
    }
}