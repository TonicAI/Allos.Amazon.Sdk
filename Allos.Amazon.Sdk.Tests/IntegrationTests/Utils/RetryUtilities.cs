using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Allos.Amazon.Sdk.Tests.IntegrationTests.Tests;
using Amazon;
using Amazon.Runtime;

namespace Allos.Amazon.Sdk.Tests.IntegrationTests.Utils
{
    [AmazonSdkFork("sdk/test/IntegrationTests/Utils/RetryUtilities.cs", "AWSSDK_DotNet.IntegrationTests.Utils")]
    public static class RetryUtilities
    {
        public static bool TestClockSkewCorrection = false;
        public static bool SetIncorrectClockOffsetFuture = false;

        public static void ConfigureClient<T>(T client)
            where T : AmazonServiceClient
        {
            // Attach events to client

            client.BeforeRequestEvent += (_, _) =>
            {
                if (TestClockSkewCorrection)
                {
                    // set clockskew correction to wrong value
                    SetIncorrectOffset(client);
                }
            };
            client.AfterResponseEvent += (_, _) =>
            {
            };
            client.ExceptionEvent += (_, _) =>
            {
            };
        }
        
        /// <summary>
        /// Disables clock skew correction until result is disposed
        /// </summary>
        /// <returns></returns>
        public static IDisposable DisableClockSkewCorrection()
        {
            return ClockSkewTemporarySwitch.Disable();
        }

        /// <summary>
        /// Class that switches clock skew correction on or off at creation,
        /// then returns to the previous setting when disposed
        /// </summary>
        [SuppressMessage("ReSharper", "MemberCanBePrivate.Local")]
        [SuppressMessage("ReSharper", "UnusedMember.Local")]
        private class ClockSkewTemporarySwitch : IDisposable
        {
            public bool OldValue { get; }

            public ClockSkewTemporarySwitch(bool temporarilyCorrectClockSkew)
            {
                OldValue = AWSConfigs.CorrectForClockSkew;

                AWSConfigs.CorrectForClockSkew = temporarilyCorrectClockSkew;
            }

            public static ClockSkewTemporarySwitch Enable() => new(temporarilyCorrectClockSkew: true);

            public static ClockSkewTemporarySwitch Disable() => new(temporarilyCorrectClockSkew: false);

            public void Dispose()
            {
                AWSConfigs.CorrectForClockSkew = OldValue;
            }
        }

        private static void SetIncorrectOffset<T>(T client)
            where T : AmazonServiceClient
        {
            var offset = SetIncorrectClockOffsetFuture ? General.IncorrectPositiveClockSkewOffset : General.IncorrectNegativeClockSkewOffset;
            General.SetClockSkewCorrection(client, offset);
        }
    }
}
