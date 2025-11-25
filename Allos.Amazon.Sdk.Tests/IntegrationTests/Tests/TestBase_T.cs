using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Allos.Amazon.Sdk.Fork;
using Allos.Amazon.Sdk.Tests.IntegrationTests.Utils;
using Amazon;
using Amazon.Runtime;

namespace Allos.Amazon.Sdk.Tests.IntegrationTests.Tests
{
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    [AmazonSdkFork("sdk/test/IntegrationTests/Tests/TestBase.cs", "AWSSDK_DotNet.IntegrationTests.Tests")]
    public class TestBase<T> : TestBase
        where T : AmazonServiceClient
    {
        private static T? _client;
        public static T Client
        {
            get
            {
                if(_client == null)
                {
                    _client = CreateClient();
                    RetryUtilities.ConfigureClient(_client);
                }
                return _client;
            }
            set => _client = value;
        }

        protected static void BaseInitialize()
        {
            const string credentialsProfileEnvVar = "AWS_PROFILE";

            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(credentialsProfileEnvVar, EnvironmentVariableTarget.Process)))
            {
                Environment.SetEnvironmentVariable(credentialsProfileEnvVar, TestAwsCredentialsProfileName, EnvironmentVariableTarget.Process);   
            }
        }

        public static void BaseClean()
        {
            if (_client != null)
            {
                _client.Dispose();
                _client = null;
            }
        }

        public static void SetEndpoint(AmazonServiceClient client, string serviceUrl, string? region = null)
        {
            var configPropertyInfo = client
                .GetType()
                .GetProperty("Config", BindingFlags.Instance | BindingFlags.Public);
            
            ArgumentNullException.ThrowIfNull(configPropertyInfo);
            
            var clientConfig = (ClientConfig?) configPropertyInfo.GetValue(client, null);
            
            ArgumentNullException.ThrowIfNull(clientConfig);
            
            clientConfig.ServiceURL = serviceUrl;
            if (region != null)
                clientConfig.AuthenticationRegion = region;
        }

        private static T CreateClient()
        {
            // Try to find a constructor that takes RegionEndpoint (needed for S3 and some other services)
            var regionConstructor = typeof(T).GetConstructor(new[] { typeof(RegionEndpoint) });
            if (regionConstructor != null)
            {
                return (T)regionConstructor.Invoke(new object[] { TestAwsRegion });
            }

            // Fall back to parameterless constructor for services that don't require region
            var defaultConstructor = typeof(T).GetConstructor(Type.EmptyTypes);
            if (defaultConstructor != null)
            {
                return (T)defaultConstructor.Invoke(null);
            }

            throw new InvalidOperationException(
                $"Type {typeof(T).Name} must have either a constructor that accepts RegionEndpoint or a parameterless constructor.");
        }
    }
}