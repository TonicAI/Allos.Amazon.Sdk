using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Serilog;

namespace Allos.Amazon.Sdk.Tests.IntegrationTests.Tests;

[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
[SuppressMessage("ReSharper", "UnusedMember.Global")]
[AmazonSdkFork("sdk/test/IntegrationTests/Tests/TestBase.cs", "AWSSDK_DotNet.IntegrationTests.Tests")]
public abstract class TestBase
{
    /// <summary>
    /// The AWS Credentials profile name to use for the tests, may also be set via the static property <see cref="TestAwsCredentialsProfileName"/>
    /// </summary>
    /// <remarks>
    /// Replace `<see cref="_testAwsCredentialsProfileName"/> = null!` with a valid `profile name` from the local AWS credentials file.
    /// </remarks>
    /// <example>
    /// e.g.
    ///         string testAwsCredentialsProfileName = "001122334455_AwsExampleUserAccess";
    ///
    /// NOTE there are no enclosing `[]` in the `profile name`
    /// </example>
    private static string _testAwsCredentialsProfileName = "543337415716_AWSAdministratorAccess";
    
    /// <summary>
    /// The AWS Credentials profile name to use for the tests
    /// </summary>
    /// <remarks>
    /// <para>
    /// The credentials file is typically located at `~/.aws/credentials` on Unix-like systems, or `%USERPROFILE%\.aws\credentials` on Windows
    /// </para>
    /// https://docs.aws.amazon.com/cli/latest/userguide/sso-configure-profile-token.html#sso-configure-profile-token-auto-sso
    /// </remarks>
    /// <example>
    ///     # AWS Credentials file example (not real credentials)
    /// 
    ///     [001122334455_AwsExampleUserAccess]
    ///     aws_access_key_id=ASIA5XXXXXXXXXXXXXXX
    ///     aws_secret_access_key=xxxXxxXxxXXxxxxxxxxXXXXXXXXXXXXXXxxxxxx
    ///     aws_session_token=xxxXxxXxxXXxxxxxxxxXXXXXXXXXXXXXXxxxxxxxxxXxxXxxXXxxxxxxxxXXXXXXXXXXXXXXxxxxxx...
    ///     region=us-east-1
    /// </example>
    public static string TestAwsCredentialsProfileName
    {
        get
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(_testAwsCredentialsProfileName);

            return _testAwsCredentialsProfileName;
        }
        set
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(value);
            
            _testAwsCredentialsProfileName = value;
        }
    }

    /// <summary>
    /// An existing bucket name to use instead of creating a new one based on the test run date
    /// </summary>
    internal static string? ExistingBucketName { get; set; }
    
    internal static string? ExistingBucketWithSseName { get; set; }

    internal static bool ShouldDeleteBucket(string? bucketName)
    {
        if (bucketName == null)
        {
            return false;
        }
        
        if (bucketName == ExistingBucketName)
        {
            return false;
        }
        
        if (bucketName == ExistingBucketWithSseName)
        {
            return false;
        }

        return true;
    }
    
    /// <summary>
    /// The local path for test output
    /// </summary>r
    protected virtual string BasePath => "../../TestOutput";
    
    public static readonly ILogger Logger = TonicLogger.ForContext(typeof(TestBase));
}