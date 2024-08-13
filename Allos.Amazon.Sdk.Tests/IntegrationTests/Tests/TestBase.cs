using System.Diagnostics.CodeAnalysis;

namespace AWSSDK_DotNet.IntegrationTests.Tests;

[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
public abstract class TestBase
{
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
    protected static string TestAwsCredentialsProfileName
    {
        get
        {
            string testAwsCredentialsProfileName = "543337415716_AWSAdministratorAccess";
         
            // NOTE replace `null!` above with a valid `profile name` from the local AWS credentials file.
            // It should look something like this (without enclosing `[]`):
            //
            //      string testAwsCredentialsProfileName = "001122334455_AwsExampleUserAccess";
            //
            ArgumentException.ThrowIfNullOrWhiteSpace(testAwsCredentialsProfileName);

            return testAwsCredentialsProfileName;
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
    protected virtual string BasePath => "./TestOutput";
}