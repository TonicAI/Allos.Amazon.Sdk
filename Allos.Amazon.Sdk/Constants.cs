using System.Diagnostics.CodeAnalysis;
using Amazon.Runtime.Internal;

namespace Allos.Amazon.Sdk;

[SuppressMessage("ReSharper", "UnusedMember.Global")]
internal static class Constants
{
    public const AttributeTargets AnyTypeDeclaration = AttributeTargets.Class |
                                                     AttributeTargets.Struct |
                                                     AttributeTargets.Delegate |
                                                     AttributeTargets.Enum |
                                                     AttributeTargets.Interface;
    
    /// <summary>
    /// This is the value the AWS SDK will use to represent an unknown content length when
    /// the actual length is not known and cannot be obtained
    /// </summary>
    /// <remarks>
    /// In this fork, `null` is used instead of this sentinel except edges where interop
    /// with the original SDK applies
    /// </remarks>
    internal const int UnknownContentLengthSentinel = -1;
        
    /// <summary>
    /// The value passed to <see cref="DefaultRetryPolicy.WaitBeforeRetry(int, int)"/> internally
    /// </summary>
    public const uint RetryMaxBackoffInMilliseconds = 5000;
}