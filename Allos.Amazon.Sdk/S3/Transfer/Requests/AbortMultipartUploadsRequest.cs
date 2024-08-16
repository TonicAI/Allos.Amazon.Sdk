using System.Diagnostics.CodeAnalysis;

namespace Allos.Amazon.Sdk.S3.Transfer;

[SuppressMessage("ReSharper", "VirtualMemberNeverOverridden.Global")]
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
[SuppressMessage("ReSharper", "InconsistentNaming")]
[SuppressMessage("ReSharper", "UnusedMember.Global")]
public class AbortMultipartUploadsRequest : BaseRequest
{
    /// <summary>
    /// 	Gets or sets the name of the bucket.
    /// </summary>
    /// <value>
    /// 	The name of the bucket.
    /// </value>
    public virtual string? BucketName { get; set; }
    
    /// <summary>
    /// Gets whether the bucket name is set.
    /// </summary>
    /// <returns>
    /// 	A value of <c>true</c> if the bucket name is set.
    ///    Returns <c>false</c> if otherwise.
    /// </returns>
    [MemberNotNullWhen(true, nameof(BucketName))]
    internal virtual bool IsSetBucketName() => !string.IsNullOrWhiteSpace(BucketName);
    
    /// <summary>
    /// 	Gets or sets the <c>InitiateDateUtc</c> property.
    /// </summary>
    /// <value>
    /// 	The <c>InitiateDateUtc</c> property.
    /// </value>
    public virtual DateTimeOffset InitiateDateUtc { get; set; }
    
    [MemberNotNullWhen(true, nameof(InitiateDateUtc))]
    internal virtual bool IsSetUnmodifiedSinceDateUtc() => InitiateDateUtc != default;
}