using System.Diagnostics.CodeAnalysis;
using Amazon.S3;

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
    /// 	Gets or sets the <c>InitiatedDateUtc</c> property.
    /// </summary>
    /// <value>
    /// 	The <c>InitiateDateUtc</c> property.
    /// </value>
    public virtual DateTimeOffset? InitiatedDateUtc { get; set; }

    /// <summary>
    /// Gets or sets the account ID of the expected bucket owner.
    /// If the account ID that you provide does not match the actual owner of the bucket,
    /// the request fails with the HTTP status code 403 Forbidden (access denied).
    /// </summary>
    /// <value>
    /// The account ID of the expected bucket owner.
    /// </value>
    public string? ExpectedBucketOwner { get; set; }

    /// <summary>
    /// Checks if ExpectedBucketOwner property is set.
    /// </summary>
    /// <returns>true if ExpectedBucketOwner property is set.</returns>
    internal virtual bool IsSetExpectedBucketOwner()
    {
        return !string.IsNullOrEmpty(ExpectedBucketOwner);
    }

    /// <summary>
    /// Gets or sets the request payer setting for the abort multipart upload operations.
    /// Confirms that the requester knows that they will be charged for the request.
    /// Bucket owners need not specify this parameter in their requests.
    /// </summary>
    /// <value>
    /// The request payer setting for the abort multipart upload operations.
    /// </value>
    public RequestPayer? RequestPayer { get; set; }

    /// <summary>
    /// Checks if RequestPayer property is set.
    /// </summary>
    /// <returns>true if RequestPayer property is set.</returns>
    internal virtual bool IsSetRequestPayer()
    {
        return RequestPayer != null;
    }
    
    [MemberNotNullWhen(true, nameof(InitiatedDateUtc))]
    internal virtual bool IsSetInitiatedDate() => InitiatedDateUtc.HasValue;
}