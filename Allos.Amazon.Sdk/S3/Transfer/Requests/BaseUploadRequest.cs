using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;

namespace Allos.Amazon.Sdk.S3.Transfer;

/// <summary>
/// The base class for requests that return Amazon S3 objects.
/// </summary>
[SuppressMessage("ReSharper", "VirtualMemberNeverOverridden.Global")]
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
[SuppressMessage("ReSharper", "InconsistentNaming")]
[SuppressMessage("ReSharper", "UnusedMember.Global")]
[DebuggerDisplay("{DebuggerDisplay}")]
[AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/BaseUploadRequest.cs", "Amazon.S3.Transfer")]
public abstract class BaseUploadRequest : BaseRequest
{
    protected MetadataCollection? _metadataCollection;
    protected HeadersCollection? _headersCollection;

    protected DateTimeOffset? _objectLockRetainUntilDate;

    /// <summary>
    /// Confirms that the requester knows that they will be charged for the request. 
    /// Bucket owners need not specify this parameter in their requests.
    /// </summary>
    public RequestPayer? RequestPayer { get; set; }

    /// <summary>
    /// 	Gets or sets the name of the bucket.
    /// </summary>
    /// <value>
    /// 	The name of the bucket.
    /// </value>
    public virtual string? BucketName { get; set; }

    /// <summary>
    /// Checks if BucketName property is set.
    /// </summary>
    /// <returns>true if BucketName property is set.</returns>
    [MemberNotNullWhen(true, nameof(BucketName))]
    internal virtual bool IsSetBucketName() => !string.IsNullOrWhiteSpace(BucketName);

    /// <summary>
    /// 	Gets or sets the content type of the uploaded Amazon S3 object.
    /// </summary>
    /// <value>
    /// 	The content type of the uploaded Amazon S3 object.
    /// </value>
    public virtual string? ContentType { get; set; }

    /// <summary>
    /// Checks if ContentType property is set.
    /// </summary>
    /// <returns>true if ContentType property is set.</returns>
    [MemberNotNullWhen(true, nameof(ContentType))]
    internal virtual bool IsSetContentType() => !string.IsNullOrWhiteSpace(ContentType);

    /// <summary>
    /// Gets and sets the ServerSideEncryptionMethod property.
    /// Specifies the encryption used on the server to
    /// store the content.
    /// </summary>
    public virtual ServerSideEncryptionMethod? ServerSideEncryptionMethod { get; set; }

    /// <summary>
    /// The Server-side encryption algorithm to be used with the customer provided key.
    ///  
    /// </summary>
    public virtual ServerSideEncryptionCustomerMethod? ServerSideEncryptionCustomerMethod { get; set; }

    /// <summary>
    /// The id of the AWS Key Management Service key that Amazon S3 should use to encrypt and decrypt the object.
    /// If a key id is not specified, the default key will be used for encryption and decryption.
    /// </summary>
    [AWSProperty(Sensitive = true)]
    public virtual string? ServerSideEncryptionKeyManagementServiceKeyId { get; set; }

    /// <summary>
    /// Checks if ServerSideEncryptionKeyManagementServiceKeyId property is set.
    /// </summary>
    /// <returns>true if ServerSideEncryptionKeyManagementServiceKeyId property is set.</returns>
    [MemberNotNullWhen(true, nameof(ServerSideEncryptionKeyManagementServiceKeyId))]
    internal virtual bool IsSetServerSideEncryptionKeyManagementServiceKeyId() =>
        !string.IsNullOrWhiteSpace(ServerSideEncryptionKeyManagementServiceKeyId);

    /// <summary>
    /// The base64-encoded encryption key for Amazon S3 to use to encrypt the object
    /// <para>
    /// Using the encryption key you provide as part of your request Amazon S3 manages both the encryption, as it writes 
    /// to disks, and decryption, when you access your objects. Therefore, you don't need to maintain any data encryption code. The only 
    /// thing you do is manage the encryption keys you provide.
    /// </para>
    /// <para>
    /// When you retrieve an object, you must provide the same encryption key as part of your request. Amazon S3 first verifies 
    /// the encryption key you provided matches, and then decrypts the object before returning the object data to you.
    /// </para>
    /// <para>
    /// Important: Amazon S3 does not store the encryption key you provide.
    /// </para>
    /// </summary>
    [AWSProperty(Sensitive = true)]
    public virtual string? ServerSideEncryptionCustomerProvidedKey { get; set; }

    /// <summary>
    /// The MD5 of the customer encryption key specified in the ServerSideEncryptionCustomerProvidedKey property. The MD5 is
    /// base 64 encoded. This field is optional, the SDK will calculate the MD5 if this is not set.
    /// </summary>
    public virtual string? ServerSideEncryptionCustomerProvidedKeyMd5 { get; set; }

    /// <summary>
    /// Gets and sets the property ChecksumAlgorithm. 
    /// <para>
    /// Indicates the algorithm used to create the checksum for the object. Amazon S3 will
    /// fail the request with a 400 error if there is no checksum associated with the object.
    /// For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">
    /// Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.
    /// </para>
    ///  
    /// <para>
    /// If you provide an individual checksum, Amazon S3 will ignore any provided <code>ChecksumAlgorithm</code>.
    /// </para>
    /// </summary>
    public virtual ChecksumAlgorithm? ChecksumAlgorithm { get; set; }

    /// <summary>
    /// 	Gets or sets the canned access control list (ACL)
    /// 	for the uploaded object.
    /// 	Please refer to 
    /// 	<see cref="T:S3CannedACL"/> for
    /// 	information on Amazon S3 canned ACLs.
    /// </summary>
    /// <value>
    /// 	The canned access control list (ACL)
    /// 	for the uploaded object.
    /// </value>
    public virtual S3CannedACL? CannedAcl { get; set; }

    /// <summary>
    /// Checks if the CannedACL property is set.
    /// </summary>
    /// <returns>true if there is the CannedACL property is set.</returns>
    [MemberNotNullWhen(true, nameof(CannedAcl))]
    internal virtual bool IsSetCannedAcl() => (CannedAcl != null);

    /// <summary>
    /// 	Removes the cannned access control list (ACL)
    /// 	for the uploaded object.
    /// </summary>
    public virtual void RemoveCannedAcl()
    {
        CannedAcl = null;
    }

    /// <summary>
    /// 	Gets or sets the storage class for the uploaded Amazon S3 object.
    /// 	Please refer to 
    /// 	<see cref="T:S3StorageClass"/> for
    /// 	information on S3 Storage Classes.
    /// </summary>
    /// <value>
    /// 	The storage class for the uploaded Amazon S3 object.
    /// </value>
    public virtual S3StorageClass? StorageClass { get; set; }

    /// <summary>
    /// The collection of metadata for the request.
    /// </summary>
    public virtual MetadataCollection Metadata
    {
        get => _metadataCollection ??= new MetadataCollection();
        internal set => _metadataCollection = value;
    }

    /// <summary>
    /// The tag-set for the object.
    /// </summary>
    public virtual List<Tag>? TagSet { get; set; }

    /// <summary>
    /// Gets and sets the property ObjectLockLegalHoldStatus. 
    /// <para>
    /// Specifies whether a legal hold will be applied to this object. For more information
    /// about S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object
    /// Lock</a>.
    /// </para>
    /// </summary>
    public virtual ObjectLockLegalHoldStatus? ObjectLockLegalHoldStatus { get; set; }

    /// <summary>
    /// Gets and sets the property ObjectLockMode. 
    /// <para>
    /// The Object Lock mode that you want to apply to this object.
    /// </para>
    /// </summary>
    public virtual ObjectLockMode? ObjectLockMode { get; set; }

    /// <summary>
    /// Gets and sets the property ObjectLockRetainUntilDate. 
    /// <para>
    /// The date and time when you want this object's Object Lock to expire.
    /// </para>
    /// </summary>
    public virtual DateTimeOffset ObjectLockRetainUntilDate
    {
        get => _objectLockRetainUntilDate.GetValueOrDefault();
        set => _objectLockRetainUntilDate = value;
    }

    // Check to see if ObjectLockRetainUntilDate property is set
    [MemberNotNullWhen(true, nameof(ObjectLockRetainUntilDate))]
    [MemberNotNullWhen(true, nameof(_objectLockRetainUntilDate))]
    internal virtual bool IsSetObjectLockRetainUntilDate() => _objectLockRetainUntilDate.HasValue;

    /// <summary>      
    /// <para><b>WARNING: Setting DisablePayloadSigning to true disables the SigV4 payload signing 
    /// data integrity check on this request.</b></para>  
    /// <para>If using SigV4, the DisablePayloadSigning flag controls if the payload should be 
    /// signed on a request by request basis. By default, this flag is null which will use the 
    /// default client behavior. The default client behavior is to sign the payload. When 
    /// DisablePayloadSigning is true, the request will be signed with an UNSIGNED-PAYLOAD value. 
    /// Setting DisablePayloadSigning to true requires that the request is sent over a HTTPS 
    /// connection.</para>        
    /// <para>Under certain circumstances, such as uploading to S3 while using MD5 hashing, it may 
    /// be desirable to use UNSIGNED-PAYLOAD to decrease signing CPU usage. This flag only applies 
    /// to Amazon S3 PutObject and UploadPart requests.</para>
    /// <para>MD5Stream, SigV4 payload signing, and HTTPS each provide some data integrity 
    /// verification. If DisableMD5Stream is true and DisablePayloadSigning is true, then the 
    /// possibility of data corruption is completely dependent on HTTPS being the only remaining 
    /// source of data integrity verification.</para>
    /// </summary>
    public virtual bool? DisablePayloadSigning { get; set; }

    /// <summary>
    /// <para><b>WARNING: Setting DisableDefaultChecksumValidation to true disables the default data 
    /// integrity check on upload requests.</b></para>
    /// <para>When true, checksum verification will not be used in upload requests. This may increase upload 
    /// performance under high CPU loads. Setting DisableDefaultChecksumValidation sets the deprecated property
    /// DisableMD5Stream to the same value. The default value is false. Set this value to true to 
    /// disable the default checksum validation used in all S3 upload requests or override this value per
    /// request by setting the DisableDefaultChecksumValidation property on <see cref="PutObjectRequest"/>,
    /// <see cref="UploadPartRequest"/>, or <see cref="UploadRequest"/>.</para>
    /// <para>Checksums, SigV4 payload signing, and HTTPS each provide some data integrity 
    /// verification. If DisableDefaultChecksumValidation is true and DisablePayloadSigning is true, then the 
    /// possibility of data corruption is completely dependent on HTTPS being the only remaining 
    /// source of data integrity verification.</para>
    /// </summary>
    public virtual bool? DisableDefaultChecksumValidation { get; set; }

    /// <summary>
    /// Gets and sets the property BucketKeyEnabled. 
    /// <para>
    /// Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption with
    /// server-side encryption using Key Management Service (KMS) keys (SSE-KMS).
    /// </para>
    ///  
    /// <para>
    /// <b>General purpose buckets</b> - Setting this header to <c>true</c> causes Amazon S3 to use an 
    /// S3 Bucket Key for object encryption with SSE-KMS. 
    /// Also, specifying this header with a PUT action doesn't affect bucket-level settings for S3 Bucket Key.
    /// </para>
    /// 
    /// <para>
    /// <b>Directory buckets</b> - S3 Bucket Keys are always enabled for <c>GET</c> and <c>PUT</c> operations in a directory bucket and can't be disabled. 
    /// S3 Bucket Keys aren't supported, when you copy SSE-KMS encrypted objects from general purpose buckets to directory buckets, 
    /// from directory buckets to general purpose buckets, or between directory buckets, through 
    /// <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html">CopyObject</a>, 
    /// <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html">UploadPartCopy</a>, 
    /// <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-Batch-Ops">the Copy operation in Batch Operations</a>, 
    /// or <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-import-job">the import jobs</a>. 
    /// 
    /// In this case, Amazon S3 makes a call to KMS every time a copy request is made for a KMS-encrypted object.
    /// </para>
    /// </summary>
    public bool? BucketKeyEnabled { get; set; }

    [MemberNotNullWhen(true, nameof(BucketKeyEnabled))]
    internal virtual bool IsSetBucketKeyEnabled()
    {
        return BucketKeyEnabled.HasValue;
    }

    /// <summary>
    /// Gets and sets the property ExpectedBucketOwner. 
    /// <para>
    /// The account ID of the expected bucket owner.
    /// If the account ID that you provide does not match the actual owner of the bucket,
    /// the request fails with the HTTP status code <c>403 Forbidden</c> (access denied).
    /// </para>
    /// </summary>
    public string? ExpectedBucketOwner { get; set; }

    /// <summary>
    /// Checks to see if ExpectedBucketOwner is set.
    /// </summary>
    /// <returns>true, if ExpectedBucketOwner property is set.</returns>
    [MemberNotNullWhen(true, nameof(ExpectedBucketOwner))]
    internal virtual bool IsSetExpectedBucketOwner()
    {
        return !string.IsNullOrEmpty(ExpectedBucketOwner);
    }

    /// <summary>
    /// Gets the access control lists (ACLs) for this request. 
    /// Please refer to <see cref="T:Amazon.S3.Model.S3Grant"/> for information on
    /// S3 Grants.
    /// </summary>
    public List<S3Grant>? Grants { get; set; }

    /// <summary>
    /// Gets and sets the property SSEKMSEncryptionContext. 
    /// <para>
    /// Specifies the Amazon Web Services KMS Encryption Context as
    /// an additional encryption context to use for object encryption.
    /// The value of this header is a Base64 encoded string of a UTF-8 encoded JSON,
    /// which contains the encryption context as key-value pairs.
    /// This value is stored as object metadata and automatically gets passed on to
    /// Amazon Web Services KMS for future <c>GetObject</c> operations on this object. 
    /// </para>
    /// <para> <b>General purpose buckets</b>
    /// - This value must be explicitly added during <c>CopyObject</c> operations
    /// if you want an additional encryption context for your object.
    /// For more information, see <see href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html#encryption-context">Encryption context</see>
    /// in the <i>Amazon S3 User Guide</i>.
    /// </para>
    /// <para> <b>Directory buckets</b>
    /// - You can optionally provide an explicit encryption context value.
    /// The value must match the default encryption context
    /// - the bucket Amazon Resource Name (ARN).
    /// An additional encryption context value is not supported.
    /// </para>
    /// </summary>
    public string? SSEKMSEncryptionContext { get; set; }

    /// <summary>
    /// Checks to see if SSEKMSEncryptionContext is set.
    /// </summary>
    /// <returns>true, if SSEKMSEncryptionContext property is set.</returns>
    [MemberNotNullWhen(true, nameof(SSEKMSEncryptionContext))]
    internal virtual bool IsSetSSEKMSEncryptionContext()
    {
        return !string.IsNullOrEmpty(SSEKMSEncryptionContext);
    }

    /// <summary>
    /// Gets and sets the property WebsiteRedirectLocation. 
    /// <para>
    /// If the bucket is configured as a website,
    /// redirects requests for this object to another object in the
    /// same bucket or to an external URL.
    /// Amazon S3 stores the value of this header in the object metadata.
    /// For information about object metadata, see <see href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object Key and Metadata</see>
    /// in the <i>Amazon S3 User Guide</i>.
    /// </para>
    /// <para>
    /// In the following example,
    /// the request header sets the redirect to an object (anotherPage.html) in the same bucket:
    /// </para>
    /// <para>
    /// <c>x-amz-website-redirect-location: /anotherPage.html</c>
    /// </para>
    /// <para>
    /// In the following example,
    /// the request header sets the object redirect to another website:
    /// </para>
    /// <para>
    /// <c>x-amz-website-redirect-location: http://www.example.com/</c>
    /// </para>
    /// <para>
    /// For more information about website hosting in Amazon S3,
    /// see <see href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting Websites on Amazon S3</see>
    /// and <see href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How to Configure Website Page Redirects</see>
    /// in the <i>Amazon S3 User Guide</i>.
    /// </para>
    /// <remarks>
    /// <para>
    /// This functionality is not supported for directory buckets.
    /// </para>
    /// </remarks>
    /// </summary>
    public string? WebsiteRedirectLocation { get; set; }

    /// <summary>
    /// Checks to see if WebsiteRedirectLocation is set.
    /// </summary>
    /// <returns>true, if WebsiteRedirectLocation property is set.</returns>
    [MemberNotNullWhen(true, nameof(WebsiteRedirectLocation))]
    internal virtual bool IsSetWebsiteRedirectLocation()
    {
        return !string.IsNullOrEmpty(WebsiteRedirectLocation);
    }

    /// <summary>
    /// The collection of headers for the request.
    /// </summary>
    public virtual HeadersCollection Headers
    {
        get => _headersCollection ??= new HeadersCollection();
        internal set => _headersCollection = value;
    }
    
    internal virtual string DebuggerDisplay => ToString() ?? GetType().Name;
}