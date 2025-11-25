using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;

namespace Allos.Amazon.Sdk.S3.Transfer
{
    /// <summary>
    /// The base class for requests that return Amazon S3 objects.
    /// </summary>
    [SuppressMessage("ReSharper", "VirtualMemberNeverOverridden.Global")]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    [DebuggerDisplay("{DebuggerDisplay}")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/BaseDownloadRequest.cs", "Amazon.S3.Transfer")]
    public abstract class BaseDownloadRequest : BaseRequest
    {
        protected DateTimeOffset? _modifiedSinceDate;
        protected DateTimeOffset? _unmodifiedSinceDate;
        protected ResponseHeaderOverrides? _responseHeaders;

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
        /// 	Gets or sets the key under which the Amazon S3 object is stored.
        /// </summary>
        /// <value>
        /// 	The key under which the Amazon S3 object is stored. 
        /// </value>
        public virtual string? Key { get; set; }

        /// <summary>
        /// 	Gets whether the key property is set.
        /// </summary>
        /// <returns>
        /// 	A value of <c>true</c> if key property is set.
        /// 	Returns <c>false</c> if otherwise.
        /// </returns>
        [MemberNotNullWhen(true, nameof(Key))]
        internal virtual bool IsSetKey() => !string.IsNullOrWhiteSpace(Key);

        /// <summary>
        /// 	Gets or sets the version ID of the Amazon S3 object.
        /// </summary>
        /// <value>
        /// 	The version ID of the Amazon S3 object.
        /// </value>
        public virtual string? VersionId { get; set; }

        /// <summary>
        /// Checks if VersionId property is set.
        /// </summary>
        /// <returns>true if VersionId property is set.</returns>
        [MemberNotNullWhen(true, nameof(VersionId))]
        internal virtual bool IsSetVersionId() => !string.IsNullOrWhiteSpace(VersionId);

        /// <summary>
        /// 	Gets or sets the <c>ModifiedSinceDate</c> property.
        /// </summary>
        /// <value>
        /// 	The <c>ModifiedSinceDate</c> property. 
        /// </value>
        public virtual DateTimeOffset ModifiedSinceDate
        {
            get => _modifiedSinceDate ?? default(DateTime);
            set => _modifiedSinceDate = value;
        }

        // Check to see if ModifiedSinceDate property is set
        [MemberNotNullWhen(true, nameof(ModifiedSinceDate))]
        [MemberNotNullWhen(true, nameof(_modifiedSinceDate))]
        internal virtual bool IsSetModifiedSinceDate() => _modifiedSinceDate.HasValue;

        /// <summary>
        /// 	Gets or sets the <c>UnmodifiedSinceDate</c> property.
        /// </summary>
        /// <value>
        /// 	The <c>UnmodifiedSinceDate</c> property.
        /// </value>
        public DateTimeOffset UnmodifiedSinceDate
        {
            get => _unmodifiedSinceDate ?? default;
            set => _unmodifiedSinceDate = value;
        }

        // Check to see if UnmodifiedSinceDate property is set
        [MemberNotNullWhen(true, nameof(UnmodifiedSinceDate))]
        [MemberNotNullWhen(true, nameof(_unmodifiedSinceDate))]
        internal virtual bool IsSetUnmodifiedSinceDate() => _unmodifiedSinceDate.HasValue;

        /// <summary>
        /// The Server-side encryption algorithm to be used with the customer provided key.
        ///  
        /// </summary>
        public virtual ServerSideEncryptionCustomerMethod? ServerSideEncryptionCustomerMethod { get; set; }

        /// <summary>
        /// The base64-encoded encryption key for Amazon S3 to use to decrypt the object
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
        [AWSProperty(Sensitive=true)]
        public virtual string? ServerSideEncryptionCustomerProvidedKey { get; set; }

        /// <summary>
        /// The MD5 of the customer encryption key specified in the ServerSideEncryptionCustomerProvidedKey property. The MD5 is
        /// base 64 encoded. This field is optional, the SDK will calculate the MD5 if this is not set.
        /// </summary>
        public virtual string? ServerSideEncryptionCustomerProvidedKeyMd5 { get; set; }

        /// <summary>
        /// Gets and sets the property ChecksumMode. 
        /// <para>
        /// This must be enabled to retrieve the checksum.
        /// </para>
        /// </summary>
        public virtual ChecksumMode? ChecksumMode { get; set; }

        /// <summary>
        /// Confirms that the requester knows that they will be charged for the request. 
        /// Bucket owners need not specify this parameter in their requests.
        /// </summary>
        public virtual RequestPayer? RequestPayer { get; set; }
        
        /// <summary>
        /// Gets and sets the property ExpectedBucketOwner. 
        /// <para>
        /// The account ID of the expected bucket owner. If the account ID that you provide does
        /// not match the actual owner of the bucket, the request fails with the HTTP status code
        /// <c>403 Forbidden</c> (access denied).
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
        /// Gets and sets the property IfMatch. 
        /// <para>
        /// Return the object only if its entity tag (ETag) is the same as the one specified in this header;
        /// otherwise, return a <c>412 Precondition Failed</c> error.
        /// </para>
        /// <para>
        /// If both of the <c>If-Match</c> and <c>If-Unmodified-Since</c> headers are present in the request as follows:
        /// <c>If-Match</c> condition evaluates to <c>true</c>, and; <c>If-Unmodified-Since</c> condition evaluates to <c>false</c>;
        /// then, S3 returns <c>200 OK</c> and the data requested.
        /// </para>
        /// <para>
        /// For more information about conditional requests, see <see href="https://tools.ietf.org/html/rfc7232">RFC 7232</see>.
        /// </para>
        /// The <see cref="IfMatch"/> property is equivalent to the <see cref="GetObjectRequest.EtagToMatch"/>.
        /// </summary>
        public string? IfMatch { get; set; }

        /// <summary>
        /// Checks to see if IfMatch is set.
        /// </summary>
        /// <returns>true, if IfMatch property is set.</returns>
        [MemberNotNullWhen(true, nameof(IfMatch))]
        internal virtual bool IsSetIfMatch()
        {
            return !string.IsNullOrEmpty(IfMatch);
        }

        /// <summary>
        /// Gets and sets the property IfNoneMatch. 
        /// <para>
        /// Return the object only if its entity tag (ETag) is different from the one specified in this header;
        /// otherwise, return a <c>304 Not Modified</c> error.
        /// </para>
        /// <para>
        /// If both of the <c>If-None-Match</c> and <c>If-Modified-Since</c> headers are present in the request as follows:
        /// <c> If-None-Match</c> condition evaluates to <c>false</c>, and; <c>If-Modified-Since</c> condition evaluates to <c>true</c>;
        /// then, S3 returns <c>304 Not Modified</c> HTTP status code.
        /// </para>
        /// <para>
        /// For more information about conditional requests, see <see href="https://tools.ietf.org/html/rfc7232">RFC 7232</see>.
        /// </para>
        /// The <see cref="IfNoneMatch"/> property is equivalent to the <see cref="GetObjectRequest.EtagToNotMatch"/>.
        /// </summary>
        public string? IfNoneMatch { get; set; }

        /// <summary>
        /// Checks to see if IfNoneMatch is set.
        /// </summary>
        /// <returns>true, if IfNoneMatch property is set.</returns>
        [MemberNotNullWhen(true, nameof(IfNoneMatch))]
        internal virtual bool IsSetIfNoneMatch()
        {
            return !string.IsNullOrEmpty(IfNoneMatch);
        }

        /// <summary>
        /// A set of response headers that should be returned with the object.
        /// </summary>
        public ResponseHeaderOverrides ResponseHeaderOverrides
        {
            get
            {
                _responseHeaders ??= new ResponseHeaderOverrides();
                return _responseHeaders;
            }
            set => _responseHeaders = value;
        }
        
        internal virtual string DebuggerDisplay => ToString() ?? GetType().Name;
    }
}