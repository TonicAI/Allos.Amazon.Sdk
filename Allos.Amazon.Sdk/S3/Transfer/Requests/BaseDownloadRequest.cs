﻿using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.Runtime.Internal;
using Amazon.S3;

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
        protected DateTimeOffset? _modifiedSinceDateUtc;
        protected DateTimeOffset? _unmodifiedSinceDateUtc;

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
        public virtual DateTimeOffset ModifiedSinceDateUtc
        {
            get => _modifiedSinceDateUtc ?? default(DateTime);
            set => _modifiedSinceDateUtc = value;
        }

        // Check to see if ModifiedSinceDateUtc property is set
        [MemberNotNullWhen(true, nameof(ModifiedSinceDateUtc))]
        [MemberNotNullWhen(true, nameof(_modifiedSinceDateUtc))]
        internal virtual bool IsSetModifiedSinceDateUtc() => _modifiedSinceDateUtc.HasValue;

        /// <summary>
        /// 	Gets or sets the <c>UnmodifiedSinceDate</c> property.
        /// </summary>
        /// <value>
        /// 	The <c>UnmodifiedSinceDate</c> property.
        /// </value>
        public DateTimeOffset UnmodifiedSinceDateUtc
        {
            get => _unmodifiedSinceDateUtc ?? default;
            set => _unmodifiedSinceDateUtc = value;
        }

        // Check to see if UnmodifiedSinceDateUtc property is set
        [MemberNotNullWhen(true, nameof(UnmodifiedSinceDateUtc))]
        [MemberNotNullWhen(true, nameof(_unmodifiedSinceDateUtc))]
        internal virtual bool IsSetUnmodifiedSinceDateUtc() => _unmodifiedSinceDateUtc.HasValue;

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
        
        internal virtual string DebuggerDisplay => ToString() ?? GetType().Name;
    }
}