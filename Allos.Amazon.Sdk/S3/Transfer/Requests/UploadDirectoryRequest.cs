using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.Util;

namespace Allos.Amazon.Sdk.S3.Transfer
{
    /// <summary>
    /// Contains all the parameters
    /// that can be set when making a request with the 
    /// <see cref="AsyncTransferUtility"/> method.
    /// </summary>
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
    [SuppressMessage("ReSharper", "PropertyCanBeMadeInitOnly.Global")]
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    [DebuggerDisplay("{DebuggerDisplay}")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityUploadDirectoryRequest.cs", "Amazon.S3.Transfer")]
    public class UploadDirectoryRequest : BaseUploadRequest
    {
        protected string _searchPattern = "*";
        protected MetadataCollection? _metadataCollection;
        protected DateTimeOffset? _objectLockRetainUntilDate;
       
        /// <summary>
        /// Gets or sets whether the payload should be signed or not
        /// </summary>
        public virtual bool DisablePayloadSigning { get; set; }

        /// <summary>
        /// 	Gets or sets the directory where files are uploaded from.
        /// </summary>
        /// <value>
        /// 	The directory where files are uploaded from.
        /// </value>
        public virtual string? Directory { get; set; }

        /// <summary>
        /// Checks if Directory property is set.
        /// </summary>
        /// <returns>true if Directory property is set.</returns>
        [MemberNotNullWhen(true, nameof(Directory))]
        internal virtual bool IsSetDirectory() => !string.IsNullOrWhiteSpace(Directory);

        /// <summary>
        /// 	Gets or sets the KeyPrefix property.  As object keys are generated for the
        /// 	files being uploaded this value will prefix the key.  This is useful when a directory
        /// 	needs to be uploaded into sub directory in the S3 Bucket.
        /// </summary>
        /// <value>
        /// 	The directory where files are uploaded from.
        /// </value>
        public virtual string? KeyPrefix { get; set; }

        /// <summary>
        /// Checks if KeyPrefix property is set.
        /// </summary>
        /// <returns>true if KeyPrefix property is set.</returns>
        [MemberNotNullWhen(true, nameof(KeyPrefix))]
        internal virtual bool IsSetKeyPrefix() => !string.IsNullOrWhiteSpace(KeyPrefix);

        /// <summary>
        /// 	Gets and sets the search pattern used to determine which
        /// 	files in the directory are uploaded.    
        /// </summary>
        /// <value>
        /// 	The search pattern used to determine which
        /// 	files in the directory are uploaded.
        /// 	The default value is "*", specifying that all files
        /// 	in the directory will be uploaded.  
        /// </value>
        public virtual string SearchPattern
        {
            get => string.IsNullOrWhiteSpace(_searchPattern) ? "*" : _searchPattern;
            set => _searchPattern = string.IsNullOrWhiteSpace(_searchPattern) ? "*" : value;
        }

        /// <summary>
        /// Checks if SearchPattern property is set.
        /// </summary>
        /// <returns>true if SearchPattern property is set.</returns>
        [MemberNotNullWhen(true, nameof(SearchPattern))]
        [MemberNotNullWhen(true, nameof(_searchPattern))]
        internal virtual bool IsSetSearchPattern() => !string.IsNullOrWhiteSpace(_searchPattern);

        /// <summary>
        /// 	Gets or sets the recursive options for the directory upload.
        /// </summary>
        /// <value>
        /// 	The recursive options for the directory upload.
        /// 	Set by default to <c>TopDirectoryOnly</c>,
        /// 	specifying that files will be uploaded from the root directory only.
        /// </value>
        public virtual SearchOption SearchOption { get; set; } = SearchOption.TopDirectoryOnly;
        
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
        /// 	Gets or sets the canned access control list (ACL)
        /// 	for the uploaded objects.
        /// 	Please refer to 
        /// 	<see cref="T:Amazon.S3.S3CannedACL"/> for
        /// 	information on Amazon S3 canned ACLs.
        /// </summary>
        /// <value>
        /// 	The canned access control list (ACL)
        /// 	for the uploaded objects.
        /// </value>
        public virtual S3CannedACL? CannedAcl { get; set; }

        /// <summary>
        /// Checks if the CannedACL property is set.
        /// </summary>
        /// <returns>true if there is the CannedACL property is set.</returns>
        [MemberNotNullWhen(true, nameof(CannedAcl))]
        internal virtual bool IsSetCannedAcl() => (CannedAcl != null &&CannedAcl != S3CannedACL.NoACL);

        /// <summary>
        /// 	Gets or sets the content type for the uploaded Amazon S3 objects.
        ///     The default behavior when this field is not set is to use the file
        ///     extension to set the content type. If this field is set to a value it
        ///     will be applied to all uploaded files in the directory, overriding
        ///     file extension inspection.
        /// </summary>
        /// <value>
        /// 	The content type for all the uploaded Amazon S3 objects.
        /// </value>
        public virtual string? ContentType { get; set; }

        /// <summary>
        /// 	Gets or sets the storage class for the uploaded Amazon S3 objects.
        /// 	Please refer to 
        /// 	<see cref="T:Amazon.S3.S3StorageClass"/> for
        /// 	information on S3 Storage Classes.
        /// </summary>
        /// <value>
        /// 	The storage class for the uploaded Amazon S3 objects.
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
        /// Gets or sets the ServerSideEncryptionMethod property.
        /// Specifies the encryption used on the server to
        /// store the content.
        /// </summary>
        public virtual ServerSideEncryptionMethod? ServerSideEncryptionMethod { get; set; }

        /// <summary>
        /// The id of the AWS Key Management Service key that Amazon S3 should use to encrypt and decrypt the object.
        /// If a key id is not specified, the default key will be used for encryption and decryption.
        /// </summary>
        [AWSProperty(Sensitive=true)]
        public virtual string? ServerSideEncryptionKeyManagementServiceKeyId { get; set; }

        /// <summary>
        /// Checks if ServerSideEncryptionKeyManagementServiceKeyId property is set.
        /// </summary>
        /// <returns>true if ServerSideEncryptionKeyManagementServiceKeyId property is set.</returns>
        [MemberNotNullWhen(true, nameof(ServerSideEncryptionKeyManagementServiceKeyId))]
        internal virtual bool IsSetServerSideEncryptionKeyManagementServiceKeyId() => !string.IsNullOrWhiteSpace(ServerSideEncryptionKeyManagementServiceKeyId);

        /// <summary>
        /// The Server-side encryption algorithm to be used with the customer provided key.
        /// </summary>
        public virtual ServerSideEncryptionCustomerMethod? ServerSideEncryptionCustomerMethod { get; set; }

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
        [AWSProperty(Sensitive=true)]
        public virtual string? ServerSideEncryptionCustomerProvidedKey { get; set; }

        /// <summary>
        /// The MD5 of the customer encryption key specified in the ServerSideEncryptionCustomerProvidedKey property. The MD5 is
        /// base 64 encoded. This field is optional, the SDK will calculate the MD5 if this is not set.
        /// </summary>
        public virtual string? ServerSideEncryptionCustomerProvidedKeyMd5 { get; set; }

        /// <summary>
        /// Gets or sets whether the Content-MD5 header should be calculated for upload.
        /// </summary>
        public virtual bool CalculateContentMd5Header { get; set; }

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
        /// Gets or sets the UploadFilesConcurrently property.
        /// Specifies if multiple files will be uploaded concurrently.
        /// The number of concurrent web requests used is controlled 
        /// by the TransferUtilityConfig.ConcurrencyLevel property.
        /// </summary>
        public virtual bool UploadFilesConcurrently { get; set; }
        
        /// <summary>
        /// The event for UploadDirectoryProgressEvent notifications. All
        /// subscribers will be notified when a new progress
        /// event is raised.
        /// <para>
        /// The UploadDirectoryProgressEvent is fired as data
        /// is uploaded to S3.  The delegates attached to the event 
        /// will be passed information detailing how much data
        /// has been uploaded as well as how much will be uploaded.
        /// </para>
        /// </summary>
        /// <remarks>
        /// Subscribe to this event if you want to receive
        /// UploadDirectoryProgressEvent notifications. Here is how:<br />
        /// 1. Define a method with a signature similar to this one:
        /// <code>
        /// private void DisplayProgress(object sender, UploadDirectoryProgressArgs args)
        /// {
        ///     Console.WriteLine(args);
        /// }
        /// </code>
        /// 2. Add this method to the UploadDirectoryProgressEvent delegate's invocation list
        /// <code>
        /// TransferUtilityUploadDirectoryRequest request = new TransferUtilityUploadDirectoryRequest();
        /// request.UploadDirectoryProgressEvent += displayProgress;
        /// </code>
        /// </remarks>
        public event EventHandler<UploadDirectoryProgressArgs>? UploadDirectoryProgressEvent;

        /// <summary>
        /// The event for modifying individual TransferUtilityUploadRequest for each file
        /// being uploaded.
        /// </summary>
        public event EventHandler<UploadDirectoryFileRequestArgs>? UploadDirectoryFileRequestEvent;

        /// <summary>
        /// Causes the UploadDirectoryProgressEvent event to be fired.
        /// </summary>
        /// <param name="uploadDirectoryProgress">Progress data for files currently being uploaded.</param>
        internal virtual void OnRaiseProgressEvent(UploadDirectoryProgressArgs uploadDirectoryProgress)
        {
            AWSSDKUtils.InvokeInBackground(UploadDirectoryProgressEvent, uploadDirectoryProgress, this);
        }

        internal virtual void RaiseUploadDirectoryFileRequestEvent(UploadRequest request)
        {
            var targetEvent = UploadDirectoryFileRequestEvent;
            if (targetEvent != null)
            {
                var args = new UploadDirectoryFileRequestArgs(request);
                targetEvent(this, args);
            }
        }

        /// <summary>
        /// Tags that will be applied to all objects in the directory.
        /// </summary>
        public virtual List<Tag>? TagSet { get; set; }
        
        internal virtual string DebuggerDisplay => ToString() ?? GetType().Name;
    }
}
