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
    /// Contains all the parameters that can be set when making a request with the <see cref="AsyncTransferUtility"/> method.
    /// </summary>
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
    [SuppressMessage("ReSharper", "PropertyCanBeMadeInitOnly.Global")]
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    [SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [DebuggerDisplay("{DebuggerDisplay}")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityUploadRequest.cs", "Amazon.S3.Transfer")]
    public class UploadRequest : BaseUploadRequest
    {
        protected ulong? _partSize;

        protected HeadersCollection? _headersCollection;
        protected MetadataCollection? _metadataCollection;

        protected DateTimeOffset? _objectLockRetainUntilDate;
        
        /// <summary>
        /// 	Gets or sets the name of the bucket.
        /// </summary>
        /// <value>
        /// 	The name of the bucket.
        /// </value>
        public string? BucketName { get; set; }

        /// <summary>
        /// Checks if BucketName property is set.
        /// </summary>
        /// <returns>true if BucketName property is set.</returns>
        [MemberNotNullWhen(true, nameof(BucketName))]
        internal bool IsSetBucketName() => !string.IsNullOrWhiteSpace(BucketName);

        /// <summary>
        /// 	Gets or sets the key under which the Amazon S3 object is to be stored.
        /// </summary>
        /// <value>
        /// 	The key under which the Amazon S3 object is to be stored. 
        /// </value>
        [MemberNotNullWhen(true, nameof(Key))]
        public string? Key { get; set; }

        /// <summary>
        /// Checks if Key property is set.
        /// </summary>
        /// <returns>true if Key property is set.</returns>
        [MemberNotNullWhen(true, nameof(Key))]
        internal bool IsSetKey() => !string.IsNullOrWhiteSpace(Key);

        /// <summary>
        /// 	Gets or sets the canned access control list (ACL)
        /// 	for the uploaded object.
        /// 	Please refer to 
        /// 	<see cref="T:Amazon.S3.S3CannedACL"/> for
        /// 	information on Amazon S3 canned ACLs.
        /// </summary>
        /// <value>
        /// 	The canned access control list (ACL)
        /// 	for the uploaded object.
        /// </value>
        public S3CannedACL? CannedAcl { get; set; }

        /// <summary>
        /// Checks if the CannedACL property is set.
        /// </summary>
        /// <returns>true if there is the CannedACL property is set.</returns>
        [MemberNotNullWhen(true, nameof(CannedAcl))]
        internal bool IsSetCannedAcl() => (CannedAcl != null);

        /// <summary>
        /// 	Removes the cannned access control list (ACL)
        /// 	for the uploaded object.
        /// </summary>
        public void RemoveCannedAcl()
        {
            CannedAcl = null;
        }

        /// <summary>
        /// 	Gets or sets the content type of the uploaded Amazon S3 object.
        /// </summary>
        /// <value>
        /// 	The content type of the uploaded Amazon S3 object.
        /// </value>
        public string? ContentType { get; set; }

        /// <summary>
        /// Checks if ContentType property is set.
        /// </summary>
        /// <returns>true if ContentType property is set.</returns>
        [MemberNotNullWhen(true, nameof(ContentType))]
        internal bool IsSetContentType() => !string.IsNullOrWhiteSpace(ContentType);

        /// <summary>
        /// 	Gets or sets the storage class for the uploaded Amazon S3 object.
        /// 	Please refer to 
        /// 	<see cref="T:Amazon.S3.S3StorageClass"/> for
        /// 	information on S3 Storage Classes.
        /// </summary>
        /// <value>
        /// 	The storage class for the uploaded Amazon S3 object.
        /// </value>
        public S3StorageClass? StorageClass { get; set; }

        /// <summary>
        /// Gets and sets the ServerSideEncryptionMethod property.
        /// Specifies the encryption used on the server to
        /// store the content.
        /// </summary>
        public ServerSideEncryptionMethod? ServerSideEncryptionMethod { get; set; }

        /// <summary>
        /// The Server-side encryption algorithm to be used with the customer provided key.
        ///  
        /// </summary>
        public ServerSideEncryptionCustomerMethod? ServerSideEncryptionCustomerMethod { get; set; }

        /// <summary>
        /// The id of the AWS Key Management Service key that Amazon S3 should use to encrypt and decrypt the object.
        /// If a key id is not specified, the default key will be used for encryption and decryption.
        /// </summary>
        [AWSProperty(Sensitive=true)]
        public string? ServerSideEncryptionKeyManagementServiceKeyId { get; set; }

        /// <summary>
        /// Checks if ServerSideEncryptionKeyManagementServiceKeyId property is set.
        /// </summary>
        /// <returns>true if ServerSideEncryptionKeyManagementServiceKeyId property is set.</returns>
        [MemberNotNullWhen(true, nameof(ServerSideEncryptionKeyManagementServiceKeyId))]
        internal bool IsSetServerSideEncryptionKeyManagementServiceKeyId() => !string.IsNullOrWhiteSpace(ServerSideEncryptionKeyManagementServiceKeyId);

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
        public string? ServerSideEncryptionCustomerProvidedKey { get; set; }

        /// <summary>
        /// The MD5 of the customer encryption key specified in the ServerSideEncryptionCustomerProvidedKey property. The MD5 is
        /// base 64 encoded. This field is optional, the SDK will calculate the MD5 if this is not set.
        /// </summary>
        public string? ServerSideEncryptionCustomerProvidedKeyMd5 { get; set; }

        /// <summary>
        /// Input stream for the request; content for the request will be read from the stream.
        /// </summary>
        public Stream? InputStream { get; set; }

        // Check to see if InputStream property is set
        [MemberNotNullWhen(true, nameof(InputStream))]
        internal bool IsSetInputStream() => InputStream != null;

        /// <summary>
        /// <para>
        /// 	Gets or sets the file path
        /// 	where the Amazon S3 object will be uploaded from.
        /// </para>
        /// </summary>
        /// <value>
        /// 	The file path where the Amazon S3 object will be uploaded from.
        /// </value>
        public string? FilePath { get; set; }

        /// <summary>
        /// Checks if FilePath property is set.
        /// </summary>
        /// <returns>true if FilePath property is set.</returns>
        [MemberNotNullWhen(true, nameof(FilePath))]
        internal bool IsSetFilePath() => !string.IsNullOrWhiteSpace(FilePath);

        /// <summary>
        /// 	Gets or sets the part size of the upload in bytes.
        /// 	The uploaded file will be divided into 
        /// 	parts the size specified and
        /// 	uploaded to Amazon S3 individually.
        /// </summary>
        /// <value>
        /// 	The part size of the upload.
        /// </value>
        public ulong PartSize
        {
            get => _partSize.GetValueOrDefault();
            set => _partSize = value;
        }
        
        /// <summary>
        /// Checks if PartSize property is set.
        /// </summary>
        /// <returns>true if PartSize property is set.</returns>
        [MemberNotNullWhen(true, nameof(PartSize))]
        [MemberNotNullWhen(true, nameof(_partSize))]
        internal bool IsSetPartSize() => _partSize.HasValue;

        /// <summary>
        /// The collection of headers for the request.
        /// </summary>
        public HeadersCollection Headers
        {
            get => _headersCollection ??= new HeadersCollection();
            internal set => _headersCollection = value;
        }

        /// <summary>
        /// The collection of metadata for the request.
        /// </summary>
        public MetadataCollection Metadata
        {
            get => _metadataCollection ??= new MetadataCollection();
            internal set => _metadataCollection = value;
        }

        /// <summary>
        /// The tag-set for the object.
        /// </summary>
        public List<Tag>? TagSet { get; set; }

        /// <summary>
        /// The event for UploadProgressEvent notifications. All
        /// subscribers will be notified when a new progress
        /// event is raised.
        /// <para>
        /// The UploadProgressEvent is fired as data
        /// is uploaded to S3.  The delegates attached to the event 
        /// will be passed information detailing how much data
        /// has been uploaded as well as how much will be uploaded.
        /// </para>
        /// </summary>
        /// <remarks>
        /// Subscribe to this event if you want to receive
        /// UploadProgressEvent notifications. Here is how:<br />
        /// 1. Define a method with a signature similar to this one:
        /// <code>
        /// private void DisplayProgress(object sender, UploadProgressArgs args)
        /// {
        ///     Console.WriteLine(args);
        /// }
        /// </code>
        /// 2. Add this method to the UploadProgressEvent delegate's invocation list
        /// <code>
        /// TransferUtilityUploadRequest request = new TransferUtilityUploadRequest();
        /// request.UploadProgressEvent += displayProgress;
        /// </code>
        /// </remarks>
        public event EventHandler<UploadProgressArgs>? UploadProgressEvent;
        
        /// <summary>
        /// Causes the UploadProgressEvent event to be fired.
        /// </summary>
        /// <param name="progressArgs">Progress data for the file being uploaded.</param>        
        internal void OnRaiseProgressEvent(UploadProgressArgs progressArgs)
        {
            AWSSDKUtils.InvokeInBackground(UploadProgressEvent, progressArgs, this);
        }
        
        /// <summary>
        /// Gets the length of the content by either checking the FileInfo.Length property or the Stream.Length property.
        /// </summary>
        /// <value>The length of the content.</value>
        internal ulong? ContentLength
        {
            get
            {
                ulong? length;
                try
                {
                    if (IsSetFilePath())
                    {
                        //System.IO.
                        FileInfo fileInfo = new FileInfo(FilePath);
                        length = fileInfo.Length.ToUInt64();
                    }
                    else if (IsSetInputStream())
                    {
                        length = (InputStream.Length - InputStream.Position).ToUInt64();
                    }
                    else
                    {
                        throw new ArgumentException(
                            $"{nameof(FilePath)} or {nameof(InputStream)} must be set before calculating the {nameof(ContentLength)}");
                    }
                }
                catch (NotSupportedException)
                {
                    //length is unknown
                    length = null;
                }
                
                return length;
            }
        }

        /// <summary>
        /// 	Gets or sets whether the stream used with this request is
        /// 	automatically closed when all the content is read from the stream.         
        /// </summary>
        /// <value>
        /// 	A value of <c>true</c> if the stream is
        /// 	automatically closed when all of the content is read from the stream.
        /// 	A value of <c>false</c> if otherwise. 		
        /// </value>
        public bool AutoCloseStream { get; set; } = true;

        /// <summary>
        /// If this value is set to true then the stream's position will be reset to the start before being read for upload.
        /// Default: true.
        /// </summary>
        public bool AutoResetStreamPosition { get; set; } = true;

        /// <summary>
        /// 	Sets whether the stream used with this request is
        /// 	automatically closed when all the content is read from the stream
        ///   	and returns this object instance, 
        /// 	enabling additional method calls to be chained together.
        /// </summary>
        /// <param name="autoCloseStream">
        /// 	A value of <c>true</c> if the stream is
        /// 	automatically closed when all the content is read from the stream.
        /// 	A value of <c>false</c> if otherwise. 	
        /// </param>
        /// <returns>
        /// 	This object instance, enabling additional method calls to be chained together.
        /// </returns>
        public UploadRequest WithAutoCloseStream(bool autoCloseStream)
        {
            AutoCloseStream = autoCloseStream;
            return this;
        }

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
        public bool? DisableDefaultChecksumValidation { get; set; }

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
        public bool? DisablePayloadSigning { get; set; }

        /// <summary>
        /// Gets or sets whether the Content-MD5 header should be calculated for upload.
        /// </summary>
        public bool CalculateContentMd5Header { get; set; }

        /// <summary>
        /// Gets and sets the property ObjectLockLegalHoldStatus. 
        /// <para>
        /// Specifies whether a legal hold will be applied to this object. For more information
        /// about S3 Object Lock, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object
        /// Lock</a>.
        /// </para>
        /// </summary>
        public ObjectLockLegalHoldStatus? ObjectLockLegalHoldStatus { get; set; }

        /// <summary>
        /// Gets and sets the property ObjectLockMode. 
        /// <para>
        /// The Object Lock mode that you want to apply to this object.
        /// </para>
        /// </summary>
        public ObjectLockMode? ObjectLockMode { get; set; }

        /// <summary>
        /// Gets and sets the property ObjectLockRetainUntilDate. 
        /// <para>
        /// The date and time when you want this object's Object Lock to expire.
        /// </para>
        /// </summary>
        public DateTimeOffset ObjectLockRetainUntilDate
        {
            get => _objectLockRetainUntilDate.GetValueOrDefault();
            set => _objectLockRetainUntilDate = value;
        }

        // Check to see if ObjectLockRetainUntilDate property is set
        [MemberNotNullWhen(true, nameof(ObjectLockRetainUntilDate))]
        [MemberNotNullWhen(true, nameof(_objectLockRetainUntilDate))]
        internal bool IsSetObjectLockRetainUntilDate() => _objectLockRetainUntilDate.HasValue;

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
        public ChecksumAlgorithm? ChecksumAlgorithm { get; set; }
        
        internal virtual string DebuggerDisplay => ToString() ?? GetType().Name;
    }
}
