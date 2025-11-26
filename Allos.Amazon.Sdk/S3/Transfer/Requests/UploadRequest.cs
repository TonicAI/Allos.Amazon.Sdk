using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
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
        protected ulong? _mpuObjectSize;

        /// <summary>
        /// 	Gets or sets the key under which the Amazon S3 object is to be stored.
        /// </summary>
        /// <value>
        /// 	The key under which the Amazon S3 object is to be stored. 
        /// </value>
        [MemberNotNullWhen(true, nameof(Key))]
        public virtual string? Key { get; set; }

        /// <summary>
        /// Checks if Key property is set.
        /// </summary>
        /// <returns>true if Key property is set.</returns>
        [MemberNotNullWhen(true, nameof(Key))]
        internal virtual bool IsSetKey() => !string.IsNullOrWhiteSpace(Key);

        /// <summary>
        /// Input stream for the request; content for the request will be read from the stream.
        /// </summary>
        public virtual Stream? InputStream { get; set; }

        // Check to see if InputStream property is set
        [MemberNotNullWhen(true, nameof(InputStream))]
        internal virtual bool IsSetInputStream() => InputStream != null;

        /// <summary>
        /// <para>
        /// 	Gets or sets the file path
        /// 	where the Amazon S3 object will be uploaded from.
        /// </para>
        /// </summary>
        /// <value>
        /// 	The file path where the Amazon S3 object will be uploaded from.
        /// </value>
        public virtual string? FilePath { get; set; }

        /// <summary>
        /// Checks if FilePath property is set.
        /// </summary>
        /// <returns>true if FilePath property is set.</returns>
        [MemberNotNullWhen(true, nameof(FilePath))]
        internal virtual bool IsSetFilePath() => !string.IsNullOrWhiteSpace(FilePath);

        /// <summary>
        /// 	Gets or sets the part size of the upload in bytes.
        /// 	The uploaded file will be divided into 
        /// 	parts the size specified and
        /// 	uploaded to Amazon S3 individually.
        /// </summary>
        /// <value>
        /// 	The part size of the upload.
        /// </value>
        public virtual ulong PartSize
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
        internal virtual bool IsSetPartSize() => _partSize.HasValue;

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
        internal virtual ulong? ContentLength
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
        public virtual bool AutoCloseStream { get; set; } = true;

        /// <summary>
        /// If this value is set to true then the stream's position will be reset to the start before being read for upload.
        /// Default: true.
        /// </summary>
        public virtual bool AutoResetStreamPosition { get; set; } = true;

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
        public virtual UploadRequest WithAutoCloseStream(bool autoCloseStream)
        {
            AutoCloseStream = autoCloseStream;
            return this;
        }
        
        /// <summary>
        /// Gets and sets the property ChecksumCRC32. 
        /// <para>
        /// This specifies the Base64 encoded, 32-bit <c>CRC-32C</c> checksum of the object. 
        /// For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a>
        /// in the <i>Amazon S3 User Guide</i>.
        /// </para>
        /// </summary>
        public virtual string? ChecksumCRC32 { get; set; }

        /// <summary>
        /// Checks if ChecksumCRC32 property is set.
        /// </summary>
        /// <returns>true if ChecksumCRC32 property is set.</returns>
        [MemberNotNullWhen(true, nameof(ChecksumCRC32))]
        internal virtual bool IsSetChecksumCRC32()
        {
            return !string.IsNullOrEmpty(ChecksumCRC32);
        }

        /// <summary>
        /// Gets and sets the property ChecksumCRC32C. 
        /// <para>
        /// This specifies the Base64 encoded, 32-bit <c>CRC-32C</c> checksum of the object. 
        /// For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a>
        /// in the <i>Amazon S3 User Guide</i>.
        /// </para>
        /// </summary>
        public virtual string? ChecksumCRC32C { get; set; }

        /// <summary>
        /// Checks if ChecksumCRC32C property is set.
        /// </summary>
        /// <returns>true if ChecksumCRC32C property is set.</returns>
        [MemberNotNullWhen(true, nameof(ChecksumCRC32C))]
        internal virtual bool IsSetChecksumCRC32C()
        {
            return !string.IsNullOrEmpty(ChecksumCRC32C);
        }

        /// <summary>
        /// Gets and sets the property ChecksumCRC64NVME. 
        /// <para>
        /// This specifies the Base64 encoded, 64-bit <c>CRC-64NVME</c> checksum of the object. 
        /// For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a>
        /// in the <i>Amazon S3 User Guide</i>.
        /// </para>
        /// </summary>
        public virtual string? ChecksumCRC64NVME { get; set; }

        /// <summary>
        /// Checks if ChecksumCRC64NVME property is set.
        /// </summary>
        /// <returns>true if ChecksumCRC64NVME property is set.</returns>
        [MemberNotNullWhen(true, nameof(ChecksumCRC64NVME))]
        internal virtual bool IsSetChecksumCRC64NVME()
        {
            return !string.IsNullOrEmpty(ChecksumCRC64NVME);
        }

        /// <summary>
        /// Gets and sets the property ChecksumSHA1. 
        /// <para>
        /// This specifies the Base64 encoded, 160-bit <c>SHA-1</c> digest of the object. 
        /// For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a>
        /// in the <i>Amazon S3 User Guide</i>.
        /// </para>
        /// </summary>
        public virtual string? ChecksumSHA1 { get; set; }

        /// <summary>
        /// Checks if ChecksumSHA1 property is set.
        /// </summary>
        /// <returns>true if ChecksumSHA1 property is set.</returns>
        [MemberNotNullWhen(true, nameof(ChecksumSHA1))]
        internal virtual bool IsSetChecksumSHA1()
        {
            return !string.IsNullOrEmpty(ChecksumSHA1);
        }

        /// <summary>
        /// Gets and sets the property ChecksumSHA256. 
        /// <para>
        /// This specifies the Base64 encoded, 256-bit <c>SHA-256</c> digest of the object. 
        /// For more information, see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking object integrity</a>
        /// in the <i>Amazon S3 User Guide</i>.
        /// </para>
        /// </summary>
        public virtual string? ChecksumSHA256 { get; set; }

        /// <summary>
        /// Checks if ChecksumSHA256 property is set.
        /// </summary>
        /// <returns>true if ChecksumSHA256 property is set.</returns>
        [MemberNotNullWhen(true, nameof(ChecksumSHA256))]
        internal virtual bool IsSetChecksumSHA256()
        {
            return !string.IsNullOrEmpty(ChecksumSHA256);
        }

        /// <summary>
        /// Gets and sets the property IfNoneMatch used when CompleteMultipartUploadRequest is called to 
        /// complete the multipart upload.
        /// <para>Uploads the object only if the object key name does not already exist in the bucket specified. Otherwise, 
        /// Amazon S3 returns a <c>412 Precondition Failed</c> error.</para> <para>If a conflicting operation occurs 
        /// during the upload S3 returns a <c>409 ConditionalRequestConflict</c> response. On a 409 failure you should 
        /// re-initiate the multipart upload with <c>CreateMultipartUpload</c> and re-upload each part.</para> <para>Expects 
        /// the '*' (asterisk) character.</para> <para>For more information about conditional requests, 
        /// see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>, or <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html">Conditional requests</a> 
        /// in the <i>Amazon S3 User Guide</i>.</para>
        /// </summary>
        public virtual string? IfNoneMatch { get; set; }

        /// <summary>
        /// Checks to see if IfNoneMatch is set.
        /// </summary>
        /// <returns>true, if IfNoneMatch property is set.</returns>
        internal virtual bool IsSetIfNoneMatch()
        {
            return !string.IsNullOrEmpty(IfNoneMatch);
        }

        /// <summary>
        /// Gets and sets the property IfMatch used when CompleteMultipartUploadRequest is called to 
        /// complete the multipart upload.
        /// <para>Uploads the object only if the ETag (entity tag) value provided during the WRITE operation matches the ETag of the object in S3. If the ETag values do not match, the operation returns a <c>412 Precondition Failed</c> error.</para>
        /// <para>If a conflicting operation occurs during the upload S3 returns a <c>409 ConditionalRequestConflict</c> response. On a 409 failure you should fetch the object's ETag and retry the upload.</para>
        /// <para>Expects the ETag value as a string.</para>
        /// <para>For more information about conditional requests, see <a href="https://tools.ietf.org/html/rfc7232">RFC 7232</a>, or <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html">Conditional requests</a> in the <i>Amazon S3 User Guide</i>.</para>
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
        /// Gets and sets the property MpuObjectSize. 
        /// <para>
        /// The expected total object size of the multipart upload request.
        /// If there's a mismatch between the specified object size value and the actual
        /// object size value, it results in an <c>HTTP 400 InvalidRequest</c> error.
        /// </para>
        /// </summary>
        public virtual ulong MpuObjectSize
        {
            get => _mpuObjectSize.GetValueOrDefault();
            set => _mpuObjectSize = value;
        }

        /// <summary>
        /// Checks if MpuObjectSize property is set.
        /// </summary>
        /// <returns>true if MpuObjectSize property is set.</returns>
        [MemberNotNullWhen(true, nameof(MpuObjectSize))]
        internal virtual bool IsSetMpuObjectSize()
        {
            return _mpuObjectSize.HasValue;
        }
        
        internal virtual bool IsMultipartUpload(IAsyncTransferConfig config)
        {
            if (ContentLength.HasValue)
            {
                return ContentLength.Value >= config.MinSizeBeforePartUpload;
            }
            //If the length is null that means when we tried to get the ContentLength, we caught a NotSupportedException,
            //or it means the length is unknown. In this case we do a MultiPartUpload. If we are uploading
            //a nonseekable stream and the ContentLength is more than zero, we also do a multipart upload.
            return true;
        }
    }
}
