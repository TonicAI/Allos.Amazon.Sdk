using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.Util;

namespace Allos.Amazon.Sdk.S3.Transfer
{
    /// <summary>
    /// Request object for downloading a directory with the TransferUtility.
    /// </summary>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [SuppressMessage("ReSharper", "PropertyCanBeMadeInitOnly.Global")]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
    [DebuggerDisplay("{DebuggerDisplay}")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityDownloadDirectoryRequest.cs", "Amazon.S3.Transfer")]
    public class DownloadDirectoryRequest
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
        ///    Otherwise, returns <c>false</c>.
        /// </returns>
        [MemberNotNullWhen(true, nameof(BucketName))]
        internal virtual bool IsSetBucketName() => !string.IsNullOrWhiteSpace(BucketName);
        
        /// <summary>
        /// 	Gets or sets the local directory where objects from Amazon S3 will be downloaded.  
		/// 	If the directory doesn't exist, it will be created.
        /// </summary>
        /// <value>
        /// 	The local directory where objects from Amazon S3 will be downloaded.
        /// </value>
        public virtual string? LocalDirectory { get; set; }

        /// <summary>
        /// 	Gets whether the LocalDirectory property is set.
        /// </summary>
        /// <returns>
        /// 	A value of <c>true</c> if LocalDirectory property is set.
        /// 	Otherwise, returns <c>false</c>.
        /// </returns>
        [MemberNotNullWhen(true, nameof(LocalDirectory))]
        internal virtual bool IsSetLocalDirectory() => !string.IsNullOrWhiteSpace(LocalDirectory);

        /// <summary>
        /// Gets or sets the Amazon S3 directory to download from.  
        /// This is translated to a key prefix; keys that have this prefix will be
        /// downloaded.
        /// 
        /// The TransferUtility will automatically add a / to the end when listing objects for 
        /// to be downloaded. This treats S3Directory field as a virtual S3 directory. In some use
        /// cases the added / slash can be undesirable. To prevent the TransferUtility from adding 
        /// the / at the end set the DisableSlashCorrection property to true.
        /// </summary>
        public virtual string? S3Directory { get; set; }
        
        /// <summary>
        /// 	Gets whether the S3Directory property is set.
        /// </summary>
        /// <returns>
        /// 	A value of <c>true</c> if S3Directory property is set.
        /// 	Otherwise, returns <c>false</c>.
        /// </returns>
        [MemberNotNullWhen(true, nameof(S3Directory))]
        internal virtual bool IsSetS3Directory() => !string.IsNullOrWhiteSpace(S3Directory);

        /// <summary>
        /// 	Gets or sets the <c>ModifiedSinceDateUtc</c> property.  
        /// 	Only objects that have been modified since this date will be
        /// 	downloaded.
        /// </summary>
        /// <value>
        /// 	The <c>ModifiedSinceDateUtc</c> property. 
        /// </value>
        public virtual DateTimeOffset ModifiedSinceDateUtc
        {
            get => _modifiedSinceDateUtc.GetValueOrDefault();
            set => _modifiedSinceDateUtc = value;
        }

        /// <summary>
        /// Checks if ModifiedSinceDateUtc property is set.
        /// </summary>
        /// <returns>A value of <c>true</c> if ModifiedSinceDateUtc property is set.
        /// 	Otherwise, returns <c>false</c>.</returns>
        [MemberNotNullWhen(true, nameof(ModifiedSinceDateUtc))]
        [MemberNotNullWhen(true, nameof(_modifiedSinceDateUtc))]
        internal virtual bool IsSetModifiedSinceDateUtc() => _modifiedSinceDateUtc.HasValue;

        /// <summary>
        /// 	Gets or sets the <c>UnmodifiedSinceDateUtc</c> property.  
        /// 	Only objects that have not been modified since this date will be downloaded.
        /// </summary>
        /// <value>
        /// 	The <c>UnmodifiedSinceDateUtc</c> property.
        /// </value>
        public virtual DateTimeOffset UnmodifiedSinceDateUtc
        {
            get => _unmodifiedSinceDateUtc.GetValueOrDefault();
            set => _unmodifiedSinceDateUtc = value;
        }

        /// <summary>
        /// Checks if UnmodifiedSinceDateUtc property is set.
        /// </summary>
        /// <returns>true if UnmodifiedSinceDateUtc property is set.</returns>
        [MemberNotNullWhen(true, nameof(UnmodifiedSinceDateUtc))]
        [MemberNotNullWhen(true, nameof(_unmodifiedSinceDateUtc))]
        internal virtual bool IsSetUnmodifiedSinceDateUtc() => _unmodifiedSinceDateUtc.HasValue;

        /// <summary>
        /// Gets or sets the DownloadFilesConcurrently property.
        /// Specifies if multiple files will be downloaded concurrently.
        /// The number of concurrent web requests used is controlled 
        /// by the TransferUtilityConfig.ConcurrencyLevel property.
        /// </summary>
        public virtual bool DownloadFilesConcurrently { get; set; }

        /// <summary>
        /// If this is set to true then the TransferUtility will not ensure the S3Directory property has a trailing / for a virtual S3 directory. 
        /// The default value is false.
        /// </summary>
        public virtual bool DisableSlashCorrection { get; set; }

        /// <summary>
        /// The Server-side encryption algorithm to be used with the customer provided key.
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
        [AWSProperty(Sensitive = true)]
        public virtual string? ServerSideEncryptionCustomerProvidedKey { get; set; }

        /// <summary>
        /// The MD5 of the customer encryption key specified in the ServerSideEncryptionCustomerProvidedKey property. The MD5 is
        /// base 64 encoded. This field is optional, the SDK will calculate the MD5 if this is not set.
        /// </summary>
        public virtual string? ServerSideEncryptionCustomerProvidedKeyMd5 { get; set; }

        /// <summary>
        /// The event for DownloadedDirectoryProgressEvent notifications. All
        /// subscribers will be notified when a new progress
        /// event is raised.
        /// <para>
        /// The DownloadedDirectoryProgressEvent is fired as data
        /// is downloaded from Amazon S3.  The delegates attached to the event 
        /// will be passed information detailing how much data
        /// has been downloaded as well as how much will be downloaded.
        /// </para>
        /// </summary>
        /// <remarks>
        /// Subscribe to this event if you want to receive
        /// DownloadedDirectoryProgressEvent notifications. Here is how:<br />
        /// 1. Define a method with a signature similar to this one:
        /// <code>
        /// private void DisplayProgress(object sender, DownloadDirectoryProgressArgs args)
        /// {
        ///     Console.WriteLine(args);
        /// }
        /// </code>
        /// 2. Add this method to the DownloadedDirectoryProgressEvent delegate's invocation list
        /// <code>
        /// TransferUtilityDownloadDirectoryRequest request = new TransferUtilityDownloadDirectoryRequest();
        /// request.DownloadedDirectoryProgressEvent += displayProgress;
        /// </code>
        /// </remarks>
        public event EventHandler<DownloadDirectoryProgressArgs>? DownloadedDirectoryProgressEvent;

        internal virtual void OnRaiseProgressEvent(DownloadDirectoryProgressArgs downloadDirectoryProgress)
        {
            AWSSDKUtils.InvokeInBackground(DownloadedDirectoryProgressEvent, downloadDirectoryProgress, this);
        }
        
        internal virtual string DebuggerDisplay => ToString() ?? GetType().Name;
    }
}
