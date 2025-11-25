using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
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
    }
}
