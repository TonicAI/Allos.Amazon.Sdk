using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.S3.Model;
using Amazon.Util;

namespace Allos.Amazon.Sdk.S3.Transfer
{
    /// <summary>
    /// Contains all the parameters
    /// that can be set when making a request with the 
    /// <see cref="AsyncTransferUtility"/> method.
    /// </summary>
    [SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityDownloadRequest.cs", "Amazon.S3.Transfer")]
    public class DownloadRequest : BaseDownloadRequest
    {
        /// <summary>
        /// 	Get or sets the file path location of where the
        /// 	downloaded Amazon S3 object will be written to.
        /// </summary>
        /// <value>
        /// 	The file path location of where the downloaded Amazon S3 object will be written to.
        /// </value>
        public virtual string? FilePath { get; set; }

        /// <summary>
        /// Checks if FilePath property is set.
        /// </summary>
        /// <returns>True if FilePath property is set.</returns>
        [MemberNotNullWhen(true, nameof(FilePath))]
        internal virtual bool IsSetFilePath() => !string.IsNullOrWhiteSpace(FilePath);

        /// <summary>
        /// The event for WriteObjectProgressEvent notifications. All
        /// subscribers will be notified when a new progress
        /// event is raised.
        /// <para>
        /// The WriteObjectProgressEvent is fired as data
        /// is downloaded from S3.  The delegates attached to the event 
        /// will be passed information detailing how much data
        /// has been downloaded as well as how much will be downloaded.
        /// </para>
        /// </summary>
        /// <remarks>
        /// Subscribe to this event if you want to receive
        /// WriteObjectProgressEvent notifications. Here is how:<br />
        /// 1. Define a method with a signature similar to this one:
        /// <code>
        /// private void DisplayProgress(object sender, WriteObjectProgressArgs args)
        /// {
        ///     Console.WriteLine(args);
        /// }
        /// </code>
        /// 2. Add this method to the WriteObjectProgressEvent delegate's invocation list
        /// <code>
        /// TransferUtilityDownloadRequest request = new TransferUtilityDownloadRequest();
        /// request.WriteObjectProgressEvent += displayProgress;
        /// </code>
        /// </remarks>
        public event EventHandler<WriteObjectProgressArgs>? WriteObjectProgressEvent;

        /// <summary>
        /// Causes the WriteObjectProgressEvent event to be fired.
        /// </summary>
        /// <param name="progressArgs">Progress data for the stream being written to file.</param>        
        internal virtual void OnRaiseProgressEvent(WriteObjectProgressArgs progressArgs)
        {
            AWSSDKUtils.InvokeInBackground(WriteObjectProgressEvent, progressArgs, this);
        }
    }
}
