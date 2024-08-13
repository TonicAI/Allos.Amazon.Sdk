using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;

namespace Allos.Amazon.Sdk.S3.Transfer;

/// <summary>
/// Encapsulates the information needed to provide
/// transfer progress to subscribers of the <c>UploadDirectory</c>
/// event.
/// </summary>
[SuppressMessage("ReSharper", "ClassWithVirtualMembersNeverInherited.Global")]
[SuppressMessage("ReSharper", "UnusedMember.Global")]
[SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
[DebuggerDisplay("{DebuggerDisplay}")]
[AmazonSdkFork("sdk/src/Services/S3/Custom/Transfer/TransferUtilityUploadDirectoryRequest.cs", "Amazon.S3.Transfer")]
public class UploadDirectoryProgressArgs : EventArgs
{
    /// <summary>
    /// Constructs a new instance of <c>UploadDirectoryProgressArgs</c>.
    /// </summary>
    /// <param name="numberOfFilesUploaded">
    /// The number of files uploaded.
    /// </param>
    /// <param name="totalNumberOfFiles">
    /// The total number of files to upload.
    /// </param>
    /// <param name="currentFile">
    /// The current file 
    /// </param>
    /// <param name="transferredBytesForCurrentFile">
    /// The number of transferred bytes for current file.
    /// </param>
    /// <param name="totalNumberOfBytesForCurrentFile">
    /// The size of the current file in bytes.
    /// </param>
    public UploadDirectoryProgressArgs(
        uint numberOfFilesUploaded, 
        uint totalNumberOfFiles, 
        string? currentFile, 
        ulong transferredBytesForCurrentFile, 
        ulong totalNumberOfBytesForCurrentFile)
    {
        NumberOfFilesUploaded = numberOfFilesUploaded;
        TotalNumberOfFiles = totalNumberOfFiles;
        CurrentFile = currentFile;
        TransferredBytesForCurrentFile = transferredBytesForCurrentFile;
        TotalNumberOfBytesForCurrentFile = totalNumberOfBytesForCurrentFile;
    }

    /// <summary>
    /// Constructs a new instance of <c>UploadDirectoryProgressArgs</c>.
    /// </summary>
    /// <param name="numberOfFilesUploaded">
    /// The number of files uploaded.
    /// </param>
    /// <param name="totalNumberOfFiles">
    /// The total number of files to upload.
    /// </param>
    /// <param name="transferredBytes">
    /// The bytes transferred across all files being uploaded.
    /// </param>
    /// <param name="totalBytes">
    /// The total number of bytes across all files being uploaded, if available.
    /// </param>
    /// <param name="currentFile">
    /// The current file being uploaded.
    /// </param>
    /// <param name="transferredBytesForCurrentFile">
    /// The number of transferred bytes for current file.
    /// </param>
    /// <param name="totalNumberOfBytesForCurrentFile">
    /// The size of the current file in bytes, if available.
    /// </param>
    public UploadDirectoryProgressArgs(
        uint numberOfFilesUploaded, 
        uint totalNumberOfFiles, 
        ulong transferredBytes, 
        ulong? totalBytes,
        string? currentFile, 
        ulong transferredBytesForCurrentFile, 
        ulong? totalNumberOfBytesForCurrentFile)
    {
        NumberOfFilesUploaded = numberOfFilesUploaded;
        TotalNumberOfFiles = totalNumberOfFiles;
        TransferredBytes = transferredBytes;
        TotalBytes = totalBytes;
        CurrentFile = currentFile;
        TransferredBytesForCurrentFile = transferredBytesForCurrentFile;
        TotalNumberOfBytesForCurrentFile = totalNumberOfBytesForCurrentFile;
    }

    /// <summary>
    /// Gets or sets the total number of files.
    /// </summary>
    /// <value>The total number of files.</value>
    public uint TotalNumberOfFiles { get; set; }

    /// <summary>
    /// Gets or sets the number of files uploaded.
    /// </summary>
    /// <value>The number of files uploaded.</value>
    public uint NumberOfFilesUploaded { get; set; }

    /// <summary>
    /// Gets or sets the total number of bytes across all files being uploaded, if available.
    /// </summary>
    /// <value>The total number of bytes across all files being uploaded, if available.</value>
    public ulong? TotalBytes { get; set; }

    /// <summary>
    /// Gets or sets the bytes transferred across all files being uploaded.
    /// </summary>
    /// <value>The bytes transferred across all files being uploaded.</value>
    public ulong TransferredBytes { get; set; }

    /// <summary>
    /// Gets or sets the current file.
    /// </summary>
    /// <remarks>
    /// This property is only valid if UploadDirectory is used without enabling concurrent file uploads (by default concurrent upload is disabled).
    /// If concurrent file uploads are enabled by setting TransferUtilityUploadDirectoryRequest.UploadFilesConcurrently to true, this property
    /// will return null.
    /// </remarks>
    /// <value>The current file.</value>
    public string? CurrentFile { get; set; }
        
    /// <summary>
    /// Gets or sets the transferred bytes for current file.
    /// </summary>
    /// <remarks>
    /// This property is only valid if UploadDirectory is used without enabling concurrent file uploads (by default concurrent upload is disabled).
    /// If concurrent file uploads are enabled by setting TransferUtilityUploadDirectoryRequest.UploadFilesConcurrently to true, this property
    /// will return 0.
    /// </remarks>
    /// <value>The transferred bytes for current file.</value>
    public ulong TransferredBytesForCurrentFile { get; set; }

    /// <summary>
    /// Gets or sets the total number of bytes for current file.
    /// </summary>
    /// <remarks>
    /// This property is only valid if UploadDirectory is used without enabling concurrent file uploads (by default concurrent upload is disabled).
    /// If concurrent file uploads are enabled by setting TransferUtilityUploadDirectoryRequest.UploadFilesConcurrently to true, this property
    /// will return 0.
    /// </remarks>
    /// <value>The total number of bytes for current file, if available.</value>
    public ulong? TotalNumberOfBytesForCurrentFile { get; set; }
    
    /// <summary>
    /// The string representation of this instance of UploadDirectoryProgressArgs.
    /// </summary>
    /// <returns>The string representation of this instance of UploadDirectoryProgressArgs.</returns>
    public override string ToString()
    {
        if (TotalBytes.HasValue)
        {
            return $"Uploaded {NumberOfFilesUploaded} of {TotalNumberOfFiles}, {TransferredBytes} bytes transferred of {TotalBytes} total bytes";
        }
        return $"Uploaded {NumberOfFilesUploaded} of {TotalNumberOfFiles}, {TransferredBytes} bytes transferred";
    }
    
    internal virtual string DebuggerDisplay => ToString();
}