using System.Diagnostics.CodeAnalysis;
using System.Net;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Sdk.Fork;

namespace Amazon.Sdk.S3.Util
{
    /// <summary>
    /// Provides utilities used by the Amazon S3 client implementation.
    /// These utilities might be useful to consumers of the Amazon S3
    /// library.
    /// </summary>
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Util/AmazonS3Util.cs", "Amazon.S3.Util")]
    [AmazonSdkFork("sdk/src/Services/S3/Custom/Util/_async/AmazonS3Util.Operations.cs", "Amazon.S3.Util")]
    public static class AmazonS3Util
    {
        private static readonly Dictionary<string, string> _extensionToMime = new(200, StringComparer.OrdinalIgnoreCase)
        {
            { ".ai", "application/postscript" },
            { ".aif", "audio/x-aiff" },
            { ".aifc", "audio/x-aiff" },
            { ".aiff", "audio/x-aiff" },
            { ".asc", "text/plain" },
            { ".au", "audio/basic" },
            { ".avi", "video/x-msvideo" },
            { ".bcpio", "application/x-bcpio" },
            { ".bin", "application/octet-stream" },
            { ".c", "text/plain" },
            { ".cc", "text/plain" },
            { ".ccad", "application/clariscad" },
            { ".cdf", "application/x-netcdf" },
            { ".class", "application/octet-stream" },
            { ".cpio", "application/x-cpio" },
            { ".cpp", "text/plain" },
            { ".cpt", "application/mac-compactpro" },
            { ".cs", "text/plain" },
            { ".csh", "application/x-csh" },
            { ".css", "text/css" },
            { ".csv", "text/csv" },
            { ".dcr", "application/x-director" },
            { ".dir", "application/x-director" },
            { ".dms", "application/octet-stream" },
            { ".doc", "application/msword" },
            { ".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document" },
            { ".dot", "application/msword" },
            { ".drw", "application/drafting" },
            { ".dvi", "application/x-dvi" },
            { ".dwg", "application/acad" },
            { ".dxf", "application/dxf" },
            { ".dxr", "application/x-director" },
            { ".eps", "application/postscript" },
            { ".etx", "text/x-setext" },
            { ".exe", "application/octet-stream" },
            { ".ez", "application/andrew-inset" },
            { ".f", "text/plain" },
            { ".f90", "text/plain" },
            { ".fli", "video/x-fli" },
            { ".gif", "image/gif" },
            { ".gtar", "application/x-gtar" },
            { ".gz", "application/x-gzip" },
            { ".h", "text/plain" },
            { ".hdf", "application/x-hdf" },
            { ".hh", "text/plain" },
            { ".hqx", "application/mac-binhex40" },
            { ".htm", "text/html" },
            { ".html", "text/html" },
            { ".ice", "x-conference/x-cooltalk" },
            { ".ief", "image/ief" },
            { ".iges", "model/iges" },
            { ".igs", "model/iges" },
            { ".ips", "application/x-ipscript" },
            { ".ipx", "application/x-ipix" },
            { ".jpe", "image/jpeg" },
            { ".jpeg", "image/jpeg" },
            { ".jpg", "image/jpeg" },
            { ".js", "application/x-javascript" },
            { ".json", "application/json" },
            { ".kar", "audio/midi" },
            { ".latex", "application/x-latex" },
            { ".lha", "application/octet-stream" },
            { ".lsp", "application/x-lisp" },
            { ".lzh", "application/octet-stream" },
            { ".m", "text/plain" },
            { ".m3u8", "application/x-mpegURL" },
            { ".man", "application/x-troff-man" },
            { ".me", "application/x-troff-me" },
            { ".mesh", "model/mesh" },
            { ".mid", "audio/midi" },
            { ".midi", "audio/midi" },
            { ".mime", "www/mime" },
            { ".mov", "video/quicktime" },
            { ".movie", "video/x-sgi-movie" },
            { ".mp2", "audio/mpeg" },
            { ".mp3", "audio/mpeg" },
            { ".mpe", "video/mpeg" },
            { ".mpeg", "video/mpeg" },
            { ".mpg", "video/mpeg" },
            { ".mpga", "audio/mpeg" },
            { ".ms", "application/x-troff-ms" },
            { ".msi", "application/x-ole-storage" },
            { ".msh", "model/mesh" },
            { ".nc", "application/x-netcdf" },
            { ".oda", "application/oda" },
            { ".pbm", "image/x-portable-bitmap" },
            { ".pdb", "chemical/x-pdb" },
            { ".pdf", "application/pdf" },
            { ".pgm", "image/x-portable-graymap" },
            { ".pgn", "application/x-chess-pgn" },
            { ".png", "image/png" },
            { ".pnm", "image/x-portable-anymap" },
            { ".pot", "application/mspowerpoint" },
            { ".ppm", "image/x-portable-pixmap" },
            { ".pps", "application/mspowerpoint" },
            { ".ppt", "application/mspowerpoint" },
            { ".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation" },
            { ".ppz", "application/mspowerpoint" },
            { ".pre", "application/x-freelance" },
            { ".prt", "application/pro_eng" },
            { ".ps", "application/postscript" },
            { ".qt", "video/quicktime" },
            { ".ra", "audio/x-realaudio" },
            { ".ram", "audio/x-pn-realaudio" },
            { ".ras", "image/cmu-raster" },
            { ".rgb", "image/x-rgb" },
            { ".rm", "audio/x-pn-realaudio" },
            { ".roff", "application/x-troff" },
            { ".rpm", "audio/x-pn-realaudio-plugin" },
            { ".rtf", "text/rtf" },
            { ".rtx", "text/richtext" },
            { ".scm", "application/x-lotusscreencam" },
            { ".set", "application/set" },
            { ".sgm", "text/sgml" },
            { ".sgml", "text/sgml" },
            { ".sh", "application/x-sh" },
            { ".shar", "application/x-shar" },
            { ".silo", "model/mesh" },
            { ".sit", "application/x-stuffit" },
            { ".skd", "application/x-koan" },
            { ".skm", "application/x-koan" },
            { ".skp", "application/x-koan" },
            { ".skt", "application/x-koan" },
            { ".smi", "application/smil" },
            { ".smil", "application/smil" },
            { ".snd", "audio/basic" },
            { ".sol", "application/solids" },
            { ".spl", "application/x-futuresplash" },
            { ".src", "application/x-wais-source" },
            { ".step", "application/STEP" },
            { ".stl", "application/SLA" },
            { ".stp", "application/STEP" },
            { ".sv4cpio", "application/x-sv4cpio" },
            { ".sv4crc", "application/x-sv4crc" },
            { ".svg", "image/svg+xml" },
            { ".swf", "application/x-shockwave-flash" },
            { ".t", "application/x-troff" },
            { ".tar", "application/x-tar" },
            { ".tcl", "application/x-tcl" },
            { ".tex", "application/x-tex" },
            { ".tif", "image/tiff" },
            { ".tiff", "image/tiff" },
            { ".tr", "application/x-troff" },
            { ".ts", "video/MP2T" },
            { ".tsi", "audio/TSP-audio" },
            { ".tsp", "application/dsptype" },
            { ".tsv", "text/tab-separated-values" },
            { ".txt", "text/plain" },
            { ".unv", "application/i-deas" },
            { ".ustar", "application/x-ustar" },
            { ".vcd", "application/x-cdlink" },
            { ".vda", "application/vda" },
            { ".vrml", "model/vrml" },
            { ".wav", "audio/x-wav" },
            { ".wrl", "model/vrml" },
            { ".xbm", "image/x-xbitmap" },
            { ".xlc", "application/vnd.ms-excel" },
            { ".xll", "application/vnd.ms-excel" },
            { ".xlm", "application/vnd.ms-excel" },
            { ".xls", "application/vnd.ms-excel" },
            { ".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" },
            { ".xlw", "application/vnd.ms-excel" },
            { ".xml", "text/xml" },
            { ".xpm", "image/x-xpixmap" },
            { ".xwd", "image/x-xwindowdump" },
            { ".xyz", "chemical/x-pdb" },
            { ".zip", "application/zip" },
            { ".m4v", "video/x-m4v" },
            { ".webm", "video/webm" },
            { ".ogv", "video/ogv" },
            { ".xap", "application/x-silverlight-app" },
            { ".mp4", "video/mp4" },
            { ".wmv", "video/x-ms-wmv" }
        };

        /// <summary>
        /// Determines MIME type from a file extension
        /// </summary>
        /// <param name="ext">The extension of the file</param>
        /// <returns>The MIME type for the extension, or text/plain</returns>
        public static string MimeTypeFromExtension(string ext)
        {
            if (_extensionToMime.TryGetValue(ext, out var extension))
            {
                return extension;
            }

            return "application/octet-stream";
        }

        internal static bool IsInstructionFile(string key)
        {
            return key.EndsWith(S3Constants.EncryptionInstructionfileSuffix, StringComparison.Ordinal) ||
                key.EndsWith(S3Constants.EncryptionInstructionfileSuffixV2, StringComparison.Ordinal);
        }
        
        /// <summary>
        /// Determines whether an S3 bucket exists or not.
        /// </summary>
        /// <param name="bucketName">The name of the bucket to check.</param>
        /// <param name="s3Client">The Amazon S3 Client to use for S3 specific operations.</param>
        /// <returns>False is returned in case S3 responds with a NoSuchBucket error.
        /// True is returned in case of success, AccessDenied error or PermanentRedirect error.
        /// An exception is thrown in case of any other error.</returns>
        /// <remarks>This method calls GetACL for the bucket.</remarks>
        public static bool DoesS3BucketExistV2(IAmazonS3 s3Client, string bucketName)
        {
            try
            {
                s3Client.GetACLAsync(bucketName).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch (AmazonS3Exception e)
            {
                switch (e.ErrorCode)
                {
                    // A redirect error or a forbidden error means the bucket exists.
                    case "AccessDenied":
                    case "PermanentRedirect":
                    case "UnknownOperationException":
                        return true;
                    case "NoSuchBucket":
                        return false;
                    default:
                        throw;
                }
            }
            return true;
        }
        
        /// <summary>
        /// Deletes an S3 bucket which contains objects.
        /// An S3 bucket which contains objects cannot be deleted until all the objects 
        /// in it are deleted. This method deletes all the objects in the specified 
        /// bucket and then deletes the bucket itself.
        /// </summary>
        /// <param name="bucketName">The bucket to be deleted.</param>
        /// <param name="s3Client">The Amazon S3 Client to use for S3 specific operations.</param>
        public static Task DeleteS3BucketWithObjectsAsync(IAmazonS3 s3Client, string bucketName)
        {
            var cancelSource = new CancellationTokenSource();
            return DeleteS3BucketWithObjectsAsync(s3Client, bucketName, cancelSource.Token);
        }

        /// <summary>
        /// Deletes an S3 bucket which contains objects.
        /// An S3 bucket which contains objects cannot be deleted until all the objects 
        /// in it are deleted. This method deletes all the objects in the specified 
        /// bucket and then deletes the bucket itself.
        /// </summary>
        /// <param name="bucketName">The bucket to be deleted.</param>
        /// <param name="s3Client">The Amazon S3 Client to use for S3 specific operations.</param>
        /// <param name="deleteOptions">Options to control the behavior of the delete operation.</param>
        public static Task DeleteS3BucketWithObjectsAsync(IAmazonS3 s3Client, string bucketName, S3DeleteBucketWithObjectsOptions deleteOptions)
        {
            var cancelSource = new CancellationTokenSource();
            return  DeleteS3BucketWithObjectsAsync(s3Client, bucketName, deleteOptions, cancelSource.Token);
        }

        /// <summary>
        /// Initiates the asynchronous execution of the DeleteS3BucketWithObjects operation. 
        /// DeleteS3BucketWithObjects deletes an S3 bucket which contains objects.
        /// An S3 bucket which contains objects cannot be deleted until all the objects 
        /// in it are deleted. This method deletes all the objects in the specified 
        /// bucket and then deletes the bucket itself.
        /// </summary>
        /// <param name="bucketName">The bucket to be deleted.</param>
        /// <param name="s3Client">The Amazon S3 Client to use for S3 specific operations.</param>
        /// <param name="token">token to check if the operation has been request to cancel.</param>
        /// <returns>An IAsyncCancelableResult that can be used to poll or wait for results, or both; 
        /// this value is also needed when invoking EndDeleteS3BucketWithObjects. IAsyncCancelableResult can also 
        /// be used to cancel the operation while it's in progress.</returns>
        public static Task DeleteS3BucketWithObjectsAsync(IAmazonS3 s3Client, string bucketName,
            CancellationToken token)
        {
            return DeleteS3BucketWithObjectsAsync(s3Client, bucketName,
                new()
                {
                    ContinueOnError = false,
                    QuietMode = true,
                },
                token);
        }

        /// <summary>
        /// Initiates the asynchronous execution of the DeleteS3BucketWithObjects operation. 
        /// DeleteS3BucketWithObjects deletes an S3 bucket which contains objects.
        /// An S3 bucket which contains objects cannot be deleted until all the objects 
        /// in it are deleted. This method deletes all the objects in the specified 
        /// bucket and then deletes the bucket itself.
        /// </summary>
        /// <param name="bucketName">The bucket to be deleted.</param>
        /// <param name="s3Client">The Amazon S3 Client to use for S3 specific operations.</param>
        /// <param name="deleteOptions">Options to control the behavior of the delete operation.</param>
        /// <param name="token">token to check if the operation has been request to cancel.</param>
        /// <returns>An IAsyncCancelableResult that can be used to poll or wait for results, or both; 
        /// this value is also needed when invoking EndDeleteS3BucketWithObjects. IAsyncCancelableResult can also 
        /// be used to cancel the operation while it's in progress.</returns>
        public static Task DeleteS3BucketWithObjectsAsync(IAmazonS3 s3Client, string bucketName,
             S3DeleteBucketWithObjectsOptions deleteOptions, CancellationToken token)
        {
            return DeleteS3BucketWithObjectsAsync(s3Client, bucketName, deleteOptions, null, token);
        }

        /// <summary>
        /// Initiates the asynchronous execution of the DeleteS3BucketWithObjects operation. 
        /// DeleteS3BucketWithObjects deletes an S3 bucket which contains objects.
        /// An S3 bucket which contains objects cannot be deleted until all the objects 
        /// in it are deleted. This method deletes all the objects in the specified 
        /// bucket and then deletes the bucket itself.
        /// </summary>
        /// <param name="bucketName">The bucket to be deleted.</param>
        /// <param name="s3Client">The Amazon S3 Client to use for S3 specific operations.</param>
        /// <param name="deleteOptions">>Options to control the behavior of the delete operation.</param>
        /// <param name="updateCallback">An callback that is invoked to send updates while delete operation is in progress.</param>
        /// <param name="token">token to check if the operation has been request to cancel.</param>
        /// <returns>An IAsyncCancelableResult that can be used to poll or wait for results, or both; 
        /// this value is also needed when invoking EndDeleteS3BucketWithObjects. IAsyncCancelableResult can also 
        /// be used to cancel the operation while it's in progress.</returns>
        public static Task DeleteS3BucketWithObjectsAsync(IAmazonS3 s3Client, string bucketName,
             S3DeleteBucketWithObjectsOptions deleteOptions, Action<S3DeleteBucketWithObjectsUpdate>? updateCallback, CancellationToken token)
        {
            var request = new S3DeleteBucketWithObjectsRequest
            {
                BucketName = bucketName,
                DeleteOptions = deleteOptions,
                UpdateCallback = updateCallback,
                S3Client = s3Client
            };
            return InvokeDeleteS3BucketWithObjects(request, token);
        }

        /// <summary>
        /// Invokes the DeleteS3BucketWithObjectsInternal method.
        /// </summary>
        /// <param name="state">The Request object that has all the data to complete the operation. </param>
        /// <param name="token">token to request the operation to be cancelled.</param>
        private static Task InvokeDeleteS3BucketWithObjects(object state, CancellationToken token)
        {
            var request = (S3DeleteBucketWithObjectsRequest)state;
            
            ArgumentNullException.ThrowIfNull(request.S3Client);
            if (!request.IsSetBucketName())
            {
                ArgumentNullException.ThrowIfNull(request.BucketName);
            }
            ArgumentNullException.ThrowIfNull(request.DeleteOptions);
            ArgumentNullException.ThrowIfNull(request.S3Client);
            
            return DeleteS3BucketWithObjectsInternalAsync(
                request.S3Client,
                request.BucketName,
                request.DeleteOptions,
                request.UpdateCallback,
                token
                );
        }

        /// <summary>
        /// Deletes an S3 bucket which contains objects.
        /// An S3 bucket which contains objects cannot be deleted until all the objects 
        /// in it are deleted. The function deletes all the objects in the specified 
        /// bucket and then deletes the bucket itself.
        /// </summary>
        /// <param name="bucketName">The bucket to be deleted.</param>
        /// <param name="s3Client">The Amazon S3 Client to use for S3 specific operations.</param>
        /// <param name="deleteOptions">Options to control the behavior of the delete operation.</param>
        /// <param name="updateCallback">The callback which is used to send updates about the delete operation.</param>
        /// <param name="token">token to check if the operation has been request to cancel.</param>
        private static async Task DeleteS3BucketWithObjectsInternalAsync(IAmazonS3 s3Client, string bucketName,
            S3DeleteBucketWithObjectsOptions deleteOptions, Action<S3DeleteBucketWithObjectsUpdate>? updateCallback,
            CancellationToken token)
        {
            // Validations.
            ArgumentNullException.ThrowIfNull(s3Client);
            ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

            var listVersionsRequest = new ListVersionsRequest
            {
                BucketName = bucketName
            };
            var listObjectsV2Request = new ListObjectsV2Request
            {
                BucketName = bucketName
            };

            ListVersionsResponse? listVersionsResponse = null;
            ListObjectsV2Response? listObjectsV2Response = null;
            bool isTruncated = false;
            // Iterate through the objects in the bucket and delete them.
            do
            {
                // Check if the operation has been canceled.
                if (token.IsCancellationRequested)
                {
                    // Signal that the operation is canceled.
                    return;
                }

                List<KeyVersion> keyVersionList;
                // List all the versions of all the objects in the bucket.
                try
                {
                    listVersionsResponse = await s3Client.ListVersionsAsync(listVersionsRequest, token).ConfigureAwait(false);
                    if (listVersionsResponse.Versions == null || listVersionsResponse.Versions.Count == 0)
                    {
                        // If the bucket has no objects break the loop.
                        break;
                    }

                    keyVersionList = new(listVersionsResponse.Versions.Count);
                    for (int index = 0; index < listVersionsResponse.Versions.Count; index++)
                    {
                        keyVersionList.Add(new()
                        {
                            Key = listVersionsResponse.Versions[index].Key,
                            VersionId = listVersionsResponse.Versions[index].VersionId
                        });
                    }
                }
                catch (AmazonS3Exception ex)
                {
                    if (ex.StatusCode != HttpStatusCode.NotImplemented)
                        throw;
                    listObjectsV2Response = await s3Client.ListObjectsV2Async(listObjectsV2Request, token).ConfigureAwait(false);
                    if (listObjectsV2Response.S3Objects == null || listObjectsV2Response.S3Objects.Count == 0)
                    {
                        // If the bucket has no objects break the loop.
                        break;
                    }
                    keyVersionList = new(listObjectsV2Response.S3Objects.Count);
                    for (int index = 0; index < listObjectsV2Response.S3Objects.Count; index++)
                    {
                        keyVersionList.Add(new()
                        {
                            Key = listObjectsV2Response.S3Objects[index].Key,
                        });
                    }
                }
                try
                {
                    // Delete the current set of objects.
                    var deleteObjectsResponse = await s3Client.DeleteObjectsAsync(new()
                    {
                        BucketName = bucketName,
                        Objects = keyVersionList,
                        Quiet = deleteOptions.QuietMode
                    }, token).ConfigureAwait(false);

                    if (!deleteOptions.QuietMode)
                    {
                        // If quiet mode is not set, update the client with list of deleted objects.
                        InvokeS3DeleteBucketWithObjectsUpdateCallback(
                                        updateCallback,
                                        new()
                                        {
                                            DeletedObjects = deleteObjectsResponse.DeletedObjects
                                        }
                                    );
                    }
                }
                catch (DeleteObjectsException deleteObjectsException)
                {
                    if (deleteOptions.ContinueOnError)
                    {
                        // Continue the delete operation if an error was encountered.
                        // Update the client with the list of objects that were deleted and the 
                        // list of objects on which the delete failed.
                        InvokeS3DeleteBucketWithObjectsUpdateCallback(
                                updateCallback,
                                new()
                                {
                                    DeletedObjects = deleteObjectsException.Response.DeletedObjects,
                                    DeleteErrors = deleteObjectsException.Response.DeleteErrors
                                }
                            );
                    }
                    else
                    {
                        // Re-throw the exception if an error was encountered.
                        throw;
                    }
                }
                // Set the markers to get next set of objects from the bucket.
                if (listVersionsResponse != null)
                {
                    listVersionsRequest.KeyMarker = listVersionsResponse.NextKeyMarker;
                    listVersionsRequest.VersionIdMarker = listVersionsResponse.NextVersionIdMarker;
                    isTruncated = listVersionsResponse.IsTruncated;
                }
                if(listObjectsV2Response != null)
                {
                    listObjectsV2Request.ContinuationToken = listObjectsV2Response.NextContinuationToken;
                    isTruncated = listObjectsV2Response.IsTruncated;
                }

            }
            // Continue listing objects and deleting them until the bucket is empty.
            while (isTruncated);

            const int maxRetries = 10;
            for (int retries = 1; retries <= maxRetries; retries++)
            {
                try
                {
                    // Bucket is empty, delete the bucket.
                    await s3Client.DeleteBucketAsync(new DeleteBucketRequest
                    {
                        BucketName = bucketName
                    }, token).ConfigureAwait(false);
                    break;
                }
                catch (AmazonS3Exception e)
                {
                    if (e.StatusCode != HttpStatusCode.Conflict || retries == maxRetries)
                        throw;
                    DefaultRetryPolicy.WaitBeforeRetry(retries, 5000);
                }
            }
        }

        /// <summary>
        /// Invokes the callback which provides updated about the delete operation.
        /// </summary>
        /// <param name="updateCallback">The callback to be invoked.</param>
        /// <param name="update">The data being passed to the callback.</param>
        /// 8
        private static void InvokeS3DeleteBucketWithObjectsUpdateCallback(
            Action<S3DeleteBucketWithObjectsUpdate>? updateCallback, S3DeleteBucketWithObjectsUpdate update)
        {
            if (updateCallback != null)
            {
                updateCallback(update);
            }
        }
    }
}
