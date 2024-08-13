using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.S3.Util;
using Amazon.Sdk.Fork;
using AWSSDK_DotNet.IntegrationTests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AWSSDK_DotNet.IntegrationTests.Tests.S3
{
    /// <summary>
    /// Integration tests for putting flexible checksums to S3
    /// </summary>
    [TestClass]
    [AmazonSdkFork("sdk/test/Services/S3/IntegrationTests/ChecksumTests.cs", "AWSSDK_DotNet.IntegrationTests.Tests.S3")]
    public class ChecksumTests : TestBase<AmazonS3Client>
    {
        private static string? _bucketName;

        private static readonly string TestContent = "Hello world";
        private const ulong MegSize = 1048576;

        private static IEnumerable<object[]> GetAlgorithmsToTest =>
            new List<object[]> {
                new object[] { CoreChecksumAlgorithm.CRC32C },
                new object[] { CoreChecksumAlgorithm.CRC32 },
                new object[] { CoreChecksumAlgorithm.SHA1 },
                new object[] { CoreChecksumAlgorithm.SHA256 }
            };

        [ClassInitialize]
        public static void Setup(TestContext context)
        {
            BaseInitialize();
            _bucketName = S3TestUtils.CreateBucketWithWait(Client);
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            if (ShouldDeleteBucket(_bucketName))
            {
                // Delete the entire bucket used for the SigV4 tests
                AmazonS3Util.DeleteS3BucketWithObjectsAsync(Client, _bucketName).GetAwaiter().GetResult();    
            }
        }

        /// <summary>
        /// Tests a SigV4 PutObject with the checksum placed in the header
        /// </summary>
        [DataTestMethod]
        [DynamicData(nameof(GetAlgorithmsToTest))]
        public async Task TestV4SignedHeadersPut(CoreChecksumAlgorithm algorithm)
        {
            var putRequest = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = $"sigv4-headers-{algorithm}",
                ContentBody = TestContent,
                ChecksumAlgorithm = ChecksumAlgorithm.FindValue(algorithm.ToString()),
                UseChunkEncoding = false
            };

            await PutAndGetChecksumTestHelper(algorithm, putRequest).ConfigureAwait(false);
        }

        /// <summary>
        /// Tests a SigV4 PutObject with the checksum placed in the trailer
        /// </summary>
        [DataTestMethod]
        [DynamicData(nameof(GetAlgorithmsToTest))]
        public async Task TestV4SignedTrailersPut(CoreChecksumAlgorithm algorithm)
        {
            var putRequest = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = $"sigv4-trailers-{algorithm}",
                ContentBody = TestContent,
                DisablePayloadSigning = false,
                ChecksumAlgorithm = ChecksumAlgorithm.FindValue(algorithm.ToString()),
                UseChunkEncoding = true
            };

            await PutAndGetChecksumTestHelper(algorithm, putRequest).ConfigureAwait(false);
        }

        /// <summary>
        /// Tests a SigV4 PutObject with an unsigned payload and the checksum in the trailer
        /// </summary>
        [DataTestMethod]
        [DynamicData(nameof(GetAlgorithmsToTest))]
        public async Task TestV4UnsignedTrailersPut(CoreChecksumAlgorithm algorithm)
        {
            var putRequest = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = $"sigv4-unsignedcontent-trailers-{algorithm}",
                ContentBody = TestContent,
                DisablePayloadSigning = true,
                ChecksumAlgorithm = ChecksumAlgorithm.FindValue(algorithm.ToString()),
                UseChunkEncoding = true
            };

            await PutAndGetChecksumTestHelper(algorithm, putRequest).ConfigureAwait(false);
        }

        /// <summary>
        /// Validates the PutObject request does not fail when adding the trailing header key for retried requests.
        /// </summary>
        /// <remarks>https://github.com/aws/aws-sdk-net/issues/3154</remarks>
        [TestMethod]
        [TestCategory("S3")]
        public void TestDuplicateTrailingHeaderKey()
        {
            var s3Config = new AmazonS3Config
            {
                // Unrealistic timeout so SDK will do a retry
                Timeout = TimeSpan.FromMilliseconds(1),
                RetryMode = RequestRetryMode.Standard,
                MaxErrorRetry = 1
            };

            using (var s3Client = new AmazonS3Client(s3Config))
            {
                var putObjectRequest = new PutObjectRequest
                {
                    BucketName = _bucketName,
                    Key = "s3-retry-bug",
                    ContentBody = TestContent,
                    ChecksumAlgorithm = ChecksumAlgorithm.CRC32C,
                };

                // Due to the low timeout in the S3Config, we'll still get an exception but verify it's not the
                // "ArgumentException" reported in the GitHub issue.
                AssertExtensions.ExpectException<AmazonServiceException>(s3Client.PutObjectAsync(putObjectRequest));
            }
        }

        /// <summary>
        /// Puts and gets an object using a flexible checksum
        /// </summary>
        /// <param name="algorithm">Checksum algorithm to use</param>
        /// <param name="putRequest">PutObject request</param>
        private async Task PutAndGetChecksumTestHelper(CoreChecksumAlgorithm algorithm, PutObjectRequest putRequest)
        {
            await Client.PutObjectAsync(putRequest).ConfigureAwait(false);

            var getObjectAttributesRequest = new GetObjectAttributesRequest
            {
                BucketName = putRequest.BucketName,
                Key = putRequest.Key,
                ObjectAttributes = new() { ObjectAttributes.Checksum }
            };

            var getObjectAttributesResponse = await Client.GetObjectAttributesAsync(getObjectAttributesRequest).ConfigureAwait(false);
            Assert.IsNotNull(getObjectAttributesResponse);

            var getRequest = new GetObjectRequest
            {
                BucketName = putRequest.BucketName,
                Key = putRequest.Key,
                ChecksumMode = ChecksumMode.ENABLED
            };

            var response = await Client.GetObjectAsync(getRequest).ConfigureAwait(false);

            Assert.AreEqual(algorithm, response.ResponseMetadata.ChecksumAlgorithm);
            Assert.AreEqual(ChecksumValidationStatus.PENDING_RESPONSE_READ, response.ResponseMetadata.ChecksumValidationStatus);

            // Ensures checksum was calculated, an exception will have been thrown if it didn't match
            await new StreamReader(response.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
            await response.ResponseStream.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Tests copying object using multipart upload with a signed body
        /// </summary>
        [DataTestMethod]
        [DynamicData(nameof(GetAlgorithmsToTest))]
        public async Task TestSignedCopyObjectUsingMultipartUpload(CoreChecksumAlgorithm algorithm)
        {
            await CopyObjectUsingMultipartTestHelper(algorithm, _bucketName).ConfigureAwait(false);
        }

        /// <summary>
        /// Tests a SigV4 multipart upload with a signed body
        /// </summary>
        [DataTestMethod]
        [DynamicData(nameof(GetAlgorithmsToTest))]
        public async Task TestV4SignedMultipartUpload(CoreChecksumAlgorithm algorithm)
        {
            await MultipartTestHelper(algorithm, _bucketName, false).ConfigureAwait(false);
        }

        /// <summary>
        /// Tests a SigV4 multipart upload with an unsigned body
        /// </summary>
        [DataTestMethod]
        [DynamicData(nameof(GetAlgorithmsToTest))]
        public async Task TestV4UnsignedMultipartUpload(CoreChecksumAlgorithm algorithm)
        {
            await MultipartTestHelper(algorithm, _bucketName, true).ConfigureAwait(false);
        }
        
        /// <summary>
        /// Test helper to test copy object using multipart upload.
        /// </summary>
        /// <param name="algorithm">checksum algorithm</param>
        /// <param name="bucketName">bucket to upload the object to</param>
        private async Task CopyObjectUsingMultipartTestHelper(CoreChecksumAlgorithm algorithm, string? bucketName)
        {
            ArgumentNullException.ThrowIfNull(bucketName);
            
            var random = new Random();
            var nextRandom = random.Next();
            var filePath = Path.Combine(Path.GetTempPath(), "multipartcopy-" + nextRandom + ".txt");
            var retrievedFilepath = Path.Combine(Path.GetTempPath(), "retreived-" + nextRandom + ".txt");
            var totalSize = MegSize * 15U;

            UtilityMethods.GenerateFile(filePath, totalSize);
            string sourceKey = "sourceKey-" + random.Next();
            string copiedKey = "sourceKey-" + random.Next() + "-copy";

            try
            {
                // Upload the source file for testing copy using multipartupload.
                var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
                var transfer = new TransferUtility(Client, transferConfig);
                await transfer.UploadAsync(new()
                {
                    BucketName = bucketName,
                    Key = sourceKey,
                    FilePath = filePath
                }).ConfigureAwait(false);

                // Test copy using multipartupload with ChecksumAlgorithm set.
                List<CopyPartResponse> copyResponses = new();
                InitiateMultipartUploadRequest initRequest = new()
                {
                    BucketName = bucketName,
                    Key = copiedKey,
                    ChecksumAlgorithm = ChecksumAlgorithm.FindValue(algorithm.ToString())
                };

                InitiateMultipartUploadResponse initResponse = await Client.InitiateMultipartUploadAsync(initRequest).ConfigureAwait(false);

                // Get the size of the object.
                GetObjectMetadataRequest metadataRequest = new()
                {
                    BucketName = bucketName,
                    Key = sourceKey
                };

                GetObjectMetadataResponse metadataResponse = await Client.GetObjectMetadataAsync(metadataRequest).ConfigureAwait(false);
                long objectSize = metadataResponse.ContentLength; // Length in bytes.

                // Copy the parts.
                long partSize = 5 * (long)Math.Pow(2, 20); // Part size is 5 MB.

                long bytePosition = 0;
                for (int i = 1; bytePosition < objectSize; i++)
                {
                    CopyPartRequest copyRequest = new()
                    {
                        DestinationBucket = bucketName,
                        DestinationKey = copiedKey,
                        SourceBucket = bucketName,
                        SourceKey = sourceKey,
                        UploadId = initResponse.UploadId,
                        FirstByte = bytePosition,
                        LastByte = bytePosition + partSize - 1 >= objectSize ? objectSize - 1 : bytePosition + partSize - 1,
                        PartNumber = i
                    };

                    copyResponses.Add(await Client.CopyPartAsync(copyRequest).ConfigureAwait(false));

                    bytePosition += partSize;
                }

                // Set up to complete the copy.
                CompleteMultipartUploadRequest completeRequest =
                new()
                {
                    BucketName = bucketName,
                    Key = copiedKey,
                    UploadId = initResponse.UploadId
                };
                completeRequest.AddPartETags(copyResponses);

                // Complete the copy.
                CompleteMultipartUploadResponse completeUploadResponse = await Client.CompleteMultipartUploadAsync(completeRequest).ConfigureAwait(false);

                Assert.IsNotNull(completeUploadResponse.ETag);
                Assert.AreEqual(copiedKey, completeUploadResponse.Key);
                Assert.IsNotNull(completeUploadResponse.Location);

                // Get the file back from S3 and assert it is still the same.
                var getRequest = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = copiedKey
                };

                var getResponse = await Client.GetObjectAsync(getRequest).ConfigureAwait(false);
                await getResponse.WriteResponseStreamToFileAsync(retrievedFilepath, append: false, CancellationToken.None).ConfigureAwait(false);
                UtilityMethods.CompareFiles(filePath, retrievedFilepath);
            }
            finally
            {
                if (File.Exists(filePath))
                    File.Delete(filePath);
                if (File.Exists(retrievedFilepath))
                    File.Delete(retrievedFilepath);
            }
        }

        /// <summary>
        /// Test helper to test a multipart upload without using the Transfer Utility
        /// </summary>
        /// <param name="algorithm">checksum algorithm</param>
        /// <param name="bucketName">bucket to upload the object to</param>
        /// <param name="disablePayloadSigning">whether the request payload should be signed</param>
        private async Task MultipartTestHelper(CoreChecksumAlgorithm algorithm, string? bucketName, bool disablePayloadSigning)
        {
            ArgumentNullException.ThrowIfNull(bucketName);
            
            var random = new Random();
            var nextRandom = random.Next();
            var filePath = Path.Combine(Path.GetTempPath(), "multi-" + nextRandom + ".txt");
            var retrievedFilepath = Path.Combine(Path.GetTempPath(), "retreived-" + nextRandom + ".txt");
            var totalSize = MegSize * 15;

            UtilityMethods.GenerateFile(filePath, totalSize);
            string key = "sourceKey-" + random.Next();

            Stream inputStream = File.OpenRead(filePath);
            try
            {
                InitiateMultipartUploadRequest initRequest = new()
                {
                    BucketName = bucketName,
                    Key = key,
                    ChecksumAlgorithm = ChecksumAlgorithm.FindValue(algorithm.ToString())
                };

                InitiateMultipartUploadResponse initResponse = await Client.InitiateMultipartUploadAsync(initRequest).ConfigureAwait(false);

                // Upload part 1
                UploadPartRequest uploadRequest = new()
                {
                    BucketName = bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId,
                    PartNumber = 1,
                    PartSize = Convert.ToInt64(5 * MegSize),
                    InputStream = inputStream,
                    ChecksumAlgorithm = ChecksumAlgorithm.FindValue(algorithm.ToString()),
                    DisablePayloadSigning = disablePayloadSigning
                };
                UploadPartResponse up1Response = await Client.UploadPartAsync(uploadRequest).ConfigureAwait(false);

                // Upload part 2
                uploadRequest = new()
                {
                    BucketName = bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId,
                    PartNumber = 2,
                    PartSize = Convert.ToInt64(5 * MegSize),
                    InputStream = inputStream,
                    ChecksumAlgorithm = ChecksumAlgorithm.FindValue(algorithm.ToString()),
                    DisablePayloadSigning = disablePayloadSigning
                };

                UploadPartResponse up2Response = await Client.UploadPartAsync(uploadRequest).ConfigureAwait(false);

                // Upload part 3
                uploadRequest = new()
                {
                    BucketName = bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId,
                    PartNumber = 3,
                    InputStream = inputStream,
                    ChecksumAlgorithm = ChecksumAlgorithm.FindValue(algorithm.ToString()),
                    DisablePayloadSigning = disablePayloadSigning,
                    IsLastPart = true
                };

                UploadPartResponse up3Response = await Client.UploadPartAsync(uploadRequest).ConfigureAwait(false);

                ListPartsRequest listPartRequest = new()
                {
                    BucketName = bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId
                };

                ListPartsResponse listPartResponse = await Client.ListPartsAsync(listPartRequest).ConfigureAwait(false);
                Assert.AreEqual(3, listPartResponse.Parts.Count);
                AssertPartsAreEqual(up1Response, listPartResponse.Parts[0]);
                AssertPartsAreEqual(up2Response, listPartResponse.Parts[1]);
                AssertPartsAreEqual(up3Response, listPartResponse.Parts[2]);

                CompleteMultipartUploadRequest compRequest = new()
                {
                    BucketName = bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId
                };
                compRequest.AddPartETags(up1Response, up2Response, up3Response);

                CompleteMultipartUploadResponse compResponse = await Client.CompleteMultipartUploadAsync(compRequest).ConfigureAwait(false);
                Assert.IsNotNull(compResponse.ETag);
                Assert.AreEqual(key, compResponse.Key);
                Assert.IsNotNull(compResponse.Location);

                // Get the file back from S3 and assert it is still the same.
                var getRequest = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    ChecksumMode = ChecksumMode.ENABLED
                };

                var getResponse = await Client.GetObjectAsync(getRequest).ConfigureAwait(false);
                await getResponse.WriteResponseStreamToFileAsync(retrievedFilepath, append: false, CancellationToken.None).ConfigureAwait(false);
                UtilityMethods.CompareFiles(filePath, retrievedFilepath);

                // We don't expect the checksum to be validated on getting an entire multipart object,
                // because it's actually the checksum-of-checksums
                Assert.AreEqual(CoreChecksumAlgorithm.NONE, getResponse.ResponseMetadata.ChecksumAlgorithm);
                Assert.AreEqual(ChecksumValidationStatus.NOT_VALIDATED, getResponse.ResponseMetadata.ChecksumValidationStatus);
            }
            finally
            {
                inputStream.Close();
                if (File.Exists(filePath))
                    File.Delete(filePath);
                if (File.Exists(retrievedFilepath))
                    File.Delete(retrievedFilepath);
            }
        }

        /// <summary>
        /// Helper to assert that uploaded parts have the same checksum as listed parts.
        /// Genearlly only one checksum is expected to be set.
        /// </summary>
        /// <param name="uploadPartResponse">Response after uploading a part</param>
        /// <param name="partDetail">Response for a single part after listing parts</param>
        private static void AssertPartsAreEqual(UploadPartResponse uploadPartResponse, PartDetail partDetail)
        {
            Assert.AreEqual(uploadPartResponse.PartNumber, partDetail.PartNumber);
            Assert.AreEqual(uploadPartResponse.ETag, partDetail.ETag);
            Assert.AreEqual(uploadPartResponse.ChecksumCRC32C, partDetail.ChecksumCRC32C);
            Assert.AreEqual(uploadPartResponse.ChecksumCRC32, partDetail.ChecksumCRC32);
            Assert.AreEqual(uploadPartResponse.ChecksumSHA1, partDetail.ChecksumSHA1);
            Assert.AreEqual(uploadPartResponse.ChecksumSHA256, partDetail.ChecksumSHA256);
        }

        [TestMethod]
        [TestCategory("S3")]
        [DataTestMethod]
        [DynamicData(nameof(GetAlgorithmsToTest))]
        public async Task TestMultipartUploadViaTransferUtility(CoreChecksumAlgorithm algorithm)
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 7000000);
            var key = UtilityMethods.GenerateName(nameof(ChecksumTests));
            var filePath = Path.Combine(Path.GetTempPath(), key + ".txt");
            var retrievedFilepath = Path.Combine(Path.GetTempPath(), "retreived-" + key + ".txt");

            try
            {
                // Create the file
                await using (StreamWriter writer = File.CreateText(filePath))
                {
                    await writer.WriteAsync(content).ConfigureAwait(false);
                }

                var uploadRequest = new TransferUtilityUploadRequest
                {
                    BucketName = _bucketName,
                    Key = key,
                    FilePath = filePath,
                    ChecksumAlgorithm = ChecksumAlgorithm.FindValue(algorithm.ToString())
                };

                await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);

                // Get the file back from S3 and assert it is still the same.
                GetObjectRequest getRequest = new()
                {
                    BucketName = _bucketName,
                    Key = uploadRequest.Key,
                    ChecksumMode = ChecksumMode.ENABLED
                };

                var getResponse = await Client.GetObjectAsync(getRequest).ConfigureAwait(false);
                var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                Assert.AreEqual(content, getBody);

                // We don't expect the checksum to be validated on getting an entire multipart object,
                // because it's actually the checksum-of-checksums
                Assert.AreEqual(CoreChecksumAlgorithm.NONE, getResponse.ResponseMetadata.ChecksumAlgorithm);
                Assert.AreEqual(ChecksumValidationStatus.NOT_VALIDATED, getResponse.ResponseMetadata.ChecksumValidationStatus);

                // Get the object attributes. Parts collection in ObjectParts is only returned if ChecksumAlgorithm is set different from default value.
                GetObjectAttributesRequest getObjectAttributesRequest = new()
                {
                    BucketName = _bucketName,
                    Key = uploadRequest.Key,
                    ObjectAttributes = new()
                    {
                        new("Checksum"),
                        new("ObjectParts"),
                        new("ObjectSize")
                    }
                };
                GetObjectAttributesResponse getObjectAttributesResponse = await Client.GetObjectAttributesAsync(getObjectAttributesRequest).ConfigureAwait(false);
                Assert.IsTrue(getObjectAttributesResponse.ObjectParts.Parts.Count > 0);
                // Number of Parts returned is controlled by GetObjectAttributesRequest.MaxParts.
                Assert.AreEqual(getObjectAttributesResponse.ObjectParts.Parts.Count, getObjectAttributesResponse.ObjectParts.TotalPartsCount);

                var firstObjectPart = getObjectAttributesResponse.ObjectParts.Parts.First();
                ChecksumAlgorithm expectedChecksumAlgorithm = ChecksumAlgorithm.FindValue(algorithm.ToString());
                if (expectedChecksumAlgorithm == ChecksumAlgorithm.CRC32)
                {
                    Assert.IsNotNull(firstObjectPart.ChecksumCRC32);
                }
                if (expectedChecksumAlgorithm == ChecksumAlgorithm.CRC32C)
                {
                    Assert.IsNotNull(firstObjectPart.ChecksumCRC32C);
                }
                if (expectedChecksumAlgorithm == ChecksumAlgorithm.SHA1)
                {
                    Assert.IsNotNull(firstObjectPart.ChecksumSHA1);
                }
                if (expectedChecksumAlgorithm == ChecksumAlgorithm.SHA256)
                {
                    Assert.IsNotNull(firstObjectPart.ChecksumSHA256);
                }
                Assert.AreEqual(1, firstObjectPart.PartNumber);
                Assert.IsTrue(firstObjectPart.Size > 0);

                // Similarily we don't expect this to validate either,
                // though it doesn't expose the reponse metadata
                await transfer.DownloadAsync(new()
                {
                    BucketName = _bucketName,
                    Key = uploadRequest.Key,
                    FilePath = retrievedFilepath,
                    ChecksumMode = ChecksumMode.ENABLED
                }).ConfigureAwait(false);
            }
            finally
            {
                if (File.Exists(filePath))
                    File.Delete(filePath);
                if (File.Exists(retrievedFilepath))
                    File.Delete(retrievedFilepath);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        [DataTestMethod]
        [DynamicData(nameof(GetAlgorithmsToTest))]
        public async Task TestSingleUploadViaTransferUtility(CoreChecksumAlgorithm algorithm)
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 5000000);
            var key = UtilityMethods.GenerateName(nameof(ChecksumTests));
            var filePath = Path.Combine(Path.GetTempPath(), key + ".txt");
            var retrievedFilepath = Path.Combine(Path.GetTempPath(), "retreived-" + key + ".txt");

            try
            {
                // Create the file
                await using (StreamWriter writer = File.CreateText(filePath))
                {
                    await writer.WriteAsync(content).ConfigureAwait(false);
                }

                var uploadRequest = new TransferUtilityUploadRequest
                {
                    BucketName = _bucketName,
                    Key = key,
                    FilePath = filePath,
                    ChecksumAlgorithm = ChecksumAlgorithm.FindValue(algorithm.ToString())
                };

                await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);

                // Get the file back from S3 and assert it is still the same.
                var getRequest = new GetObjectRequest
                {
                    BucketName = _bucketName,
                    Key = uploadRequest.Key,
                    ChecksumMode = ChecksumMode.ENABLED
                };

                var getResponse = await Client.GetObjectAsync(getRequest).ConfigureAwait(false);
                var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                Assert.AreEqual(content, getBody);

                Assert.AreEqual(algorithm.ToString(), getResponse.ResponseMetadata.ChecksumAlgorithm.ToString(), true);
                Assert.AreEqual(ChecksumValidationStatus.PENDING_RESPONSE_READ, getResponse.ResponseMetadata.ChecksumValidationStatus);

                // This should validate the checksum, so "assert" that no exceptions are thrown,
                // though it doesn't expose the response metadata like above
                await transfer.DownloadAsync(new()
                {
                    BucketName = _bucketName,
                    Key = uploadRequest.Key,
                    FilePath = retrievedFilepath,
                    ChecksumMode = ChecksumMode.ENABLED
                }).ConfigureAwait(false);
            }
            finally
            {
                if (File.Exists(filePath))
                    File.Delete(filePath);
                if (File.Exists(retrievedFilepath))
                    File.Delete(retrievedFilepath);
            }
        }
    }
}
