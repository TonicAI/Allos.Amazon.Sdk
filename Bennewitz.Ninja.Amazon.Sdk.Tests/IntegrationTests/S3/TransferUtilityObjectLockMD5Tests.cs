using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Amazon.Sdk.Fork;
using Amazon.Util;
using Amazon.Sdk.S3.Transfer;
using AWSSDK_DotNet.IntegrationTests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AmazonS3Client = Amazon.S3.AmazonS3Client;

namespace AWSSDK_DotNet.IntegrationTests.Tests.S3
{
    /// <summary>
    /// Integration tests for the TransferUtility upload operations on S3 bucket with object lock and data governance enabled.
    /// </summary>
    [TestClass]
    [AmazonSdkFork("sdk/test/Services/S3/IntegrationTests/TransferUtilityObjectLockMD5Tests.cs", "AWSSDK_DotNet.IntegrationTests.Tests.S3")]
    public class TransferUtilityObjectLockMd5Tests : TestBase<AmazonS3Client>
    {
        private static string? _bucketName;
        
        [ClassInitialize]
        public static void Initialize(TestContext a)
        {
            BaseInitialize();
            CreateBucketWithObjectLockConfiguration().GetAwaiter().GetResult();
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            if (ShouldDeleteBucket(_bucketName))
            {
                DeleteBucketObjectsIncludingLocked(Client, _bucketName).GetAwaiter().GetResult();
                AmazonS3Util.DeleteS3BucketWithObjectsAsync(Client, _bucketName).GetAwaiter().GetResult();   
            }

            BaseClean();
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestMultipartUploadStreamViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 7000000);
            var contentStream = new MemoryStream(Encoding.UTF8.GetBytes(content));

            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests)),
                CalculateContentMd5Header = true,
                InputStream = contentStream
            };

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);

            using (var getResponse = await Client.GetObjectAsync(_bucketName, uploadRequest.Key).ConfigureAwait(false))
            {
                var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                Assert.AreEqual(content, getBody);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestMultipartUploadFileViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 7000000);
            var key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests));
            var filePath = Path.Combine(Path.GetTempPath(), key + ".txt");

            // Create the file
            await using (StreamWriter writer = File.CreateText(filePath))
            {
                await writer.WriteAsync(content).ConfigureAwait(false);
            }

            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = key,
                CalculateContentMd5Header = true,
                FilePath = filePath
            };

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);

            using (var getResponse = await Client.GetObjectAsync(_bucketName, uploadRequest.Key).ConfigureAwait(false))
            {
                var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                Assert.AreEqual(content, getBody);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        [ExpectedException(typeof(AmazonS3Exception), "Content-MD5 HTTP header is required for Put Part requests with Object Lock parameters")]
        public async Task TestMultipartUploadFileFailViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };

            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 7000000);
            var key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests));
            var filePath = Path.Combine(Path.GetTempPath(), key + ".txt");

            // Create the file
            await using (StreamWriter writer = File.CreateText(filePath))
            {
                await writer.WriteAsync(content).ConfigureAwait(false);
            }

            // Do not set CalculateContentMD5Header as true which should cause upload to fail.
            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = key,
                FilePath = filePath
            };

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        [ExpectedException(typeof(AmazonS3Exception), "Content-MD5 HTTP header is required for Put Part requests with Object Lock parameters")]
        public async Task TestSimpleUploadFileFailViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };

            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 2000000);
            var key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests));
            var filePath = Path.Combine(Path.GetTempPath(), key + ".txt");

            // Create the file
            await using (StreamWriter writer = File.CreateText(filePath))
            {
                await writer.WriteAsync(content).ConfigureAwait(false);
            }

            // Do not set CalculateContentMD5Header as true which should cause upload to fail.
            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = key,
                FilePath = filePath
            };

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        [ExpectedException(typeof(AmazonS3Exception), "Content-MD5 HTTP header is required for Put Part requests with Object Lock parameters")]
        public async Task TestMultipartUploadStreamFailViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 7000000);
            var contentStream = new MemoryStream(Encoding.UTF8.GetBytes(content));

            // Do not set CalculateContentMD5Header as true which should cause upload to fail.
            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests)),
                InputStream = contentStream
            };

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        [ExpectedException(typeof(AmazonS3Exception), "Content-MD5 HTTP header is required for Put Part requests with Object Lock parameters")]
        public async Task TestSimpleUploadStreamFailViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 2000000);
            var contentStream = new MemoryStream(Encoding.UTF8.GetBytes(content));

            // Do not set CalculateContentMD5Header as true which should cause upload to fail.
            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests)),
                InputStream = contentStream
            };

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestSimpleUploadStreamViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 2000000);
            var contentStream = new MemoryStream(Encoding.UTF8.GetBytes(content));

            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests)),
                CalculateContentMd5Header = true,
                InputStream = contentStream,
            };

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);

            using (var getResponse = await Client.GetObjectAsync(_bucketName, uploadRequest.Key).ConfigureAwait(false))
            {
                var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                Assert.AreEqual(content, getBody);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestSimpleUploadStreamViaTransferUtility_ExplicitMD5()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 2000000);
            var contentStream = new MemoryStream(Encoding.UTF8.GetBytes(content));

            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests)),
                CalculateContentMd5Header = true,
                InputStream = contentStream,
            };
            uploadRequest.Headers.ContentMD5 = AWSSDKUtils.GenerateMD5ChecksumForStream(contentStream);

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);

            using (var getResponse = await Client.GetObjectAsync(_bucketName, uploadRequest.Key).ConfigureAwait(false))
            {
                var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                Assert.AreEqual(content, getBody);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestSimpleUploadFileViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 2000000);
            var key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests));
            var filePath = Path.Combine(Path.GetTempPath(), key + ".txt");

            // Create the file
            await using (StreamWriter writer = File.CreateText(filePath))
            {
                await writer.WriteAsync(content).ConfigureAwait(false);
            }

            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = key,
                CalculateContentMd5Header = true,
                FilePath = filePath
            };

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);

            using (var getResponse = await Client.GetObjectAsync(_bucketName, uploadRequest.Key).ConfigureAwait(false))
            {
                var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                Assert.AreEqual(content, getBody);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestSimpleUploadFileViaTransferUtility_ExplicitMD5()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 2000000);
            var key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests));
            var filePath = Path.Combine(Path.GetTempPath(), key + ".txt");

            // Create the file
            await using (StreamWriter writer = File.CreateText(filePath))
            {
                await writer.WriteAsync(content).ConfigureAwait(false);
            }

            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = key,
                CalculateContentMd5Header = true,
                FilePath = filePath
            };

            await using (FileStream fileStream = File.OpenRead(filePath))
            {
                uploadRequest.Headers.ContentMD5 = AWSSDKUtils.GenerateMD5ChecksumForStream(fileStream);
            }

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);

            using (var getResponse = await Client.GetObjectAsync(_bucketName, uploadRequest.Key).ConfigureAwait(false))
            {
                var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                Assert.AreEqual(content, getBody);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestUploadDirectoryViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var directoryKey = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests));
            var directoryPath = Path.Combine(Path.GetTempPath(), directoryKey);
            Dictionary<string, int> filesWithSize = new()
            {
                { directoryKey + "_1.txt", 7000000}, // MultipartUpload
                { directoryKey + "_2.txt", 2000000}, // SimpleUpload
                { directoryKey + "_3.txt", 4000000}, // SimpleUpload
            };

            // Create directory with files.
            Directory.CreateDirectory(directoryPath);
            foreach (var file in filesWithSize)
            {
                var filePath = Path.Combine(directoryPath, file.Key);
                var content = new string('a', file.Value);
                // Create the file
                await using (StreamWriter writer = File.CreateText(filePath))
                {
                    await writer.WriteAsync(content).ConfigureAwait(false);
                }
            }

            var uploadDirectoryRequest = new TransferUtilityUploadDirectoryRequest
            {
                BucketName = _bucketName,
                Directory = directoryPath,
                CalculateContentMd5Header = true
            };

            await transfer.UploadDirectoryAsync(uploadDirectoryRequest).ConfigureAwait(false);

            // Verify the files
            foreach (var file in filesWithSize)
            {
                using (var getResponse = await Client.GetObjectAsync(_bucketName, file.Key).ConfigureAwait(false))
                {
                    var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                    Assert.AreEqual(new('a', file.Value), getBody);
                }
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestSimpleUploadFileWithObjectLockViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 2000000);
            var key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests));
            var filePath = Path.Combine(Path.GetTempPath(), key + ".txt");

            // NOTE: In ObjectLockMode.Compliance mode, a protected object version can't be deleted by any user, including the root user (refer https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-overview.html#object-lock-retention-modes).
            ObjectLockLegalHoldStatus desiredObjectLockLegalHoldStatus = ObjectLockLegalHoldStatus.Off;
            ObjectLockMode desiredObjectLockMode = ObjectLockMode.Governance;
            DateTime desiredObjectLockRetainUntilDate = DateTime.UtcNow.Date.AddDays(5);

            // Create the file
            await using (StreamWriter writer = File.CreateText(filePath))
            {
                await writer.WriteAsync(content).ConfigureAwait(false);
            }

            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = key,
                CalculateContentMd5Header = true,
                FilePath = filePath,
                ObjectLockLegalHoldStatus = desiredObjectLockLegalHoldStatus,
                ObjectLockMode = desiredObjectLockMode,
                ObjectLockRetainUntilDate = desiredObjectLockRetainUntilDate
            };

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);

            using (var getResponse = await Client.GetObjectAsync(_bucketName, uploadRequest.Key).ConfigureAwait(false))
            {
                var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                Assert.AreEqual(content, getBody);
                Assert.AreEqual(desiredObjectLockLegalHoldStatus, getResponse.ObjectLockLegalHoldStatus);
                Assert.AreEqual(desiredObjectLockMode, getResponse.ObjectLockMode);
                Assert.AreEqual(desiredObjectLockRetainUntilDate.Date, getResponse.ObjectLockRetainUntilDate.ToUniversalTime().Date);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestUploadDirectoryWithObjectLockViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var directoryKey = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests));
            var directoryPath = Path.Combine(Path.GetTempPath(), directoryKey);
            Dictionary<string, int> filesWithSize = new()
            {
                { directoryKey + "_1.txt", 7000000}, // MultipartUpload
                { directoryKey + "_2.txt", 2000000}, // SimpleUpload
                { directoryKey + "_3.txt", 4000000}, // SimpleUpload
            };

            // NOTE: In ObjectLockMode.Compliance mode, a protected object version can't be deleted by any user, including the root user (refer https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-overview.html#object-lock-retention-modes).
            ObjectLockLegalHoldStatus desiredObjectLockLegalHoldStatus = ObjectLockLegalHoldStatus.Off;
            ObjectLockMode desiredObjectLockMode = ObjectLockMode.Governance;
            DateTime desiredObjectLockRetainUntilDate = DateTime.UtcNow.Date.AddDays(5);

            // Create directory with files.
            Directory.CreateDirectory(directoryPath);
            foreach (var file in filesWithSize)
            {
                var filePath = Path.Combine(directoryPath, file.Key);
                var content = new string('a', file.Value);
                // Create the file
                await using (StreamWriter writer = File.CreateText(filePath))
                {
                    await writer.WriteAsync(content).ConfigureAwait(false);
                }
            }

            var uploadDirectoryRequest = new TransferUtilityUploadDirectoryRequest
            {
                BucketName = _bucketName,
                Directory = directoryPath,
                CalculateContentMd5Header = true,
                ObjectLockLegalHoldStatus = desiredObjectLockLegalHoldStatus,
                ObjectLockMode = desiredObjectLockMode,
                ObjectLockRetainUntilDate = desiredObjectLockRetainUntilDate
            };

            await transfer.UploadDirectoryAsync(uploadDirectoryRequest).ConfigureAwait(false);

            // Verify the files
            foreach (var file in filesWithSize)
            {
                using (var getResponse = await Client.GetObjectAsync(_bucketName, file.Key).ConfigureAwait(false))
                {
                    var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                    Assert.AreEqual(new('a', file.Value), getBody);
                    Assert.AreEqual(desiredObjectLockLegalHoldStatus, getResponse.ObjectLockLegalHoldStatus);
                    Assert.AreEqual(desiredObjectLockMode, getResponse.ObjectLockMode);
                    Assert.AreEqual(desiredObjectLockRetainUntilDate.Date, getResponse.ObjectLockRetainUntilDate.ToUniversalTime().Date);
                }
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestMultipartUploadFileWithObjectLockViaTransferUtility()
        {
            var transferConfig = new TransferUtilityConfig { MinSizeBeforePartUpload = 6000000 };
            var transfer = new TransferUtility(Client, transferConfig);
            var content = new string('a', 7000000);
            var key = UtilityMethods.GenerateName(nameof(TransferUtilityObjectLockMd5Tests));
            var filePath = Path.Combine(Path.GetTempPath(), key + ".txt");

            // NOTE: In ObjectLockMode.Compliance mode, a protected object version can't be deleted by any user, including the root user (refer https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-overview.html#object-lock-retention-modes).
            ObjectLockLegalHoldStatus desiredObjectLockLegalHoldStatus = ObjectLockLegalHoldStatus.Off;
            ObjectLockMode desiredObjectLockMode = ObjectLockMode.Governance;
            DateTime desiredObjectLockRetainUntilDate = DateTime.UtcNow.Date.AddDays(5);

            // Create the file
            await using (StreamWriter writer = File.CreateText(filePath))
            {
                await writer.WriteAsync(content).ConfigureAwait(false);
            }

            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = key,
                CalculateContentMd5Header = true,
                FilePath = filePath,
                ObjectLockLegalHoldStatus = desiredObjectLockLegalHoldStatus,
                ObjectLockMode = desiredObjectLockMode,
                ObjectLockRetainUntilDate = desiredObjectLockRetainUntilDate
            };

            await transfer.UploadAsync(uploadRequest).ConfigureAwait(false);

            using (var getResponse = await Client.GetObjectAsync(_bucketName, uploadRequest.Key).ConfigureAwait(false))
            {
                var getBody = await new StreamReader(getResponse.ResponseStream).ReadToEndAsync().ConfigureAwait(false);
                Assert.AreEqual(content, getBody);
                Assert.AreEqual(desiredObjectLockLegalHoldStatus, getResponse.ObjectLockLegalHoldStatus);
                Assert.AreEqual(desiredObjectLockMode, getResponse.ObjectLockMode);
                Assert.AreEqual(desiredObjectLockRetainUntilDate.Date, getResponse.ObjectLockRetainUntilDate.ToUniversalTime().Date);
            }
        }

        private static async Task CreateBucketWithObjectLockConfiguration()
        {
            _bucketName = S3TestUtils.CreateBucketWithWait(Client, new PutBucketRequest
            {
                ObjectLockEnabledForBucket = true,
            });

            var objectLockConfiguration = new ObjectLockConfiguration();
            objectLockConfiguration.ObjectLockEnabled = ObjectLockEnabled.Enabled;
            objectLockConfiguration.Rule = new()
            {
                DefaultRetention = new()
                {
                    Days = 1,
                    Mode = ObjectLockRetentionMode.Governance
                }
            };

            var putRequest = new PutObjectLockConfigurationRequest
            {
                BucketName = _bucketName,
                RequestPayer = RequestPayer.Requester,
                ObjectLockConfiguration = objectLockConfiguration
            };

            _ = await Client.PutObjectLockConfigurationAsync(putRequest).ConfigureAwait(false);
        }

        private static async Task DeleteBucketObjectsIncludingLocked(IAmazonS3 s3Client, string? bucketName)
        {            
            ArgumentNullException.ThrowIfNull(bucketName);
            var listVersionsRequest = new ListVersionsRequest
            {
                BucketName = bucketName
            };

            ListVersionsResponse listVersionsResponse;

            // Iterate through the objects in the bucket and delete them.
            do
            {
                // List all the versions of all the objects in the bucket.
                listVersionsResponse = await s3Client.ListVersionsAsync(listVersionsRequest).ConfigureAwait(false);

                if (listVersionsResponse.Versions.Count == 0)
                {
                    // If the bucket has no objects break the loop.
                    break;
                }

                var keyVersionList = new List<KeyVersion>(listVersionsResponse.Versions.Count);
                for (int index = 0; index < listVersionsResponse.Versions.Count; index++)
                {
                    keyVersionList.Add(new()
                    {
                        Key = listVersionsResponse.Versions[index].Key,
                        VersionId = listVersionsResponse.Versions[index].VersionId
                    });
                }

                try
                {
                    // Delete the current set of objects.
                    _ = await s3Client.DeleteObjectsAsync(new()
                    {
                        BucketName = bucketName,
                        Objects = keyVersionList,
                        BypassGovernanceRetention = true
                    }).ConfigureAwait(false);
                }
                catch (AmazonS3Exception s3Ex) when (s3Ex.IsSenderException())
                {
                    throw;
                }
                catch (Exception ex) when (ex is not AmazonS3Exception)
                {
                }

                // Set the markers to get next set of objects from the bucket.
                listVersionsRequest.KeyMarker = listVersionsResponse.NextKeyMarker;
                listVersionsRequest.VersionIdMarker = listVersionsResponse.NextVersionIdMarker;

            }
            // Continue listing objects and deleting them until the bucket is empty.
            while (listVersionsResponse.IsTruncated);
        }
    }
}