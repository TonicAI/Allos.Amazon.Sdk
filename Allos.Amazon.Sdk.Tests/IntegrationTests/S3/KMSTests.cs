using System.Net;
using System.Text;
using Amazon;
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
    [TestClass]
    [AmazonSdkFork("sdk/test/Services/S3/IntegrationTests/KMSTests.cs", "AWSSDK_DotNet.IntegrationTests.Tests.S3")]
    public class KmsTests : TestBase<AmazonS3Client>
    {
        private const string Key = "foo.txt";
        private const string TestContents = "Test contents";
        private static readonly string _largeTestContents = new('@', (int)(AsyncTransferUtilityTests.MegSize * 19));
        private static readonly string _fileContents = "Test file contents";
        protected override string BasePath => Path.Combine(base.BasePath, nameof(KmsTests));

        [ClassCleanup]
        public static void Cleanup()
        {
            BaseClean();
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task GetObjectFromNonDefaultEndpoint()
        {
            var client = new AmazonS3Client(RegionEndpoint.USWest2);
            var bucketName = S3TestUtils.CreateBucketWithWait(client);
            try
            {
                var putObjectRequest = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = Key,
                    ContentBody = TestContents,
                    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AWSKMS
                };
                await client.PutObjectAsync(putObjectRequest).ConfigureAwait(false);

                using (var response = await client.GetObjectAsync(bucketName, Key).ConfigureAwait(false))
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    var data = await reader.ReadToEndAsync().ConfigureAwait(false);
                    Assert.AreEqual(TestContents, data);
                }
            }
            finally
            {
                if (ShouldDeleteBucket(bucketName))
                {
                    await AmazonS3Util.DeleteS3BucketWithObjectsAsync(client, bucketName).ConfigureAwait(false);
                }

                client.Dispose();
            }
        }
        [TestMethod]
        [TestCategory("S3")]
        public async Task GetObjectFromNonDefaultEndpointWithDoubleEncryption()
        {
            var client = new AmazonS3Client(RegionEndpoint.USEast2);
            var bucketName = S3TestUtils.CreateBucketWithWait(client);
            try
            {
                var putObjectRequest = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = Key,
                    ContentBody = TestContents,
                    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AWSKMSDSSE
                };
                await client.PutObjectAsync(putObjectRequest).ConfigureAwait(false);
                using (var response = await client.GetObjectAsync(bucketName, Key).ConfigureAwait(false))
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    var data = await reader.ReadToEndAsync().ConfigureAwait(false);
                    Assert.AreEqual(TestContents, data);
                }

            }
            finally
            {
                if (ShouldDeleteBucket(bucketName))
                {
                    await AmazonS3Util.DeleteS3BucketWithObjectsAsync(client, bucketName).ConfigureAwait(false);
                }

                client.Dispose();
            }
        }
        [TestMethod]
        [TestCategory("S3")]
        public async Task GetObjectFromDefaultEndpointBeforeDnsResolution()
        {
            var client = new AmazonS3Client(RegionEndpoint.USWest2);
            var defaultEndpointClient = new AmazonS3Client(RegionEndpoint.USEast1);
            var bucketName = S3TestUtils.CreateBucketWithWait(client);
            try
            {
                var putObjectRequest = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = Key,
                    ContentBody = TestContents,
                    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AWSKMS
                };
                await client.PutObjectAsync(putObjectRequest).ConfigureAwait(false);

                using (var response = await defaultEndpointClient.GetObjectAsync(bucketName, Key).ConfigureAwait(false))
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    var data = await reader.ReadToEndAsync().ConfigureAwait(false);
                    Assert.AreEqual(TestContents, data);
                }
            }
            finally
            {
                if (ShouldDeleteBucket(bucketName))
                {
                    await AmazonS3Util.DeleteS3BucketWithObjectsAsync(client, bucketName).ConfigureAwait(false);
                }

                client.Dispose();
                defaultEndpointClient.Dispose();
            }
        }
        [TestMethod]
        [TestCategory("S3")]
        public async Task GetObjectFromDefaultEndpointBeforeDnsResolutionWithDoubleEncryption()
        {
            var client = new AmazonS3Client(RegionEndpoint.USEast2);
            var defaultEndpointClient = new AmazonS3Client(RegionEndpoint.USEast1);
            var bucketName = S3TestUtils.CreateBucketWithWait(client);
            try
            {
                var putObjectRequest = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = Key,
                    ContentBody = TestContents,
                    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AWSKMSDSSE
                };
                await client.PutObjectAsync(putObjectRequest).ConfigureAwait(false);

                using (var response = await defaultEndpointClient.GetObjectAsync(bucketName, Key).ConfigureAwait(false))
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    var data = await reader.ReadToEndAsync().ConfigureAwait(false);
                    Assert.AreEqual(TestContents, data);
                }
            }
            finally
            {
                if (ShouldDeleteBucket(bucketName))
                {
                    await AmazonS3Util.DeleteS3BucketWithObjectsAsync(client, bucketName).ConfigureAwait(false);
                }

                client.Dispose();
                defaultEndpointClient.Dispose();
            }
        }

        // To run this test set bucketName to a valid bucket name.
        [TestMethod]
        [TestCategory("S3")]
        public async Task GetObjectFromDefaultEndpointAfterDnsResolution()
        {
            var client = new AmazonS3Client(RegionEndpoint.USWest2);
            var defaultEndpointClient = new AmazonS3Client(RegionEndpoint.USEast1);

            var bucketName = S3TestUtils.CreateBucketWithWait(client);
            try
            {
                var putObjectRequest = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = Key,
                    ContentBody = TestContents,
                    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AWSKMS
                };
                await client.PutObjectAsync(putObjectRequest).ConfigureAwait(false);

                using (var response = await defaultEndpointClient.GetObjectAsync(bucketName, Key).ConfigureAwait(false))
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    var data = await reader.ReadToEndAsync();
                    Assert.AreEqual(TestContents, data);
                }
            }
            finally
            {
                client.Dispose();
                defaultEndpointClient.Dispose();
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestKmsOverHttp()
        {
            var config = new AmazonS3Config
            {
                RegionEndpoint = AWSConfigs.RegionEndpoint,
                UseHttp = true
            };
            using(var client = new AmazonS3Client(config))
            {
                var bucketName = S3TestUtils.CreateBucketWithWait(client);
                try
                {
                    var putObjectRequest = new PutObjectRequest
                    {
                        BucketName = bucketName,
                        Key = Key,
                        ContentBody = TestContents,
                        ServerSideEncryptionMethod = ServerSideEncryptionMethod.AWSKMS
                    };
                    Task action = client.PutObjectAsync(putObjectRequest);

                    await AssertExtensions.ExpectException(action, typeof(AmazonS3Exception)).ConfigureAwait(false);
                }
                finally
                {
                    if (ShouldDeleteBucket(bucketName))
                    {
                        await AmazonS3Util.DeleteS3BucketWithObjectsAsync(client, bucketName).ConfigureAwait(false);
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task DefaultKeyTests()
        {
            await TestSseKms(keyId: null, ServerSideEncryptionMethod.AWSKMS).ConfigureAwait(false);
            await TestPresignedUrls(keyId: null, ServerSideEncryptionMethod.AWSKMS).ConfigureAwait(false);

        }
        [TestMethod]
        [TestCategory("S3")]
        public async Task KmsDsseTest()
        {
            await TestSseKms(null, ServerSideEncryptionMethod.AWSKMSDSSE).ConfigureAwait(false);

        }
        [TestMethod]
        [TestCategory("S3")]
        public async Task TestKmsDssePresignedUrls()
        {
            await TestPresignedUrls(null, ServerSideEncryptionMethod.AWSKMSDSSE).ConfigureAwait(false);
        }

        // https://github.com/aws/aws-sdk-net/issues/200 (and 197), 3rd-party
        // storage providers compatible with S3 should not be included
        // in the test to use Signature V4
        [TestMethod]
        [TestCategory("S3")]
        public void TestNonS3EndpointDetection()
        {
            string[] thirdPartyProviderUriExamples =
            {
                "http://storage.googleapis.com",
                "http://bucket.storage.googleapis.com",
                "http://s3.mycompany.com",
                "http://storage.s3.company.com"
            };

            string[] s3UriExamples =
            {
                "http://s3.amazonaws.com",
                "http://s3-external-1.amazonaws.com",
                "http://s3-us-west-2.amazonaws.com",
                "http://bucketname.s3-us-west-2.amazonaws.com",
                "http://s3.eu-central-1.amazonaws.com",
                "http://bucketname.s3.eu-central-1.amazonaws.com",
                "http://s3.cn-north-1.amazonaws.com.cn",
            };

            foreach (var uri in thirdPartyProviderUriExamples)
            {
                Assert.IsFalse(AmazonS3Uri.IsAmazonS3Endpoint(uri));    
            }

            foreach (var uri in s3UriExamples)
            {
                Assert.IsTrue(AmazonS3Uri.IsAmazonS3Endpoint(uri));
            }
        }

        public async Task TestPresignedUrls(string? keyId, ServerSideEncryptionMethod serverSideEncryptionMethod)
        {
            var oldSigV4 = AWSConfigsS3.UseSignatureVersion4;
            AWSConfigsS3.UseSignatureVersion4 = true;

            using (var newClient = new AmazonS3Client())
            {
                var bucketName = S3TestUtils.CreateBucketWithWait(newClient);
                try
                {
                    await VerifyPresignedPut(bucketName, Key, keyId, serverSideEncryptionMethod).ConfigureAwait(false);
                    await VerifyObjectWithTransferUtility(bucketName).ConfigureAwait(false);
                    await TestPresignedGet(bucketName, Key, keyId).ConfigureAwait(false);

                    var key2 = Key + "Copy2";
                    var copyResponse = await newClient.CopyObjectAsync(new()
                    {
                        SourceBucket = bucketName,
                        SourceKey = Key,
                        DestinationBucket = bucketName,
                        DestinationKey = key2
                    }).ConfigureAwait(false);
                    Assert.IsNotNull(copyResponse);
                    var usedKeyId = copyResponse.ServerSideEncryptionKeyManagementServiceKeyId;
                    Assert.IsNull(usedKeyId);
                }
                finally
                {
                    if (ShouldDeleteBucket(bucketName))
                    {
                        await AmazonS3Util.DeleteS3BucketWithObjectsAsync(newClient, bucketName).ConfigureAwait(false);
                    }

                    AWSConfigsS3.UseSignatureVersion4 = oldSigV4;
                }
            }
        }

        private async Task TestSseKms(string? keyId, ServerSideEncryptionMethod serverSideEncryptionMethod)
        {
            var bucketName = S3TestUtils.CreateBucketWithWait(Client);
            try
            {
                var putObjectRequest = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = Key,
                    ContentBody = TestContents,
                    ServerSideEncryptionMethod = serverSideEncryptionMethod
                };
                putObjectRequest.ServerSideEncryptionKeyManagementServiceKeyId = keyId;
                var putObjectResponse = await Client.PutObjectAsync(putObjectRequest).ConfigureAwait(false);
                Assert.IsNotNull(putObjectResponse.ServerSideEncryptionKeyManagementServiceKeyId);
                var usedKeyId = putObjectResponse.ServerSideEncryptionKeyManagementServiceKeyId;
                VerifyKeyId(keyId, usedKeyId);
                await VerifyObject(bucketName, Key, usedKeyId, serverSideEncryptionMethod).ConfigureAwait(false);
                await VerifyObjectWithTransferUtility(bucketName).ConfigureAwait(false);

                await TestCopyPart(bucketName, Key, keyId, serverSideEncryptionMethod).ConfigureAwait(false);

                var key2 = Key + "Copy";
                var copyResponse = await Client.CopyObjectAsync(new()
                {
                    SourceBucket = bucketName,
                    SourceKey = Key,
                    DestinationBucket = bucketName,
                    DestinationKey = key2,
                    ServerSideEncryptionMethod = serverSideEncryptionMethod,
                    ServerSideEncryptionKeyManagementServiceKeyId = keyId
                }).ConfigureAwait(false);
                Assert.IsNotNull(copyResponse);
                usedKeyId = copyResponse.ServerSideEncryptionKeyManagementServiceKeyId;
                VerifyKeyId(keyId, usedKeyId);
                await VerifyObject(bucketName, key2, usedKeyId, serverSideEncryptionMethod).ConfigureAwait(false);

                AsyncTransferUtility utility = new(Client);
                var smallUploadRequest = new TransferUtilityUploadRequest
                {
                    BucketName = bucketName,
                    Key = Key,
                    ServerSideEncryptionMethod = serverSideEncryptionMethod,
                    ServerSideEncryptionKeyManagementServiceKeyId = keyId,
                    InputStream = new MemoryStream(Encoding.UTF8.GetBytes(TestContents))
                };
                await utility.UploadAsync(smallUploadRequest).ConfigureAwait(false);
                await VerifyObject(bucketName, Key, keyId, serverSideEncryptionMethod).ConfigureAwait(false);

                var largeUploadRequest = new TransferUtilityUploadRequest
                {
                    BucketName = bucketName,
                    Key = Key,
                    ServerSideEncryptionMethod = serverSideEncryptionMethod,
                    ServerSideEncryptionKeyManagementServiceKeyId = keyId,
                    InputStream = new MemoryStream(Encoding.UTF8.GetBytes(_largeTestContents))
                };
                await utility.UploadAsync(largeUploadRequest).ConfigureAwait(false);
                await VerifyObject(bucketName, Key, keyId, serverSideEncryptionMethod).ConfigureAwait(false);

                await TestUploadDirectory(bucketName, keyId, serverSideEncryptionMethod).ConfigureAwait(false);
            }
            finally
            {
                if (ShouldDeleteBucket(bucketName))
                {
                    await AmazonS3Util.DeleteS3BucketWithObjectsAsync(Client, bucketName).ConfigureAwait(false);
                }
            }
        }

        private async Task TestUploadDirectory(string bucketName, string? keyId, ServerSideEncryptionMethod serverSideEncryptionMethod)
        {
            var directoryName = UtilityMethods.GenerateName("UploadDirectoryTest");

            var directoryPath = Path.Combine(BasePath, directoryName);
            for (int i = 0; i < 5; i++)
            {
                var filePath = Path.Combine(Path.Combine(directoryPath, i.ToString()), "file.txt");
                UtilityMethods.WriteFile(filePath, _fileContents);
            }

            var config = new TransferUtilityConfig
            {
                ConcurrentServiceRequests = 10,
            };
            var transferUtility = new AsyncTransferUtility(Client, config);
            var request = new TransferUtilityUploadDirectoryRequest
            {
                BucketName = bucketName,
                Directory = directoryPath,
                KeyPrefix = directoryName,
                SearchPattern = "*",
                SearchOption = SearchOption.AllDirectories,
                ServerSideEncryptionMethod = serverSideEncryptionMethod,
                ServerSideEncryptionKeyManagementServiceKeyId = keyId
            };

            HashSet<string> keys = new();
            request.UploadDirectoryFileRequestEvent += (_, e) =>
            {
                if (!e.UploadRequest.IsSetKey())
                {
                    ArgumentNullException.ThrowIfNull(e.UploadRequest.Key);   
                }
                keys.Add(e.UploadRequest.Key);
            };
            await transferUtility.UploadDirectoryAsync(request).ConfigureAwait(false);
            Assert.AreEqual(5, keys.Count);

            foreach (var key in keys)
                await VerifyObject(bucketName, key, keyId, serverSideEncryptionMethod).ConfigureAwait(false);
        }
        private async Task TestPresignedGet(string bucketName, string key, string? keyId)
        {
            GetPreSignedUrlRequest getPresignedUrlRequest = new()
            {
                BucketName = bucketName,
                Key = key,
                Expires = DateTime.Now.AddMinutes(5)
            };
            var url = await Client.GetPreSignedURLAsync(getPresignedUrlRequest).ConfigureAwait(false);
            
            using (HttpClient httpClient = new())
            {
                HttpRequestMessage request = new(HttpMethod.Get, url);

                using (var response = await httpClient.SendAsync(request).ConfigureAwait(false))
                {
                    var usedKeyId = response.Headers.GetValues(HeaderKeys.XAmzServerSideEncryptionAwsKmsKeyIdHeader).FirstOrDefault();
                    ArgumentNullException.ThrowIfNull(usedKeyId);
                    VerifyKeyId(keyId, usedKeyId);
                    
                    Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
                    var contents = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                    VerifyContents(contents);
                }
            }
        }
        private async Task VerifyPresignedPut(string bucketName, string? key, string? keyId, ServerSideEncryptionMethod serverSideEncryptionMethod)
        {
            GetPreSignedUrlRequest getPresignedUrlRequest = new()
            {
                BucketName = bucketName,
                Key = key,
                Verb = HttpVerb.PUT,
                ServerSideEncryptionMethod = serverSideEncryptionMethod,
                ServerSideEncryptionKeyManagementServiceKeyId = keyId,
                Expires = DateTime.Now.AddMinutes(5)
            };
            var url = await Client.GetPreSignedURLAsync(getPresignedUrlRequest).ConfigureAwait(false);

            string? usedKeyId = null;
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    usedKeyId = await VerifyPresignedPut(keyId, url, serverSideEncryptionMethod).ConfigureAwait(false);
                    break;
                }
                catch (AmazonS3Exception s3Ex) when (s3Ex.IsSenderException())
                {
                    throw;
                }
                catch (Exception ex) when (ex is not AmazonS3Exception)
                {
                }
            }

            Assert.IsNotNull(usedKeyId);
            VerifyKeyId(keyId, usedKeyId);
            await VerifyObject(bucketName, key, usedKeyId, serverSideEncryptionMethod).ConfigureAwait(false);
        }
        private async Task TestCopyPart(string bucketName, string key, string? keyId, ServerSideEncryptionMethod serverSideEncryptionMethod)
        {
            string dstKey = "dstObject";
            string srcKey = key;
            string srcVersionId;
            string srcETag;
            DateTime srcTimeStamp;
            string? uploadId = null;

            try
            {
                //Get the srcObjectTimestamp
                GetObjectMetadataResponse gomr = await Client.GetObjectMetadataAsync(new()
                {
                    BucketName = bucketName,
                    Key = srcKey
                }).ConfigureAwait(false);
                srcTimeStamp = gomr.LastModified;
                srcVersionId = gomr.VersionId;
                srcETag = gomr.ETag;

                //Start the multipart upload
                InitiateMultipartUploadResponse imur = await Client.InitiateMultipartUploadAsync(new()
                {
                    BucketName = bucketName,
                    Key = dstKey,
                    ServerSideEncryptionMethod = serverSideEncryptionMethod,
                    ServerSideEncryptionKeyManagementServiceKeyId = keyId
                }).ConfigureAwait(false);
                Assert.AreEqual(serverSideEncryptionMethod, imur.ServerSideEncryptionMethod);
                var usedKeyId = imur.ServerSideEncryptionKeyManagementServiceKeyId;
                VerifyKeyId(keyId, usedKeyId);
                uploadId = imur.UploadId;


                CopyPartRequest request = new()
                {
                    DestinationBucket = bucketName,
                    DestinationKey = dstKey,
                    SourceBucket = bucketName,
                    SourceKey = srcKey,
                    UploadId = uploadId,
                    PartNumber = 1,
                };
                CopyPartResponse response = await Client.CopyPartAsync(request).ConfigureAwait(false);
                Assert.AreEqual(serverSideEncryptionMethod, response.ServerSideEncryptionMethod);
                usedKeyId = response.ServerSideEncryptionKeyManagementServiceKeyId;
                VerifyKeyId(keyId, usedKeyId);

                //ETag
                Assert.IsNotNull(response.ETag);
                Assert.IsTrue(response.ETag is { Length: > 0 });

                //LastModified
                Assert.IsNotNull(response.LastModified);
                Assert.AreNotEqual(DateTime.MinValue, response.LastModified);

                //PartNumber
                Assert.IsTrue(response.PartNumber == 1);

                var completeResponse = await Client.CompleteMultipartUploadAsync(new()
                {
                    BucketName = bucketName,
                    Key = dstKey,
                    UploadId = uploadId,
                    PartETags = new()
                    {
                        new() { ETag = response.ETag, PartNumber = response.PartNumber }
                    }
                }).ConfigureAwait(false);
                Assert.AreEqual(serverSideEncryptionMethod, completeResponse.ServerSideEncryptionMethod);
                usedKeyId = completeResponse.ServerSideEncryptionKeyManagementServiceKeyId;
                VerifyKeyId(keyId, usedKeyId);
            }
            finally
            {
                //abort the multipart upload
                if (uploadId != null)
                {
                    await Client.AbortMultipartUploadAsync(new()
                    {
                        BucketName = bucketName,
                        Key = dstKey,
                        UploadId = uploadId
                    }).ConfigureAwait(false);
                }
            }
        }

        private async Task<string?> VerifyPresignedPut(string? keyId, string url, ServerSideEncryptionMethod serverSideEncryptionMethod)
        {
            using (HttpClient httpClient = new())
            {
                HttpRequestMessage request = new(HttpMethod.Put, url);
                
                if (keyId != null)
                    request.Headers.Add(HeaderKeys.XAmzServerSideEncryptionAwsKmsKeyIdHeader, keyId);
                request.Headers.Add(HeaderKeys.XAmzServerSideEncryptionHeader, serverSideEncryptionMethod.Value);
                
                request.Content = new StringContent(TestContents, Encoding.UTF8);

                using (var response = await httpClient.SendAsync(request).ConfigureAwait(false))
                {
                    string? usedKeyId = response.Headers.GetValues(HeaderKeys.XAmzServerSideEncryptionAwsKmsKeyIdHeader).FirstOrDefault();
                    return usedKeyId;
                }
            }
        }
        private async Task VerifyObject(string bucketName, string? key, string? usedKeyId, ServerSideEncryptionMethod serverSideEncryptionMethod)
        {
            var metadata = await Client.GetObjectMetadataAsync(bucketName, key).ConfigureAwait(false);
            if (usedKeyId != null)
                Assert.IsTrue(metadata.ServerSideEncryptionKeyManagementServiceKeyId.IndexOf(usedKeyId, StringComparison.OrdinalIgnoreCase) >= 0);

            using (var response = await Client.GetObjectAsync(bucketName, key).ConfigureAwait(false))
            {
                Assert.AreEqual(serverSideEncryptionMethod, response.ServerSideEncryptionMethod);
                Assert.IsNotNull(response.ServerSideEncryptionKeyManagementServiceKeyId);
                if (usedKeyId != null)
                    Assert.IsTrue(response.ServerSideEncryptionKeyManagementServiceKeyId.IndexOf(usedKeyId, StringComparison.OrdinalIgnoreCase) >= 0);

                using (var reader = new StreamReader(response.ResponseStream))
                {
                    var data = await reader.ReadToEndAsync().ConfigureAwait(false);
                    VerifyContents(data);
                }
            }
        }
        private async Task VerifyObjectWithTransferUtility(string bucketName)
        {
            var transferUtility = new AsyncTransferUtility(Client);
            var filePath = Path.GetFullPath("downloadedFile.txt");
            await transferUtility.DownloadAsync(new()
            {
                BucketName = bucketName,
                Key = Key,
                FilePath = filePath
            }).ConfigureAwait(false);
            var fileContents = await File.ReadAllTextAsync(filePath).ConfigureAwait(false);
            VerifyContents(fileContents);
        }

        private static void VerifyContents(string contents)
        {
            if (contents.Length == TestContents.Length)
                Assert.IsTrue(string.Equals(TestContents, contents, StringComparison.Ordinal));
            else if (contents.Length == _largeTestContents.Length)
                Assert.IsTrue(string.Equals(_largeTestContents, contents, StringComparison.Ordinal));
            else
                Assert.IsTrue(string.Equals(_fileContents, contents, StringComparison.Ordinal));
        }
        private static void VerifyKeyId(string? suppliedKeyId, string returnedKeyId)
        {
            if (suppliedKeyId != null)
            {
                var index = returnedKeyId.IndexOf(suppliedKeyId, StringComparison.OrdinalIgnoreCase);
                Assert.IsTrue(index >= 0);
            }
        }
    }
}
