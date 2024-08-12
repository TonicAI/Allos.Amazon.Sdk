using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text;
using Amazon.Extensions.S3.Encryption;
using Amazon.Extensions.S3.Encryption.Primitives;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Amazon.Sdk.Fork;
using Amazon.Sdk.S3.Transfer;
using AWSSDK_DotNet.IntegrationTests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AWSSDK_DotNet.IntegrationTests.Tests.S3
{
    [TestClass]
    [AmazonSdkFork("sdk/test/Services/S3/IntegrationTests/EncryptionTests.cs", "AWSSDK_DotNet.IntegrationTests.Tests.S3")]
    public class EncryptionTests : TestBase<AmazonS3Client>
    {
        private const long MegSize = 1048576;
        private const string SampleContent = "Encryption Client Testing!";

        private static readonly byte[] SampleContentBytes = Encoding.UTF8.GetBytes(SampleContent);
        private static readonly string FilePath = Path.Combine(Path.GetTempPath(), "EncryptionPutObjectFile.txt");

        private static readonly Random _random = new();
        private static string? _bucketName;
        
        private static AmazonS3EncryptionClientV2? _s3EncryptionClientMetadataMode;
        private static AmazonS3EncryptionClientV2? _s3EncryptionClientFileMode;
        
        private static string? _basePath; //set by instance property `BasePath` for `ClassCleanup`
        protected override string BasePath
        {
            get
            {
                _basePath = Path.Combine(base.BasePath, nameof(EncryptionTests));
                return _basePath;
            }
        }

        [ClassInitialize]
        public static void Initialize(TestContext a)
        {
            BaseInitialize();
            var encryptionMaterials = new EncryptionMaterialsV2(RSA.Create(), AsymmetricAlgorithmType.RsaOaepSha1);

            AmazonS3CryptoConfigurationV2 config = new(SecurityProfile.V2);

            _s3EncryptionClientMetadataMode = new(config, encryptionMaterials);
            RetryUtilities.ConfigureClient(_s3EncryptionClientMetadataMode);

            config.StorageMode = CryptoStorageMode.InstructionFile;

            _s3EncryptionClientFileMode = new(config, encryptionMaterials);
            RetryUtilities.ConfigureClient(_s3EncryptionClientFileMode);

            using (StreamWriter writer = File.CreateText(FilePath))
            {
                writer.Write(SampleContent);
            }
            _bucketName = S3TestUtils.CreateBucketWithWait(_s3EncryptionClientFileMode);
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            if (ShouldDeleteBucket(_bucketName))
            {
                AmazonS3Util.DeleteS3BucketWithObjectsAsync(_s3EncryptionClientMetadataMode, _bucketName)
                    .ConfigureAwait(false).GetAwaiter().GetResult();
            }

            _s3EncryptionClientMetadataMode?.Dispose();
            _s3EncryptionClientFileMode?.Dispose();
            if (_basePath != null && Directory.Exists(_basePath))
            {
                Directory.Delete(_basePath, true);   
            }
            if (File.Exists(FilePath))
                File.Delete(FilePath);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestTransferUtilityS3EncryptionClientFileMode()
        {
            await TestTransferUtility(BasePath, _s3EncryptionClientFileMode).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestTransferUtilityS3EncryptionClientMetadataMode()
        {
            await TestTransferUtility(BasePath, _s3EncryptionClientMetadataMode).ConfigureAwait(false);
        }

        private static async Task TestTransferUtility(string basePath, AmazonS3EncryptionClientV2? s3EncryptionClient)
        {
            ArgumentNullException.ThrowIfNull(s3EncryptionClient);
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var directory = TransferUtilityTests.CreateTestDirectory(basePath, 10 * TransferUtilityTests.KiloSize);
            var keyPrefix = directory.Name;
            var directoryPath = directory.FullName;

            using (var transferUtility = new TransferUtility(s3EncryptionClient))
            {
                TransferUtilityUploadDirectoryRequest uploadRequest = CreateUploadDirRequest(directoryPath, keyPrefix);
                await transferUtility.UploadDirectoryAsync(uploadRequest).ConfigureAwait(false);

                var newDir = TransferUtilityTests.GenerateDirectoryPath(basePath);
                await transferUtility.DownloadDirectoryAsync(_bucketName, keyPrefix, newDir).ConfigureAwait(false);
                await TransferUtilityTests.ValidateDirectoryContents(s3EncryptionClient, _bucketName, keyPrefix, directory).ConfigureAwait(false);
            }
        }

        private static TransferUtilityUploadDirectoryRequest CreateUploadDirRequest(string directoryPath, string keyPrefix)
        {
            TransferUtilityUploadDirectoryRequest uploadRequest =
                new()
                {
                    BucketName = _bucketName,
                    Directory = directoryPath,
                    KeyPrefix = keyPrefix,
                    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256,
                    SearchOption = SearchOption.AllDirectories,
                    SearchPattern = "*"
                };
            return uploadRequest;
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task PutGetFileUsingMetadataMode()
        {
            await TestPutGet(_s3EncryptionClientMetadataMode, FilePath, null, null, null, SampleContent).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task PutGetFileUsingInstructionFileMode()
        {
            await TestPutGet(_s3EncryptionClientFileMode, FilePath, null, null, null, SampleContent).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task PutGetStreamUsingMetadataMode()
        {
            await TestPutGet(_s3EncryptionClientMetadataMode, null, SampleContentBytes, null, null, SampleContent).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task PutGetStreamUsingInstructionFileMode()
        {
            await TestPutGet(_s3EncryptionClientFileMode, null, SampleContentBytes, null, null, SampleContent).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task PutGetContentUsingMetadataMode()
        {
            await TestPutGet(_s3EncryptionClientMetadataMode, null, null, SampleContent, S3CannedACL.AuthenticatedRead, SampleContent).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task PutGetZeroLengthContentUsingMetadataMode()
        {
            await TestPutGet(_s3EncryptionClientMetadataMode, null, null, "", S3CannedACL.AuthenticatedRead, "").ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task PutGetNullContentContentUsingMetadataMode()
        {
            await TestPutGet(_s3EncryptionClientMetadataMode, null, null, null, S3CannedACL.AuthenticatedRead, "").ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task PutGetContentUsingInstructionFileMode()
        {
            await TestPutGet(_s3EncryptionClientFileMode, null, null, SampleContent, S3CannedACL.AuthenticatedRead, SampleContent).ConfigureAwait(false);
        }
        
        [TestMethod]
        [TestCategory("S3")]
        public async Task MultipartEncryptionTestMetadataMode()
        {
            await MultipartEncryptionTest(_s3EncryptionClientMetadataMode).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task MultipartEncryptionTestInstructionFile()
        {
            await MultipartEncryptionTest(_s3EncryptionClientFileMode).ConfigureAwait(false);
        }

        private async Task MultipartEncryptionTest(AmazonS3EncryptionClientV2? s3EncryptionClient)
        {
            ArgumentNullException.ThrowIfNull(s3EncryptionClient);
            
            var nextRandom = _random.Next();
            var filePath = Path.Combine(Path.GetTempPath(), "multi-" + nextRandom + ".txt");
            var retrievedFilepath = Path.Combine(Path.GetTempPath(), "retreived-" + nextRandom + ".txt");
            var totalSize = MegSize * 15;

            UtilityMethods.GenerateFile(filePath, totalSize);
            string key = "key-" + _random.Next();

            Stream inputStream = File.OpenRead(filePath);
            try
            {
                InitiateMultipartUploadRequest initRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    StorageClass = S3StorageClass.ReducedRedundancy,
                    ContentType = "text/html",
                    CannedACL = S3CannedACL.PublicRead
                };

                InitiateMultipartUploadResponse initResponse = await s3EncryptionClient.InitiateMultipartUploadAsync(initRequest).ConfigureAwait(false);

                // Upload part 1
                UploadPartRequest uploadRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId,
                    PartNumber = 1,
                    PartSize = 5 * MegSize,
                    InputStream = inputStream,
                };

                UploadPartResponse up1Response = await s3EncryptionClient.UploadPartAsync(uploadRequest).ConfigureAwait(false);

                // Upload part 2
                uploadRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId,
                    PartNumber = 2,
                    PartSize = 5 * MegSize,
                    InputStream = inputStream,
                };

                UploadPartResponse up2Response = await s3EncryptionClient.UploadPartAsync(uploadRequest).ConfigureAwait(false);

                // Upload part 3
                uploadRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId,
                    PartNumber = 3,
                    InputStream = inputStream,
                    IsLastPart = true
                };

                UploadPartResponse up3Response = await s3EncryptionClient.UploadPartAsync(uploadRequest).ConfigureAwait(false);

                ListPartsRequest listPartRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId
                };

                ListPartsResponse listPartResponse = await s3EncryptionClient.ListPartsAsync(listPartRequest).ConfigureAwait(false);
                Assert.AreEqual(3, listPartResponse.Parts.Count);
                Assert.AreEqual(up1Response.PartNumber, listPartResponse.Parts[0].PartNumber);
                Assert.AreEqual(up1Response.ETag, listPartResponse.Parts[0].ETag);
                Assert.AreEqual(up2Response.PartNumber, listPartResponse.Parts[1].PartNumber);
                Assert.AreEqual(up2Response.ETag, listPartResponse.Parts[1].ETag);
                Assert.AreEqual(up3Response.PartNumber, listPartResponse.Parts[2].PartNumber);
                Assert.AreEqual(up3Response.ETag, listPartResponse.Parts[2].ETag);

                listPartRequest.MaxParts = 1;
                listPartResponse = await s3EncryptionClient.ListPartsAsync(listPartRequest).ConfigureAwait(false);
                Assert.AreEqual(1, listPartResponse.Parts.Count);

                // Complete the response
                CompleteMultipartUploadRequest compRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId
                };
                compRequest.AddPartETags(up1Response, up2Response, up3Response);

                CompleteMultipartUploadResponse compResponse = await s3EncryptionClient.CompleteMultipartUploadAsync(compRequest).ConfigureAwait(false);
                Assert.AreEqual(_bucketName, compResponse.BucketName);
                Assert.IsNotNull(compResponse.ETag);
                Assert.AreEqual(key, compResponse.Key);
                Assert.IsNotNull(compResponse.Location);

                // Get the file back from S3 and make sure it is still the same.
                GetObjectRequest getRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key
                };

                GetObjectResponse getResponse = await s3EncryptionClient.GetObjectAsync(getRequest).ConfigureAwait(false);
                await getResponse.WriteResponseStreamToFileAsync(retrievedFilepath, append: false, CancellationToken.None).ConfigureAwait(false);

                UtilityMethods.CompareFiles(filePath, retrievedFilepath);

                GetObjectMetadataRequest metaDataRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key
                };
                GetObjectMetadataResponse metaDataResponse = await s3EncryptionClient.GetObjectMetadataAsync(metaDataRequest).ConfigureAwait(false);
                Assert.AreEqual("text/html", metaDataResponse.Headers.ContentType);
            }
            finally
            {
                inputStream.Close();
                if (File.Exists(filePath))
                    File.Delete(filePath);
                if (File.Exists(retrievedFilepath))
                    File.Delete(retrievedFilepath);
            }
            // run the async version of the same test
            await WaitForAsyncTask(MultipartEncryptionTestAsync(s3EncryptionClient)).ConfigureAwait(false);
        }

        private static async Task TestPutGet(AmazonS3EncryptionClientV2? s3EncryptionClient,
            string? filePath, byte[]? inputStreamBytes, string? contentBody, S3CannedACL? cannedAcl, string expectedContent)
        {
            ArgumentNullException.ThrowIfNull(s3EncryptionClient);
            
            PutObjectRequest request = new()
            {
                BucketName = _bucketName,
                Key = "key-" + _random.Next(),
                FilePath = filePath,
                InputStream = inputStreamBytes == null ? null : new MemoryStream(inputStreamBytes),
                ContentBody = contentBody,
                CannedACL = cannedAcl
            };

            _ = await s3EncryptionClient.PutObjectAsync(request).ConfigureAwait(false);
            await TestGet(request.Key, expectedContent, s3EncryptionClient).ConfigureAwait(false);

            // run the async version of the same test
            await WaitForAsyncTask(TestPutGetAsync(s3EncryptionClient, filePath, inputStreamBytes, contentBody, cannedAcl, expectedContent)).ConfigureAwait(false);
        }

        private static async Task TestGet(string key, string uploadedData, AmazonS3EncryptionClientV2 s3EncryptionClient)
        {
            GetObjectRequest getObjectRequest = new()
            {
                BucketName = _bucketName,
                Key = key
            };

            using (GetObjectResponse getObjectResponse = await s3EncryptionClient.GetObjectAsync(getObjectRequest).ConfigureAwait(false))
            await using (var stream = getObjectResponse.ResponseStream)
            using (var reader = new StreamReader(stream))
            {
                string data = await reader.ReadToEndAsync().ConfigureAwait(false);
                Assert.AreEqual(uploadedData, data);
            }
        }

        private static async Task WaitForAsyncTask(Task asyncTask)
        {
            try
            {
                await asyncTask;
            }
            catch (AggregateException e)
            {
                if (e.InnerException != null)
                {
                    ExceptionDispatchInfo.Capture(e.InnerException).Throw();
                }

                throw;
            }
        }

        private async Task MultipartEncryptionTestAsync(AmazonS3EncryptionClientV2 s3EncryptionClient)
        {
            var nextRandom = _random.Next();
            var filePath =  Path.Combine(BasePath, $"multi-{nextRandom}.txt");
            var retrievedFilepath = Path.Combine(BasePath, $"retreived-{nextRandom}.txt"); 
            var totalSize = MegSize * 15;

            UtilityMethods.GenerateFile(filePath, totalSize);
            string key = "key-" + _random.Next();

            Stream inputStream = File.OpenRead(filePath);
            try
            {
                InitiateMultipartUploadRequest initRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    StorageClass = S3StorageClass.ReducedRedundancy,
                    ContentType = "text/html",
                    CannedACL = S3CannedACL.PublicRead
                };

                InitiateMultipartUploadResponse initResponse =
                    await s3EncryptionClient.InitiateMultipartUploadAsync(initRequest).ConfigureAwait(false);

                // Upload part 1
                UploadPartRequest uploadRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId,
                    PartNumber = 1,
                    PartSize = 5 * MegSize,
                    InputStream = inputStream,
                };

                UploadPartResponse up1Response = await s3EncryptionClient.UploadPartAsync(uploadRequest).ConfigureAwait(false);

                // Upload part 2
                uploadRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId,
                    PartNumber = 2,
                    PartSize = 5 * MegSize,
                    InputStream = inputStream,
                };

                UploadPartResponse up2Response = await s3EncryptionClient.UploadPartAsync(uploadRequest).ConfigureAwait(false);

                // Upload part 3
                uploadRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId,
                    PartNumber = 3,
                    InputStream = inputStream,
                    IsLastPart = true
                };

                UploadPartResponse up3Response = await s3EncryptionClient.UploadPartAsync(uploadRequest).ConfigureAwait(false);

                ListPartsRequest listPartRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId
                };

                ListPartsResponse listPartResponse = await s3EncryptionClient.ListPartsAsync(listPartRequest).ConfigureAwait(false);
                Assert.AreEqual(3, listPartResponse.Parts.Count);
                Assert.AreEqual(up1Response.PartNumber, listPartResponse.Parts[0].PartNumber);
                Assert.AreEqual(up1Response.ETag, listPartResponse.Parts[0].ETag);
                Assert.AreEqual(up2Response.PartNumber, listPartResponse.Parts[1].PartNumber);
                Assert.AreEqual(up2Response.ETag, listPartResponse.Parts[1].ETag);
                Assert.AreEqual(up3Response.PartNumber, listPartResponse.Parts[2].PartNumber);
                Assert.AreEqual(up3Response.ETag, listPartResponse.Parts[2].ETag);

                listPartRequest.MaxParts = 1;
                listPartResponse = await s3EncryptionClient.ListPartsAsync(listPartRequest).ConfigureAwait(false);
                Assert.AreEqual(1, listPartResponse.Parts.Count);

                // Complete the response
                CompleteMultipartUploadRequest compRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    UploadId = initResponse.UploadId
                };
                compRequest.AddPartETags(up1Response, up2Response, up3Response);

                CompleteMultipartUploadResponse compResponse =
                    await s3EncryptionClient.CompleteMultipartUploadAsync(compRequest).ConfigureAwait(false);
                Assert.AreEqual(_bucketName, compResponse.BucketName);
                Assert.IsNotNull(compResponse.ETag);
                Assert.AreEqual(key, compResponse.Key);
                Assert.IsNotNull(compResponse.Location);

                // Get the file back from S3 and make sure it is still the same.
                GetObjectRequest getRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key
                };

                GetObjectResponse getResponse =
                    await s3EncryptionClient.GetObjectAsync(getRequest).ConfigureAwait(false);
                await getResponse.WriteResponseStreamToFileAsync(retrievedFilepath, append: false, CancellationToken.None).ConfigureAwait(false);

                UtilityMethods.CompareFiles(filePath, retrievedFilepath);

                GetObjectMetadataRequest metaDataRequest = new()
                {
                    BucketName = _bucketName,
                    Key = key
                };
                GetObjectMetadataResponse metaDataResponse =
                    await s3EncryptionClient.GetObjectMetadataAsync(metaDataRequest).ConfigureAwait(false);
                Assert.AreEqual("text/html", metaDataResponse.Headers.ContentType);
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

        private static async Task TestPutGetAsync(AmazonS3EncryptionClientV2 s3EncryptionClient,
            string? filePath, byte[]? inputStreamBytes, string? contentBody, S3CannedACL? cannedAcl, string expectedContent)
        {
            PutObjectRequest request = new()
            {
                BucketName = _bucketName,
                Key = "key-" + _random.Next(),
                FilePath = filePath,
                InputStream = inputStreamBytes == null ? null : new MemoryStream(inputStreamBytes),
                ContentBody = contentBody,
                CannedACL = cannedAcl
            };
            _ = await s3EncryptionClient.PutObjectAsync(request).ConfigureAwait(false);
            await TestGetAsync(request.Key, expectedContent, s3EncryptionClient).ConfigureAwait(false);
        }

        private static async Task TestGetAsync(string key, string uploadedData, AmazonS3EncryptionClientV2 s3EncryptionClient)
        {
            GetObjectRequest getObjectRequest = new()
            {
                BucketName = _bucketName,
                Key = key
            };

            using (GetObjectResponse getObjectResponse = await s3EncryptionClient.GetObjectAsync(getObjectRequest).ConfigureAwait(false))
            {
                await using (var stream = getObjectResponse.ResponseStream)
                using (var reader = new StreamReader(stream))
                {
                    string data = await reader.ReadToEndAsync().ConfigureAwait(false);
                    Assert.AreEqual(uploadedData, data);
                }
            }
        }
    }
}
