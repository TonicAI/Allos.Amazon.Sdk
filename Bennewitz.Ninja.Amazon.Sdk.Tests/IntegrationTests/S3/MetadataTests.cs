using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Amazon.Sdk.Fork;
using Amazon.Sdk.S3.Transfer;
using AWSSDK_DotNet.IntegrationTests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AmazonS3Client = Amazon.S3.AmazonS3Client;

namespace AWSSDK_DotNet.IntegrationTests.Tests.S3
{
    [TestClass]
    [AmazonSdkFork("sdk/test/Services/S3/IntegrationTests/MetadataTests.cs", "AWSSDK_DotNet.IntegrationTests.Tests.S3")]
    public class MetadataTests : TestBase<AmazonS3Client>
    {
        private readonly Random _random = new();
        private static string? _bucketName;
        private static readonly Dictionary<string, string?> Metadata = new(StringComparer.Ordinal)
        {
            { "date", DateTime.Now.ToFileTime().ToString() },
            { "test", "true" },
            { "null-value", null },
            { "aaa", "aaa" },
            { "a-a-a", "adada" },
            { "a|a|a", "apapa" },
            { "a^a^a", "acaca" },
            { "a_a_a", "apapa" },
            { "a~a~a", "apapa" }
        };
        private static readonly Dictionary<string, string?> UnicodeMetadata = new(StringComparer.Ordinal)
        {
            { "test", "test" },
            { "test2", "£" },
            { "test3", "no non ascii characters %" },
            { "test4", "1 non ascii character £ %" }
        };
        private static readonly Dictionary<string, string> Headers = new(StringComparer.Ordinal)
        {
            { "Content-Type", "text/html" },
            { "Content-Disposition", "attachment; filename=\"fname.ext\"" }
        };
        private const string TempFile = "tempFile.txt";
        private static readonly long _smallFileSize = TransferUtilityTests.KiloSize * 100;
        private static readonly long _largeFileSize = TransferUtilityTests.MegSize * 20;
        private static readonly string _basePath = Path.GetFullPath(@"\transferutility\");

        private static readonly List<string> _keysToValidate = new();

        [ClassInitialize]
        public static void Initialize(TestContext a)
        {
            BaseInitialize();
            _bucketName = S3TestUtils.CreateBucketWithWait(Client);
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            if (ShouldDeleteBucket(_bucketName))
            {
                AmazonS3Util.DeleteS3BucketWithObjectsAsync(Client, _bucketName).ConfigureAwait(false).GetAwaiter().GetResult();    
            }
            if (Directory.Exists(_basePath))
                Directory.Delete(_basePath, true);

            BaseClean();
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestSingleUploads()
        {
            await TestSingleUploadsHelper(Client).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestSingleUploadsSigV2()
        {
            var client = new AmazonS3Client(new AmazonS3Config { SignatureVersion = "2" });
            await TestSingleUploadsHelper(client).ConfigureAwait(false);
            client.Dispose();
        }

        private async Task TestSingleUploadsHelper(AmazonS3Client client)
        {
            // Test simple PutObject upload
            var key = "contentBodyPut" + _random.Next();
            PutObjectRequest putObjectRequest = new()
            {
                BucketName = _bucketName,
                Key = key,
                ContentBody = "This is the content body!",
            };

            SetMetadataAndHeaders(putObjectRequest);
            await client.PutObjectAsync(putObjectRequest).ConfigureAwait(false);
            await ValidateObjectMetadataAndHeaders(key).ConfigureAwait(false);

            using (var tu = new TransferUtility(client))
            {
                // Test small TransferUtility upload
                key = "transferUtilitySmall" + _random.Next();
                UtilityMethods.GenerateFile(TempFile, _smallFileSize);
                var smallRequest = new TransferUtilityUploadRequest
                {
                    BucketName = _bucketName,
                    Key = key,
                    FilePath = TempFile
                };
                SetMetadataAndHeaders(smallRequest);
                await tu.UploadAsync(smallRequest).ConfigureAwait(false);
                await ValidateObjectMetadataAndHeaders(key).ConfigureAwait(false);

                // Test large TransferUtility upload
                // disable clock skew testing, this is a multithreaded operation
                using (RetryUtilities.DisableClockSkewCorrection())
                {
                    key = "transferUtilityLarge" + _random.Next();
                    UtilityMethods.GenerateFile(TempFile, _largeFileSize);
                    var largeRequest = new TransferUtilityUploadRequest
                    {
                        BucketName = _bucketName,
                        Key = key,
                        FilePath = TempFile
                    };
                    SetMetadataAndHeaders(largeRequest);
                    await tu.UploadAsync(largeRequest).ConfigureAwait(false);
                    await ValidateObjectMetadataAndHeaders(key).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Ensure that when escaped, a SigV4 request with unicode metadata succeeds.
        /// </summary>
        [TestMethod]
        [TestCategory("S3")]
        public async Task TestSingleUploadWithUnicodeMetadata()
        {
            await TestSingleUploadWithUnicodeMetadataHelper(Client).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensure that when escaped, a SigV2 request with unicode metadata succeeds
        /// </summary>
        [TestMethod]
        public async Task TestSingleUploadWithUnicodeMetadataSigV2()
        {
            var client = new AmazonS3Client(new AmazonS3Config { SignatureVersion = "2" });
            await TestSingleUploadWithUnicodeMetadataHelper(client).ConfigureAwait(false);
            client.Dispose();
        }

        private async Task TestSingleUploadWithUnicodeMetadataHelper(AmazonS3Client client)
        {
            // Test simple PutObject upload
            AWSConfigsS3.EnableUnicodeEncodingForObjectMetadata = true;
            var key = "contentBodyPut" + _random.Next();
            PutObjectRequest putObjectRequest = new()
            {
                BucketName = _bucketName,
                Key = key,
                ContentBody = "This is the content body!",
            };

            SetMetadataAndHeaders(putObjectRequest, true);
            await client.PutObjectAsync(putObjectRequest).ConfigureAwait(false);
            await ValidateObjectMetadataAndHeaders(key, true).ConfigureAwait(false);
            AWSConfigsS3.EnableUnicodeEncodingForObjectMetadata = false;
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestDirectoryUploads()
        {
            var progressValidator = new TransferUtilityTests.DirectoryProgressValidator<UploadDirectoryProgressArgs>();
            TransferUtilityTests.ConfigureProgressValidator(progressValidator);

            _keysToValidate.Clear();
            await UploadDirectory(10 * TransferUtilityTests.MegSize, progressValidator, validate: true).ConfigureAwait(false);
            progressValidator.AssertOnCompletion();

            foreach (var key in _keysToValidate)
                await ValidateObjectMetadataAndHeaders(key).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task TestSingleUploadWithSpacesInMetadata()
        {
            string metadataName = "document";
            string metadataValue = " A  B  C  ";
            // Test simple PutObject upload
            var key = "contentBodyPut" + _random.Next();
            PutObjectRequest putObjectRequest = new()
            {
                BucketName = _bucketName,
                Key = key,
                ContentBody = "This is the content body!",
            };

            putObjectRequest.Metadata[metadataName] = metadataValue;

            await Client.PutObjectAsync(putObjectRequest).ConfigureAwait(false);
            using (var response = await Client.GetObjectAsync(_bucketName, key).ConfigureAwait(false)) // Validate metadata
            {
                Assert.AreEqual(metadataValue.Trim(), response.Metadata[metadataName]);
            }

            using (var tu = new TransferUtility(Client))
            {
                // Test small TransferUtility upload
                key = "transferUtilitySmall" + _random.Next();
                UtilityMethods.GenerateFile(TempFile, _smallFileSize);
                var smallRequest = new TransferUtilityUploadRequest
                {
                    BucketName = _bucketName,
                    Key = key,
                    FilePath = TempFile
                };

                smallRequest.Metadata[metadataName] = metadataValue;

                await tu.UploadAsync(smallRequest).ConfigureAwait(false);
                using (var response = await Client.GetObjectAsync(_bucketName, key).ConfigureAwait(false)) // Validate metadata
                {
                    Assert.AreEqual(metadataValue.Trim(), response.Metadata[metadataName]);
                }

                // Test large TransferUtility upload
                // disable clock skew testing, this is a multithreaded operation
                using (RetryUtilities.DisableClockSkewCorrection())
                {
                    key = "transferUtilityLarge" + _random.Next();
                    UtilityMethods.GenerateFile(TempFile, _largeFileSize);
                    var largeRequest = new TransferUtilityUploadRequest
                    {
                        BucketName = _bucketName,
                        Key = key,
                        FilePath = TempFile
                    };
                    largeRequest.Metadata[metadataName] = metadataValue;

                    await tu.UploadAsync(largeRequest).ConfigureAwait(false);
                    using (var response = await Client.GetObjectAsync(_bucketName, key).ConfigureAwait(false)) // Validate metadata
                    {
                        Assert.AreEqual(metadataValue.Trim(), response.Metadata[metadataName]);
                    }
                }
            }
        }

        private async Task UploadDirectory(long size, TransferUtilityTests.DirectoryProgressValidator<UploadDirectoryProgressArgs> progressValidator, bool validate = true)
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var directory = TransferUtilityTests.CreateTestDirectory(BasePath, size);
            var directoryPath = directory.FullName;
            var keyPrefix = directory.Name;

            var config = new TransferUtilityConfig
            {
                ConcurrentServiceRequests = 10,
            };
            var transferUtility = new TransferUtility(Client, config);
            var request = new TransferUtilityUploadDirectoryRequest
            {
                BucketName = _bucketName,
                Directory = directoryPath,
                KeyPrefix = keyPrefix,
                SearchPattern = "*",
                SearchOption = SearchOption.AllDirectories,
            };

            request.UploadDirectoryProgressEvent += progressValidator.OnProgressEvent;

            HashSet<string> files = new();
            request.UploadDirectoryProgressEvent += (_, e) =>
            {
                ArgumentNullException.ThrowIfNull(e.CurrentFile);
                files.Add(e.CurrentFile);
                Console.WriteLine("Progress callback = " + e);
            };
            request.UploadDirectoryFileRequestEvent += (_, e) =>
            {
                var uploadRequest =e.UploadRequest;
                var key = uploadRequest.Key;
                
                ArgumentNullException.ThrowIfNull(key);
                _keysToValidate.Add(key);
                SetMetadataAndHeaders(uploadRequest);
            };

            await transferUtility.UploadDirectoryAsync(request).ConfigureAwait(false);

            Assert.AreEqual(5, files.Count);

            if (validate)
                await TransferUtilityTests.ValidateDirectoryContents(Client, _bucketName, keyPrefix, directory).ConfigureAwait(false);
        }

        private static async Task ValidateObjectMetadataAndHeaders(string key, bool unicode = false)
        {
            using (var response = await Client.GetObjectAsync(_bucketName, key).ConfigureAwait(false))
            {
                ValidateMetadataAndHeaders(response, unicode);
            }
        }

        private static void SetMetadataAndHeaders(TransferUtilityUploadRequest request)
        {
            SetMetadata(request.Metadata);
            SetHeaders(request.Headers);
        }
        private static void SetMetadataAndHeaders(PutObjectRequest request, bool unicode = false)
        {
            SetMetadata(request.Metadata, unicode);
            SetHeaders(request.Headers);
        }
        private static void SetMetadata(MetadataCollection mc, bool unicode = false)
        {
            foreach (var kvp in unicode ? UnicodeMetadata : Metadata)
                mc[kvp.Key] = kvp.Value;
        }
        private static void SetHeaders(HeadersCollection hc)
        {
            foreach (var kvp in Headers)
                hc[kvp.Key] = kvp.Value;
        }
        private static void ValidateMetadataAndHeaders(GetObjectResponse response, bool unicode = false)
        {
            foreach (var kvp in unicode ? UnicodeMetadata : Metadata)
            {
                var name = kvp.Key;
                var expectedValue = kvp.Value ?? string.Empty;   // putting a null value comes back as an empty string
                var actualValue = response.Metadata[name];
                Assert.AreEqual(expectedValue, actualValue);
            }

            foreach (var kvp in Headers)
            {
                var name = kvp.Key;
                var expectedValue = kvp.Value;
                var actualValue = response.Headers[name];
                Assert.AreEqual(expectedValue, actualValue);
            }
        }
    }
}
