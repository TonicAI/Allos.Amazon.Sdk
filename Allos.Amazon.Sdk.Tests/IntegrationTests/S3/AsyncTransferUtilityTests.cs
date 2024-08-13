using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using Allos.Amazon.Sdk.Fork;
using Allos.Amazon.Sdk.S3.Transfer;
using Allos.Amazon.Sdk.S3.Util;
using Allos.Amazon.Sdk.Tests.IntegrationTests.Utils;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AmazonS3Client = Amazon.S3.AmazonS3Client;

namespace Allos.Amazon.Sdk.Tests.IntegrationTests.Tests.S3
{
    [TestClass]
    [AmazonSdkFork("sdk/test/Services/S3/IntegrationTests/TransferUtilityTests.cs", "AWSSDK_DotNet.IntegrationTests.Tests.S3")]
    public class AsyncTransferUtilityTests : TestBase<AmazonS3Client>
    {
        public static readonly ulong MegSize = (uint)Math.Pow(2, 20);
        public static readonly ulong KiloSize = (uint)Math.Pow(2, 10);

        private static string? _basePath; //set by instance property `BasePath` for `ClassCleanup`
        protected override string BasePath
        {
            get
            {
                _basePath = Path.Combine(base.BasePath, nameof(AsyncTransferUtilityTests));
                return _basePath;
            }
        }

        private static string? _bucketName;
        private static string? _ssecBucketName;
        private static readonly string _octetStreamContentType = "application/octet-stream";
        private static readonly string _plainTextContentType = "text/plain";
        private static string? _fullPath;
        private const string TestContent = "This is the content body!";
        private const string TestFile = "PutObjectFile.txt";

        [ClassInitialize]
        public static void ClassInitialize(TestContext a)
        {
            BaseInitialize();
            // Create standard bucket for operations
            _bucketName = S3TestUtils.CreateBucketWithWait(Client);

            // Create a bucket specifically for the SSE-C tests as a bucket policy has to be set on it to require SSE-C.
            _ssecBucketName = S3TestUtils.CreateBucketWithWait(Client, createForSse: true);
            // Apply the bucket policy to SSE-C: https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html
            Client.PutBucketPolicyAsync(new()
            {
                Policy =
                @"{
                    ""Version"": ""2012-10-17"",
                    ""Id"": ""PutObjectPolicy"",
                    ""Statement"": [
                        {
                            ""Sid"": ""RequireSSECObjectUploads"",
                            ""Effect"": ""Deny"",
                            ""Principal"": ""*"",
                            ""Action"": ""s3:PutObject"",
                            ""Resource"": ""arn:aws:s3:::" + _ssecBucketName + @"/*"",
                            ""Condition"": {
                                ""Null"": {
                                    ""s3:x-amz-server-side-encryption-customer-algorithm"": ""true""
                                }
                            }
                        }
                    ]
                }",
                BucketName = _ssecBucketName
            }).ConfigureAwait(false).GetAwaiter().GetResult();

            _fullPath = Path.GetFullPath(TestFile);
            File.WriteAllText(_fullPath, TestContent);
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            if (_bucketName != null && ShouldDeleteBucket(_bucketName))
            {
                AmazonS3Util.DeleteS3BucketWithObjectsAsync(Client, _bucketName).ConfigureAwait(false).GetAwaiter().GetResult();    
            }
            if (_ssecBucketName != null && ShouldDeleteBucket(_ssecBucketName))
            {
                AmazonS3Util.DeleteS3BucketWithObjectsAsync(Client, _ssecBucketName).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            BaseClean();
            if (Directory.Exists(_basePath))
            {
                Directory.Delete(_basePath, true);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task SimpleUploadTest()
        {
            var fileName = UtilityMethods.GenerateName(@"SimpleUploadTest\SmallFile");
            await Upload(fileName, 10 * MegSize, null).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task SimpleUploadProgressTest()
        {
            var fileName = UtilityMethods.GenerateName(@"SimpleUploadTest\SmallFile");
            var progressValidator = new UploadProgressValidator<UploadProgressArgs>
            {
                Validate = p =>
                {
                    Assert.AreEqual(p.FilePath, Path.Combine(BasePath, fileName));
                }
            };
            await Upload(fileName, 10U * MegSize, progressValidator).ConfigureAwait(false);
            progressValidator.AssertOnCompletion();
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task SimpleUpload()
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(_fullPath);
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var client = Client;
            using (var tu = new AsyncTransferUtility(client))
            {
                await tu.UploadAsync(_fullPath, _bucketName).ConfigureAwait(false);

                var response = await client.GetObjectMetadataAsync(new()
                {
                    BucketName = _bucketName,
                    Key = TestFile
                }).ConfigureAwait(false);
                Assert.IsTrue(response.ETag.Length > 0);

                var downloadPath = _fullPath + ".download";
                var downloadRequest = new DownloadRequest
                {
                    BucketName = _bucketName,
                    Key = TestFile,
                    FilePath = downloadPath
                };
                await tu.DownloadAsync(downloadRequest).ConfigureAwait(false);
                TestDownloadedFile(downloadPath);

                // empty out file, except for 1 byte
                await File.WriteAllTextAsync(downloadPath, TestContent.Substring(0,1)).ConfigureAwait(false);
                Assert.IsTrue(File.Exists(downloadPath));
                await tu.DownloadAsync(downloadRequest).ConfigureAwait(false);
                TestDownloadedFile(downloadPath);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task UploadUnSeekableStreamTest()
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var client = Client;
            var fileName = UtilityMethods.GenerateName(@"SimpleUploadTest\SmallFile");
            var path = Path.Combine(BasePath, fileName);
            var fileSize = 20 * MegSize;
            UtilityMethods.GenerateFile(path, fileSize);
            //take the generated file and turn it into an unseekable stream
            
            var stream = GenerateUnseekableStreamFromFile(path);
            using (var tu = new AsyncTransferUtility(client))
            {
                await tu.UploadAsync(stream, _bucketName, fileName).ConfigureAwait(false);

                var metadata = await Client.GetObjectMetadataAsync(new()
                {
                    BucketName = _bucketName,
                    Key = fileName
                }).ConfigureAwait(false);
                Assert.AreEqual(fileSize, metadata.ContentLength.ToUInt64());

                //Download the file and validate content of downloaded file is equal.
                var downloadPath = path + ".download";
                var downloadRequest = new DownloadRequest
                {
                    BucketName = _bucketName,
                    Key = fileName,
                    FilePath = downloadPath
                };
                await tu.DownloadAsync(downloadRequest).ConfigureAwait(false);
                UtilityMethods.CompareFiles(path, downloadPath);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task UploadUnSeekableStreamFileSizeSmallerThanMinPartTest()
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var client = Client;
            var fileName = UtilityMethods.GenerateName(@"SimpleUploadTest\SmallerThanMinPart");
            var path = Path.Combine(BasePath, fileName);
            var fileSize = 4 * MegSize;
            UtilityMethods.GenerateFile(path, fileSize);
            //take the generated file and turn it into an unseekable stream

            var stream = GenerateUnseekableStreamFromFile(path);
            using (var tu = new AsyncTransferUtility(client))
            {
                await tu.UploadAsync(stream, _bucketName, fileName).ConfigureAwait(false);

                var metadata = await Client.GetObjectMetadataAsync(new()
                {
                    BucketName = _bucketName,
                    Key = fileName
                }).ConfigureAwait(false);
                Assert.AreEqual(fileSize, metadata.ContentLength.ToUInt64());

                //Download the file and validate content of downloaded file is equal.
                var downloadPath = path + ".download";
                var downloadRequest = new DownloadRequest
                {
                    BucketName = _bucketName,
                    Key = fileName,
                    FilePath = downloadPath
                };
                await tu.DownloadAsync(downloadRequest).ConfigureAwait(false);
                UtilityMethods.CompareFiles(path, downloadPath);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task UploadUnSeekableStreamFileSizeEqualToMinPartTest()
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var client = Client;
            var fileName = UtilityMethods.GenerateName(@"SimpleUploadTest\EqualToMinPartSize");
            var path = Path.Combine(BasePath, fileName);
            var fileSize = 5 * MegSize;

            UtilityMethods.GenerateFile(path, fileSize);
            //take the generated file and turn it into an unseekable stream

            var stream = GenerateUnseekableStreamFromFile(path);
            using (var tu = new AsyncTransferUtility(client))
            {
                await tu.UploadAsync(stream, _bucketName, fileName).ConfigureAwait(false);

                var metadata = await Client.GetObjectMetadataAsync(new()
                {
                    BucketName = _bucketName,
                    Key = fileName
                }).ConfigureAwait(false);
                Assert.AreEqual(fileSize, metadata.ContentLength.ToUInt64());

                //Download the file and validate content of downloaded file is equal.
                var downloadPath = path + ".download";
                var downloadRequest = new DownloadRequest
                {
                    BucketName = _bucketName,
                    Key = fileName,
                    FilePath = downloadPath
                };
                await tu.DownloadAsync(downloadRequest).ConfigureAwait(false);
                UtilityMethods.CompareFiles(path, downloadPath);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task UploadUnSeekableStreamFileSizeEqualToPartBufferSize()
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var client = Client;
            var fileName = UtilityMethods.GenerateName(@"SimpleUploadTest\EqualToPartBufferSize");
            var path = Path.Combine(BasePath, fileName);
            var fileSize = 5 * MegSize + 8192;

            UtilityMethods.GenerateFile(path, fileSize);
            //take the generated file and turn it into an unseekable stream

            var stream = GenerateUnseekableStreamFromFile(path);
            using (var tu = new AsyncTransferUtility(client))
            {
                await tu.UploadAsync(stream, _bucketName, fileName).ConfigureAwait(false);

                var metadata = await Client.GetObjectMetadataAsync(new()
                {
                    BucketName = _bucketName,
                    Key = fileName
                }).ConfigureAwait(false);
                Assert.AreEqual(fileSize, metadata.ContentLength.ToUInt64());

                //Download the file and validate content of downloaded file is equal.
                var downloadPath = path + ".download";
                var downloadRequest = new DownloadRequest
                {
                    BucketName = _bucketName,
                    Key = fileName,
                    FilePath = downloadPath
                };
                await tu.DownloadAsync(downloadRequest).ConfigureAwait(false);
                UtilityMethods.CompareFiles(path, downloadPath);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task UploadUnseekableStreamFileSizeBetweenMinPartSizeAndPartBufferSize()
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var client = Client;
            var fileName = UtilityMethods.GenerateName(@"SimpleUploadTest\BetweenMinPartSizeAndPartBufferSize");
            var path = Path.Combine(BasePath, fileName);
            // there was a bug where the transfer utility was uploading 13MB file
            // when the file size was between 5MB and (5MB + 8192). 8192 is the s3Client.Config.BufferSize
            var fileSize = 5 * MegSize + 1;

            UtilityMethods.GenerateFile(path, fileSize);
            //take the generated file and turn it into an unseekable stream

            var stream = GenerateUnseekableStreamFromFile(path);
            using (var tu = new AsyncTransferUtility(client))
            {
                await tu.UploadAsync(stream, _bucketName, fileName).ConfigureAwait(false);

                var metadata = await Client.GetObjectMetadataAsync(new()
                {
                    BucketName = _bucketName,
                    Key = fileName
                }).ConfigureAwait(false);
                Assert.AreEqual(fileSize, metadata.ContentLength.ToUInt64());

                //Download the file and validate content of downloaded file is equal.
                var downloadPath = path + ".download";
                var downloadRequest = new DownloadRequest
                {
                    BucketName = _bucketName,
                    Key = fileName,
                    FilePath = downloadPath
                };
                await tu.DownloadAsync(downloadRequest).ConfigureAwait(false);
                UtilityMethods.CompareFiles(path, downloadPath);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task UploadUnSeekableStreamWithZeroLengthTest()
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            const long zeroFileSize = 0;
            var client = Client;
            var key = UtilityMethods.GenerateName(@"SimpleUploadTest\EmptyFile");

            var stream = new UnseekableStream(setZeroLengthStream: true);
            using (var tu = new AsyncTransferUtility(client))
            {
                await tu.UploadAsync(stream, _bucketName, key).ConfigureAwait(false);

                var metadata = await Client.GetObjectMetadataAsync(new()
                {
                    BucketName = _bucketName,
                    Key = key
                }).ConfigureAwait(false);
                Assert.AreEqual(zeroFileSize, metadata.ContentLength);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task UploadUnSeekableStreamTestWithEmptyFile()
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var client = Client;
            var fileName = UtilityMethods.GenerateName(@"UnSeekableStream\EmptyFile");
            var path = Path.Combine(BasePath, fileName);
            var fileSize = 0;
            UtilityMethods.GenerateFile(path, fileSize.ToUInt32());
            //take the generated file and turn it into an unseekable stream

            var stream = GenerateUnseekableStreamFromFile(path);
            using (var tu = new AsyncTransferUtility(client))
            {
                await tu.UploadAsync(stream, _bucketName, fileName).ConfigureAwait(false);

                var metadata = await Client.GetObjectMetadataAsync(new()
                {
                    BucketName = _bucketName,
                    Key = fileName
                }).ConfigureAwait(false);
                Assert.AreEqual(fileSize, metadata.ContentLength);
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task UploadUnSeekableStreamWithMetadataAndHeadersTest()
        {
            var client = Client;
            var fileName = UtilityMethods.GenerateName(@"SimpleUploadTest\SmallFile");
            var path = Path.Combine(BasePath, fileName);
            var fileSize = 20 * MegSize;
            UtilityMethods.GenerateFile(path, fileSize);
            //take the generated file and turn it into an unseekable stream

            var stream = GenerateUnseekableStreamFromFile(path);
            using (var tu = new AsyncTransferUtility(client))
            {
                UploadRequest uploadRequest = new()
                {
                    BucketName = _bucketName,
                    Key = fileName,
                    InputStream = stream
                };

                uploadRequest.Metadata.Add("testmetadata", "testmetadatavalue");
                uploadRequest.Headers["Content-Disposition"] = "attachment; filename=\"" + fileName + "\"";

                await tu.UploadAsync(uploadRequest).ConfigureAwait(false);

                var metadata = await Client.GetObjectMetadataAsync(new()
                {
                    BucketName = _bucketName,
                    Key = fileName
                }).ConfigureAwait(false);
                Assert.AreEqual(fileSize, metadata.ContentLength.ToUInt64());
                Assert.IsTrue(metadata.Metadata.Count > 0);
                Assert.AreEqual("testmetadatavalue", metadata.Metadata["testmetadata"]);
                Assert.IsTrue(metadata.Headers.Count > 0);
                Assert.AreEqual("attachment; filename=\"" + fileName + "\"", metadata.Headers["Content-Disposition"]);

                //Download the file and validate content of downloaded file is equal.
                var downloadPath = path + ".download";
                var downloadRequest = new DownloadRequest
                {
                    BucketName = _bucketName,
                    Key = fileName,
                    FilePath = downloadPath
                };
                await tu.DownloadAsync(downloadRequest).ConfigureAwait(false);
                UtilityMethods.CompareFiles(path, downloadPath);
            }
        }

        private UnseekableStream GenerateUnseekableStreamFromFile(string filePath)
        {
            try
            {
                byte[] fileBytes = File.ReadAllBytes(filePath);
                UnseekableStream unseekableStream = new(fileBytes);
                unseekableStream.Position = 0;

                return unseekableStream;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred while generating the stream: {ex.Message}");
                throw;
            }
        }

        private async Task UploadWithSSE_C(ulong fileSize, string name)
        {
            // Create a fileSize file to upload
            var fileName = UtilityMethods.GenerateName(name);
            var fullFilePath = Path.Combine(BasePath, fileName);
            UtilityMethods.GenerateFile(fullFilePath, fileSize);

            // Create an encryption key                
            Aes aesEncryption = Aes.Create();
            aesEncryption.KeySize = 256;
            aesEncryption.GenerateKey();
            string base64Key = Convert.ToBase64String(aesEncryption.Key);

            // Upload the file. A permission denied exception would be thrown if an incorrect request is made
            // missing the required ServerSideEncryptionCustomerMethod and ServerSideEncryptionCustomerProvidedKey
            // values.
            var tu = new AsyncTransferUtility(Client);
            var request = new UploadRequest
            {
                BucketName = _ssecBucketName,
                FilePath = fullFilePath,
                Key = fileName,
                ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                ServerSideEncryptionCustomerProvidedKey = base64Key
            };

            await tu.UploadAsync(request).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task SimpleUploadWithSSE_C_SmallFile()
        {
            await UploadWithSSE_C(KiloSize, @"SimpleUploadTest\SmallFile").ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task SimpleUploadWithSSE_C_LargeFile()
        {
            await UploadWithSSE_C(16 * MegSize, @"SimpleUploadTest\LargeFile").ConfigureAwait(false);            
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task DirectoryUploadDownloadWithSSE_C()
        {
            var directoryTest = CreateTestDirectory(BasePath);
            var directoryTestPath = directoryTest.FullName;
            var remoteDirectory = directoryTest.Name;

            // Create an encryption key
            Aes aesEncryption = Aes.Create();
            aesEncryption.KeySize = 256;
            aesEncryption.GenerateKey();
            string base64Key = Convert.ToBase64String(aesEncryption.Key);

            // Upload test directory with SSE-C
            var transferUtility = new AsyncTransferUtility(Client);
            var requestUpload = new UploadDirectoryRequest
            {
                BucketName = _bucketName,
                Directory = directoryTestPath,
                KeyPrefix = remoteDirectory,
                SearchPattern = "*",
                SearchOption = SearchOption.AllDirectories,

                ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                ServerSideEncryptionCustomerProvidedKey = base64Key
            };

            await transferUtility.UploadDirectoryAsync(requestUpload).ConfigureAwait(false);

            // Download remote test directory with SSE-C
            var downloadPath = GenerateDirectoryPath(BasePath);

            var requestDownload = new DownloadDirectoryRequest
            {
                BucketName = _bucketName,
                S3Directory = remoteDirectory,
                LocalDirectory = downloadPath,

                ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                ServerSideEncryptionCustomerProvidedKey = base64Key
            };

            await transferUtility.DownloadDirectoryAsync(requestDownload).ConfigureAwait(false);

            // Compare each file in both directories
            var sourceFiles = Directory.EnumerateFiles(directoryTestPath, "*", SearchOption.AllDirectories).ToList();
            var downloadedFiles = Directory.EnumerateFiles(downloadPath, "*", SearchOption.AllDirectories).ToList();

            Assert.AreEqual(sourceFiles.Count, downloadedFiles.Count);
            
            sourceFiles.Sort();
            downloadedFiles.Sort();
            for (var i = 0; i < sourceFiles.Count; i++)
            {
                UtilityMethods.CompareFiles(sourceFiles[i], downloadedFiles[i]);
            }
        }

        private void TestDownloadedFile(string downloadPath)
        {
            var fileExists = File.Exists(downloadPath);
            Assert.IsTrue(fileExists);
            var fileContent = File.ReadAllText(downloadPath);
            Assert.AreEqual(TestContent, fileContent);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task MultipartUploadProgressTest()
        {
            // disable clock skew testing, this is a multithreaded test
            using (RetryUtilities.DisableClockSkewCorrection())
            {
                var fileName = UtilityMethods.GenerateName(@"MultipartUploadTest\File");
                var progressValidator = new UploadProgressValidator<UploadProgressArgs>
                {
                    ValidateProgressInterval = false,
                    Validate = p =>
                    {
                        Assert.AreEqual(p.FilePath, Path.Combine(BasePath, fileName));
                    }
                };
                await Upload(fileName, 20 * MegSize, progressValidator).ConfigureAwait(false);
                progressValidator.AssertOnCompletion();
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task MultipartGetNumberTest()
        {
            string key = "SomeTest";

            await Upload(key, 20 * MegSize, null, Client).ConfigureAwait(false);

            try
            {
                var objectMetadataResponse = await Client.GetObjectMetadataAsync(new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    PartNumber = 1,
                }).ConfigureAwait(false);

                Assert.IsTrue(objectMetadataResponse.PartsCount > 1);

                int? count = objectMetadataResponse.PartsCount;
                for (int i = 1; i <= count; i++)
                {
                    var objectResponse = await Client.GetObjectAsync(new()
                    {
                        BucketName = _bucketName,
                        Key = key,
                        PartNumber = i
                    }).ConfigureAwait(false);

                    Assert.AreEqual(objectResponse.PartsCount, count);
                }
            }
            finally
            {
                await Client.DeleteObjectAsync(new()
                {
                    BucketName = _bucketName,
                    Key = key
                }).ConfigureAwait(false);
            }
        }

        private async Task Upload(
            string fileName, 
            ulong size, 
            UploadProgressValidator<UploadProgressArgs>? progressValidator, 
            AmazonS3Client? client = null)
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var key = fileName;
            await Client.DeleteObjectAsync(new()
            {
                BucketName = _bucketName,
                Key = key
            }).ConfigureAwait(false);

            var path = Path.Combine(BasePath, fileName);
            UtilityMethods.GenerateFile(path, size);
            var config = new AsyncTransferConfig
            {
                //ConcurrentServiceRequests = 1,
                //MinSizeBeforePartUpload = MEG_SIZE
            };
            var transferUtility = client != null ? new(client, config)
                : new AsyncTransferUtility(Client, config);
            var request = new UploadRequest
            {
                BucketName = _bucketName,
                FilePath = path,
                Key = key,
                ContentType = _octetStreamContentType
            };

            if (progressValidator != null)
            {
                request.UploadProgressEvent += progressValidator.OnProgressEvent;
            }

            await transferUtility.UploadAsync(request).ConfigureAwait(false);

            var metadata = await Client.GetObjectMetadataAsync(new()
            {
                BucketName = _bucketName,
                Key = key
            }).ConfigureAwait(false);
            Console.WriteLine("Expected Size: {0} , Actual Size {1}", size, metadata.ContentLength);
            Assert.AreEqual(_octetStreamContentType, metadata.Headers.ContentType);
            Assert.AreEqual(size, metadata.ContentLength.ToUInt64());
            await ValidateFileContents(Client, _bucketName, key, path).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task UploadDirectoryWithProgressTracker()
        {
            var progressValidator = new DirectoryProgressValidator<UploadDirectoryProgressArgs>();
            ConfigureProgressValidator(progressValidator);

            await UploadDirectory(10U * MegSize, progressValidator).ConfigureAwait(false);
            progressValidator.AssertOnCompletion();
        }

        private async Task<DirectoryInfo> UploadDirectory(ulong size,
             DirectoryProgressValidator<UploadDirectoryProgressArgs>? progressValidator, bool validate = true)
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var directory = CreateTestDirectory(BasePath, size);
            var keyPrefix = directory.Name;
            var directoryPath = directory.FullName;

            var config = new AsyncTransferConfig
            {
                ConcurrentServiceRequests = 10,
            };
            var transferUtility = new AsyncTransferUtility(Client, config);
            var request = new UploadDirectoryRequest
            {
                BucketName = _bucketName,
                Directory = directoryPath,
                KeyPrefix = keyPrefix,
                ContentType = _plainTextContentType,
                SearchPattern = "*",
                SearchOption = SearchOption.AllDirectories,
            };

            //if (concurrent)
            //    request.UploadFilesConcurrently = true;

            if (progressValidator != null)
            {
                request.UploadDirectoryProgressEvent += progressValidator.OnProgressEvent;
            }

            HashSet<string> files = new();
            request.UploadDirectoryProgressEvent += (_, e) =>
            {
                ArgumentNullException.ThrowIfNull(e.CurrentFile);
                
                files.Add(e.CurrentFile);
                Console.WriteLine("Progress callback = " + e);
            };

            await transferUtility.UploadDirectoryAsync(request).ConfigureAwait(false);

            Assert.AreEqual(5, files.Count);

            if (validate)
                await ValidateDirectoryContents(Client, _bucketName, keyPrefix, directory, _plainTextContentType).ConfigureAwait(false);

            return directory;
        }

         [TestMethod]
         [TestCategory("S3")]
         public async Task DownloadDirectoryProgressTest()
         {
             // disable clock skew testing, this is a multithreaded test
             using (RetryUtilities.DisableClockSkewCorrection())
             {
                 var progressValidator = new DirectoryProgressValidator<DownloadDirectoryProgressArgs>();
                 ConfigureProgressValidator(progressValidator);

                 await DownloadDirectory(progressValidator).ConfigureAwait(false);
                 progressValidator.AssertOnCompletion();
             }
         }

        private async Task DownloadDirectory(DirectoryProgressValidator<DownloadDirectoryProgressArgs> progressValidator)
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var directory = await UploadDirectory(20U * MegSize, null, false).ConfigureAwait(false);
            var directoryPath = directory.FullName;
            var keyPrefix = directory.Name;
            Directory.Delete(directoryPath, true);

            var transferUtility = new AsyncTransferUtility(Client);
            var request = new DownloadDirectoryRequest
            {
                BucketName = _bucketName,
                LocalDirectory = directoryPath,
                S3Directory = keyPrefix
            };

            request.DownloadedDirectoryProgressEvent += progressValidator.OnProgressEvent;

            await transferUtility.DownloadDirectoryAsync(request).ConfigureAwait(false);
            await ValidateDirectoryContents(Client, _bucketName, keyPrefix, directory).ConfigureAwait(false);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task DownloadDirectoryWithDisableSlashCorrectionForS3DirectoryProgressTest()
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            // disable clock skew testing, this is a multithreaded test
            using (RetryUtilities.DisableClockSkewCorrection())
            {
                var progressValidator = new DirectoryProgressValidator<DownloadDirectoryProgressArgs>();
                ConfigureProgressValidator(progressValidator);

                uint numberOfTestFiles = 5;
                var downloadDirectory = await DownloadDirectoryWithDisableSlashCorrectionForS3Directory(
                    numberOfTestFiles, 
                    progressValidator
                    ).ConfigureAwait(false);
                progressValidator.AssertOnCompletion();

                Assert.AreEqual(
                    numberOfTestFiles,
                    downloadDirectory.GetFiles("*", SearchOption.AllDirectories).Length.ToUInt32()
                    );
                await ValidateDirectoryContents(Client, _bucketName, string.Empty, downloadDirectory).ConfigureAwait(false);
            }
        }

        private async Task<DirectoryInfo> DownloadDirectoryWithDisableSlashCorrectionForS3Directory(
            uint numberOfTestFiles, 
            DirectoryProgressValidator<DownloadDirectoryProgressArgs> progressValidator)
        {
            var keyPrefix = DateTime.Now.ToString("yyyy-MM-dd");
            var directory = await UploadDirectoryWithKeyPrefix(
                1 * KiloSize, 
                null, 
                keyPrefix, 
                numberOfTestFiles, 
                false
                ).ConfigureAwait(false);
            var directoryPath = directory.FullName;
            Directory.Delete(directoryPath, true);

            var transferUtility = new AsyncTransferUtility(Client);
            var request = new DownloadDirectoryRequest
            {
                BucketName = _bucketName,
                LocalDirectory = directoryPath,
                S3Directory = keyPrefix,
                DisableSlashCorrection = true
            };

            request.DownloadedDirectoryProgressEvent += progressValidator.OnProgressEvent;

            await transferUtility.DownloadDirectoryAsync(request).ConfigureAwait(false);

            return directory;
        }

        private async Task<DirectoryInfo> UploadDirectoryWithKeyPrefix(
            ulong size, 
            DirectoryProgressValidator<UploadDirectoryProgressArgs>? progressValidator, 
            string keyPrefix, 
            uint numberOfTestFiles, 
            bool validate = true)
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var directory = CreateTestDirectoryWithFilePrefix(BasePath, size, keyPrefix, numberOfTestFiles);
            var directoryPath = directory.FullName;

            var config = new AsyncTransferConfig
            {
                ConcurrentServiceRequests = 10,
            };
            var transferUtility = new AsyncTransferUtility(Client, config);
            var request = new UploadDirectoryRequest
            {
                BucketName = _bucketName,
                Directory = directoryPath,
                ContentType = _plainTextContentType,
                SearchPattern = "*",
                SearchOption = SearchOption.AllDirectories,
            };

            if (progressValidator != null)
            {
                request.UploadDirectoryProgressEvent += progressValidator.OnProgressEvent;
            }

            request.UploadDirectoryProgressEvent += (_, e) =>
            {
                Console.WriteLine("Progress callback = " + e);
            };

            await transferUtility.UploadDirectoryAsync(request).ConfigureAwait(false);

            if (validate)
                await ValidateDirectoryContents(Client, _bucketName, string.Empty, directory, _plainTextContentType).ConfigureAwait(false);

            return directory;
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task DownloadProgressTest()
        {
            var fileName = UtilityMethods.GenerateName(@"DownloadTest\File");
            var progressValidator = new TransferProgressValidator<WriteObjectProgressArgs>
            {
                Validate = p =>
                {
                    Assert.AreEqual(p.BucketName, _bucketName);
                    Assert.AreEqual(p.Key, fileName);
                    Assert.IsNotNull(p.FilePath);
                    Assert.IsTrue(p.FilePath.Contains(fileName));
                }
            };
            await Download(fileName, 10 * MegSize, progressValidator).ConfigureAwait(false);
            progressValidator.AssertOnCompletion();
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task DownloadProgressZeroLengthFileTest()
        {
            var fileName = UtilityMethods.GenerateName(@"DownloadTest\File");
            var progressValidator = new TransferProgressValidator<WriteObjectProgressArgs>
            {
                Validate = p =>
                {
                    Assert.AreEqual(p.BucketName, _bucketName);
                    Assert.AreEqual(p.Key, fileName);
                    Assert.IsNotNull(p.FilePath);
                    Assert.IsTrue(p.FilePath.Contains(fileName));
                    Assert.AreEqual(p.TotalBytes, 0);
                    Assert.AreEqual(p.TransferredBytes, 0);
                    Assert.AreEqual(p.PercentDone, 100);
                }
            };
            await Download(fileName, 0, progressValidator).ConfigureAwait(false);
            progressValidator.AssertOnCompletion();
        }

        private async Task Download(
            string fileName, 
            ulong size, 
            TransferProgressValidator<WriteObjectProgressArgs>? progressValidator)
        {
            var key = fileName;
            var originalFilePath = Path.Combine(BasePath, fileName);
            UtilityMethods.GenerateFile(originalFilePath, size);

            await Client.PutObjectAsync(new()
            {
                BucketName = _bucketName,
                Key = key,
                FilePath = originalFilePath
            }).ConfigureAwait(false);

            var downloadedFilePath = originalFilePath + ".dn";

            var transferUtility = new AsyncTransferUtility(Client);
            var request = new DownloadRequest
            {
                BucketName = _bucketName,
                FilePath = downloadedFilePath,
                Key = key
            };
            if (progressValidator != null)
            {
                request.WriteObjectProgressEvent += progressValidator.OnProgressEvent;
            }
            await transferUtility.DownloadAsync(request).ConfigureAwait(false);

            UtilityMethods.CompareFiles(originalFilePath, downloadedFilePath);
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task OpenStreamTest()
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var fileName = UtilityMethods.GenerateName(@"OpenStreamTest\File");
            var key = fileName;
            var originalFilePath = Path.Combine(BasePath, fileName);
            UtilityMethods.GenerateFile(originalFilePath, 2 * MegSize);
            await Client.PutObjectAsync(new()
            {
                BucketName = _bucketName,
                Key = key,
                FilePath = originalFilePath
            }).ConfigureAwait(false);

            var transferUtility = new AsyncTransferUtility(Client);
            var stream = await transferUtility.OpenStreamAsync(_bucketName, key).ConfigureAwait(false);
            Assert.IsNotNull(stream);
            Assert.IsTrue(stream.CanRead);
            stream.Close();
        }

        /// <summary>
        /// Partial download resumption support can erroneously trigger retry with
        /// byte range of 0 to Long.MaxValue if a zero length object is the first object
        /// to be download to a new folder path - S3 then yields an invalid byte range 
        /// error on the retry.
        /// Test ensures the fix, to test that the folder path exists before trying to
        /// access it, so we don't trigger a retry.
        /// </summary>
        [TestMethod]
        [TestCategory("S3")]
        public async Task TestZeroLengthDownloadToNonExistingPath()
        {
            var objectKey = "folder1/folder2/empty_file.txt";

            await Client.PutObjectAsync(new()
            {
                BucketName = _bucketName,
                Key = objectKey,
                ContentBody = ""
            }).ConfigureAwait(false);

            var filename = UtilityMethods.GenerateName(objectKey.Replace('/', '\\'));
            var filePath = Path.Combine(BasePath, filename);
            var transferUtility = new AsyncTransferUtility(Client);
            await transferUtility.DownloadAsync(new()
            {
                BucketName = _bucketName,
                FilePath = filePath,
                Key = objectKey
            }).ConfigureAwait(false);

            Assert.IsTrue(File.Exists(filePath));
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task UploadAsyncCancellationTest()
        {
            var fileName = UtilityMethods.GenerateName(@"SimpleUploadTest\CancellationTest");
            var path = Path.Combine(BasePath, fileName);
            UtilityMethods.GenerateFile(path, 20 * MegSize);

            UploadRequest uploadRequest = new()
            {
                BucketName = _bucketName,
                Key = fileName,
                FilePath = path
            };

            var tokenSource = new CancellationTokenSource();
            CancellationToken token = tokenSource.Token;

            Task? uploadTask = null;
            using (var transferUtility = new AsyncTransferUtility(Client))
            {
                try
                {
                    uploadTask = transferUtility.UploadAsync(uploadRequest, token);
                    tokenSource.CancelAfter(100);
                    await uploadTask;
                }
                catch (OperationCanceledException)
                {
                    Assert.IsTrue(uploadTask?.IsCanceled);
                    return;
                }
            }
            Assert.Fail("An OperationCanceledException was not thrown.");
        }

        private static void ConfigureProgressValidator(DirectoryProgressValidator<DownloadDirectoryProgressArgs> progressValidator)
        {
            progressValidator.Validate = (progress, lastProgress) =>
            {
                if (lastProgress != null)
                {
                    Assert.IsTrue(progress.NumberOfFilesDownloaded >= lastProgress.NumberOfFilesDownloaded);
                    Assert.IsTrue(progress.TransferredBytes >= lastProgress.TransferredBytes);
                    if (progress.NumberOfFilesDownloaded == lastProgress.NumberOfFilesDownloaded)
                    {
                        Assert.IsTrue(progress.TransferredBytes - lastProgress.TransferredBytes >= 100 * KiloSize);
                    }
                }

                if (progress.NumberOfFilesDownloaded == progress.TotalNumberOfFiles)
                {
                    Assert.AreEqual(progress.TransferredBytes, progress.TotalBytes);
                    progressValidator.IsProgressEventComplete = true;
                }

                Console.WriteLine(progress.ToString());
            };
        }

        public static void ConfigureProgressValidator(DirectoryProgressValidator<UploadDirectoryProgressArgs> progressValidator)
        {
            progressValidator.Validate = (progress, lastProgress) =>
            {
                // Skip validation if testing clock skew correction
                if (RetryUtilities.TestClockSkewCorrection)
                    return;

                Assert.IsFalse(string.IsNullOrWhiteSpace(progress.CurrentFile));
                Assert.IsTrue(progress.TotalNumberOfBytesForCurrentFile > 0);
                Assert.IsTrue(progress.TransferredBytesForCurrentFile > 0);

                if (lastProgress != null)
                {
                    Assert.IsTrue(progress.NumberOfFilesUploaded >= lastProgress.NumberOfFilesUploaded);
                    Assert.IsTrue(progress.TransferredBytes > lastProgress.TransferredBytes);
                    if (progress.NumberOfFilesUploaded == lastProgress.NumberOfFilesUploaded)
                    {
                        Assert.IsTrue(progress.TransferredBytes - lastProgress.TransferredBytes >= 100U * KiloSize);
                    }
                    else
                    {
                        Assert.AreEqual(progress.TransferredBytesForCurrentFile, progress.TotalNumberOfBytesForCurrentFile);
                    }
                }

                if (progress.NumberOfFilesUploaded == progress.TotalNumberOfFiles)
                {
                    Assert.AreEqual(progress.TransferredBytes, progress.TotalBytes);
                    progressValidator.IsProgressEventComplete = true;
                }

                Console.Write("\t{0} : {1}/{2}\t", progress.CurrentFile,
                    progress.TransferredBytesForCurrentFile, progress.TotalNumberOfBytesForCurrentFile);
                Console.WriteLine(progress.ToString());
            };
        }

        private static async Task ValidateFileContents(IAmazonS3 s3Client, string bucketName, string key, string path)
        {
            // test assumes we used a known extension and added it to the file key
            var ext = Path.GetExtension(key);
            await ValidateFileContents(s3Client, bucketName, key, path, AmazonS3Util.MimeTypeFromExtension(ext)).ConfigureAwait(false);
        }

        private static Task ValidateFileContents(IAmazonS3 s3Client, string bucketName, string key, string path, string? contentType)
        {
            var downloadPath = path + ".chk";
            var request = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = key,
            };

            UtilityMethods.WaitUntil(() =>
            {
                using (var response = s3Client.GetObjectAsync(request).ConfigureAwait(false).GetAwaiter().GetResult())
                {
                    if (!string.IsNullOrWhiteSpace(contentType))
                    {
                        Assert.AreEqual(contentType, response.Headers.ContentType);
                    }
                    response.WriteResponseStreamToFileAsync(downloadPath, append: false, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                return true;
            }, sleepSeconds: 2, maxWaitSeconds: 10);
            UtilityMethods.CompareFiles(path, downloadPath);

            return Task.CompletedTask;
        }

        public static async Task ValidateDirectoryContents(IAmazonS3 s3Client, string bucketName, string keyPrefix, DirectoryInfo sourceDirectory)
        {
            await ValidateDirectoryContents(s3Client, bucketName, keyPrefix, sourceDirectory, null).ConfigureAwait(false);
        }

        private static async Task ValidateDirectoryContents(IAmazonS3 s3Client, string bucketName, string keyPrefix, DirectoryInfo sourceDirectory, string? contentType)
        {
            var directoryPath = sourceDirectory.FullName;
            var files = sourceDirectory.GetFiles("*", SearchOption.AllDirectories);
            foreach (var file in files)
            {
                var filePath = file.FullName;
                var key = filePath.Substring(directoryPath.Length + 1);
                key = (!string.IsNullOrWhiteSpace(keyPrefix) ? keyPrefix + "/" : string.Empty) + key.Replace("\\", "/");
                await ValidateFileContents(s3Client, bucketName, key, filePath, contentType).ConfigureAwait(false);
            }
        }

        public static DirectoryInfo CreateTestDirectory(string basePath, ulong size = 0, uint numberOfTestFiles = 5)
        {
            if (size == 0)
                size = 1 * MegSize;

            var directoryPath = GenerateDirectoryPath(basePath);
            for (int i = 0; i < numberOfTestFiles; i++)
            {
                var filePath = Path.Combine(Path.Combine(directoryPath, i.ToString()), "file.txt");
                UtilityMethods.GenerateFile(filePath, size);
            }

            return new(directoryPath);
        }

        private static DirectoryInfo CreateTestDirectoryWithFilePrefix(
            string basePath, 
            ulong size = 0, 
            string? filePrefix = null, 
            uint numberOfTestFiles = 5)
        {
            if (string.IsNullOrWhiteSpace(filePrefix))
            {
                return CreateTestDirectory(basePath, size, numberOfTestFiles);
            }

            uint numberOfTestFilesInChildDirectory = numberOfTestFiles / 2;
            uint numberOfTestFilesInParentDirectory = numberOfTestFiles - numberOfTestFilesInChildDirectory;

            if (size == 0)
                size = 1 * KiloSize;

            var parentDirectory = GenerateDirectoryPath(basePath);
            for (int i = 0; i < numberOfTestFilesInParentDirectory; i++)
            {
                var parentDirectoryFilePath = Path.Combine(parentDirectory, filePrefix.Trim() + i + "file.txt");
                UtilityMethods.GenerateFile(parentDirectoryFilePath, size);
            }

            var childDirectory = Path.Combine(parentDirectory, filePrefix);
            for (int i = 0; i < numberOfTestFilesInChildDirectory; i++)
            {
                var childDirectoryFilePath = Path.Combine(childDirectory, i + "file.txt");
                UtilityMethods.GenerateFile(childDirectoryFilePath, size);
            }

            return new(parentDirectory);
        }

        public static string GenerateDirectoryPath(string basePath, string baseName = "DirectoryTest")
        {
            var directoryName = UtilityMethods.GenerateName(baseName);
            var directoryPath = Path.Combine(basePath, directoryName);
            return directoryPath;
        }

        [SuppressMessage("ReSharper", "MemberCanBeProtected.Global")]
        public abstract class ProgressValidator<T>
        {
            public T? LastProgressEventValue { get; set; }

            public bool IsProgressEventComplete { get; set; }

            public Exception? ProgressEventException { get; set; }

            public void AssertOnCompletion()
            {
                // Skip validation if testing clock skew correction
                if (RetryUtilities.TestClockSkewCorrection)
                    return;

                if (ProgressEventException != null)
                    throw ProgressEventException;

                // Add some time for the background thread to finish before checking the complete
                for (int retries = 1; retries < 5 && !IsProgressEventComplete; retries++)
                {
                    Thread.Sleep(1000 * retries);
                }
                Assert.IsTrue(
                    IsProgressEventComplete, 
                    $"{nameof(IsProgressEventComplete)} is false." + Environment.NewLine +
                    $"Last Progress Event: {LastProgressEventValue}"
                    );
            }
        }
        
        /// <remarks>
        /// <see cref="UploadProgressArgs"/> shadows properties with `new` so a dedicated
        /// validator for it is needed to accurately read property values 
        /// </remarks>
        private class UploadProgressValidator<T> : ProgressValidator<T> 
            where T : UploadProgressArgs
        {
            public Action<T>? Validate { get; set; }

            public bool ValidateProgressInterval { get; set; }

            public UploadProgressValidator()
            {
                ValidateProgressInterval = true;
            }

            public void OnProgressEvent(object? sender, T progress)
            {
                try
                {
                    Console.WriteLine("Progress Event : {0}%\t{1}/{2}", progress.PercentDone, progress.TransferredBytes, progress.TotalBytes);
                    Assert.IsFalse(progress.PercentDone > 100, "Progress percent done cannot be greater than 100%");
                    if (IsProgressEventComplete)
                        Assert.Fail("A progress event was received after completion.");

                    if (progress.TransferredBytes == progress.TotalBytes)
                    {
                        Assert.AreEqual(progress.PercentDone, 100U);
                        IsProgressEventComplete = true;
                    }

                    if (LastProgressEventValue != null)
                    {
                        if (progress.PercentDone < LastProgressEventValue.PercentDone)
                            Console.WriteLine("Progress Event : --------------------------");

                        Assert.IsTrue(progress.PercentDone >= LastProgressEventValue.PercentDone);
                        Assert.IsTrue(progress.TransferredBytes > LastProgressEventValue.TransferredBytes);

                        if (progress.TransferredBytes < progress.TotalBytes)
                        {
                            if (progress.TransferredBytes - LastProgressEventValue.TransferredBytes < 100 * KiloSize)
                                Console.WriteLine("Progress Event : *******Part Uploaded********");

                            if (ValidateProgressInterval)
                            {
                                // When TransferUtility uploads using multipart upload, the TransferredBytes
                                // will be less than the interval for last chunk of each upload part request.
                                Assert.IsTrue(progress.TransferredBytes - LastProgressEventValue.TransferredBytes >= 100 * KiloSize);
                            }
                        }
                    }

                    ArgumentNullException.ThrowIfNull(Validate);
                    Validate(progress);
                    LastProgressEventValue = progress;
                }
                catch (Exception ex)
                {
                    ProgressEventException = ex;
                    Console.WriteLine("Exception caught: {0}", ex.Message);
                    throw;
                }
            }
        }

        private class TransferProgressValidator<T> : ProgressValidator<T> 
            where T : TransferProgressArgs
        {
            public Action<T>? Validate { get; set; }

            public bool ValidateProgressInterval { get; set; }

            public TransferProgressValidator()
            {
                ValidateProgressInterval = true;
            }

            public void OnProgressEvent(object? sender, T progress)
            {
                try
                {
                    if (progress is UploadProgressArgs uploadProgressArgs)
                    {
                        //because `UploadProgressArgs` shadows properties with `new`
                        //a cast is needed to accurately read properties
                    }
                    else
                    {
                        
                    }
                    Console.WriteLine("Progress Event : {0}%\t{1}/{2}", progress.PercentDone, progress.TransferredBytes, progress.TotalBytes);
                    Assert.IsFalse(progress.PercentDone > 100, "Progress percent done cannot be greater than 100%");
                    if (IsProgressEventComplete)
                        Assert.Fail("A progress event was received after completion.");

                    if (progress.TransferredBytes == progress.TotalBytes)
                    {
                        Assert.AreEqual(progress.PercentDone, 100);
                        IsProgressEventComplete = true;
                    }

                    if (LastProgressEventValue != null)
                    {
                        if (progress.PercentDone < LastProgressEventValue.PercentDone)
                            Console.WriteLine("Progress Event : --------------------------");

                        Assert.IsTrue(progress.PercentDone >= LastProgressEventValue.PercentDone);
                        Assert.IsTrue(progress.TransferredBytes > LastProgressEventValue.TransferredBytes);

                        if (progress.TransferredBytes < progress.TotalBytes)
                        {
                            if (progress.TransferredBytes.ToUInt64() - LastProgressEventValue.TransferredBytes.ToUInt64() < 100 * KiloSize)
                                Console.WriteLine("Progress Event : *******Part Uploaded********");

                            if (ValidateProgressInterval)
                            {
                                // When TransferUtility uploads using multipart upload, the TransferredBytes
                                // will be less than the interval for last chunk of each upload part request.
                                Assert.IsTrue(progress.TransferredBytes.ToUInt64() - LastProgressEventValue.TransferredBytes.ToUInt64() >= 100 * KiloSize);
                            }
                        }
                    }

                    ArgumentNullException.ThrowIfNull(Validate);
                    Validate(progress);
                    LastProgressEventValue = progress;
                }
                catch (Exception ex)
                {
                    ProgressEventException = ex;
                    Console.WriteLine("Exception caught: {0}", ex.Message);
                    throw;
                }
            }
        }

        public class DirectoryProgressValidator<T> : ProgressValidator<T>
        {
            public Action<T, T?>? Validate { get; set; }

            public void OnProgressEvent(object? sender, T progress)
            {
                try
                {
                    ArgumentNullException.ThrowIfNull(Validate);
                    Validate(progress, LastProgressEventValue);
                }
                catch (Exception ex)
                {
                    ProgressEventException = ex;
                    Console.WriteLine("Exception caught: {0}", ex.Message);
                    throw;
                }
                finally
                {
                    LastProgressEventValue = progress;
                }
            }
        }
        private class UnseekableStream : MemoryStream
        {
            private readonly bool _setZeroLengthStream;

            public UnseekableStream(byte[] buffer) : base(buffer) { }
            public UnseekableStream(bool setZeroLengthStream)
            {
                _setZeroLengthStream = setZeroLengthStream;
            }

            public override bool CanSeek => false;

            public override long Length
            {
                get
                {
                    if (_setZeroLengthStream)
                    {
                        return 0;
                    }

                    throw new NotSupportedException($"Length property is not supported by {nameof(UnseekableStream)}");
                }
            }
        }
    }

}
