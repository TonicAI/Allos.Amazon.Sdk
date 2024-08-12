using Amazon.S3;
using Amazon.S3.Util;
using Amazon.Sdk.Fork;
using Amazon.Sdk.S3.Transfer;
using AWSSDK_DotNet.IntegrationTests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AWSSDK_DotNet.IntegrationTests.Tests.S3
{
    [TestClass]
    [AmazonSdkFork("sdk/test/Services/S3/IntegrationTests/S3ExpressTests.cs", "AWSSDK_DotNet.IntegrationTests.Tests.S3")]
    public class S3ExpressTests : TestBase<AmazonS3Client>
    {
        public static readonly int MegSize = (int)Math.Pow(2, 20);
        private const string Content = "Test content";
        private static string? _bucketName;
        private static readonly List<string> _keys = new()
        {
            "a/b/c",
            "a/b/d",
            "a/e",
            "a/f",
            "a/g\rh",
            "a/g\ni",
            "a/g&j",
        };

        [ClassInitialize]
        public static void Initialize(TestContext a)
        {
            BaseInitialize();
            _bucketName = S3TestUtils.CreateS3ExpressBucketWithWait(Client, "use1-az5", createForSse: false);

            foreach (var key in _keys)
            {
                Client.PutObjectAsync(new()
                {
                    BucketName = _bucketName,
                    Key = key,
                    ContentBody = Content
                }).ConfigureAwait(false).GetAwaiter().GetResult();
            }
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            if (ShouldDeleteBucket(_bucketName))
            {
                AmazonS3Util.DeleteS3BucketWithObjectsAsync(Client, _bucketName).ConfigureAwait(false).GetAwaiter().GetResult();   
            }
            BaseClean();
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task Test_TransferUtility()
        {
            ArgumentNullException.ThrowIfNull(_bucketName);
            
            var random = new Random();

            var key = "key-" + random.Next() + ".txt";
            var filePath = Path.Combine(Path.GetTempPath(), key);

            var retrievedFilepath = filePath + ".download";
            var totalSize = MegSize * 15;

            UtilityMethods.GenerateFile(filePath, totalSize);

            try
            {
                using (var tu = new AsyncTransferUtility(Client))
                {
                    await tu.UploadAsync(filePath, _bucketName).ConfigureAwait(false);

                    var getObjectMetadataResponse = await Client.GetObjectMetadataAsync(new()
                    {
                        BucketName = _bucketName,
                        Key = key
                    }).ConfigureAwait(false);
                    Assert.IsTrue(getObjectMetadataResponse.ETag.Length > 0);

                    var downloadRequest = new TransferUtilityDownloadRequest
                    {
                        BucketName = _bucketName,
                        Key = key,
                        FilePath = retrievedFilepath
                    };
                    await tu.DownloadAsync(downloadRequest).ConfigureAwait(false);

                    var fileExists = File.Exists(retrievedFilepath);
                    Assert.IsTrue(fileExists);
                    var fileContent = await File.ReadAllTextAsync(retrievedFilepath).ConfigureAwait(false);
                    var testContent = await File.ReadAllTextAsync(filePath).ConfigureAwait(false);
                    Assert.AreEqual(testContent, fileContent);
                }

                await Client.DeleteObjectAsync(new()
                {
                    BucketName = _bucketName,
                    Key = key
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
        public async Task Test_TransferUtility_Directory()
        {
            var size = 1 * MegSize;
            var random = new Random();

            var key = "key-" + random.Next();
            var directoryPath = Path.Combine(Path.GetTempPath(), key);

            for (int i = 0; i < 5; i++)
            {
                var filePath = Path.Combine(Path.Combine(directoryPath, i.ToString()), "file.txt");
                UtilityMethods.GenerateFile(filePath, size);
            }

            var retrievedDirectoryPath = directoryPath + "download";

            try
            {
                var directory = new DirectoryInfo(directoryPath);
                var retrievedDirectory = new DirectoryInfo(retrievedDirectoryPath);

                using (var tu = new AsyncTransferUtility(Client))
                {
                    var uploadDirectoryRequest = new TransferUtilityUploadDirectoryRequest
                    {
                        BucketName = _bucketName,
                        Directory = directoryPath,
                        KeyPrefix = directory.Name,
                        SearchPattern = "*",
                        SearchOption = SearchOption.AllDirectories,
                    };

                    HashSet<string> files = new();
                    uploadDirectoryRequest.UploadDirectoryProgressEvent += (_, e) =>
                    {
                        ArgumentNullException.ThrowIfNull(e.CurrentFile);
                        files.Add(e.CurrentFile);
                    };

                    await tu.UploadDirectoryAsync(uploadDirectoryRequest).ConfigureAwait(false);

                    Assert.AreEqual(5, files.Count);

                    var transferUtility = new AsyncTransferUtility(Client);
                    var request = new TransferUtilityDownloadDirectoryRequest
                    {
                        BucketName = _bucketName,
                        LocalDirectory = retrievedDirectoryPath,
                        S3Directory = directory.Name
                    };

                    await transferUtility.DownloadDirectoryAsync(request).ConfigureAwait(false);

                    var oldFiles = directory.GetFiles("*", SearchOption.AllDirectories);
                    var retrievedFiles = retrievedDirectory.GetFiles("*", SearchOption.AllDirectories);

                    foreach (var file in oldFiles)
                    {
                        var retrievedFile = retrievedFiles.FirstOrDefault(e => e.Name == file.Name);
                        Assert.IsTrue(retrievedFile != null);

                        var fileExists = File.Exists(retrievedFile.FullName);
                        Assert.IsTrue(fileExists);

                        var retrievedContent = await File.ReadAllTextAsync(retrievedFile.FullName).ConfigureAwait(false);
                        var fileContent = await File.ReadAllTextAsync(file.FullName).ConfigureAwait(false);
                        Assert.AreEqual(retrievedContent, fileContent);
                    }
                }
            }
            finally
            {
                if (Directory.Exists(directoryPath))
                    Directory.Delete(directoryPath, true);
                if (File.Exists(retrievedDirectoryPath))
                    Directory.Delete(retrievedDirectoryPath, true);
            }
        }
    }
}