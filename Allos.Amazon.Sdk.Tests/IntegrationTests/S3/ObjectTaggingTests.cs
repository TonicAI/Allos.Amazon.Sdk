using Allos.Amazon.Sdk.Fork;
using Allos.Amazon.Sdk.S3.Transfer;
using Allos.Amazon.Sdk.Tests.IntegrationTests.Utils;
using Amazon.S3.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AmazonS3Client = Amazon.S3.AmazonS3Client;

namespace Allos.Amazon.Sdk.Tests.IntegrationTests.Tests.S3
{
    [TestClass]
    [AmazonSdkFork("sdk/test/Services/S3/IntegrationTests/ObjectTaggingTests.cs", "AWSSDK_DotNet.IntegrationTests.Tests.S3")]
    public class MultipartTaggingTest : TestBase<AmazonS3Client>
    {
        private string? _tempFilePath;
        private string? _bucketName;
        private readonly string _objectKey = "helloworld";

        private AsyncTransferUtility? _transferClient;
        
        [TestInitialize]
        public void TestInitialize()
        {
            _transferClient = new(Client);

            _tempFilePath = Path.GetTempFileName();
            _bucketName = S3TestUtils.CreateBucketWithWait(Client);

            UtilityMethods.GenerateFile(_tempFilePath, 1024 * 1024 * 20);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (File.Exists(_tempFilePath)) File.Delete(_tempFilePath);

            if (ShouldDeleteBucket(_bucketName))
            {
                AmazonS3Util.DeleteS3BucketWithObjectsAsync(Client, _bucketName).ConfigureAwait(false).GetAwaiter()
                    .GetResult();
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task MultipartObjectTaggingTest()
        {
            ArgumentNullException.ThrowIfNull(_transferClient);
            ArgumentException.ThrowIfNullOrWhiteSpace(_bucketName);
            
            await _transferClient.UploadAsync(new()
            {
                BucketName = _bucketName,
                Key = _objectKey,
                FilePath = _tempFilePath,
                TagSet = new()
                {
                    new() {Key = "hello", Value="world"}
                }
            }).ConfigureAwait(false);

            var response = await Client.GetObjectTaggingAsync(new()
            {
                BucketName = _bucketName,
                Key = _objectKey
            }).ConfigureAwait(false);

            Assert.AreEqual(response.Tagging.Count, 1);
            Assert.AreEqual(response.Tagging[0].Key, "hello");
            Assert.AreEqual(response.Tagging[0].Value, "world");
        }
    }
}