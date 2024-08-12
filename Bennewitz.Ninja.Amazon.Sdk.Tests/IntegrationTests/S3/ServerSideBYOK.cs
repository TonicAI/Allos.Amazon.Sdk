using System.Net;
using System.Security.Cryptography;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Amazon.Sdk.Fork;
using Amazon.Sdk.S3.Transfer;
using AWSSDK_DotNet.IntegrationTests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ThirdParty.MD5;
using AmazonS3Client = Amazon.S3.AmazonS3Client;

namespace AWSSDK_DotNet.IntegrationTests.Tests.S3
{
    [TestClass]
    [AmazonSdkFork("sdk/test/Services/S3/IntegrationTests/ServerSideBYOK.cs", "AWSSDK_DotNet.IntegrationTests.Tests.S3")]
    public class ServerSideByok : TestBase<AmazonS3Client>
    {
        private const string Key = "Encrypted|Object.png";

        [ClassCleanup]
        public static void Cleanup()
        {
            BaseClean();
        }

        //internal static string ComputeEncodedMD5FromEncodedString(string base64EncodedString)
        private static readonly MD5Managed _md5 = new();
        private static string ComputeEncodedMd5FromEncodedString(string base64EncodedString)
        {
            var unencodedValue = Convert.FromBase64String(base64EncodedString);
            var valueMd5 = _md5.ComputeHash(unencodedValue);
            var encodedMd5 = Convert.ToBase64String(valueMd5);
            return encodedMd5;
        }

        private static AmazonS3Client CreateHttpClient()
        {
            var config = new AmazonS3Config { UseHttp = true };
            var client = new AmazonS3Client(config);

            return client;
        }


        [TestMethod]
        [TestCategory("S3")]
        public async Task ServerSideEncryptionByokPutAndGet()
        {
            var bucketName = S3TestUtils.CreateBucketWithWait(Client);

            try
            {
                Aes aesEncryption = Aes.Create();
                aesEncryption.KeySize = 256;
                aesEncryption.GenerateKey();
                string base64Key = Convert.ToBase64String(aesEncryption.Key);
                string base64KeyMd5 = ComputeEncodedMd5FromEncodedString(base64Key);

                PutObjectRequest putRequest = new()
                {
                    BucketName = bucketName,
                    Key = Key,
                    ContentBody = "The Data To Encrypt in S3",

                    ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                    ServerSideEncryptionCustomerProvidedKey = base64Key,
                    ServerSideEncryptionCustomerProvidedKeyMD5 = base64KeyMd5
                };

                await Client.PutObjectAsync(putRequest).ConfigureAwait(false);

                GetObjectMetadataRequest getObjectMetadataRequest = new()
                {
                    BucketName = bucketName,
                    Key = Key,

                    ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                    ServerSideEncryptionCustomerProvidedKey = base64Key,
                    ServerSideEncryptionCustomerProvidedKeyMD5 = base64KeyMd5
                };

                GetObjectMetadataResponse getObjectMetadataResponse = await Client.GetObjectMetadataAsync(getObjectMetadataRequest).ConfigureAwait(false);
                Assert.AreEqual(ServerSideEncryptionCustomerMethod.AES256, getObjectMetadataResponse.ServerSideEncryptionCustomerMethod);

                GetObjectRequest getObjectRequest = new()
                {
                    BucketName = bucketName,
                    Key = Key,

                    ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                    ServerSideEncryptionCustomerProvidedKey = base64Key,
                    ServerSideEncryptionCustomerProvidedKeyMD5 = base64KeyMd5
                };

                using (GetObjectResponse getResponse = await Client.GetObjectAsync(getObjectRequest).ConfigureAwait(false))
                using (StreamReader reader = new(getResponse.ResponseStream))
                {
                    string content = await reader.ReadToEndAsync().ConfigureAwait(false);
                    Assert.AreEqual(putRequest.ContentBody, content);
                    Assert.AreEqual(ServerSideEncryptionCustomerMethod.AES256, getResponse.ServerSideEncryptionCustomerMethod);
                }

                GetPreSignedUrlRequest getPresignedUrlRequest = new()
                {
                    BucketName = bucketName,
                    Key = Key,
                    ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                    Expires = DateTime.Now.AddMinutes(5)
                };
                var url = await Client.GetPreSignedURLAsync(getPresignedUrlRequest).ConfigureAwait(false);

                using (HttpClient httpClient = new())
                {
                    HttpRequestMessage request = new(HttpMethod.Get, url);
                    request.Headers.Add("x-amz-server-side-encryption-customer-algorithm", "AES256");
                    request.Headers.Add("x-amz-server-side-encryption-customer-key", base64Key);
                    request.Headers.Add("x-amz-server-side-encryption-customer-key-MD5", base64KeyMd5);

                    using (var response = await httpClient.SendAsync(request).ConfigureAwait(false))
                    {
                        Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
                        var contents = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                        Assert.AreEqual(putRequest.ContentBody, contents);
                    }
                }

                aesEncryption.GenerateKey();
                string copyBase64Key = Convert.ToBase64String(aesEncryption.Key);

                CopyObjectRequest copyRequest = new()
                {
                    SourceBucket = bucketName,
                    SourceKey = Key,
                    DestinationBucket = bucketName,
                    DestinationKey = "EncryptedObject_Copy",

                    CopySourceServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                    CopySourceServerSideEncryptionCustomerProvidedKey = base64Key,
                    ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                    ServerSideEncryptionCustomerProvidedKey = copyBase64Key
                };
                await Client.CopyObjectAsync(copyRequest).ConfigureAwait(false);

                getObjectMetadataRequest = new()
                {
                    BucketName = bucketName,
                    Key = "EncryptedObject_Copy",

                    ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                    ServerSideEncryptionCustomerProvidedKey = copyBase64Key
                };

                getObjectMetadataResponse = await Client.GetObjectMetadataAsync(getObjectMetadataRequest).ConfigureAwait(false);
                Assert.AreEqual(ServerSideEncryptionCustomerMethod.AES256, getObjectMetadataResponse.ServerSideEncryptionCustomerMethod);

                // Test calls against HTTP client, some should fail on the client
                using (var httpClient = CreateHttpClient())
                {
                    getObjectMetadataRequest.ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.None;
                    getObjectMetadataRequest.ServerSideEncryptionCustomerProvidedKey = null;
                    await AssertExtensions.ExpectException(httpClient.GetObjectMetadataAsync(getObjectMetadataRequest), typeof(AmazonS3Exception)).ConfigureAwait(false);

                    getObjectMetadataRequest.ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256;
                    await AssertExtensions.ExpectException(httpClient.GetObjectMetadataAsync(getObjectMetadataRequest), typeof(AmazonS3Exception)).ConfigureAwait(false);

                    getObjectMetadataRequest.ServerSideEncryptionCustomerProvidedKey = copyBase64Key;
                    await AssertExtensions.ExpectException(httpClient.GetObjectMetadataAsync(getObjectMetadataRequest), typeof(AmazonS3Exception)).ConfigureAwait(false);

                    url = await httpClient.GetPreSignedURLAsync(getPresignedUrlRequest).ConfigureAwait(false);
                    Assert.IsFalse(string.IsNullOrWhiteSpace(url));
                }
            }
            finally
            {
                if (ShouldDeleteBucket(bucketName))
                {
                    await AmazonS3Util.DeleteS3BucketWithObjectsAsync(Client, bucketName).ConfigureAwait(false);
                }
            }
        }

        [TestMethod]
        [TestCategory("S3")]
        public async Task ServerSideEncryptionByokTransferUtility()
        {
            var bucketName = S3TestUtils.CreateBucketWithWait(Client);
            try
            {
                Aes aesEncryption = Aes.Create();
                aesEncryption.KeySize = 256;
                aesEncryption.GenerateKey();
                string base64Key = Convert.ToBase64String(aesEncryption.Key);

                TransferUtility utility = new(Client);

                var uploadRequest = new TransferUtilityUploadRequest
                {
                    BucketName = bucketName,
                    Key = Key,
                    ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                    ServerSideEncryptionCustomerProvidedKey = base64Key
                };

                uploadRequest.InputStream = new MemoryStream("Encrypted Content"u8.ToArray());

                await utility.UploadAsync(uploadRequest).ConfigureAwait(false);

                GetObjectMetadataRequest getObjectMetadataRequest = new()
                {
                    BucketName = bucketName,
                    Key = Key,

                    ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                    ServerSideEncryptionCustomerProvidedKey = base64Key
                };

                GetObjectMetadataResponse getObjectMetadataResponse = await Client.GetObjectMetadataAsync(getObjectMetadataRequest).ConfigureAwait(false);
                Assert.AreEqual(ServerSideEncryptionCustomerMethod.AES256, getObjectMetadataResponse.ServerSideEncryptionCustomerMethod);

                var openRequest = new TransferUtilityOpenStreamRequest
                {
                    BucketName = bucketName,
                    Key = Key,

                    ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                    ServerSideEncryptionCustomerProvidedKey = base64Key
                };

                using(var stream = new StreamReader(await utility.OpenStreamAsync(openRequest).ConfigureAwait(false)))
                {
                    var content = await stream.ReadToEndAsync().ConfigureAwait(false);
                    Assert.AreEqual(content, "Encrypted Content");
                }
            }
            finally
            {
                if (ShouldDeleteBucket(bucketName))
                {
                    await AmazonS3Util.DeleteS3BucketWithObjectsAsync(Client, bucketName).ConfigureAwait(false);
                }
            }
        }
    }
}
