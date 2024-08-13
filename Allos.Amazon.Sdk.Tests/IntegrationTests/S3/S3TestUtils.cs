using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Allos.Amazon.Sdk.S3.Util;
using Allos.Amazon.Sdk.Tests.IntegrationTests.Utils;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace Allos.Amazon.Sdk.Tests.IntegrationTests.Tests.S3
{
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [AmazonSdkFork("sdk/test/Services/S3/IntegrationTests/S3TestUtils.cs", "AWSSDK_DotNet.IntegrationTests.Tests.S3")]
    public static class S3TestUtils
    {
        private const uint MaxSpinLoops = 100;

        [SuppressMessage("ReSharper", "UnusedParameter.Global")]
        public static string CreateBucket(IAmazonS3 s3Client, bool createForSse)
        {
            string bucketName = TestBase.ExistingBucketName ?? UtilityMethods.UniqueTestBucketName();
            
            if (AmazonS3Util.DoesS3BucketExistV2(s3Client, bucketName))
            {
                return bucketName;
            }

            s3Client.PutBucketAsync(new PutBucketRequest { BucketName = bucketName }).ConfigureAwait(false).GetAwaiter().GetResult();
            
            return bucketName;
        }

        [SuppressMessage("ReSharper", "UnusedParameter.Global")]
        public static string CreateBucket(IAmazonS3 s3Client, PutBucketRequest bucketRequest, bool createForSse)
        {
            string bucketName = string.IsNullOrWhiteSpace(bucketRequest.BucketName) ?
                TestBase.ExistingBucketName ?? UtilityMethods.UniqueTestBucketName() :
                bucketRequest.BucketName;

            if (AmazonS3Util.DoesS3BucketExistV2(s3Client, bucketName))
            {
                return bucketName;
            }

            bucketRequest.BucketName = bucketName;

            s3Client.PutBucketAsync(bucketRequest).ConfigureAwait(false).GetAwaiter().GetResult();
            return bucketName;
        }
        public static string CreateS3ExpressBucketWithWait(IAmazonS3 s3Client, string regionCode, bool createForSse)
        {
            string bucketName = TestBase.ExistingBucketName ?? $"{UtilityMethods.SdkTestPrefix}-{DateTime.Now.Ticks}--{regionCode}--x-s3";
            
            if (AmazonS3Util.DoesS3BucketExistV2(s3Client, bucketName))
            {
                return bucketName;
            }

            s3Client.PutBucketAsync(new PutBucketRequest
            {
                BucketName = bucketName,
                PutBucketConfiguration = new()
                {
                    BucketInfo = new() { DataRedundancy = DataRedundancy.SingleAvailabilityZone, Type = BucketType.Directory },
                    Location = new() { Name = regionCode, Type = LocationType.AvailabilityZone }
                }
            }).ConfigureAwait(false).GetAwaiter().GetResult();
            WaitForBucket(s3Client, bucketName, true);


            return bucketName;
        }
        public static string CreateBucketWithWait(IAmazonS3 s3Client, bool setPublicAcLs = false, bool createForSse = false)
        {
            string bucketName = CreateBucket(s3Client, createForSse);
            WaitForBucket(s3Client, bucketName);
            if (setPublicAcLs)
            {
                SetPublicBucketAcLs(s3Client, bucketName);
            }
            return bucketName;
        }

        public static string CreateBucketWithWait(IAmazonS3 s3Client, PutBucketRequest bucketRequest, bool setPublicAcLs = false, bool createForSse = false)
        {
            string bucketName = CreateBucket(s3Client, bucketRequest, createForSse);
            WaitForBucket(s3Client, bucketName);
            if (setPublicAcLs)
            {
                SetPublicBucketAcLs(s3Client, bucketName);
            }
            return bucketName;
        }

        private static void SetPublicBucketAcLs(IAmazonS3 client, string bucketName)
        {
             client.PutBucketOwnershipControlsAsync(new()
             {
                 BucketName = bucketName,
                 OwnershipControls = new()
                 {
                     Rules = new()
                     {
                             new() {ObjectOwnership = ObjectOwnership.BucketOwnerPreferred}
                         }
                 }
             }).ConfigureAwait(false).GetAwaiter().GetResult();
            
             client.PutPublicAccessBlockAsync(new()
             {
                 BucketName = bucketName,
                 PublicAccessBlockConfiguration = new()
                 {
                     BlockPublicAcls = false
                 }
             }).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public static void WaitForBucket(IAmazonS3 client, string bucketName, bool skipDoubleCheck = false)
        {
            UtilityMethods.WaitUntilSuccess(() => {
                //Check if a bucket exists by trying to put an object in it
                var key = Guid.NewGuid() + "_existskey";

                _ = client.PutObjectAsync(new()
                {
                    BucketName = bucketName,
                    Key = key,
                    ContentBody = "exists..."
                }).ConfigureAwait(false).GetAwaiter().GetResult();

                try
                {
                    client.DeleteAsync(bucketName, key, null).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                catch (AmazonS3Exception s3Ex) when (s3Ex.IsSenderException(TestBase.Logger))
                {
                    throw;
                }
                catch
                {
                    Console.WriteLine($"Eventual consistency error: failed to delete key {key} from bucket {bucketName}");
                }

                return true;
            });

            if (skipDoubleCheck) return;

            //Double check the bucket still exists using the DoesBucketExistV2 method
            _ = WaitForConsistency(() => AmazonS3Util.DoesS3BucketExistV2(client, bucketName) ? (bool?) true : null);
        }

        public static void WaitForObject(IAmazonS3 client, string bucketName, string key, uint maxSeconds)
        {
            var sleeper = UtilityMethods.ListSleeper.Create();
            UtilityMethods.WaitUntilSuccess(() => { client.GetObjectAsync(bucketName, key).ConfigureAwait(false).GetAwaiter().GetResult(); }, sleeper, maxSeconds);
        }

        /// <summary>
        /// Deletes all objects in a bucket.
        /// Based on DeleteS3BucketWithObjects, but 
        /// without deleting the bucket at the end.
        /// </summary>
        /// <param name="client">S3 Client</param>
        /// <param name="bucketName">Bucket whose objects to delete</param>
        public static void DeleteObjects(IAmazonS3 client, string bucketName)
        {
            var listVersionsRequest = new ListVersionsRequest
            {
                BucketName = bucketName
            };
            ListVersionsResponse listVersionsResponse;

            do
            {
                // List all the versions of all the objects in the bucket.
                listVersionsResponse = client.ListVersionsAsync(listVersionsRequest).ConfigureAwait(false).GetAwaiter().GetResult();

                if (listVersionsResponse.Versions == null || listVersionsResponse.Versions.Count == 0)
                {
                    // If the bucket has no objects we're finished
                    return;
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

                // Delete the current set of objects.
                client.DeleteObjectsAsync(new()
                {
                    BucketName = bucketName,
                    Objects = keyVersionList
                }).ConfigureAwait(false).GetAwaiter().GetResult();

                // Set the markers to get next set of objects from the bucket.
                listVersionsRequest.KeyMarker = listVersionsResponse.NextKeyMarker;
                listVersionsRequest.VersionIdMarker = listVersionsResponse.NextVersionIdMarker;
            }
            // Continue listing objects and deleting them until the bucket is empty.
            while (listVersionsResponse.IsTruncated);
        }

        public static T? WaitForConsistency<T>(Func<T> loadFunction)
        {
            //First try waiting up to 60 seconds.    
            uint firstWaitSeconds = 60;
            try
            {
                return UtilityMethods.WaitUntilSuccess(loadFunction, 10, firstWaitSeconds);
            }
            catch (AmazonS3Exception s3Ex) when (s3Ex.IsSenderException(TestBase.Logger))
            {
                throw;
            }
            catch
            {
                Console.WriteLine($"Eventual consistency wait: could not resolve eventual consistency after {firstWaitSeconds} seconds. Attempting to resolve...");
            }

            //Spin through request to try to get the expected result. As soon as we get a non null result use it.
            for (var spinCounter = 0; spinCounter < MaxSpinLoops; spinCounter++)
            {
                try
                {
                    T result = loadFunction();
                    if (result != null)
                    {
                        if (spinCounter != 0)
                        {
                            //Only log that a wait happened if it didn't do it on the first time.
                            Console.WriteLine($"Eventual consistency wait successful on attempt {spinCounter + 1}.");
                        }

                        return result;
                    }
                }
                catch (AmazonS3Exception s3Ex) when (s3Ex.IsSenderException(TestBase.Logger))
                {
                    throw;
                }

                Thread.Sleep(0);
            }

            //If we don't have an ok result then spend the normal wait period to wait for eventual consistency.
            Console.WriteLine($"Eventual consistency wait: could not resolve eventual consistency after {MaxSpinLoops}. Waiting normally...");
            uint lastWaitSeconds = 240; //4 minute wait.
            return UtilityMethods.WaitUntilSuccess(loadFunction, 5, lastWaitSeconds);
        }

        public static IDisposable UseSignatureVersion4(bool newValue)
        {
            return new SigV4Disposable(newValue);
        }

        public static void TestWithVariableSigV4(Action action, bool useSigV4)
        {
            using (_ = UseSignatureVersion4(useSigV4))
            {
                action();
            }
        }

        private class SigV4Disposable : IDisposable
        {
            private readonly bool _oldSigV4;
            public SigV4Disposable(bool newSigV4)
            {
                _oldSigV4 = AWSConfigsS3.UseSignatureVersion4;
                AWSConfigsS3.UseSignatureVersion4 = newSigV4;
            }

            public void Dispose()
            {
                AWSConfigsS3.UseSignatureVersion4 = _oldSigV4;
            }
        }
    }
}
