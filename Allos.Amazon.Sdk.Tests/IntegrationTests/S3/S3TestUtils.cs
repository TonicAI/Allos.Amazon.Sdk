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
        public static async Task<string> CreateBucket(IAmazonS3 s3Client, bool createForSse)
        {
            string bucketName = TestBase.ExistingBucketName ?? UtilityMethods.UniqueTestBucketName();
            
            if (await AmazonS3Util.DoesS3BucketExistV2Async(s3Client, bucketName))
            {
                return bucketName;
            }

            await s3Client.PutBucketAsync(new PutBucketRequest { BucketName = bucketName }).ConfigureAwait(false);
            
            return bucketName;
        }

        [SuppressMessage("ReSharper", "UnusedParameter.Global")]
        public static async Task<string> CreateBucket(IAmazonS3 s3Client, PutBucketRequest bucketRequest, bool createForSse)
        {
            string bucketName = string.IsNullOrWhiteSpace(bucketRequest.BucketName) ?
                TestBase.ExistingBucketName ?? UtilityMethods.UniqueTestBucketName() :
                bucketRequest.BucketName;

            if (await AmazonS3Util.DoesS3BucketExistV2Async(s3Client, bucketName))
            {
                return bucketName;
            }

            bucketRequest.BucketName = bucketName;

            await s3Client.PutBucketAsync(bucketRequest).ConfigureAwait(false);
            return bucketName;
        }
        public static async Task<string> CreateS3ExpressBucketWithWait(IAmazonS3 s3Client, string regionCode, bool createForSse)
        {
            string bucketName = TestBase.ExistingBucketName ?? $"{UtilityMethods.SdkTestPrefix}-{DateTime.Now.Ticks}--{regionCode}--x-s3";
            
            if (await AmazonS3Util.DoesS3BucketExistV2Async(s3Client, bucketName))
            {
                return bucketName;
            }

            await s3Client.PutBucketAsync(new PutBucketRequest
            {
                BucketName = bucketName,
                PutBucketConfiguration = new()
                {
                    BucketInfo = new()
                        { DataRedundancy = DataRedundancy.SingleAvailabilityZone, Type = BucketType.Directory },
                    Location = new() { Name = regionCode, Type = LocationType.AvailabilityZone }
                }
            }).ConfigureAwait(false);
            await WaitForBucket(s3Client, bucketName, true);


            return bucketName;
        }
        public static async Task<string> CreateBucketWithWait(IAmazonS3 s3Client, bool setPublicAcLs = false, bool createForSse = false)
        {
            string bucketName = await CreateBucket(s3Client, createForSse);
            await WaitForBucket(s3Client, bucketName);
            if (setPublicAcLs)
            {
                await SetPublicBucketAcLs(s3Client, bucketName);
            }
            return bucketName;
        }

        public static async Task<string> CreateBucketWithWait(IAmazonS3 s3Client, PutBucketRequest bucketRequest, bool setPublicAcLs = false, bool createForSse = false)
        {
            string bucketName = await CreateBucket(s3Client, bucketRequest, createForSse);
            await WaitForBucket(s3Client, bucketName);
            if (setPublicAcLs)
            {
                await SetPublicBucketAcLs(s3Client, bucketName);
            }
            return bucketName;
        }

        private static async Task SetPublicBucketAcLs(IAmazonS3 client, string bucketName)
        {
            await client.PutBucketOwnershipControlsAsync(new()
            {
                BucketName = bucketName,
                OwnershipControls = new()
                {
                    Rules = new()
                    {
                        new() { ObjectOwnership = ObjectOwnership.BucketOwnerPreferred }
                    }
                }
            }).ConfigureAwait(false);

            await client.PutPublicAccessBlockAsync(new()
            {
                BucketName = bucketName,
                PublicAccessBlockConfiguration = new()
                {
                    BlockPublicAcls = false
                }
            }).ConfigureAwait(false);
        }

        public static async Task WaitForBucket(IAmazonS3 client, string bucketName, bool skipDoubleCheck = false)
        {
            await UtilityMethods.WaitUntilSuccess(async () => {
                //Check if a bucket exists by trying to put an object in it
                var key = Guid.NewGuid() + "_existskey";

                await client.PutObjectAsync(new()
                {
                    BucketName = bucketName,
                    Key = key,
                    ContentBody = "exists..."
                });

                try
                {
                    await client.DeleteAsync(bucketName, key, null);
                }
                catch (AmazonS3Exception s3Ex) when (s3Ex.IsSenderException(TestBase.Logger))
                {
                    throw;
                }
                catch
                {
                    Console.WriteLine($"Eventual consistency error: failed to delete key {key} from bucket {bucketName}");
                }

                return Task.FromResult(true);
            });

            if (skipDoubleCheck) return;

            //Double check the bucket still exists using the DoesBucketExistV2 method
            _ = WaitForConsistency(async () => await AmazonS3Util.DoesS3BucketExistV2Async(client, bucketName) ? (bool?) true : null);
        }

        public static async Task WaitForObject(IAmazonS3 client, string bucketName, string key, uint maxSeconds)
        {
            var sleeper = UtilityMethods.ListSleeper.Create();
            await UtilityMethods.WaitUntilSuccess(async () =>
            {
                await client.GetObjectAsync(bucketName, key).ConfigureAwait(false);
            }, sleeper, maxSeconds);
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
            while (listVersionsResponse.IsTruncated.GetValueOrDefault());
        }

        public static async Task<T?> WaitForConsistency<T>(Func<Task<T>> loadFunction)
        {
            //First try waiting up to 60 seconds.    
            uint firstWaitSeconds = 60;
            try
            {
                return await UtilityMethods.WaitUntilSuccess(loadFunction, 10, firstWaitSeconds);
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
                    T result = await loadFunction();
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
            return await UtilityMethods.WaitUntilSuccess(loadFunction, 5, lastWaitSeconds);
        }
    }
}
