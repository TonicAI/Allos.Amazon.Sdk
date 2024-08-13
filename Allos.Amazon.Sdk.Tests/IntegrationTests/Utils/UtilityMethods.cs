using System.Diagnostics.CodeAnalysis;
using System.Text;
using Allos.Amazon.Sdk.Fork;
using Allos.Amazon.Sdk.Tests.IntegrationTests.Tests;
using Amazon.Runtime;
using Amazon.S3;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Serilog;
using ThirdParty.MD5;

namespace Allos.Amazon.Sdk.Tests.IntegrationTests.Utils
{
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    [AmazonSdkFork("sdk/test/IntegrationTests/Utils/UtilityMethods.cs", "AWSSDK_DotNet.IntegrationTests.Utils")]
    public static class UtilityMethods
    {
        [SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
        public static string SdkTestPrefix { get; set; } = "allos-amz-sdk-tests";

        public static string UniqueTestBucketName() => SdkTestPrefix + DateTimeOffset.Now.Ticks;
        
        public static void CompareFiles(string file1, string file2)
        {
            byte[] file1Md5 = ComputeHash(file1);
            byte[] file2Md5 = ComputeHash(file2);

            Assert.AreEqual(file1Md5.Length, file2Md5.Length);
            for (int i = 0; i < file1Md5.Length; i++)
            {
                Assert.AreEqual(file1Md5[i], file2Md5[i], "MD5 of files do not match");
            }
        }

        private static byte[] ComputeHash(string file)
        {
            Stream fileStream = File.OpenRead(file);
            byte[] fileMd5 = new MD5Managed().ComputeHash(fileStream);
            fileStream.Close();
            return fileMd5;
        }

        public static T? WaitUntilSuccess<T>(Func<T> loadFunction, uint sleepSeconds = 5, uint maxWaitSeconds = 300)
        {
            T? result = default;            
            WaitUntil(() =>
            {
                try
                {
                    result = loadFunction();
                    return result != null;
                }
                catch (AmazonS3Exception s3Ex) when (s3Ex.IsSenderException(TestBase.Logger))
                {
                    throw;
                }
                catch
                {                
                    return false;
                }
            }, sleepSeconds, maxWaitSeconds);
            
            return result;
        }

        public static void WaitUntilException(Action action, uint sleepSeconds = 5, uint maxWaitSeconds = 300)
        {        
            WaitUntil(() =>
            {
                action();
                return false;
            }, sleepSeconds, maxWaitSeconds);
        }

        public static void WaitUntilSuccess(Action action, uint sleepSeconds = 5, uint maxWaitSeconds = 300)
        {
            if (sleepSeconds < 0) throw new ArgumentOutOfRangeException(nameof(sleepSeconds));
            WaitUntilSuccess(action, new ListSleeper(sleepSeconds * 1000), maxWaitSeconds);
        }

        public static void WaitUntilSuccess(Action action, ListSleeper sleeper, uint maxWaitSeconds = 300)
        {
            WaitUntil(() =>
            {
                try
                {
                    action();
                    return true;
                }
                catch (AmazonS3Exception s3Ex) when (s3Ex.IsSenderException(TestBase.Logger))
                {
                    throw;
                }
                catch
                {
                    return false;
                }
            }, sleeper, maxWaitSeconds);
        }

        public static void WaitUntil(Func<bool> matchFunction, uint sleepSeconds = 5, uint maxWaitSeconds = 300)
        {
            if (sleepSeconds < 0) throw new ArgumentOutOfRangeException(nameof(sleepSeconds));
            WaitUntil(matchFunction, new ListSleeper(sleepSeconds * 1000), maxWaitSeconds);
        }

        private static void WaitUntil(Func<bool> matchFunction, ListSleeper sleeper, uint maxWaitSeconds = 300)
        {
            if (maxWaitSeconds < 0) throw new ArgumentOutOfRangeException(nameof(maxWaitSeconds));

            var maxTime = TimeSpan.FromSeconds(maxWaitSeconds);
            var endTime = DateTime.Now + maxTime;

            while(DateTime.Now < endTime)
            {
                if (matchFunction())
                    return;
                sleeper.Sleep();
            }

            throw new TimeoutException($"Wait condition was not satisfied for {maxWaitSeconds} seconds");
        }

        public static void WriteFile(string path, string contents)
        {
            string fullPath = Path.GetFullPath(path);
            var directoryName = Path.GetDirectoryName(fullPath);
            if (directoryName != null)
            {
                new DirectoryInfo(directoryName).Create();    
            }
            File.WriteAllText(fullPath, contents);
        }
        public static void GenerateFile(string path, ulong size)
        {
            string contents = GenerateTestContents(Convert.ToInt64(size));
            WriteFile(path, contents);
        }

        private static string GenerateTestContents(long size)
        {
            StringBuilder sb = new();
            for (long i = 0; i < size; i++)
            {
                char c = (char)('a' + (i % 26));
                sb.Append(c);
            }
            string contents = sb.ToString();
            return contents;
        }
        
        public static string GenerateName(string name)
        {
            return name + new Random().Next();
        }
        
        public class ListSleeper
        {
            private int _attempt;
            private readonly int[] _millisecondsList;

            public ListSleeper(params uint[] millisecondsList)
            {
                if (millisecondsList.Length < 1)
                    throw new ArgumentException($"There must be at least one sleep period in {millisecondsList}", nameof(millisecondsList));

                _attempt = 0;
                _millisecondsList = millisecondsList.Select(x => x.ToInt32()).ToArray();
            }

            public void Sleep()
            {
                // if there are more attempts than array elements just keep using the last one
                var index = Math.Min(_attempt, _millisecondsList.Length - 1);
                Thread.Sleep(_millisecondsList[index]);
                _attempt++;
            }

            /// <summary>
            /// Create a new exponential growth sleeper. The following sleeper will be created:
            /// ListSleeper(500, 1000, 2000, 5000)
            /// </summary>
            /// <returns>A new ListSleeper with exponential growth</returns>
            public static ListSleeper Create() => new (500, 1000, 2000, 5000);
        }

        public static bool IsSenderException(this AmazonS3Exception ex, ILogger logger)
        {
            var result = ex.ErrorType == ErrorType.Sender;
            if (result)
            {
                logger.Error(ex, ex.ToString());
            }
            return result;
        }
            
    }
}
