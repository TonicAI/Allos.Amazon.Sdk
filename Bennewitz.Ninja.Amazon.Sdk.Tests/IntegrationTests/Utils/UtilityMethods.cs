using System.Text;
using Amazon.Runtime;
using Amazon.Runtime.Internal.Util;
using Amazon.S3;
using Amazon.Sdk.Fork;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ThirdParty.MD5;

namespace AWSSDK_DotNet.IntegrationTests.Utils
{
    [AmazonSdkFork("sdk/test/IntegrationTests/Utils/UtilityMethods.cs", "AWSSDK_DotNet.IntegrationTests.Utils")]
    public static class UtilityMethods
    {
        public const string SdkTestPrefix = "aws-net-sdk";
        private static Logger Logger => Logger.GetLogger(typeof(UtilityMethods));
        
        public static void  CompareFiles(string file1, string file2)
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

        public static T? WaitUntilSuccess<T>(Func<T> loadFunction, int sleepSeconds = 5, int maxWaitSeconds = 300)
        {
            T? result = default;            
            WaitUntil(() =>
            {
                try
                {
                    result = loadFunction();
                    return result != null;
                }
                catch (AmazonS3Exception s3Ex) when (s3Ex.IsSenderException())
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

        public static void WaitUntilException(Action action, int sleepSeconds = 5, int maxWaitSeconds = 300)
        {        
            WaitUntil(() =>
            {
                action();
                return false;
            }, sleepSeconds, maxWaitSeconds);
        }

        public static void WaitUntilSuccess(Action action, int sleepSeconds = 5, int maxWaitSeconds = 300)
        {
            if (sleepSeconds < 0) throw new ArgumentOutOfRangeException(nameof(sleepSeconds));
            WaitUntilSuccess(action, new ListSleeper(sleepSeconds * 1000), maxWaitSeconds);
        }

        public static void WaitUntilSuccess(Action action, ListSleeper sleeper, int maxWaitSeconds = 300)
        {
            WaitUntil(() =>
            {
                try
                {
                    action();
                    return true;
                }
                catch (AmazonS3Exception s3Ex) when (s3Ex.IsSenderException())
                {
                    throw;
                }
                catch
                {
                    return false;
                }
            }, sleeper, maxWaitSeconds);
        }

        public static void WaitUntil(Func<bool> matchFunction, int sleepSeconds = 5, int maxWaitSeconds = 300)
        {
            if (sleepSeconds < 0) throw new ArgumentOutOfRangeException(nameof(sleepSeconds));
            WaitUntil(matchFunction, new ListSleeper(sleepSeconds * 1000), maxWaitSeconds);
        }

        private static void WaitUntil(Func<bool> matchFunction, ListSleeper sleeper, int maxWaitSeconds = 300)
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
        public static void GenerateFile(string path, long size)
        {
            string contents = GenerateTestContents(size);
            WriteFile(path, contents);
        }

        public static string GenerateTestContents(long size)
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

            public ListSleeper(params int[] millisecondsList)
            {
                if (millisecondsList.Length < 1)
                    throw new ArgumentException($"There must be at least one sleep period in {millisecondsList}", nameof(millisecondsList));

                _attempt = 0;
                _millisecondsList = millisecondsList;
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
            public static ListSleeper Create()
            {
                return new(500, 1000, 2000, 5000);
            }
        }

        public static bool IsSenderException(this AmazonS3Exception ex)
        {
            var result = ex.ErrorType == ErrorType.Sender;
            if (result)
            {
                Logger.Error(ex, ex.ToString());
            }
            return result;
        }
            
    }
}
