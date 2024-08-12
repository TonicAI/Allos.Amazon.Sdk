using Amazon.Sdk.Fork;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AWSSDK_DotNet.IntegrationTests.Utils
{
    [AmazonSdkFork("sdk/test/Common/Utils/AssertExtensions.cs", "AWSSDK_DotNet.IntegrationTests.Utils")]
    public static class AssertExtensions
    {
        public static Task ExpectException<T>(Task action) 
            where T : Exception
        {
            var exceptionType = typeof(T);
            return ExpectException(action, exceptionType, (string?)null);
        }

        public static Task<Exception?> ExpectException(Task action, Type exceptionType)
        {
            return ExpectException(action, exceptionType, (string?)null);
        }

        public static Task<Exception?> ExpectException(Task action, Type exceptionType, string? expectedMessage)
        {
            Action<string>? validateMessage = expectedMessage == null ? null :
                message =>
                {
                    Assert.AreEqual(expectedMessage, message);
                };
            return ExpectException(action, exceptionType, validateMessage);
        }

        public static async Task<Exception?> ExpectException(Task action, Type exceptionType, Action<string>? validateMessage)
        {
            bool gotException = false;
            Exception? exception = null;
            try
            {
                await action;
            }
            catch (Exception e)
            {
                exception = e;

                if (exceptionType != null)
                {
                    Assert.IsTrue(exceptionType == e.GetType(), e.ToString());
                }

                if (validateMessage != null)
                {
                    validateMessage(e.Message);
                }
                gotException = true;
            }

            string message = (exceptionType == null) ?
                "Failed to get an exception." :
                String.Format("Failed to get expected exception: {0}", exceptionType.FullName);

            Assert.IsTrue(gotException, message);

            return exception;
        }
    }
}
