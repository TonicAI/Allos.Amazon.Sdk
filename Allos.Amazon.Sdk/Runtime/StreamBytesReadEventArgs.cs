using Amazon.Sdk.Fork;

namespace Amazon.Sdk;

/// <summary>
/// Adapted from internal EventStream.ReadProgress delegate
/// </summary>
[AmazonSdkFork("sdk/src/Core/Amazon.Runtime/Internal/Util/EventStream.cs", "Amazon.Runtime.Internal.Util")]
public class StreamBytesReadEventArgs : EventArgs
{
    public StreamBytesReadEventArgs(int bytesRead)
    {
        BytesRead = (uint) bytesRead;
    }
    
    public uint BytesRead { get; }
}