using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.Fork;
using Amazon.Runtime.Internal.Util;

namespace Allos.Amazon.Sdk;

[SuppressMessage("ReSharper", "RedundantExtendsListEntry")]
public partial class EventStream : WrapperStream
{
    /// <summary>
    /// Event arguments for <see cref="OnRead"/>
    /// </summary>
    /// <remarks>
    /// Adapted from internal EventStream.ReadProgress delegate
    /// </remarks>
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
    [AmazonSdkFork("sdk/src/Core/Amazon.Runtime/Internal/Util/EventStream.cs", "Amazon.Runtime.Internal.Util")]
    public sealed class StreamBytesReadEventArgs : EventArgs
    {
        public StreamBytesReadEventArgs(int bytesRead, long totalBytesRead, bool isEndOfStream)
        {
            BytesRead = bytesRead;
            TotalBytesRead = totalBytesRead.ToUInt64();
            IsEndOfStream = isEndOfStream;
        }

        /// <summary>
        /// 
        /// </summary>
        public int BytesRead { get; }

        /// <summary>
        /// The total number of bytes that the <see cref="EventStream"/> has EVER read
        /// </summary>
        /// <remarks>
        /// This value will not necessarily equal <see cref="Stream.Length"/> even after
        /// the stream is completely read because read may have begun at an offset or <see cref="Stream.Seek"/>
        /// was called
        /// </remarks>
        public ulong TotalBytesRead { get; }
        
        /// <summary>
        /// No more could be read from the <see cref="Stream"/>
        /// </summary>
        public bool IsEndOfStream { get; }
    }
}