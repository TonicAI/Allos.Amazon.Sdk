using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Allos.Amazon.Sdk.Fork;
using Amazon.Runtime.Internal.Util;
using ILogger = Serilog.ILogger;

namespace Allos.Amazon.Sdk
{
    /// <summary>
    /// An implementation of <see cref="WrapperStream"/> that provides an <see cref="OnRead"/> event for each read operation.
    /// </summary>
    [SuppressMessage("ReSharper", "RedundantExtendsListEntry")]
    [SuppressMessage("ReSharper", "EventNeverSubscribedTo.Global")]
    [SuppressMessage("ReSharper", "UnusedType.Global")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [AmazonSdkFork("sdk/src/Core/Amazon.Runtime/Internal/Util/EventStream.cs", "Amazon.Runtime.Internal.Util")]
    public partial class EventStream : WrapperStream
    {
        public event EventHandler<StreamBytesReadEventArgs>? OnRead;
        
        protected readonly bool _leaveStreamOpen;
        
        [SuppressMessage("ReSharper", "NotAccessedField.Local")] 
        private readonly ILogger _logger;
        
        protected long _totalBytesRead;
        protected bool _isEndOfStream;

        public EventStream(Stream stream, bool leaveStreamOpen = true, ILogger? logger = null)
            : base(stream)
        {
            _leaveStreamOpen = leaveStreamOpen;
            _logger = logger ?? TonicLogger.ForContext<EventStream>();
        }

        public override bool CanRead => BaseStream.CanRead;

        public override bool CanSeek => BaseStream.CanSeek;

        public override bool CanTimeout => BaseStream.CanTimeout;

        public override bool CanWrite => BaseStream.CanWrite;

        public override long Length => BaseStream.Length;

        public override long Position
        {
            get => BaseStream.Position;
            set => BaseStream.Position = value;
        }

        public override int ReadTimeout
        {
            get => BaseStream.ReadTimeout;
            set => BaseStream.ReadTimeout = value;
        }

        public override int WriteTimeout
        {
            get => BaseStream.WriteTimeout;
            set => BaseStream.WriteTimeout = value;
        }

        public override void Flush()
        {
            BaseStream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int bytesRead = BaseStream.Read(buffer, offset, count);
            
            _totalBytesRead += bytesRead;
            _isEndOfStream = bytesRead == 0 && _totalBytesRead > 0;
            
            OnRead?.Invoke(this, new StreamBytesReadEventArgs(bytesRead, _totalBytesRead, _isEndOfStream));

            return bytesRead;
        }

        public override long Seek(long offset, SeekOrigin origin) => BaseStream.Seek(offset, origin);

        public override void SetLength(long value) => BaseStream.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) => throw new NotImplementedException();

        public override void WriteByte(byte value) => throw new NotImplementedException();

        public override Task FlushAsync(CancellationToken cancellationToken) => 
            BaseStream.FlushAsync(cancellationToken);

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            int bytesRead = await BaseStream.ReadAsync(buffer, offset, count, cancellationToken)
                .ConfigureAwait(false);

            _totalBytesRead += bytesRead;
            _isEndOfStream = bytesRead == 0 && _totalBytesRead > 0;
            
            OnRead?.Invoke(this, new StreamBytesReadEventArgs(bytesRead, _totalBytesRead, _isEndOfStream));

            return bytesRead;
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            throw new NotImplementedException();
        
        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback? callback, object? state)
        {
            var result = new AsyncResult(
                new ManualResetEvent(true),
                state,
                completedSynchronously: true,
                isCompleted: true
            );

            try
            {
                int bytesRead = Read(buffer, offset, count);
                result.Return = bytesRead;
            }
            catch (Exception e)
            {
                result.Return = e;
            }

            callback?.Invoke(result);

            return result;
        }

        public override IAsyncResult BeginWrite(
            byte[] buffer, 
            int offset, 
            int count, 
            AsyncCallback? callback, 
            object? state) 
            => throw new NotImplementedException();

        public override void Close()
        {
            if (!_leaveStreamOpen)
            {
                BaseStream.Close();
            }
        }

        public override int EndRead(IAsyncResult asyncResult)
        {
            var result = asyncResult as AsyncResult;
            if (result == null)
            {
                throw new ArgumentException(
                    $"Parameter {nameof(asyncResult)} was not of type {typeof(AsyncResult).FullName}",
                    nameof(asyncResult));
            }

            if (result.Return is Exception ex)
            {
                throw ex;
            }

            return Convert.ToInt32(result.Return, CultureInfo.InvariantCulture);
        }

        public override void EndWrite(IAsyncResult asyncResult) =>
            throw new NotImplementedException();

        private sealed class AsyncResult : IAsyncResult
        {
            public AsyncResult(
                WaitHandle asyncWaitHandle,
                object? asyncState,
                bool completedSynchronously,
                bool isCompleted
                )
            {
                AsyncWaitHandle = asyncWaitHandle;
                AsyncState = asyncState;
                CompletedSynchronously = completedSynchronously;
                IsCompleted = isCompleted;
            }
            
            public object? AsyncState { get; }
            public WaitHandle AsyncWaitHandle { get; }
            public bool CompletedSynchronously { get; }
            public bool IsCompleted { get; }
            public object? Return { get; set; }
        }
        
        protected override void Dispose(bool disposing)
        {

        }
    }
}
