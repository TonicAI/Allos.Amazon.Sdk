using Allos.Amazon.Sdk.S3.Transfer;
using Allos.Amazon.Sdk.S3.Transfer.Internal;

namespace Allos.Amazon.Sdk;

/// <summary>
/// Factory for creating <see cref="UploadProgressArgs"/> instances.
/// </summary>
public interface IUploadProgressArgsFactory
{
    UploadProgressArgs Create(
        ITransferCommand command, 
        ulong incrementTransferred, 
        ulong transferred, 
        ulong? total);
    
    UploadProgressArgs Create(
        ITransferCommand command, 
        ulong incrementTransferred, 
        ulong transferred, 
        ulong? total, 
        string filePath
        );
    
    UploadProgressArgs Create(
        ITransferCommand command, 
        ulong incrementTransferred,
        ulong transferred,
        ulong? total,
        ulong compensationForRetry,
        string? filePath
    );
    
    UploadProgressArgs Create(
        UploadProgressArgs argsWithoutCompensation, 
        ulong compensationForRetry
        );
}

internal class DefaultUploadProgressArgsFactory : IUploadProgressArgsFactory
{
    public UploadProgressArgs Create(
        ITransferCommand command, 
        ulong incrementTransferred, 
        ulong transferred, 
        ulong? total) =>
        new UploadProgressArgs(
            command,
            incrementTransferred,
            transferred,
            total
        );

    public UploadProgressArgs Create(
        ITransferCommand command,
        ulong incrementTransferred, 
        ulong transferred, 
        ulong? total, 
        string filePath
        ) =>
        new UploadProgressArgs(
            command,
            incrementTransferred,
            transferred,
            total,
            0,
            filePath
        );

    public UploadProgressArgs Create(
        ITransferCommand command,
        ulong incrementTransferred, 
        ulong transferred, 
        ulong? total, 
        ulong compensationForRetry,
        string? filePath) =>
        new UploadProgressArgs(
            command,
            incrementTransferred, 
            transferred, 
            total,
            compensationForRetry, 
            filePath);

    public UploadProgressArgs Create(
        UploadProgressArgs argsWithoutCompensation, 
        ulong compensationForRetry
        ) =>
        new UploadProgressArgs(argsWithoutCompensation, compensationForRetry);
}