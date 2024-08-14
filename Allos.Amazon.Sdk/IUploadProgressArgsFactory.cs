using Allos.Amazon.Sdk.S3.Transfer;

namespace Allos.Amazon.Sdk;

public interface IUploadProgressArgsFactory
{
    static IUploadProgressArgsFactory Instance { get; set; } = new DefaultUploadProgressArgsFactory();
    
    UploadProgressArgs Create(ulong incrementTransferred, ulong transferred, ulong? total);
    UploadProgressArgs Create(ulong incrementTransferred, ulong transferred, ulong? total, string filePath);
    UploadProgressArgs Create(
        ulong incrementTransferred,
        ulong transferred,
        ulong? total,
        ulong compensationForRetry,
        string? filePath
    );
    UploadProgressArgs Create(UploadProgressArgs argsWithoutCompensation, ulong compensationForRetry);
}

public class DefaultUploadProgressArgsFactory : IUploadProgressArgsFactory
{
    public UploadProgressArgs Create(ulong incrementTransferred, ulong transferred, ulong? total) =>
        new UploadProgressArgs(
            incrementTransferred,
            transferred,
            total
        );

    public UploadProgressArgs Create(ulong incrementTransferred, ulong transferred, ulong? total, string filePath) =>
        new UploadProgressArgs(
            incrementTransferred,
            transferred,
            total,
            0,
            filePath
        );

    public UploadProgressArgs Create(
        ulong incrementTransferred, 
        ulong transferred, 
        ulong? total, 
        ulong compensationForRetry,
        string? filePath) =>
        new UploadProgressArgs(
            incrementTransferred, 
            transferred, 
            total,
            compensationForRetry, 
            filePath);

    public UploadProgressArgs Create(UploadProgressArgs argsWithoutCompensation, ulong compensationForRetry) =>
        new UploadProgressArgs(argsWithoutCompensation, compensationForRetry);
}