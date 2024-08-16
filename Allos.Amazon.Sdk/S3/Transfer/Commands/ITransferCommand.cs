using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Amazon.S3;

namespace Allos.Amazon.Sdk.S3.Transfer.Internal;

/// <summary>
/// Interface that holds a reference to an `internal` type that implements <see cref="BaseCommand"/>
/// so it can be exposed on `public` contracts to assemblies that can leverage the `internal`
/// functionality via <see cref="InternalsVisibleToAttribute"/>
/// </summary>
[SuppressMessage("ReSharper", "UnusedMemberInSuper.Global")]
[SuppressMessage("ReSharper", "UnusedMember.Global")]
public interface ITransferCommand
{
    /// <summary>
    /// The <see cref="IAsyncTransferUtility"/> that created this command.
    /// </summary>
    IAsyncTransferUtility Utility { get; }

    /// <inheritdoc cref="IAsyncTransferUtility.S3Client"/>
    IAmazonS3 S3Client { get; }
    
    /// <inheritdoc cref="IAsyncTransferUtility.Config"/>
    IAsyncTransferConfig Config { get; }
    
    /// <inheritdoc cref="ITransferRequest"/>
    ITransferRequest Request { get; }
    
    /// <inheritdoc cref="IExtensionData"/>
    IExtensionData ExtensionData { get; }
}