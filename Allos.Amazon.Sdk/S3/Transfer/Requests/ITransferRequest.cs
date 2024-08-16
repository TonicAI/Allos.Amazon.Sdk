using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Allos.Amazon.Sdk.S3.Transfer;

/// <summary>
/// Interface that holds a reference to an `internal` type that implements <see cref="BaseRequest"/>
/// so it can be exposed on `public` contracts to assemblies that can leverage the `internal`
/// functionality via <see cref="InternalsVisibleToAttribute"/>
/// </summary>
[SuppressMessage("ReSharper", "UnusedMemberInSuper.Global")]
public interface ITransferRequest
{
    /// <inheritdoc cref="IExtensionData"/>
    IExtensionData ExtensionData { get; }
}