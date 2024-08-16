using System.Diagnostics.CodeAnalysis;

namespace Allos.Amazon.Sdk.S3.Transfer;

[SuppressMessage("ReSharper", "ConvertConstructorToMemberInitializers")]
public abstract class BaseRequest : ITransferRequest
{
    protected BaseRequest()
    {
        ExtensionData = IExtensionData.Create();
    }
    
    public IExtensionData ExtensionData { get; }
}