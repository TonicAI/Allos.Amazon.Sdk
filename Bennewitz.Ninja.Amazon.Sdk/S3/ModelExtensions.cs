using System.Runtime.CompilerServices;

namespace Amazon.S3.Model;

/// <summary>
/// Extensions that simplify working with types in Amazon.S3.<see cref="Amazon.S3.Model"/>
/// </summary>
public static class ModelExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void AddRange(this HeadersCollection headers, HeadersCollection headersToAdd)
    {
        foreach(var name in headersToAdd.Keys)
        {
            headers[name] = headersToAdd[name];
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void AddRange(this MetadataCollection metadata, MetadataCollection metadataToAdd)
    {
        foreach(var name in metadataToAdd.Keys)
        {
            metadata[name] = metadataToAdd[name];
        }
    }
}