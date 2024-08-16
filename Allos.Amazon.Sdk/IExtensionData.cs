using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Allos.Amazon.Sdk.S3.Transfer;
using Allos.Amazon.Sdk.S3.Transfer.Internal;

namespace Allos.Amazon.Sdk;

/// <summary>
/// Extensible storage for custom state stored on <see cref="ITransferRequest"/> and/or <see cref="ITransferCommand"/>
/// </summary>
[SuppressMessage("ReSharper", "UnusedMember.Global")]
public interface IExtensionData
{
    object? this[string key] { get; set; }
    bool TryGetValue(string key, out object? value);
    bool ContainsKey(string key);
    bool Remove(string key);
    void Clear();
    
    public static IExtensionData Create() => new ExtensionData();
    
    private sealed class ExtensionData : IExtensionData
    {
        private readonly ConcurrentDictionary<string, object?> _dictionary = 
            new ConcurrentDictionary<string, object?>();

        public object? this[string key]
        {
            get => _dictionary[key];
            set => _dictionary[key] = value;
        }
    
        public bool TryGetValue(string key, out object? value) => _dictionary.TryGetValue(key, out value);
    
        public bool ContainsKey(string key) => _dictionary.ContainsKey(key);
    
        public bool Remove(string key) => _dictionary.TryRemove(key, out _);
    
        public void Clear() => _dictionary.Clear();
    }
}