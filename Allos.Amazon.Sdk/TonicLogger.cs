using System.Diagnostics.CodeAnalysis;
using Serilog;

namespace Amazon.Sdk;

public static class TonicLogger
{
    /// <summary>
    /// Hook to link a base <see cref="ILogger"/> in consuming applications
    /// </summary>
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
    public static ILogger BaseLogger { get; set; } = Log.Logger;
    
#pragma warning disable RS0030
    // ReSharper disable once ContextualLoggerProblem
    internal static ILogger ForContext<T>() => BaseLogger.ForContext<T>();
    internal static ILogger ForContext(Type type) => BaseLogger.ForContext(type);
#pragma warning restore RS0030
}