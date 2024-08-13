using System.Diagnostics.CodeAnalysis;
using Serilog;
using Logger = Amazon.Runtime.Internal.Util.Logger;

namespace Allos.Amazon.Sdk;

/// <summary>
/// Factory used to create <see cref="ILogger"/>
/// </summary>
/// <remarks>
/// <see cref="ILogger"/> replaces usages of <see cref="Logger"/> in this fork
/// </remarks>
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