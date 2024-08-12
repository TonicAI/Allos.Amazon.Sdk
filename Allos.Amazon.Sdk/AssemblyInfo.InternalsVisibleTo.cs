using System.Runtime.CompilerServices;

// grant access to 'internal' declarations to all Tonic assemblies
// this allows methods to be exposed for unit tests via 'internal' instead of 'public'
// https://docs.microsoft.com/en-us/dotnet/api/system.runtime.compilerservices.internalsvisibletoattribute?view=net-6.0

[assembly: InternalsVisibleTo("Allos.Amazon.Sdk.Tests")]