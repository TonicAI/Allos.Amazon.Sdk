using System.Runtime.CompilerServices;

// grant access to 'internal' declarations to all Tonic assemblies
// this allows methods to be exposed for unit tests via 'internal' instead of 'public'
// https://docs.microsoft.com/en-us/dotnet/api/system.runtime.compilerservices.internalsvisibletoattribute?view=net-6.0

[assembly: InternalsVisibleTo("Allos.Amazon.Sdk.Tests")]

// keep the list below in sync with the list in `allos_backend/AssemblyInfo.InternalsVisibleTo.cs`
[assembly: InternalsVisibleTo("Allos.Api")]
[assembly: InternalsVisibleTo("Allos.Backend.Test")]
[assembly: InternalsVisibleTo("Allos.Api.IntegrationTest")]
[assembly: InternalsVisibleTo("Allos.Upserter.Test")]
[assembly: InternalsVisibleTo("Allos.Benchmark")]
[assembly: InternalsVisibleTo("Allos.Console")]
[assembly: InternalsVisibleTo("Allos.Core")]
[assembly: InternalsVisibleTo("Allos.CustomValueProcessorFramework")]
[assembly: InternalsVisibleTo("Allos.DLP")]
[assembly: InternalsVisibleTo("Allos.DockerAcl.Common")]
[assembly: InternalsVisibleTo("Allos.DockerAcl.DockerCompose")]
[assembly: InternalsVisibleTo("Allos.EF")]
[assembly: InternalsVisibleTo("Allos.Files")]
[assembly: InternalsVisibleTo("Allos.ForeignDb")]
[assembly: InternalsVisibleTo("Allos.Generators")]
[assembly: InternalsVisibleTo("Allos.HostIntegration.Common")]
[assembly: InternalsVisibleTo("Allos.HostIntegration")]
[assembly: InternalsVisibleTo("Allos.HostIntegration.Services")]
[assembly: InternalsVisibleTo("Allos.HostIntegration.Test")]
[assembly: InternalsVisibleTo("Allos.Integration.Test.Api")]
[assembly: InternalsVisibleTo("Allos.JavaConsistency")]
[assembly: InternalsVisibleTo("Allos.JobFlow")]
[assembly: InternalsVisibleTo("Allos.Lambda")]
[assembly: InternalsVisibleTo("Allos.MathLib")]
[assembly: InternalsVisibleTo("Allos.Notifications")]
[assembly: InternalsVisibleTo("Allos.OracleIntegrationTest")]
[assembly: InternalsVisibleTo("Allos.SchemaStorage")]
[assembly: InternalsVisibleTo("Allos.Spark")]
[assembly: InternalsVisibleTo("Allos.Statistics")]
[assembly: InternalsVisibleTo("Allos.Upserter")]
[assembly: InternalsVisibleTo("Allos.StatBuilders")]
[assembly: InternalsVisibleTo("Allos.UnsafeLib")]
[assembly: InternalsVisibleTo("LicenseGenerator")]
[assembly: InternalsVisibleTo("OracleDriverAnalyzer")]