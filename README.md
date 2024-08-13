# Allos.Amazon.Sdk

A manually created forked subset of the official `AWS SDK` (from [aws-sdk-net](https://github.com/aws/aws-sdk-net.git)) limited to the `S3` type `Amazon.S3.Transfer.TransferUtility`, required supporting types, and relevant tests

#### Approach used by this fork:

- discard functionality annotated with `ObsoleteAttribute`

- use `async` functionality by default
  - discard synchronous functionality exposed by contracts
  - uses `Task` instead of `Thread` patterns

- prefer modern `C#` syntax to that of the original source code

- enabled [Nullable reference types](https://learn.microsoft.com/en-us/dotnet/csharp/nullable-references)  (i.e. `#nullable enable`)
  - code has been refactored to successfully build / test with them enabled

- coalesce `partial` definitions where possible

- annotate forked type definitions with `AmazonSdkForkAttribute` 

  - documents the `tag` of the forked version of the original source
  - documents source files and namespaces
  - documents what type or types in the original source are relevant to the annotated type including:
    - directly analogous definitions 
      - multiple in the case of `partial` definitions
    - non-analogous definitions from which any code was sourced
  - can be used by scripts to reconstitute an original file structure for diff purposes

- expose `private` functionality for extensible types
  - changed modifier to `protected` 

- expose `internal` functionality
  - changed modifier to `protected` for extensible types
  - exposed remaining `internal` to a list of known assemblies via `InternalsVisibleToAttribute`

- modify functionality with `virtual` to make it extensible

- correct, improv, and add additional `XML Documentation`

- prefer future-proof value types in exposed contracts and signatures

  **Examples**:

  - `Nullable<T>` is used instead of sentinel `T` values that represent `null`
  - `DateTimeOffset` instead of `DateTime`
  - `uint` instead of `int` when value cannot be negative
  - `ulong` instead of `long` when value cannot be negative

  > **NOTE** the underlying `aws sdk` reference may not yet make use of these and in that case they fallback to those value types for internal calls to that SDK

- change namespaces to match extracted subset functionality and containing solution

- change logging to use `Serilog`

  - changed `Amazon.Runtime.Internal.Util.Logger` to `Serilog.ILogger`


#### Logging

Consuming applications should set the static property `Allos.Amazon.Sdk.TonicLogger.BaseLogger` to the desired `Serilog.ILogger` to hook into this library's internal logging

#### Testing

The tests use the `AWS` credentials file to authenticate with `S3`

> **NOTE** The credentials file is typically located at `~/.aws/credentials` on Unix-like systems, or `%USERPROFILE%\.aws\credentials` on Windows
>
> See also: [sso-configure-profile-token-auto-sso](https://docs.aws.amazon.com/cli/latest/userguide/sso-configure-profile-token.html#sso-configure-profile-token-auto-sso)

In the definition for `Allos.Amazon.Sdk.Tests.IntegrationTests.Tests.TestBase` on which the property `TestAwsCredentialsProfileName` should be set to a profile name that exists in the local credential file. 

> NOTE a default could also be hardcoded in the private field `_testAwsCredentialsProfileName`
