# Allos.Amazon.Sdk

A manually created fork of a subset of the official `AWS SDK`: [aws-sdk-net](https://github.com/aws/aws-sdk-net.git) that discards `[Obsolete]` and synchronous functionality and prefers modern `C#` syntax to that of the original source code. 

The current version is limited to a fork `Amazon.S3.Transfer.TransferUtility` and associated tests.

> NOTE In addition to syntax changes, [Nullable reference types](https://learn.microsoft.com/en-us/dotnet/csharp/nullable-references) have been enabled (i.e. `#nullable enable`), and code has been refactored to successfully build with them enabled

To run tests, in the class `TestBase` property `TestAwsCredentialsProfileName` set the value of `testAwsCredentialsProfileName` to an appropriate credentials file profile name:

```csharp
string testAwsCredentialsProfileName = null!;

// NOTE replace `null!` above with a valid `profile name` from the local AWS credentials file.
// It should look something like this (without enclosing `[]`):
//
//      string testAwsCredentialsProfileName = "001122334455_AwsExampleUserAccess";
//
```

