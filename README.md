# Allos.Amazon.Sdk

A manually created fork of a subset of the official `AWS SDK`: [aws-sdk-net](https://github.com/aws/aws-sdk-net.git) that discards `[Obsolete]` and synchronous functionality and prefers modern `C#` syntax to that of the original source code. 

The current version is limited to a fork `Amazon.S3.Transfer.TransferUtility` and associated tests.

> NOTE In addition to syntax changes, [Nullable reference types](https://learn.microsoft.com/en-us/dotnet/csharp/nullable-references) have been enabled (i.e. `#nullable enable`), and code has been refactored to successfully build with them enabled
