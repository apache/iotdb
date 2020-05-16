# IoTDB C# .NET Core Thrift Client.

In this directory we have instructed the client binding for .NET Core on Linux. We have provided two projects:

- **IoTDBService.csproj**. A project for compiling the thrift generated code.
- **IoTDBClient/IoTDBClient.csproj**. A project for compiling a client.

In the folder IoTDBClient, there is a simple C# .NET Core program that acceses to the database.

For using the binding you should:

1. Call the file `compile.sh` for generating C# files in the folder `src/`
2. Perform a `dotnet build`
3. If you want to compile the sample client go inside folder `IoTDBClient.` and perform a `dotnet build` 



