<!--
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
-->

# IoTDB C# .NET Core Thrift Client.

In this directory we have instructed the client binding for .NET Core on Linux. We have provided two projects:

- **IoTDBService.csproj**. A project for compiling the thrift generated code.
- **IoTDBClient/IoTDBClient.csproj**. A project for compiling a client.

In the folder IoTDBClient, there is a simple C# .NET Core program that acceses to the database.

For using the binding you should:

1. Call the file `compile.sh` for generating C# files in the folder `src/`
2. Perform a `dotnet build`
3. If you want to compile the sample client go inside folder `IoTDBClient.` and perform a `dotnet build` 



