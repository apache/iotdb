<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

<!-- TOC -->

## Outline
- How to develop IoTDB in IDE
    - IDEA
    - Eclipse
    - Debugging IoTDB

<!-- /TOC -->
# How to develop IoTDB in IDE

There are many ways to compile the source code of IoTDB,
e.g., modify and compile with IDEA or Eclipse.

Once all UTs are passed after you modify codes, your modification basically works! 

## IDEA

* "File" -> "Open" -> choose the root path of IoTDB source code. 
* use `mvn clean compile -Dmaven.test.skip=true`to get target.
* mark directory ***server/target/generated-sources/antlr4*** as source code
* mark directory ***service-rpc/target/generated-sources/thrift*** as source code 

## Eclipse

Using Eclipse to develop IoTDB is also simple but requires some plugins of Eclipse.

- If your Eclipse version is released before 2019, Antlr plugin maybe not work in Eclipse. In this way, you have to run the command in your console first: `mvn eclipse:eclipse -DskipTests`. 
After the command is done, you can import IoTDB as an existing project:
  - Choose menu "import" -> "General" -> "Existing Projects into Workspace" -> Choose IoTDB
   root path;
  - Done.

- If your Eclipse version is fashion enough (e.g., you are using the latest version of Eclipse),
you can just choose menu "import" -> "Maven" -> "Existing Maven Projects".
 
## Debugging IoTDB
The main class of IoTDB server is `org.apache.iotdb.db.service.IoTDB`.
The main class of IoTDB cli is `org.apache.iotdb.client.Client` 
(or `org.apache.iotdb.client.WinClient` on Win OS).

You can run/debug IoTDB by using the two classes as the entrance.

Another way to understand IoTDB is to read and try Unit Tests.
