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

## Code Summary Information ##
_The summary file provides a simple way to navigate through the source code to understand the architecture of the application. It follows the below principles:_

* **Word** highlighted in **bold** are **java class** files within the source code
* The relative path of these **java class** files can be found in the attached _**csv**_ file
* _**Word**_ highlighted in both _**bold**_ and _**italic**_ are _**standard java class**_ files

#### IoTDB ####
The entry point for the server is **[IoTDB](https://github.com/apache/iotdb/tree/master/server/src/main/java/org/apache/iotdb/db/service/IoTDB.java)**. It performs following tasks during the setup

* It calls **StartupChecks** and performs the following check
  * Checks JDK Version if it is greater than 8
  * Checks JMX Port
Next, it performs following activities and registers the essential services:

* Adds **IoTDBShutdownHook** to the _**Runtime**_
* Adds new instance of **IoTDBDefaultThreadExceptionHandler** into _**Thread**_
* Uses **RegisterManager** to register following services
  * **JMXService**
  * **FlushManager**
  * **MultiFileLogNodeManager**
  * **Monitor**
  * **StatMonitor**
  * **Measurement**
  * **ManageDynamicParameters**
  * **TVListAllocator**
  * **CacheHitRatioMonitor**
  * **StorageEngine**
  * **RPCService**
  * **MetricsService**
  * **MQTTService**
  * **SyncServerManager**
  * **UpgradeSevice**
  * **MergeManager**
  * **HotCompactionMergeTaskPoolManager**

#### IoTDB Cli ####
The entry point for the IoTDB Cli is **[Cli](https://github.com/apache/iotdb/blob/master/cli/src/main/java/org/apache/iotdb/cli/Cli.java)**. 
