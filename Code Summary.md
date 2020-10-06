## Code Summary Information ##
_The summary file provides a simple way to navigate through the source code to understand the architecture of the application. It follows the below principles:_
* **Word** highlighted in **bold** are **java class** files within the source code
* The relative path of these **java class** files can be found in the attached _**csv**_ file
* _**Word**_ highlighted in both _**bold**_ and _**italic**_ are _**standard java class**_ files

#### IOTDB ####
The entry point for the server is **[IOTDB](https://github.com/apache/iotdb/tree/master/server/src/main/java/org/apache/iotdb/db/service/IOTDB.java)**. It performs following tasks during the setup
* It calls **StartupChecks** and performs the following check
  * Checks JDK Version if it is greater than 8
  * Checks JMX Port
Next, it performs following activities and registers the essential services:
* Adds **IOTDBShutdownHook** to the _**Runtime**_
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

#### IOTDB Cli ####
The entry point for the IOTDB Cli is **[Cli](https://github.com/apache/iotdb/blob/master/cli/src/main/java/org/apache/iotdb/cli/Cli.java)**. 
