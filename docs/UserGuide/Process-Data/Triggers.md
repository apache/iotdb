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



# Triggers

The trigger provides a mechanism for listening to changes in time series data. With user-defined logic, tasks such as alerting, data cleaning, and data forwarding can be conducted.

The trigger is implemented based on the reflection mechanism. Users can monitor data changes by implementing the Java interfaces. IoTDB allows users to dynamically register and drop triggers without restarting the server.

The document will help you learn to define and manage triggers.



## Triggers Implementation

### Dependency

You need to implement the trigger by writing a Java class, where the dependency shown below is required. If you use [Maven](http://search.maven.org/), you can search for them directly from the [Maven repository](http://search.maven.org/).

```xml
<dependency>
  <groupId>org.apache.iotdb</groupId>
  <artifactId>iotdb-server</artifactId>
  <version>0.13.0-SNAPSHOT</version>
  <scope>provided</scope>
</dependency>
```

Note that the dependency version should be correspondent to the target server version.



### Programming Interfaces

To implement a trigger, you need to implement the `org.apache.iotdb.db.engine.trigger.api.Trigger` class.

This class provides two types of programming interfaces: **life cycle hooks** and **data change listening hooks**. All the interfaces in this class are not required to be implemented. When the interfaces are not implemented, the trigger will not respond to the data changes. You can implement only some of these interfaces according to your needs.

Descriptions of the interfaces are as followed.



#### Life Cycle Hooks

| Interface Definition                                         | Description                                                  |
| :----------------------------------------------------------- | ------------------------------------------------------------ |
| `void onCreate(TriggerAttributes attributes) throws Exception` | When you use the `CREATE TRIGGER` statement to register the trigger, the hook will be called. In the life cycle of each instance, the hook will and only will be called once. This hook mainly has the following functions: 1. To parse custom attributes in SQL statements (using `TriggerAttributes`). 2. To create or apply for resources, such as establishing external links, opening files, etc. |
| `void onDrop() throws Exception`                             | When you use the `DROP TRIGGER` statement to drop the trigger, the hook will be called. In the life cycle of each instance, the hook will and only will be called once. The main function of this hook is to perform operations such as releasing resources. |
| `void onStart() throws Exception`                            | This hook will be called when you use the `START TRIGGER` statement to manually start the trigger (whose state should be `STOPPED`). |
| `void onStop() throws Exception`                             | This hook will be called when you use the `STOP TRIGGER` statement to manually stop the trigger (whose state should be `STARTED`). |



#### Data Change Listening Hooks

Currently, triggers can only listen for data insertion operations.

The timing of calling the data change listener hooks is explicitly specified in the `CREATE TRIGGER` statement rather than at the programming interface level.



##### Single Point Insertion Listening Hooks

``` java
Integer fire(long timestamp, Integer value) throws Exception;
Long fire(long timestamp, Long value) throws Exception;
Float fire(long timestamp, Float value) throws Exception;
Double fire(long timestamp, Double value) throws Exception;
Boolean fire(long timestamp, Boolean value) throws Exception;
Binary fire(long timestamp, Binary value) throws Exception;
```

For each data insertion in the registered time series, the trigger will call `fire` as a response. The input parameters `timestamp` and `value` are the time and value of the data point inserted this time. You can write any logic to process data in the `fire` hook.

Note that currently the return value of the hook is meaningless.



##### Batch Data Insertion Listening Hooks

```java
int[] fire(long[] timestamps, int[] values) throws Exception;
long[] fire(long[] timestamps, long[] values) throws Exception;
float[] fire(long[] timestamps, float[] values) throws Exception;
double[] fire(long[] timestamps, double[] values) throws Exception;
boolean[] fire(long[] timestamps, boolean[] values) throws Exception;
Binary[] fire(long[] timestamps, Binary[] values) throws Exception;
```

If you need to use the `insertTablet` interface or the `insertTablets` interface of the Session API, you can reduce the overhead of trigger calls by implementing the above batch data insertion listening hooks.

It is recommended that the behaviors of the batch data insertion listening hooks and the single point insertion listening hooks be consistent. The default implemetation of the listening hook for batch data insertion is as followed.

```java
default int[] fire(long[] timestamps, int[] values) throws Exception {
  int size = timestamps.length;
  for (int i = 0; i < size; ++i) {
    fire(timestamps[i], values[i]);
  }
  return values;
}
```

Note that currently the return value of the hook is meaningless.



#### Important Notes

* Triggers registered on different time series are difference instances, so you can maintain states in triggers.
* The states maintained by triggers will be cleared after the system stops (unless you persist the data by yourself). The states of the triggers will be default values after the system is restarted.
* All hook calls of a trigger are serialized.



## Triggers Management

You can register, drop, start or stop a trigger instance through SQL statements, and you can also query all registered triggers through SQL statements.



### Trigger States

Triggers have two states: `STARTED` and `STOPPED`. You can start or stop a trigger by executing `START TRIGGER` or `STOP TRIGGER`. 

When the state of a trigger is `STOPPED`, it will not respond to the operations on the registered time series (such as inserting a data point), but all status (trigger variables), and registration information will be saved.

Note that the default state of the triggers registered by the `CREATE TRIGGER` statement is `STARTED`.



### Register Triggers

A trigger can only be registered on an existing time series. Only one trigger can be registered for any time series.

The time series registered with the trigger will be listened to by the trigger. When there is a data change in the time series, the corresponding hook in the trigger will be called.



Registering a trigger can be carried out as follows:

1. Implement a complete Trigger class. Assume that the full class name of this class is `org.apache.iotdb.db.engine.trigger.example.AlertListener`.

2. Pack the project into a JAR package. If you use Maven to manage the project, you can refer to the above Maven project example.

3. Put the JAR package in the directory `iotdb-server-0.13.0-SNAPSHOT/ext/trigger` (or a subdirectory of `iotdb-server-0.13.0-SNAPSHOT/ext/trigger`).

   > You can specify the root path to load the trigger JAR packages by modifying the `trigger_root_dir` in the configuration file.

4. Use the SQL statement to register the trigger. Assume that the name given to the trigger is `alert-listener-sg1d1s1`.

   ```sql
   CREATE TRIGGER `alert-listener-sg1d1s1`
   AFTER INSERT
   ON root.sg1.d1.s1
   AS 'org.apache.iotdb.db.engine.trigger.example.AlertListener'
   WITH (
     'lo' = '0', 
     'hi' = '100.0'
   )
   ```



The following shows the SQL syntax of registering a trigger.

```sql
CREATE TRIGGER <TRIGGER-NAME>
(BEFORE | AFTER) INSERT
ON <FULL-PATH>
AS <CLASSNAME>
```

You can also set any number of key-value pair attributes for the trigger through the `WITH` clause:

```sql
CREATE TRIGGER <TRIGGER-NAME>
(BEFORE | AFTER) INSERT
ON <FULL-PATH>
AS <CLASSNAME>
WITH (
  <KEY-1>=<VALUE-1>, 
  <KEY-2>=<VALUE-2>, 
  ...
)
```

`TRIGGER-NAME` is a globally unique ID of the trigger, which is case sensitive.

At present, the trigger can listen to all data insertion operations on the time series. The hook can be called `BEFORE`  or `AFTER` the data is inserted.

`FULL-PATH` is the name of the time series that the trigger listens to. The path must be a measurement path.

`CLASSNAME` is the full class name of the trigger.

Note that `CLASSNAME`,  `KEY` and `VALUE` in the attributes need to be quoted in single or double quotes.



### Drop Triggers

Triggers will be dropped in the following scenarios:

1. When the user executes `DELETE TIMESERIES`, the triggers registered on the time series will be dropped.
2. When the user executes `DELETE STORAGE GROUP`, the triggers registered under the storage group will be dropped.
3. When the user executes the `DROP TRIGGER` statement.



The following shows the SQL syntax of dropping a trigger:

```sql
DROP TRIGGER <TRIGGER-NAME>
```

The following is an example of a `DROP TRIGGER` statement:

```sql
DROP TRIGGER `alert-listener-sg1d1s1`
```



### Start Triggers

This operation changes the state of the trigger from `STOPPED` to `STARTED`, which will make the trigger re-listen to the operations on the registered time series and respond to data changes.



The following shows the SQL syntax of starting a trigger:

```sql
START TRIGGER <TRIGGER-NAME>
```



The following is an example of a `START TRIGGER` statement:

```sql
START TRIGGER `alert-listener-sg1d1s1`
```



Note that the triggers registered by the `CREATE TRIGGER` statements are `STARTED` by default.



### Stop Triggers

This operation changes the state of the trigger from `STARTED` to `STOPPED`. When the status of a trigger is `STOPPED`, it will not respond to the operations on the registered time series (such as inserting a data point). You can restart a trigger using the `START TRIGGER` statement.



The following shows the SQL syntax of stopping a trigger:

```sql
STOP TRIGGER <TRIGGER-NAME>
```



The following is an example of a `STOP TRIGGER` statement:

```sql
STOP TRIGGER `alert-listener-sg1d1s1`
```



### Show All Registered Triggers

``` sql
SHOW TRIGGERS
```



### User Authority Management

When a user manages triggers, 4 types of authorities will be involved:

* `CREATE_TRIGGER`: Only users with this authority are allowed to register triggers. This authority is path dependent.
* `DROP_TRIGGER`: Only users with this authority are allowed to drop triggers. This authority is path dependent.
* `START_TRIGGER`: Only users with this authority are allowed to start triggers. This authority is path dependent.
* `STOP_TRIGGER`: Only users with this authority are allowed to stop triggers. This authority is path dependent.

For more information, refer to [Authority Management Statement](../Administration-Management/Administration.md).



## Utilities

Utility classes provide programming paradigms and execution frameworks for the common requirements, which can simplify part of your work of implementing triggers.



### Windowing Utility

The windowing utility can help you define sliding windows and the data processing logic on them. It can construct two types of sliding windows: one has a fixed time interval (`SlidingTimeWindowEvaluationHandler`), and the other has fixed number of data points (`SlidingSizeWindowEvaluationHandler`).

The windowing utility allows you to define a hook (`Evaluator`) on the window (`Window`). Whenever a new window is formed, the hook you defined will be called once. You can define any data processing-related logic in the hook. The call of the hook is asynchronous. Therefore, the current thread will not be blocked when the window processing logic is executed.

It is worth noting that whether it is `SlidingTimeWindowEvaluationHandler` or `SlidingSizeWindowEvaluationHandler`, **they can only handle sequences with strictly monotonically increasing timestamps**, and incoming data points that do not meet the requirements will be discarded.

For the definition of `Window` and `Evaluator`, please refer to the `org.apache.iotdb.db.utils.windowing.api` package.



#### SlidingSizeWindowEvaluationHandler

##### Window Construction

There are two construction methods.

The first method requires you to provide the type of data points that the window collects, the window size, the sliding step, and a hook (`Evaluator`).

``` java
final TSDataType dataType = TSDataType.INT32;
final int windowSize = 10;
final int slidingStep = 5;

SlidingSizeWindowEvaluationHandler handler =
    new SlidingSizeWindowEvaluationHandler(
        new SlidingSizeWindowConfiguration(dataType, windowSize, slidingStep),
        window -> {
          // do something
        });
```

The second method requires you to provide the type of data points that the window collects, the window size, and a hook (`Evaluator`). The sliding step is equal to the window size by default.

``` java
final TSDataType dataType = TSDataType.INT32;
final int windowSize = 10;

SlidingSizeWindowEvaluationHandler handler =
    new SlidingSizeWindowEvaluationHandler(
        new SlidingSizeWindowConfiguration(dataType, windowSize),
        window -> {
          // do something
        });
```

The window size and the sliding step must be positive.



#####  Datapoint Collection

``` java
final long timestamp = 0;
final int value = 0;
hander.collect(timestamp, value);
```

Note that the type of the second parameter accepted by the `collect` method needs to be consistent with the `dataType` parameter provided during construction.

In addition, the `collect` method will only respond to data points whose timestamps are monotonically increasing. If the time stamp of the data point collected by the `collect` method is less than or equal to the time stamp of the data point collected by the previous `collect` method call, the data point collected this time will be discarded.

Also note that the `collect` method is not thread-safe.



#### SlidingTimeWindowEvaluationHandler

##### Window Construction

There are two construction methods.

The first method requires you to provide the type of data points that the window collects, the time interval, the sliding step, and a hook (`Evaluator`).

``` java
final TSDataType dataType = TSDataType.INT32;
final long timeInterval = 1000;
final long slidingStep = 500;

SlidingTimeWindowEvaluationHandler handler =
    new SlidingTimeWindowEvaluationHandler(
        new SlidingTimeWindowConfiguration(dataType, timeInterval, slidingStep),
        window -> {
          // do something
        });
```

The second method requires you to provide the type of data points that the window collects, the time interval, and a hook (`Evaluator`). The sliding step is equal to the time interval by default.

``` java
final TSDataType dataType = TSDataType.INT32;
final long timeInterval = 1000;

SlidingTimeWindowEvaluationHandler handler =
    new SlidingTimeWindowEvaluationHandler(
        new SlidingTimeWindowConfiguration(dataType, timeInterval),
        window -> {
          // do something
        });
```

The time interval and the sliding step must be positive.



#####  Datapoint Collection

``` java
final long timestamp = 0;
final int value = 0;
hander.collect(timestamp, value);
```

Note that the type of the second parameter accepted by the `collect` method needs to be consistent with the `dataType` parameter provided during construction.

In addition, the `collect` method will only respond to data points whose timestamps are monotonically increasing. If the time stamp of the data point collected by the `collect` method is less than or equal to the time stamp of the data point collected by the previous `collect` method call, the data point collected this time will be discarded.

Also note that the `collect` method is not thread-safe.



#### Rejection Policy

The execution of window evaluation tasks is asynchronous.

When asynchronous tasks cannot be consumed by the execution thread pool in time, tasks will accumulate. In extreme cases, the accumulation of asynchronous tasks can cause the system OOM. Therefore, the number of tasks that the window evaluation thread pool allows to accumulate is set to a finite value.

When the number of accumulated tasks exceeds the limit, the newly submitted tasks will not be able to enter the thread pool for execution. At this time, the system will call the rejection policy hook `onRejection` that you have implemented in the listening hook (`Evaluator`) for processing.

The default behavior of `onRejection` is as follows.

````java
default void onRejection(Window window) {
  throw new RejectedExecutionException();
}
````

The way to implement a rejection strategy hook is as follows.

```java
SlidingTimeWindowEvaluationHandler handler =
    new SlidingTimeWindowEvaluationHandler(
        new SlidingTimeWindowConfiguration(TSDataType.INT32, 1, 1),
        new Evaluator() {
          @Override
          public void evaluate(Window window) {
            // do something
          }
          
          @Override
          public void onRejection(Window window) {
            // do something
          }
    });
```



#### Configurable Properties

##### concurrent_window_evaluation_thread

The number of threads that can be used for evaluating sliding windows. The value is equals to CPU core number by default.



##### max_pending_window_evaluation_tasks

The maximum number of window evaluation tasks that can be pending for execution. The value is 64 by default.



### Sink Utility

The sink utility provides the ability for triggers to connect to external systems.

It provides a programming paradigm. Each sink utility contains a `Handler` for processing data sending, a `Configuration` for configuring `Handler`, and an `Event` for describing the sending data.



#### LocalIoTDBSink

`LocalIoTDBSink` is used to insert data points to the local sequence.

Before writing data, it is not required that the time series have been created.

**Note**, in the scenario used for triggers, the listening time series and the written target time series should not be in the same storage group.

Example:

```java
final String device = "root.alerting";
final String[] measurements = new String[] {"local"};
final TSDataType[] dataTypes = new TSDataType[] {TSDataType.DOUBLE};

LocalIoTDBHandler localIoTDBHandler = new LocalIoTDBHandler();
localIoTDBHandler.open(new LocalIoTDBConfiguration(device, measurements, dataTypes));

// insert 100 data points
for (int i = 0; i < 100; ++i) {
  final long timestamp = i;
  final double value = i;
  localIoTDBHandler.onEvent(new LocalIoTDBEvent(timestamp, value));
}
```

Note that when you need to insert data points to a time series of type `TEXT`, you need to use `org.apache.iotdb.tsfile.utils.Binary`:

```java
// insert 100 data points
for (int i = 0; i < 100; ++i) {
  final long timestamp = i;
  final String value = "" + i;
  localIoTDBHandler.onEvent(new LocalIoTDBEvent(timestamp, Binary.valueOf(value)));
}
```



#### MQTTSink

In triggers, you can use `MQTTSink` to send data points to other IoTDB instances.

Before sending data, it is not required that the time series have been created.

Example:

```java
final String host = "127.0.0.1";
final int port = 1883;
final String username = "root";
final String password = "root";
final PartialPath device = new PartialPath("root.alerting");
final String[] measurements = new String[] {"remote"};

MQTTHandler mqttHandler = new MQTTHandler();
mqttHandler.open(new MQTTConfiguration(host, port, username, password, device, measurements));

final String topic = "test";
final QoS qos = QoS.EXACTLY_ONCE;
final boolean retain = false;
// send 100 data points
for (int i = 0; i < 100; ++i) {
  final long timestamp = i;
  final double value = i;
  mqttHandler.onEvent(new MQTTEvent(topic, qos, retain, timestamp, value));
}
```



#### AlertManagerSink

In a trigger, you can use `AlertManagerSink` to send messages to AlertManager。

You need to specify the endpoint to send alerts of your AlertManager when constructing
`AlertManagerConfiguration` 

```java
AlertManagerConfiguration(String endpoint);
```

`AlertManagerEvent` offers three types of constructors：
```java
AlertManagerEvent(String alertname);
AlertManagerEvent(String alertname, Map<String, String> extraLabels);
AlertManagerEvent(String alertname, Map<String, String> extraLabels, Map<String, String> annotations);
```

* `alertname` is a required parameter to identify an `alert`. The `alertname` field can be used for grouping and deduplication when the `AlertManager` sends an alert.
* `extraLabels` is optional. In the backend, it is combined with `alertname` to form `labels` to identify an `alert`, which can be used for grouping and deduplication when `AlertManager` sends alarms.
* `annotations` is optional, and its value can use Go style template 
    ```
    {{.<label_key>}}
    ```
    It will be replaced with `labels[<label_key>]` when the message is finally generated.
* `labels` and `annotations` will be parsed into json string and sent to `AlertManager`:
```json
{
    "labels": {
      "alertname": "<requiredAlertName>",
      "<labelname>": "<labelvalue>",
      ...
    },
    "annotations": {
      "<labelname>": "<labelvalue>",
      ...
    }
}
```

Call the `onEvent(AlertManagerEvent event)` method of `AlertManagerHandler` to send an alert.



**Example 1:**

Only pass `alertname`.

```java
AlertManagerHandler alertManagerHandler = new AlertManagerHandler();

alertManagerHandler.open(new AlertManagerConfiguration("http://127.0.0.1:9093/api/v1/alerts"));

final String alertName = "test0";

AlertManagerEvent alertManagerEvent = new AlertManagerEvent(alertName);

alertManagerHandler.onEvent(alertManagerEvent);
```



**Example 2：**

Pass `alertname` and `extraLabels`.

```java
AlertManagerHandler alertManagerHandler = new AlertManagerHandler();

alertManagerHandler.open(new AlertManagerConfiguration("http://127.0.0.1:9093/api/v1/alerts"));

final String alertName = "test1";

final HashMap<String, String> extraLabels = new HashMap<>();
extraLabels.put("severity", "critical");
extraLabels.put("series", "root.ln.wt01.wf01.temperature");
extraLabels.put("value", String.valueOf(100.0));

AlertManagerEvent alertManagerEvent = new AlertManagerEvent(alertName, extraLabels);

alertManagerHandler.onEvent(alertManagerEvent);
```



**Example 3：**

Pass `alertname`， `extraLabels` 和 `annotations`.

The final value of the `description` field will be parsed as `test2: root.ln.wt01.wf01.temperature is 100.0`.

```java
AlertManagerHandler alertManagerHandler = new AlertManagerHandler();

alertManagerHandler.open(new AlertManagerConfiguration("http://127.0.0.1:9093/api/v1/alerts"));

final String alertName = "test2";

final HashMap<String, String> extraLabels = new HashMap<>();
extraLabels.put("severity", "critical");
extraLabels.put("series", "root.ln.wt01.wf01.temperature");
extraLabels.put("value", String.valueOf(100.0));

final HashMap<String, String> annotations = new HashMap<>();
annotations.put("summary", "high temperature");
annotations.put("description", "{{.alertname}}: {{.series}} is {{.value}}");

alertManagerHandler.onEvent(new AlertManagerEvent(alertName, extraLabels, annotations));
```



## Maven Project Example

If you use [Maven](http://search.maven.org/), you can refer to our sample project **trigger-example**.

You can find it [here](https://github.com/apache/iotdb/tree/master/example/trigger).

It shows:

* How to use Maven to manage your trigger project
* How to listen to data changes based on the user programming interface
* How to use the windowing utility
* How to use the sink utility

```java
package org.apache.iotdb.trigger;

import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.engine.trigger.api.TriggerAttributes;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTEvent;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTHandler;
import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBEvent;
import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBHandler;
import org.apache.iotdb.db.utils.windowing.configuration.SlidingSizeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.handler.SlidingSizeWindowEvaluationHandler;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.fusesource.mqtt.client.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerExample implements Trigger {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerExample.class);

  private static final String TARGET_DEVICE = "root.alerting";

  private final LocalIoTDBHandler localIoTDBHandler = new LocalIoTDBHandler();
  private final MQTTHandler mqttHandler = new MQTTHandler();

  private SlidingSizeWindowEvaluationHandler windowEvaluationHandler;

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    LOGGER.info("onCreate(TriggerAttributes attributes)");

    double lo = attributes.getDouble("lo");
    double hi = attributes.getDouble("hi");

    openSinkHandlers();

    windowEvaluationHandler =
        new SlidingSizeWindowEvaluationHandler(
            new SlidingSizeWindowConfiguration(TSDataType.DOUBLE, 5, 5),
            window -> {
              double avg = 0;
              for (int i = 0; i < window.size(); ++i) {
                avg += window.getDouble(i);
              }
              avg /= window.size();

              if (avg < lo || hi < avg) {
                localIoTDBHandler.onEvent(new LocalIoTDBEvent(window.getTime(0), avg));
                mqttHandler.onEvent(
                    new MQTTEvent("test", QoS.EXACTLY_ONCE, false, window.getTime(0), avg));
              }
            });
  }

  @Override
  public void onDrop() throws Exception {
    LOGGER.info("onDrop()");
    closeSinkHandlers();
  }

  @Override
  public void onStart() throws Exception {
    LOGGER.info("onStart()");
    openSinkHandlers();
  }

  @Override
  public void onStop() throws Exception {
    LOGGER.info("onStop()");
    closeSinkHandlers();
  }

  @Override
  public Double fire(long timestamp, Double value) {
    windowEvaluationHandler.collect(timestamp, value);
    return value;
  }

  @Override
  public double[] fire(long[] timestamps, double[] values) {
    for (int i = 0; i < timestamps.length; ++i) {
      windowEvaluationHandler.collect(timestamps[i], values[i]);
    }
    return values;
  }

  private void openSinkHandlers() throws Exception {
    localIoTDBHandler.open(
        new LocalIoTDBConfiguration(
            TARGET_DEVICE, new String[]{"local"}, new TSDataType[]{TSDataType.DOUBLE}));
    mqttHandler.open(
        new MQTTConfiguration(
            "127.0.0.1",
            1883,
            "root",
            "root",
            new PartialPath(TARGET_DEVICE),
            new String[]{"remote"}));
  }

  private void closeSinkHandlers() throws Exception {
    localIoTDBHandler.close();
    mqttHandler.close();
  }
}
```

You can try this trigger by following the steps below:

* Enable MQTT service by modifying `iotdb-engine.properties`

  ``` properties
  # whether to enable the mqtt service.
  enable_mqtt_service=true
  ```

* Start the IoTDB server

* Create time series via cli

  ``` sql
  CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=DOUBLE, ENCODING=PLAIN;
  ```

* Place the JAR (`trigger-example-0.13.0-SNAPSHOT.jar`) of **trigger-example** in the directory `iotdb-server-0.13.0-SNAPSHOT/ext/trigger` (or in a subdirectory of `iotdb-server-0.13.0-SNAPSHOT/ext/trigger`)

  > You can specify the root path to load the trigger JAR package by modifying the `trigger_root_dir` in the configuration file.

* Use the SQL statement to register the trigger, assuming that the name given to the trigger is `window-avg-alerter`

* Use the `CREATE TRIGGER` statement to register the trigger via cli

  ```sql
  CREATE TRIGGER `window-avg-alerter`
  AFTER INSERT
  ON root.sg1.d1.s1
  AS 'org.apache.iotdb.trigger.TriggerExample'
  WITH (
    'lo' = '0',
    'hi' = '10.0'
  )
  ```

* Use cli to insert test data

  ``` sql
  INSERT INTO root.sg1.d1(timestamp, s1) VALUES (1, 0);
  INSERT INTO root.sg1.d1(timestamp, s1) VALUES (2, 2);
  INSERT INTO root.sg1.d1(timestamp, s1) VALUES (3, 4);
  INSERT INTO root.sg1.d1(timestamp, s1) VALUES (4, 6);
  INSERT INTO root.sg1.d1(timestamp, s1) VALUES (5, 8);
  
  INSERT INTO root.sg1.d1(timestamp, s1) VALUES (6, 10);
  INSERT INTO root.sg1.d1(timestamp, s1) VALUES (7, 12);
  INSERT INTO root.sg1.d1(timestamp, s1) VALUES (8, 14);
  INSERT INTO root.sg1.d1(timestamp, s1) VALUES (9, 16);
  INSERT INTO root.sg1.d1(timestamp, s1) VALUES (10, 18);
  ```

* Use cli to query data to verify the behavior of the trigger

  ``` sql
  SELECT * FROM root.alerting;
  ```

* Under normal circumstances, the following results should be shown

  ``` sql
  IoTDB> SELECT * FROM root.alerting;
  +-----------------------------+--------------------+-------------------+
  |                         Time|root.alerting.remote|root.alerting.local|
  +-----------------------------+--------------------+-------------------+
  |1970-01-01T08:00:00.006+08:00|                14.0|               14.0|
  +-----------------------------+--------------------+-------------------+
  Total line number = 1
  It costs 0.006s
  ```

That's all, please enjoy it :D



## Important Notes

* The trigger is implemented based on the reflection mechanism. Triggers can be dynamically registered and dropped without restarting the server.

* It is best not to have classes with the same full class name but different function implementations in different JAR packages under `trigger_root_dir`. For example: the triggers `trigger1` and `trigger2` correspond to `trigger1.jar` and `trigger2.jar` respectively. If both JAR packages contain a `org.apache.iotdb.db.engine.trigger.example.AlertListener` class, when this class is used by a `CREATE TRIGGER` statement, the system will randomly load the class in one of the JAR packages, which may lead to inconsistent trigger behaviors and other problems.

* Version management of trigger classes with the same full class name. Triggers with the same full class name but different versions (logic) are not allowed to register in the system.

  Related question: IoTDB pre-registered 10 `org.apache.iotdb.db.engine.trigger.example.AlertListener` trigger instances and DBA updated the implementation and corresponding JAR package of `org.apache.iotdb.db.engine.trigger.example.AlertListener`. Is it possible to drop only 5 of the instances and replace them with 5 updated trigger instances?

  Answer: No. Only by dropping all the 10 pre-registered triggers can you register the updated triggers. If all the original triggers are not dropped, the newly registered triggers with the same full class name will behave in the same way as the existing triggers.

