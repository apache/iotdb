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
  <version>0.12.0-SNAPSHOT</version>
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

3. Put the JAR package in the directory `iotdb-server-0.12.0-SNAPSHOT/ext/trigger` (or a subdirectory of `iotdb-server-0.12.0-SNAPSHOT/ext/trigger`).

   > You can specify the root path to load the trigger JAR packages by modifying the `trigger_root_dir` in the configuration file.

4. Use the SQL statement to register the trigger. Assume that the name given to the trigger is `alert-listener-sg1d1s1`.

   ```sql
   CREATE TRIGGER alert-listener-sg1d1s1
   AFTER INSERT
   ON root.sg1.d1.s1
   AS "org.apache.iotdb.db.engine.trigger.example.AlertListener"
   WITH (
     "lo" = "0", 
     "hi" = "100.0"
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
DROP TRIGGER alert-listener-sg1d1s1
```



### Start Triggers

This operation changes the state of the trigger from `STOPPED` to `STARTED`, which will make the trigger re-listen to the operations on the registered time series and respond to data changes.



The following shows the SQL syntax of starting a trigger:

```sql
START TRIGGER <TRIGGER-NAME>
```



The following is an example of a `START TRIGGER` statement:

```sql
START TRIGGER alert-listener-sg1d1s1
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
STOP TRIGGER alert-listener-sg1d1s1
```



### Show All Registered Triggers

``` sql
SHOW TRIGGERS
```



### User Authority Management

When a user manages triggers, 4 types of authorities will be involved:

* `CREATE_TRIGGER`: Only users with this authority are allowed to register triggers.
* `DROP_TRIGGER`: Only users with this authority are allowed to drop triggers.
* `START_TRIGGER`: Only users with this authority are allowed to start triggers.
* `STOP_TRIGGER`: Only users with this authority are allowed to stop triggers.

For more information, refer to [Authority Management Statement](../Operation%20Manual/Administration.md).



### Important Notes

* The trigger is implemented based on the reflection mechanism. Triggers can be dynamically registered and dropped without restarting the server.

* It is best not to have classes with the same full class name but different function implementations in different JAR packages under `trigger_root_dir`. For example: the triggers `trigger1` and `trigger2` correspond to `trigger1.jar` and `trigger2.jar` respectively. If both JAR packages contain a `org.apache.iotdb.db.engine.trigger.example.AlertListener` class, when this class is used by a `CREATE TRIGGER` statement, the system will randomly load the class in one of the JAR packages, which may lead to inconsistent trigger behaviors and other problems.

* Version management of trigger classes with the same full class name. Triggers with the same full class name but different versions (logic) are not allowed to register in the system.

  Related question: IoTDB pre-registered 10 `org.apache.iotdb.db.engine.trigger.example.AlertListener` trigger instances and DBA updated the implementation and corresponding JAR package of `org.apache.iotdb.db.engine.trigger.example.AlertListener`. Is it possible to drop only 5 of the instances and replace them with 5 updated trigger instances?

  Answer: No. Only by dropping all the 10 pre-registered triggers can you register the updated triggers. If all the original triggers are not dropped, the newly registered triggers with the same full class name will behave in the same way as the existing triggers.

