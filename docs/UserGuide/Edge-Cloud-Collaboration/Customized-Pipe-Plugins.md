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

# Customized Pipe Plugins

## Overview

A collaboration task consists of three phases in Apache IoTDB: data collecting, data processing, and data transferring, which are done by PipeCollector, PipeProcessor and PipeConnector respectively. In addition to the default implementations of the three components, you can also customize PipeProcessor and PipeConnector to perform specific tasks, such as transforming data using your own algorithms or importing to any data platform using other network transfer protocols.

This document describes how to customize PipeProcessor and PipeConnector and how to use them in a collaboration task.

## Development Dependencies

It is recommended to build the project using Maven and add the following dependencies to the pom.xml file. Please note that you must select the same dependency version as the target IoTDB server version for development.
``` xml
<dependency>
  <groupId>org.apache.iotdb</groupId>
  <artifactId>pipe-api</artifactId>
  <version>1.0.0</version>
  <scope>provided</scope>
</dependency>
```

## Conception

### Event

Before customizing PipeProcessor and PipeConnector, you need to understand the concept of Event. 

Event is the abstraction of the operational event in a collaboration task, which is captured by PipeCollector and passed between PipeProcessor and PipeConnector components in turn, and finally transferred to the external system by PipeConnector.

During runtime, there are three types of Event:
* TabletInsertionEvent - the event of data writing
* TsFileInsertionEvent - the event of TsFile writing
* DeletionEvent - the event of data deletion

### TabletInsertionEvent

TabletInsertionEvent describes the data writing event. The Event stores in memory, which is not compressed or encoded, and can be processed directly for computation.

The `process` method is used to process the data of TabletInsertionEvent in three ways:
1. Process one row at a time
2. Process data freely by iterator
3. Process data directly

After processing, use `RowCollector` to collect the data you want to retain.

``` java
public class TabletInsertionEvent {

    public TabletInsertionEvent processRowByRow(BiConsumer<Row, RowCollector> consumer);

    public TabletInsertionEvent processByInterator(BiConsumer<RowIterator, RowCollector> consumer);

    public TabletInsertionEvent process(BiConsumer<Tablet, RowCollector> consumer);
}
```

### TsFileInsertionEvent

TsFileInsertionEvent describes the TsFile writing event. The Event data stores in disk, which is compressed and encoded, requires a IO cost of files for computational processing. In addition, TsFile have a high compression ratio, and Transferring TsFile directly can save network transfer traffic and load to Apache IoTDB quickly with fewer resources. 

TsFileInsertionEvent appears in Pipe for two cases: 
* Historical Data: All written data in disk is stored as TsFile, and TsFileInsertionEvent is used as the abstraction of processing historical data.
* Real-time Data: When the real-time processing in data stream is slower than the data writing speed, the data in memory will be persisted to disk as TsFile.

The `getStatistic` method is used to get the statistical information of the TsFile. A common approach is to use data statistics to make judgements efficiently, instead of parsing entire data. The approach is a good idea to skip unnecessary TsFile processing due to the long-time data processing for entire TsFile.

The `toTabletInsertionEvents` method is used to convert TsFileInsertionEvent into TabletInsertionEvent.

The `toTsFileInsertionEvents` static method is used to merge several TabletInsertionEvents into one TsFileInsertionEvent. By compressing and encoding the data into TsFile format, it will facilitate network transmission and data loading.

``` java
public class TsFileInsertionEvent { 
    
    public List<TabletInsertionEvent> toTabletInsertionEvents(); 

    public TsFileStatistic getStatistic(); 
 
    public static TsFileInsertionEvent toTsFileInsertionEvent(Iterable<TabletInsertionEvent> iterable);    
}
```

### DeletionEvent

DeletionEvent describes the data deletion event. Deletion operation will be split into one or more DeletionEvents.

DeletionEvent consists of the deletion path and the deletion time range, and `getPath` and `getTimeRange` methods are used to get these variables.

``` java
public class DeletionEvent{
    public String getPath();

    public TimeRange getTimeRange();
}
```

## Implement Customized Pipe Plugins

### PipeProcessor SDK

#### Overview

PipeProcessor is used to filter and transform the Event formed by the PipeCollector.

You can handle the Events in any way you want by implementing the process methods.

``` java
public interface PipeProcessor extends AutoClosable {

    void validate(PipeValidator validator);
    
    void beforStart(PipeParameter param，ProcessorRuntimeConfiguration config) throws Exception;

    void process(TabletInsertionEvent te, EventCollector ec) throws Exception;
    
    void process(TsFileInsertionEvent te, EventCollector ec) throws Exception;
    
    void process(DeletionEvent de, EventCollector ec) throws Exception;
}
```

The lifecycle of a PipeProcessor is as follows:
* Before the sync task starts, the KV pair of `WITH PROCESSOR` clause in SQL is parsed and `validate` method is called to validate the parameters.
* When the sync task starts, load and initialize the PipeProcessor instance, and then call `beforeStart`method
* while the sync task is in progress 
  * PipeCollector capture the writes and deletes events and generates three types of Event 
  * the Event is delivered to the corresponding process method in PipeProcessor 
  * the Event is delivered to the PipeConnector after processing is completed
* When the sync task is cancelled(When `DROP PIPE` is executed), the PipeProcessor calls the autoClose method.

#### API

##### validate

You can use PipeValidator to specify requirements for the key-value pairs in `WITH PROCESSOR` clause, such as defining which parameters are required or the parameter types.

``` java
public class PipeValidator{
    public PipeValidator validateRequiredAttribute(String key) throws PipeAttributeNotProvidedException;
}
```

##### beforeStart

Before processing data, you need to understand how to build a PipeProcessor.

PipeProcessor is initialized using PipeParameter and ProcessorRuntimeConfiguration. The former is composed of key-value attributes in the `WITH PROCESSOR` clause of the `CREATE PIPE` statement that builds the Processor. The latter is used to define the runtime behavior of the Processor.

``` java
public class PipeParameter{
    // ...
    
    public boolean hasAttribute(String key);
    
    public String getStringOrDefault(String key, String defaultValue);
    
    // ...
}
```

##### process

After the PipeProcessor is built, you need to implement the three `process` methods to clarify how to process the three types of Event. The event stream composed of Event from PipeCollector will call the corresponding `process` method of PipeProcessor in turn.

After processing each `process` method, use EventCollector to collect the Event you want to retain.


#### Example

The following introduces a simple implementation of PipeProcessor, which can filter out all data which is greater than or equal to `threshold`.

TODO:
``` java
/**
  * This is a simple example of PipeProcessor, which can filter out all data which is greater than or equal to threshold.
  */
public class FilterProcessor implements PipeProcessor{

    private long threshold;
    
    void validate(PipeValidator validator) {
        validator.validateRequiredAttribute("threshold");
    }
    
    void beforStart(ProcessorParameter param，ProcessorRuntimeConfiguration config) throws Exception {
        threshold = String.valueOf(param.getValue("threshold");
    }

    void process(TabletInsertionEvent te, EventCollector ec) throws Exception {
        te.process((row, collector) -> {
            boolean anomalous = false;
            for (long v : row.getLongs()) {
                anomalous |= (v >= threshold);
            }
            if (anomalous) {
                collector.collect(row);
            }
        }
        
        ec.collect(te);
    }
    
    void process(TsFileInsertionEvent te, EventCollector ec) throws Exception {
        if (te.getStatistic().maxValue() < threshold) {
            return;
        }
        
        for (TabletInsertionEvent event : te.toTabletInsertionEvents()) {
            process(event, ec);
        }
    }
    
    void process(DeletionEvent te, EventCollector ec) throws Exception {}
}
```

### PipeConnector SDK

#### Overview

PipeConnector as the network layer, supports access to multiple transport protocols such as ThriftRPC, HTTP, TCP, etc. for transferring data

The definition and methods of PipeConnector are as follows:

``` java
public interface PipeConnector extends AutoClosable{

  void validate(PipeValidator validator) throws Exception;
  
  void beforeStart(PipeParameters params, ConnectorRuntimeConfiguration config) throws Exception;

  void handshake() throws Exception;
  
  void heartbeat() throws Exception;

  default void transfer(TabletInsertionEvent te) throws Exception {}
  default void transfer(TsFileInsertionEvent te) throws Exception {}
  default void transfer(DeletionEvent de) throws Exception {}
}
```

The lifecycle of a PipeConnector is as follows:
* Before the sync task starts, the KV pair of `WITH CONNECTOR` clause in SQL is parsed and `validate` method is called to validate the parameters.
* When the sync task starts, load and initialize the PipeConnector instance, and then call `beforeStart` method.
* while the sync task is in progress:
  * PipeConnector calls `handshake` method to establish connections between servers 
  * PipeConnector calls `transfer` method to transfer data 
  * PipeConnector provides `heartbeat` method
* When the sync task is stopped, the PipeConnector stops transferring data.
* When the sync task is cancelled, the PipeConnector calls close method automatically.

#### API

##### validate

As with PipeProcessor, you can use PipeValidator to make requirements for key-value pairs in the `WITH CONNECTOR` clause, e.g. to define which parameters are required or the parameter types.

##### beforeStart

The first parameter PipeParameter in the `beforeStart` method consists of the key-value attributes in the `WITH CONNECTOR` clause of the SQL statement `CREATE PIPE`, and the PipeParameter structure is the same as described in the `beforeStart` method of PipeProcessor.

The second parameter ConnectorRuntimeConfiguration defines the runtime behavior.
``` java
public class ConnectorRuntimeConfiguration {

    public ConnectorRuntimeConfiguration enableReused();

    public ConnectorRuntimeConfiguration enableParallel();
    
    public ConnectorRuntimeConfiguration setRetryStrategy();
}
```

##### handshake

The `handshake` method establishes the connection between servers configured in the PipeParameter.

##### transfer

The `transfer` method is used to transfer three types of Event: TabletEvent, TsFileEvent, DeletionEvent.

##### heartbeat

The `heartbeat` method detects the heartbeat with the destination servers. When it fails to connect, PipeConnector will try to handshake reconnection according to the parameters configured in ConnectorRuntimeConfiguration.

#### Example

The following is a simple implementation of PipeConnector to complete the sending of collaborative tasks.

``` java
public class exampleConnector implements PipeConnector{

    private final String host;
    private final int port;
    private final boolean compression;
    private TTransport transport = null;
    private volatile IClientRPCService.Client serviceClient = null;

    void validate(PipeValidator validator) {
        validator.validateRequiredAttribute("host");
        
        validator.validateRequiredAttribute("port");
        
        validator.validateRequiredAttribute("compression");
    }

    void beforeStart(PipeParameter params，ConnectorRuntimeConfiguration configs) throws Exception {
      host = String.valueOf(param.getValue("host");
      port = Integer.valueOf(param.getValue("port");
      compression = Boolean.valueOf(param.getValue("compression");
    }
    
    void transfer(TabletInsertionEvent te) throws Exception {
        serviceclient.sendEvent(te);
    }
    
    void transfer(TsFileInsertionEvent te) throws Exception {
        serviceclient.sendEvent(te);
    }

    void transfer(DeletionEvent de) throws Exception {
        serviceclient.sendEvent(de);
    }
}
```
## Manage Customized Plugins Package

In order to use your PipeProcessor and PipeConnector in Apache IoTDB, you need to package the above implementation as a jar package and then load it into Apache IoTDB using the relevant commands. Once it is done, you can use your customized plugins when `CREATE PIPE`.

### Load Plugins

If you have finished packaging, you need to use the CLI to connect to any DataNode on the cluster and load the target plugins into Apache IoTDB using the following command:

``` SQL
CREATE PIPEPLUGIN <ALIAS> AS <'FULL CLASS NAME'> USING <'JAR PACKAGE URI'>
```

For example, you write the PipeProcessor implementation class `com.pipe.plugin.MyProcessor` and the PipeConnector implementation class `com.pipe.plugin.MyConnector` and load both into the Apache IoTDB using the following commands:

``` SQL
CREATE PIPEPLUGIN p as 'com.pipe.plugin.MyProcessor' USING '/iotdb/pipe-plugin.jar'
CREATE PIPEPLUGIN c as 'com.pipe.plugin.MyConnector' USING 'https://example.com:8080/iotdb/pipe-plugin.jar'
```

加载完成之后，您可以在CREATE PIPE语句中使用相关插件来构建任务了。
After loading, you can use the relevant plugins to build tasks in `CREATE PIPE` clause.

``` SQL
CREATE PIPE my_pipe
WITH COLLECTOR (
    'collector' = 'org.apache.iotdb.pipe.collector.DefaultCollector'
    'collector' = 'root.db.**'
) WITH PROCESSOR (
    'processor' = 'p'
) WITH CONNECTOR (
    'connector' = 'c'
)
```
### Drop Plugins

You can also delete plugins that you do not want to remain in the Apache IoTDB.

``` Shell
IoTDB> DROP PIPEPLUGIN <ALIAS>
```

For example, you can delete `com.pipe.plugin.MyProcessor`.

``` Shell
IoTDB> DROP PIPEPLUGIN p
```

### Show Plugins

You can use the following command to view the plugins loaded into Apache IoTDB. The result set consists of three columns: alias, full class name and type.

``` Shell
IoTDB> SHOW PIPEPLUGINS
+-------+----------------------------+--------------+
|     Id|               FullClassName|          Type|
+-------+----------------------------+--------------+
|      p| com.pipe.plugin.MyProcessor| PipeProcessor|
+-------+----------------------------+--------------+
|      c| com.pipe.plugin.MyConnector| PipeConnector|
+-------+----------------------------+--------------+
Total line number = 2
It costs 0.001s
```
