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

# 自定义管道插件

## 概述

在 Apache IoTDB 中，一个协同任务由三个阶段：收集，处理和发送组成，分别由 Pipe Collector，Pipe Processor 和 Pipe Connector 完成。三个组件除了使用系统默认的实现之外，您还可以通过自定义 PipeProcessor 和 PipeConnector 来完成特定任务，例如使用您的算法变换数据或是使用其他网络传输协议导入到任意数据平台。

根据此文档，您将学会如何编写自定义 PipeProcessor，PipeConnector，以及将其组装成一个同步任务并运行它。

## 开发依赖
推荐采用 Maven 构建项目，在`pom.xml`中添加以下依赖。请注意选择和 Apache IoTDB 服务器版本相同的依赖版本。

``` xml
<dependency>
  <groupId>org.apache.iotdb</groupId>
  <artifactId>pipe-api</artifactId>
  <version>1.0.0</version>
  <scope>provided</scope>
</dependency>
```

## 概念介绍

### Event

Event 在编写自定义 PipeProcessor 和 PipeConnector 之前，您需要了解 Event 概念。

Event 是协同任务中操作事件的抽象，由 PipeCollector 捕获并依次在 PipeProcessor 和 PipeConnector 组件之间传递，最后由 PipeConnector 传输至外部系统。

在实际运行过程中，Event 有三种具体类型：
* TabletInsertionEvent 数据写入事件
* TsFileInsertionEvent 数据文件写入事件
* DeletionEvent 数据删除事件 

### TabletInsertionEvent

TabletInsertionEvent 代表数据写入事件，事件数据存在内存中，未经过压缩或编码，可以直接进行计算处理。

您可以使用 `process` 函数处理 TabletInsertionEvent 包含的数据，有两种处理方式：

1. 每次处理一行数据
2. 获得当前 Event 包含的数据的迭代器，自由处理数据 

当您处理完成数据之后，使用 RowCollector 来收集您希望保留的数据

``` java
public class TabletInsertionEvent {

    public TabletInsertionEvent processRowByRow(BiConsumer<Row, RowCollector> consumer);

    public TabletInsertionEvent processByInterator(BiConsumer<RowIterator, RowCollector> consumer);

    public TabletInsertionEvent process(BiConsumer<Tablet, RowCollector> consumer);
}
```

### TsFileInsertionEvent

TsFileInsertionEvent 代表数据文件写入事件，事件数据以 TsFile 的格式存在磁盘上，经过压缩和编码，进行计算处理时需要付出文件 I/O 代价。另一方面，TsFile 格式的数据文件具有高压缩比特点，直接传输 TsFile 可以节省后续网络传输流量并以较少资源快速加载至 Apache IoTDB。

TsFileInsertionEvent 出现在管道事件流中有两种情况：
* 历史数据：所有已经落盘的写入数据都以 TsFile 的形式存在，处理历史数据时，以 TsFileInsertionEvent 作为抽象
* 实时数据：当数据流中实时处理数据的速度慢于写入速度一定进度之后，内存中的数据会被持久化至磁盘，以 TsFile 的形式存在，进入管道处理时，以 TsFileInsertionEvent 作为抽象 

您可以使用getStatistic方法获得该 TsFile 的统计量。一种比较常见的作法利用数据统计量进行高效判断，而无需解析全部数据。由于解析全部数据耗时较长，因此利用此策略跳过处理一些不必要处理的 TsFile 是明智的。 

若您需要对 TsFileInsertionEvent 进行处理，您需要使用 toTabletInsertionEvents 方法将其转换成TabletInsertionEvent，再使用相关方法处理数据。

此外您可以使用静态方法`toTsFileInsertionEvents`方法将多个 TabletInsertionEvent 合并成一个 TsFileInsertionEvent。通过对数据压缩编码成 TsFile 格式，将有助于后续的网络传输和数据加载。

``` java
public class TsFileInsertionEvent { 
    
    public List<TabletInsertionEvent> toTabletInsertionEvents(); 

    public TsFileStatistic getStatistic(); 
 
    public static TsFileInsertionEvent toTsFileInsertionEvent(Iterable<TabletInsertionEvent> iterable);    
}
```

### DeletionEvent

DeletionEvent 代表数据删除事件。您的删除操作将被分解成一个或多个删除事件。 

删除事件主要由删除路径和删除的事件范围组成，可以使用`getPath`和`getTimeRange`方法获得这些变量。

``` java
public class DeletionEvent{

    public String getPath();

    public TimeRange getTimeRange();
}
```

## 编写自定义插件

### PipeProcessor SDK

#### 概览

PipeProcessor 专注于 PipeCollector 采集的数据形成的 Event 进行筛选以及变换。

通过实现接口中的变换方法，您可以以任意方式处理这些 Event。

``` java
public interface PipeProcessor extends AutoClosable {

    void validate(PipeValidator validator);
    
    void beforStart(PipeParameter param，ProcessorRuntimeConfiguration config) throws Exception;

    void process(TabletInsertionEvent te, EventCollector ec) throws Exception;
    
    void process(TsFileInsertionEvent te, EventCollector ec) throws Exception;
    
    void process(DeletionEvent de, EventCollector ec) throws Exception;
}
```

PipeProcessor 的完整的生命周期如下
* 同步任务开始前，解析 SQL 中`WITH PROCESSOR`子句的 KV 对，调用`validate`方法
* 同步任务开始时，加载类并初始化 PipeProcessor，调用`beforeStart`方法
* 同步任务进行时
    * PipeCollector 捕获写入，删除事件并生成三种 Event
    * Event 被判断类型之后交付 PipeProcessor 对应的`process`函数
    * 处理完成之后 Event 被交付给 PipeConnector
* 同步任务取消时（执行`DROP PIPE`时），PipeProcessor 自动调用`close`方法

#### API

##### validate

您可以使用 PipeValidator 来对`WITH PROCESSOR`子句中的 KV 参数对进行要求，例如定义哪些参数是必须的或是参数类型。

``` java
public class PipeValidator{
    public PipeValidator requiredAttribute(String key);
}
```

##### beforeStart

在处理数据之前，您需要了解如何构建一个 PipeProcessor。

PipeProcessor 使用 PipeParameter 和 ProcessorRuntimeConfiguration 初始化。前者由`CREATE PIPE`中构建 Processor 的`WITH PROCESSOR`子句中的 key-value 属性组成。后者用于定义 Processor 的运行时行为。

``` java
public class PipeParameter{
    // ...
    
    public boolean hasAttribute(String key);
    
    public String getStringOrDefault(String key, String defaultValue);
    
    // ...
}
```

##### process

PipeProcessor 完成构建之后，您需要实现三个`process`方法以明确如何处理三种 Event。来自 PipeCollector 的 Event 组成的事件流，框架将依次对每个 Event 调用 PipeProcessor 对应的`process`方法。

当您在每个`process`方法完成对应 Event 的处理之后，使用 EventCollector 收集您希望保留的 Event。

#### 示例

下面将介绍一个简单的 PipeProcessor 的实现，它能够过滤出序列中所有值大于等于`threshold`的异常数据用于接下来的同步。

``` java
/**
 * 这是一个简单的 PipeProcessor 的示例，此 Processor 能过滤出超过阈值的异常数据点
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

#### 概述

PipeConnector 作为数据协同框架的网络层，接口上支持接入 ThriftRPC、 HTTP、TCP 等多种传输协议进行协同数据的发送。

PipeConnector 的定义和方法如下：

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

PipeConnector 完整的生命周期如下：
* 协同任务开始前，执行`validate`方法，用于检测用户输入参数是否合法
* 协同任务开始时，加载类并初始化 PipeConnector，调用 `beforeStart`方法
* 协同任务运行时：
  * 调用`handshake`方法完成发送端与目的端的握手连接
  * 调用`transfer`方法开始协同任务的传输
  * 提供`heartbeat`心跳检测方法
* 协同任务暂停时，PipeConnector 停止传输数据
* 协同任务取消时，PipeConnector 调用`close`方法结束
当`heartbeat`方法心跳检测失败时，会调用`handshake`方法尝试重新建立连接。

#### API

##### validate

与 PipeProcessor 一致，您可以使用 PipeValidator 来对`WITH CONNECTOR`子句中的 KV 参数对进行要求，例如定义哪些参数是必须的或是参数类型。

##### beforeStart

`beforeStart` 方法中第一个参数 PipeParameter 由 SQL 语句 `CREATE PIPE` 的`WITH CONNECTOR`子句中的 key-value 属性组成，PipeParameter 结构与 PipeProcessor 中`beforeStart`方法的同名参数描述的一致。


第二个参数 ConnectorRuntimeConfiguration 定义运行时的行为。
``` java
public class ConnectorRuntimeConfiguration {

    // 是否复用
    public ConnectorRuntimeConfiguration enableReused();
    
    // 运行是否并行
    public ConnectorRuntimeConfiguration enableParallel();
    
    // 重连策略
    public ConnectorRuntimeConfiguration setRetryStrategy();
}
```

##### handshake

`handshake`方法完成发送端与 PipeParameter 中配置的目的端的连接。

##### transfer

`transfer`方法用于传输三种不同事件类 TabletEvent、TsFileEvent、DeletionEvent。

##### heartbeat

`heartbeat`方法检测与目的端的心跳。当无法连接上目的端时，PipeConnector 按照 ConnectorRuntimeConfiguration 配置的参数进行尝试 handshake重连。

##### close

PipeConnector 的结束方法，您可以在此方法中进行一些资源释放等操作。

此方法由框架调用。对于一个 PipeConnector 类实例而言，生命周期中会且只会被调用一次，即在协同任务取消时调用。

#### 示例
下面将介绍一个简单的 PipeConnector 的实现，完成协同任务的发送。

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

## 管理自定义插件包

为了在 Apache IoTDB 中使用您编写的 PipeProcessor 及 PipeConnector，您需要将上述实现打包成 jar 包，然后使用相关命令加载进 Apache IoTDB。完成这些之后您就可以在`CREATE PIPE`时使用您自定义的插件了。

### 加载插件

若您已经完成打包工作，您需要使用 CLI 连接上发送端集群任意 DataNode，使用以下指令将目标插件加载进 Apache IoTDB。

``` SQL
CREATE PIPEPLUGIN <别名> AS <'全类名'> USING <'JAR包URI'>
```

例如，您编写完成 PipeProcessor 实现类 com.pipe.plugin.MyProcessor，PipeConnector 实现类 com.pipe.plugin.MyConnector，使用以下指令将两者加载进 Apache IoTDB。

``` SQL
CREATE PIPEPLUGIN p as 'com.pipe.plugin.MyProcessor' USING '/iotdb/pipe-plugin.jar'
CREATE PIPEPLUGIN c as 'com.pipe.plugin.MyConnector' USING 'https://example.com:8080/iotdb/pipe-plugin.jar'
```

加载完成之后，您可以在CREATE PIPE语句中使用相关插件来构建任务了。

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
### 删除插件

对于您不想保留在 Apache IoTDB 中的插件，您也可以将其从 Apache IoTDB 中删除。

``` Shell
IoTDB> DROP PIPEPLUGIN <别名>
```

例如，您可以删除 com.pipe.plugin.MyProcessor。

``` Shell
IoTDB> DROP PIPEPLUGIN p
```

### 查看插件

您可以使用指令查看已经加载进 Apache IoTDB 的插件。返回结果集由三列：别名，全类名以及类型组成。

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
