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

## 触发器

触发器提供了一种侦听序列数据变动的机制。配合用户自定义逻辑，可完成告警、数据清洗、数据转发等功能。

触发器基于 Java 反射机制实现。用户通过简单实现 Java 接口，即可实现数据侦听。IoTDB 允许用户动态装载、卸载触发器，在装载、卸载期间，无需启停服务器。

根据此文档，您将会很快学会触发器的编写与管理。

### 编写触发器

#### 触发器依赖

触发器的逻辑需要您编写 Java 类进行实现。

在编写触发器逻辑时，需要使用到下面展示的依赖。如果您使用 [Maven](http://search.maven.org/)，则可以直接从 [Maven 库](http://search.maven.org/) 中搜索到它们。

``` xml
<dependency>
  <groupId>org.apache.iotdb</groupId>
  <artifactId>iotdb-server</artifactId>
  <version>0.13.0-SNAPSHOT</version>
  <scope>provided</scope>
</dependency>
```

请注意选择和目标服务器版本相同的依赖版本。

#### 用户编程接口

编写一个触发器需要实现`org.apache.iotdb.db.engine.trigger.api.Trigger`类。

该类提供了两类编程接口：**生命周期钩子**和**数据变动侦听钩子**。该类中所有的接口都不是必须实现的，当您不实现它们时，它们不会对流经的数据操作产生任何响应。您可以根据实际需要，只实现其中若干接口。

下面是所有可供用户进行实现的接口的说明。

##### 生命周期钩子

| 接口定义                                                     | 描述                                                         |
| :----------------------------------------------------------- | ------------------------------------------------------------ |
| `void onCreate(TriggerAttributes attributes) throws Exception` | 当您使用`CREATE TRIGGER`语句注册触发器后，该钩子会被调用一次。在每一个实例的生命周期内，该钩子会且仅仅会被调用一次。该钩子主要有如下作用：1. 帮助用户解析 SQL 语句中的自定义属性（使用`TriggerAttributes`）。 2. 创建或申请资源，如建立外部链接、打开文件等。 |
| `void onDrop() throws Exception`                             | 当您使用`DROP TRIGGER`语句删除触发器后，该钩子会被调用。在每一个实例的生命周期内，该钩子会且仅仅会被调用一次。该钩子的主要作用是进行一些资源释放等的操作。 |
| `void onStart() throws Exception`                            | 当您使用`START TRIGGER`语句手动启动（被`STOP TRIGGER`语句停止的）触发器后，该钩子会被调用。 |
| `void onStop() throws Exception`                             | 当您使用`STOP TRIGGER`语句手动停止触发器后，该钩子会被调用。 |

##### 数据变动侦听钩子

目前触发器仅能侦听数据插入的操作。

数据变动侦听钩子的调用时机由`CREATE TRIGGER`语句显式指定，在编程接口层面不作区分。

###### 单点数据插入侦听钩子

``` java
Integer fire(long timestamp, Integer value) throws Exception;
Long fire(long timestamp, Long value) throws Exception;
Float fire(long timestamp, Float value) throws Exception;
Double fire(long timestamp, Double value) throws Exception;
Boolean fire(long timestamp, Boolean value) throws Exception;
Binary fire(long timestamp, Binary value) throws Exception;
```

对于注册序列上的每一点数据插入，触发器都会调用`fire`作为响应，钩子的入参`timestamp`和`value`即是这一次插入数据点的时间和数据值。您可以在`fire`钩子中编写处理数据的任意逻辑。

注意，目前钩子的返回值是没有任何意义的。

###### 批量数据插入侦听钩子

```java
int[] fire(long[] timestamps, int[] values) throws Exception;
long[] fire(long[] timestamps, long[] values) throws Exception;
float[] fire(long[] timestamps, float[] values) throws Exception;
double[] fire(long[] timestamps, double[] values) throws Exception;
boolean[] fire(long[] timestamps, boolean[] values) throws Exception;
Binary[] fire(long[] timestamps, Binary[] values) throws Exception;
```

如果您需要在业务场景中使用到 Session API 的`insertTablet`接口或`insertTablets`接口，那么您可以通过实现上述数据插入的侦听钩子来降低触发器的调用开销。

推荐您在实现上述批量数据插入的侦听钩子时， 保证批量数据插入侦听钩子与单点数据插入侦听钩子的行为具有一致性。当您不实现批量数据插入的侦听钩子时，它将遵循下面的默认逻辑。

```java
default int[] fire(long[] timestamps, int[] values) throws Exception {
  int size = timestamps.length;
  for (int i = 0; i < size; ++i) {
    fire(timestamps[i], values[i]);
  }
  return values;
}
```

注意，目前钩子的返回值是没有任何意义的。

##### 重要注意事项

* 每条序列上注册的触发器都是一个完整的触发器类的实例，因此您可以在触发器中维护一些状态数据。
* 触发器维护的状态会在系统停止后被清空（除非您在钩子中主动将状态持久化）。换言之，系统启动后触发器的状态将会默认为初始值。
* 一个触发器所有钩子的调用都是串行化的。

### 管理触发器

您可以通过 SQL 语句注册、卸载、启动或停止一个触发器实例，您也可以通过 SQL 语句查询到所有已经注册的触发器。

#### 触发器的状态

触发器有两种运行状态：`STARTED`和`STOPPED`，您需要执行`START TRIGGER`或者`STOP TRIGGER`来启动或者停止一个触发器。

当一个触发器的状态为`STOPPED`时，它将不会响应被注册序列上的操作（如插入数据点的操作），对外表现就会像是这个序列没有被注册过触发器一样，但是它会保存所有的状态（触发器类变量）信息，同时也会保存所有的注册信息。

注意，通过`CREATE TRIGGER`语句注册的触发器默认是`STARTED`的。

#### 注册触发器

触发器只能注册在一个已经存在的时间序列上。任何时间序列只允许注册一个触发器。

被注册有触发器的序列将会被触发器侦听，当序列上有数据变动时，触发器中对应的钩子将会被调用。

注册一个触发器可以按如下流程进行：

1. 实现一个完整的 Trigger 类，假定这个类的全类名为`org.apache.iotdb.db.engine.trigger.example.AlertListener`

2. 将项目打成 JAR 包，如果您使用 Maven 管理项目，可以参考上述 Maven 项目示例的写法

3. 将 JAR 包放置到目录 `iotdb-server-0.13.0-SNAPSHOT/ext/trigger` （也可以是`iotdb-server-0.13.0-SNAPSHOT/ext/trigger`的子目录）下。

   > 您可以通过修改配置文件中的`trigger_root_dir`来指定加载触发器 JAR 包的根路径。

4. 使用 SQL 语句注册该触发器，假定赋予该触发器的名字为`alert-listener-sg1d1s1`

5. 使用`CREATE TRIGGER`语句注册该触发器

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

   

注册触发器的详细 SQL 语法如下：

```sql
CREATE TRIGGER <TRIGGER-NAME>
(BEFORE | AFTER) INSERT
ON <FULL-PATH>
AS <CLASSNAME>
```

同时，您还可以通过`WITH`子句传入任意数量的自定义属性值：

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

`TRIGGER-NAME`是用于标定触发器的全局唯一 ID，它是大小写敏感的。

目前触发器可以侦听序列上的所有的数据插入操作，触发器可以选择在数据插入前（`BEFORE INSERT`）或者数据插入后（`AFTER INSERT`）触发钩子调用。

`FULL-PATH`是触发器侦听的目标序列名称，这个序列必须是一个测点。

`CLASSNAME`是触发器类的全类名。

请注意，`CLASSNAME`以及属性值中的`KEY`和`VALUE`都需要被单引号或者双引号引用起来。

#### 卸载触发器

触发器会在下面几种情景下被卸载：

1. 用户执行`DELETE TIMESERIES`时，序列上注册的触发器会被卸载
2. 用户执行`DELETE STORAGE GROUP`时，对应存储组下注册的触发器会全部被卸载
3. 用户使用`DROP TRIGGER`语句主动卸载

卸载触发器的 SQL 语法如下：

```sql
DROP TRIGGER <TRIGGER-NAME>
```

`TRIGGER-NAME`是用于标定触发器的全局唯一 ID。

下面是一个`DROP TRIGGER`语句的例子：

```sql
DROP TRIGGER `alert-listener-sg1d1s1`
```

#### 启动触发器

该操作是“停止触发器”的逆操作。它将运行状态为`STOPPED`的触发器的运行状态变更为`STARTED`，这会使得触发器重新侦听被注册序列上的操作，并对数据变动产生响应。

启动触发器的 SQL 语法如下：

```sql
START TRIGGER <TRIGGER-NAME>
```

`TRIGGER-NAME`是用于标定触发器的全局唯一 ID。

下面是一个`START TRIGGER`语句的例子：

```sql
START TRIGGER `alert-listener-sg1d1s1`
```

注意，通过`CREATE TRIGGER`语句注册的触发器默认是`STARTED`的。

#### 停止触发器

该操作将触发器的状态由`STARTED`变为`STOPPED`。当一个触发器的状态为`STOPPED`时，它将不会响应被注册序列上的操作（如插入数据点的操作），对外表现就会像是这个序列没有被注册过触发器一样。您可以使用`START TRIGGER`语句重新启动一个触发器。

停止触发器的 SQL 语法如下：

```sql
STOP TRIGGER <TRIGGER-NAME>
```

`TRIGGER-NAME`是用于标定触发器的全局唯一 ID。

下面是一个`STOP TRIGGER`语句的例子：

```sql
STOP TRIGGER `alert-listener-sg1d1s1`
```

#### 查询所有注册的触发器

查询触发器的 SQL 语句如下：

``` sql
SHOW TRIGGERS
```

该语句展示已注册触发器的 ID、运行状态、触发时机、被注册的序列、触发器实例的全类名和注册触发器时用到的自定义属性。

#### 用户权限管理

用户在使用触发器时会涉及到 4 种权限：

* `CREATE_TRIGGER`：具备该权限的用户才被允许注册触发器操作。该权限需要与触发器的路径绑定。
* `DROP_TRIGGER`：具备该权限的用户才被允许卸载触发器操作。该权限需要与触发器的路径绑定。
* `START_TRIGGER`：具备该权限的用户才被允许启动已被停止的触发器。该权限需要与触发器的路径绑定。
* `STOP_TRIGGER`：具备该权限的用户才被允许停止正在运行的触发器。该权限需要与触发器的路径绑定。

更多用户权限相关的内容，请参考 [权限管理语句](../Administration-Management/Administration.md)。

### 实用工具类

实用工具类为常见的需求提供了编程范式和执行框架，它能够简化您编写触发器的一部分工作。

#### 窗口工具类

窗口工具类能够辅助您定义滑动窗口以及窗口上的数据处理逻辑。它能够构造两类滑动窗口：一种滑动窗口是固定窗口内时间长度的（`SlidingTimeWindowEvaluationHandler`），另一种滑动窗口是固定窗口内数据点数的（`SlidingSizeWindowEvaluationHandler`）。

窗口工具类允许您在窗口（`Window`）上定义侦听钩子（`Evaluator`）。每当一个新的窗口形成，您定义的侦听钩子就会被调用一次。您可以在这个侦听钩子内定义任何数据处理相关的逻辑。侦听钩子的调用是异步的，因此，在执行钩子内窗口处理逻辑的时候，是不会阻塞当前线程的。

值得注意的是，不论是`SlidingTimeWindowEvaluationHandler`还是`SlidingSizeWindowEvaluationHandler`，他们都**只能够处理时间戳严格单调递增的序列**，传入的不符合要求的数据点会被工具类抛弃。

`Window`与`Evaluator`接口的定义见`org.apache.iotdb.db.utils.windowing.api`包。

##### 固定窗口内数据点数的滑动窗口

###### 窗口构造

共两种构造方法。

第一种方法需要您提供窗口接受数据点的类型、窗口大小、滑动步长和一个侦听钩子（`Evaluator`）。

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

第二种方法需要您提供窗口接受数据点的类型、窗口大小和一个侦听钩子（`Evaluator`）。这种构造方法下的窗口滑动步长等于窗口大小。

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

窗口大小、滑动步长必须为正数。

######  数据接收

``` java
final long timestamp = 0;
final int value = 0;
hander.collect(timestamp, value);
```

注意，`collect`方法接受的第二个参数类型需要与构造时传入的`dataType`声明一致。

此外，`collect`方法只会对时间戳是单调递增的数据点产生响应。如果某一次`collect`方法采集到的数据点的时间戳小于等于上一次`collect`方法采集到的数据点时间戳，那么这一次采集的数据点将会被抛弃。

还需要注意的是，`collect`方法不是线程安全的。

##### 固定窗口内时间长度的滑动窗口

###### 窗口构造

共两种构造方法。

第一种方法需要您提供窗口接受数据点的类型、窗口内时间长度、滑动步长和一个侦听钩子（`Evaluator`）。

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

第二种方法需要您提供窗口接受数据点的类型、窗口内时间长度和一个侦听钩子（`Evaluator`）。这种构造方法下的窗口滑动步长等于窗口内时间长度。

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

窗口内时间长度、滑动步长必须为正数。

######  数据接收

``` java
final long timestamp = 0;
final int value = 0;
hander.collect(timestamp, value);
```

注意，`collect`方法接受的第二个参数类型需要与构造时传入的`dataType`声明一致。

此外，`collect`方法只会对时间戳是单调递增的数据点产生响应。如果某一次`collect`方法采集到的数据点的时间戳小于等于上一次`collect`方法采集到的数据点时间戳，那么这一次采集的数据点将会被抛弃。

还需要注意的是，`collect`方法不是线程安全的。

##### 拒绝策略

窗口计算的任务执行是异步的。

当异步任务无法被执行线程池及时消费时，会产生任务堆积。在极端情况下，异步任务的堆积会导致系统 OOM。因此，窗口计算线程池允许堆积的任务数量被设定为有限值。

当堆积的任务数量超出限值时，新提交的任务将无法进入线程池执行，此时，系统会调用您在侦听钩子（`Evaluator`）中制定的拒绝策略钩子`onRejection`进行处理。

`onRejection`的默认行为如下。

````java
default void onRejection(Window window) {
  throw new RejectedExecutionException();
}
````

制定拒绝策略钩子的方式如下。

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

##### 配置参数

###### concurrent_window_evaluation_thread

窗口计算线程池的默认线程数。默认为 CPU 核数。

###### max_pending_window_evaluation_tasks

最多允许堆积的窗口计算任务。默认为 64 个。

#### Sink 工具类

Sink 工具类为触发器提供了连接外部系统的能力。

它提供了一套编程范式。每一个 Sink 工具都包含一个用于处理数据发送的`Handler`、一个用于配置`Handler`的`Configuration`，还有一个用于描述发送数据的`Event`。

##### LocalIoTDBSink

`LocalIoTDBSink`用于向本地序列写入数据点。

在写入数据前，不要求时间序列已被创建。

**注意**，在触发器场景中，侦听的时间序列和写入的目标时间序列不要在同一个存储组下。

使用示例：

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

注意，当您需要向某个`TEXT`类型的序列写入数据时，您需要借助`org.apache.iotdb.tsfile.utils.Binary`：

```java
// insert 100 data points
for (int i = 0; i < 100; ++i) {
  final long timestamp = i;
  final String value = "" + i;
  localIoTDBHandler.onEvent(new LocalIoTDBEvent(timestamp, Binary.valueOf(value)));
}
```

##### MQTTSink

触发器可以使用`MQTTSink`向其他的 IoTDB 实例发送数据点。

在发送数据前，不要求时间序列已被创建。

使用示例：

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

##### AlertManagerSink

触发器可以使用`AlertManagerSink` 向 AlertManager 发送消息。

`AlertManagerConfiguration` 的构造需传入 AlertManager 的发送告警的 endpoint。
```java
AlertManagerConfiguration(String endpoint);
```

`AlertManagerEvent` 提供三种构造函数：
```java
AlertManagerEvent(String alertname);
AlertManagerEvent(String alertname, Map<String, String> extraLabels);
AlertManagerEvent(String alertname, Map<String, String> extraLabels, Map<String, String> annotations);
```
其中：
* `alertname` 是必传参数，用于标识一个 `alert`，`alertname` 字段可用于 `AlertManager` 发送告警时的分组和消重。
* `extraLabels` 可选传，在后台与 `alertname` 组合成 `labels` 一起标识一个 `alert`，可用于 `AlertManager` 发送告警时的分组和消重。
* `annotations` 可选传，它的 value 值可使用 Go 语言模板风格的 
    ```
    {{.<label_key>}}
    ```
    它在最终生成消息时会被替换为 `labels[<label_key>]`。
* `labels` 和 `annotations` 会被解析成 json 字符串发送给 `AlertManager`：
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

调用  `AlertManagerHandler` 的 `onEvent(AlertManagerEvent event)` 方法发送一个告警。

**使用示例 1：**

只传 `alertname`。

```java
AlertManagerHandler alertManagerHandler = new AlertManagerHandler();

alertManagerHandler.open(new AlertManagerConfiguration("http://127.0.0.1:9093/api/v1/alerts"));

final String alertName = "test0";

AlertManagerEvent alertManagerEvent = new AlertManagerEvent(alertName);

alertManagerHandler.onEvent(alertManagerEvent);
```

 

**使用示例 2：**

传入 `alertname` 和 `extraLabels`。

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

**使用示例 3：**

传入 `alertname`， `extraLabels` 和 `annotations` 。

最终 `description` 字段的值会被解析为 `test2: root.ln.wt01.wf01.temperature is 100.0`。

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

### 完整的 Maven 示例项目

如果您使用 [Maven](http://search.maven.org/)，可以参考我们编写的示例项目 **trigger-example**。

您可以在 [这里](https://github.com/apache/iotdb/tree/master/example/trigger) 找到它。

它展示了：

* 如何使用 Maven 管理您的 trigger 项目
* 如何基于触发器的用户编程接口实现数据侦听
* 如何使用窗口工具类
* 如何使用 Sink 工具类

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
            new PartialPath(TARGET_DEVICE),
            new String[]{"remote"}));
  }

  private void closeSinkHandlers() throws Exception {
    localIoTDBHandler.close();
    mqttHandler.close();
  }
}
```

您可以按照下面的步骤试用这个触发器：

* 在`iotdb-engine.properties`中启用 MQTT 服务

  ``` properties
  # whether to enable the mqtt service.
  enable_mqtt_service=true
  ```

* 启动 IoTDB 服务器

* 通过 cli 创建时间序列

  ``` sql
  CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=DOUBLE, ENCODING=PLAIN;
  ```

* 将 **trigger-example** 中打包好的 JAR（`trigger-example-0.13.0-SNAPSHOT.jar`）放置到目录 `iotdb-server-0.13.0-SNAPSHOT/ext/trigger` （也可以是`iotdb-server-0.13.0-SNAPSHOT/ext/trigger`的子目录）下

  > 您可以通过修改配置文件中的`trigger_root_dir`来指定加载触发器 JAR 包的根路径。

* 使用 SQL 语句注册该触发器，假定赋予该触发器的名字为`window-avg-alerter`

* 使用`CREATE TRIGGER`语句注册该触发器

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

* 使用 cli 插入测试数据

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

* 使用 cli 查询数据以验证触发器的行为

  ``` sql
  SELECT * FROM root.alerting;
  ```

* 正常情况下，得到如下结果

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

以上就是基本的使用方法，希望您能喜欢 :D

### 重要注意事项

* 触发器是通过反射技术动态装载的，因此您在装载过程中无需启停服务器。

* 不同的 JAR 包中最好不要有全类名相同但功能实现不一样的类。例如：触发器`trigger1`、`trigger2`分别对应资源`trigger1.jar`、`trigger2.jar`。如果两个 JAR 包里都包含一个`org.apache.iotdb.db.engine.trigger.example.AlertListener`类，当`CREATE TRIGGER`使用到这个类时，系统会随机加载其中一个 JAR 包中的类，最终导致触发器执行行为不一致以及其他的问题。

* 拥有同一个全类名的触发器类的版本管理问题。IoTDB 不允许系统中存在拥有同一全类名但是版本（逻辑）不一样的触发器。

  相关问题：IoTDB 预先注册了 10 个`org.apache.iotdb.db.engine.trigger.example.AlertListener`触发器实例，DBA 更新了`org.apache.iotdb.db.engine.trigger.example.AlertListener`的实现和对应的 JAR 包，是否可以只卸载其中 5 个，将这 5 个替换为新的实现？

  回答：无法做到。只有将预先注册的 10 个触发器全部卸载，才能装载到新的触发器实例。在原有触发器没有全部被卸载的情况下，新注册的拥有相同全类名的触发器行为只会与现有触发器的行为一致。
