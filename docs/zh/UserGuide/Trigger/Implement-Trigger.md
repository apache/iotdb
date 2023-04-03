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



# 编写触发器

## 触发器依赖

触发器的逻辑需要您编写 Java 类进行实现。
在编写触发器逻辑时，需要使用到下面展示的依赖。如果您使用 [Maven](http://search.maven.org/)，则可以直接从 [Maven 库](http://search.maven.org/)中搜索到它们。请注意选择和目标服务器版本相同的依赖版本。

``` xml
<dependency>
  <groupId>org.apache.iotdb</groupId>
  <artifactId>iotdb-server</artifactId>
  <version>1.0.0</version>
  <scope>provided</scope>
</dependency>
```

## 接口说明

编写一个触发器需要实现 `org.apache.iotdb.trigger.api.Trigger` 类。

```java
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.tsfile.write.record.Tablet;

public interface Trigger {

  /**
   * This method is mainly used to validate {@link TriggerAttributes} before calling {@link
   * Trigger#onCreate(TriggerAttributes)}.
   *
   * @param attributes TriggerAttributes
   * @throws Exception e
   */
  default void validate(TriggerAttributes attributes) throws Exception {}

  /**
   * This method will be called when creating a trigger after validation.
   *
   * @param attributes TriggerAttributes
   * @throws Exception e
   */
  default void onCreate(TriggerAttributes attributes) throws Exception {}

  /**
   * This method will be called when dropping a trigger.
   *
   * @throws Exception e
   */
  default void onDrop() throws Exception {}

  /**
   * When restarting a DataNode, Triggers that have been registered will be restored and this method
   * will be called during the process of restoring.
   *
   * @throws Exception e
   */
  default void restore() throws Exception {}

  /**
   * Overrides this method to set the expected FailureStrategy, {@link FailureStrategy#OPTIMISTIC}
   * is the default strategy.
   *
   * @return {@link FailureStrategy}
   */
  default FailureStrategy getFailureStrategy() {
    return FailureStrategy.OPTIMISTIC;
  }

  /**
   * @param tablet see {@link Tablet} for detailed information of data structure. Data that is
   *     inserted will be constructed as a Tablet and you can define process logic with {@link
   *     Tablet}.
   * @return true if successfully fired
   * @throws Exception e
   */
  default boolean fire(Tablet tablet) throws Exception {
    return true;
  }
}
```

该类主要提供了两类编程接口：**生命周期相关接口**和**数据变动侦听相关接口**。该类中所有的接口都不是必须实现的，当您不实现它们时，它们不会对流经的数据操作产生任何响应。您可以根据实际需要，只实现其中若干接口。

下面是所有可供用户进行实现的接口的说明。

### 生命周期相关接口

| 接口定义                                                     | 描述                                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| *default void validate(TriggerAttributes attributes) throws Exception {}* | 用户在使用 `CREATE TRIGGER` 语句创建触发器时，可以指定触发器需要使用的参数，该接口会用于验证参数正确性。 |
| *default void onCreate(TriggerAttributes attributes) throws Exception {}* | 当您使用`CREATE TRIGGER`语句创建触发器后，该接口会被调用一次。在每一个触发器实例的生命周期内，该接口会且仅会被调用一次。该接口主要有如下作用：帮助用户解析 SQL 语句中的自定义属性（使用`TriggerAttributes`）。 可以创建或申请资源，如建立外部链接、打开文件等。 |
| *default void onDrop() throws Exception {}*                  | 当您使用`DROP TRIGGER`语句删除触发器后，该接口会被调用。在每一个触发器实例的生命周期内，该接口会且仅会被调用一次。该接口主要有如下作用：可以进行资源释放的操作。可以用于持久化触发器计算的结果。 |
| *default void restore() throws Exception {}*                 | 当重启 DataNode 时，集群会恢复 DataNode 上已经注册的触发器实例，在此过程中会为该 DataNode 上的有状态触发器调用一次该接口。有状态触发器实例所在的 DataNode 宕机后，集群会在另一个可用 DataNode 上恢复该触发器的实例，在此过程中会调用一次该接口。该接口可以用于自定义恢复逻辑。 |

### 数据变动侦听相关接口

#### 侦听接口

```java
 /**
   * @param tablet see {@link Tablet} for detailed information of data structure. Data that is
   *     inserted will be constructed as a Tablet and you can define process logic with {@link
   *     Tablet}.
   * @return true if successfully fired
   * @throws Exception e
   */
  default boolean fire(Tablet tablet) throws Exception {
    return true;
  }
```

数据变动时，触发器以 Tablet 作为触发操作的单位。您可以通过 Tablet 获取相应序列的元数据和数据，然后进行相应的触发操作，触发成功则返回值应当为 true。该接口返回 false 或是抛出异常我们均认为触发失败。在触发失败时，我们会根据侦听策略接口进行相应的操作。 

进行一次 INSERT 操作时，对于其中的每条时间序列，我们会检测是否有侦听该路径模式的触发器，然后将符合同一个触发器所侦听的路径模式的时间序列数据组装成一个新的 Tablet 用于触发器的 fire 接口。可以理解成：

```java
Map<PartialPath, List<Trigger>> pathToTriggerListMap => Map<Trigger, Tablet>
```

**请注意，目前我们不对触发器的触发顺序有任何保证。**

下面是示例：

假设有三个触发器，触发器的触发时机均为 BEFORE INSERT

- 触发器 Trigger1 侦听路径模式：root.sg.*
- 触发器 Trigger2 侦听路径模式：root.sg.a
- 触发器 Trigger3 侦听路径模式：root.sg.b

写入语句：

```sql
insert into root.sg(time, a, b) values (1, 1, 1);
```

序列 root.sg.a 匹配 Trigger1 和 Trigger2，序列 root.sg.b 匹配 Trigger1 和 Trigger3，那么：

- root.sg.a 和 root.sg.b 的数据会被组装成一个新的 tablet1，在相应的触发时机进行 Trigger1.fire(tablet1)
- root.sg.a 的数据会被组装成一个新的 tablet2，在相应的触发时机进行 Trigger2.fire(tablet2)
- root.sg.b 的数据会被组装成一个新的 tablet3，在相应的触发时机进行 Trigger3.fire(tablet3)

#### 侦听策略接口

在触发器触发失败时，我们会根据侦听策略接口设置的策略进行相应的操作，您可以通过下述接口设置 `org.apache.iotdb.trigger.api.enums.FailureStrategy`，目前有乐观和悲观两种策略：

- 乐观策略：触发失败的触发器不影响后续触发器的触发，也不影响写入流程，即我们不对触发失败涉及的序列做额外处理，仅打日志记录失败，最后返回用户写入数据成功，但触发部分失败。
- 悲观策略：失败触发器影响后续所有 Pipeline 的处理，即我们认为该 Trigger 触发失败会导致后续所有触发流程不再进行。如果该触发器的触发时机为 BEFORE INSERT，那么写入也不再进行，直接返回写入失败。

```java
  /**
   * Overrides this method to set the expected FailureStrategy, {@link FailureStrategy#OPTIMISTIC}
   * is the default strategy.
   *
   * @return {@link FailureStrategy}
   */
  default FailureStrategy getFailureStrategy() {
    return FailureStrategy.OPTIMISTIC;
  }
```

您可以参考下图辅助理解，其中 Trigger1 配置采用乐观策略，Trigger2 配置采用悲观策略。Trigger1 和 Trigger2 的触发时机是 BEFORE INSERT，Trigger3 和 Trigger4 的触发时机是 AFTER INSERT。 正常执行流程如下：

<img src="https://alioss.timecho.com/docs/img/UserGuide/Process-Data/Triggers/Trigger_Process_Flow.jpg?raw=true">

<img src="https://alioss.timecho.com/docs/img/UserGuide/Process-Data/Triggers/Trigger_Process_Strategy.jpg?raw=true">


## 示例

如果您使用 [Maven](http://search.maven.org/)，可以参考我们编写的示例项目 trigger-example。您可以在 [这里](https://github.com/apache/iotdb/tree/master/example/trigger) 找到它。后续我们会加入更多的示例项目供您参考。

下面是其中一个示例项目的代码：

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.trigger;

import org.apache.iotdb.db.engine.trigger.sink.alertmanager.AlertManagerConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.alertmanager.AlertManagerEvent;
import org.apache.iotdb.db.engine.trigger.sink.alertmanager.AlertManagerHandler;
import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class ClusterAlertingExample implements Trigger {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterAlertingExample.class);

  private final AlertManagerHandler alertManagerHandler = new AlertManagerHandler();

  private final AlertManagerConfiguration alertManagerConfiguration =
      new AlertManagerConfiguration("http://127.0.0.1:9093/api/v2/alerts");

  private String alertname;

  private final HashMap<String, String> labels = new HashMap<>();

  private final HashMap<String, String> annotations = new HashMap<>();

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    alertname = "alert_test";

    labels.put("series", "root.ln.wf01.wt01.temperature");
    labels.put("value", "");
    labels.put("severity", "");

    annotations.put("summary", "high temperature");
    annotations.put("description", "{{.alertname}}: {{.series}} is {{.value}}");

    alertManagerHandler.open(alertManagerConfiguration);
  }

  @Override
  public void onDrop() throws IOException {
    alertManagerHandler.close();
  }

  @Override
  public boolean fire(Tablet tablet) throws Exception {
    List<MeasurementSchema> measurementSchemaList = tablet.getSchemas();
    for (int i = 0, n = measurementSchemaList.size(); i < n; i++) {
      if (measurementSchemaList.get(i).getType().equals(TSDataType.DOUBLE)) {
        // for example, we only deal with the columns of Double type
        double[] values = (double[]) tablet.values[i];
        for (double value : values) {
          if (value > 100.0) {
            LOGGER.info("trigger value > 100");
            labels.put("value", String.valueOf(value));
            labels.put("severity", "critical");
            AlertManagerEvent alertManagerEvent =
                new AlertManagerEvent(alertname, labels, annotations);
            alertManagerHandler.onEvent(alertManagerEvent);
          } else if (value > 50.0) {
            LOGGER.info("trigger value > 50");
            labels.put("value", String.valueOf(value));
            labels.put("severity", "warning");
            AlertManagerEvent alertManagerEvent =
                new AlertManagerEvent(alertname, labels, annotations);
            alertManagerHandler.onEvent(alertManagerEvent);
          }
        }
      }
    }
    return true;
  }
}
```
