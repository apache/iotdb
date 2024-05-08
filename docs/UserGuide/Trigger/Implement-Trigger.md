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



# How to implement a trigger

You need to implement the trigger by writing a Java class, where the dependency shown below is required. If you use [Maven](http://search.maven.org/), you can search for them directly from the [Maven repository](http://search.maven.org/).

## Dependency

```xml
<dependency>
  <groupId>org.apache.iotdb</groupId>
  <artifactId>iotdb-server</artifactId>
  <version>1.0.0</version>
  <scope>provided</scope>
</dependency>
```

Note that the dependency version should be correspondent to the target server version.

## Interface Description

To implement a trigger, you need to implement the `org.apache.iotdb.trigger.api.Trigger` class.

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

This class provides two types of programming interfaces: **Lifecycle related interfaces** and **data change listening related interfaces**. All the interfaces in this class are not required to be implemented. When the interfaces are not implemented, the trigger will not respond to the data changes. You can implement only some of these interfaces according to your needs.

Descriptions of the interfaces are as followed.

### Lifecycle related interfaces

| Interface                                                    | Description                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| *default void validate(TriggerAttributes attributes) throws Exception {}* | When you creates a trigger using the `CREATE TRIGGER` statement, you can specify the parameters that the trigger needs to use, and this interface will be used to verify the correctness of the parameters。 |
| *default void onCreate(TriggerAttributes attributes) throws Exception {}* | This interface is called once when you create a trigger using the `CREATE TRIGGER` statement. During the lifetime of each trigger instance, this interface will be called only once. This interface is mainly used for the following functions: helping users to parse custom attributes in SQL statements (using `TriggerAttributes`). You can create or apply for resources, such as establishing external links, opening files, etc. |
| *default void onDrop() throws Exception {}*                  | This interface is called when you drop a trigger using the `DROP TRIGGER` statement. During the lifetime of each trigger instance, this interface will be called only once. This interface mainly has the following functions: it can perform the operation of resource release and can be used to persist the results of trigger calculations. |
| *default void restore() throws Exception {}*                 | When the DataNode is restarted, the cluster will restore the trigger instance registered on the DataNode, and this interface will be called once for stateful trigger during the process. After the DataNode where the stateful trigger instance is located goes down, the cluster will restore the trigger instance on another available DataNode, calling this interface once in the process. This interface can be used to customize recovery logic. |

### Data change listening related interfaces

#### Listening interface

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

When the data changes, the trigger uses the Tablet as the unit of firing operation. You can obtain the metadata and data of the corresponding sequence through Tablet, and then perform the corresponding trigger operation. If the fire process is successful, the return value should be true. If the interface returns false or throws an exception, we consider the trigger fire process as failed. When the trigger fire process fails, we will perform corresponding operations according to the listening strategy interface.

When performing an INSERT operation, for each time series in it, we will detect whether there is a trigger that listens to the path pattern, and then assemble the time series data that matches the path pattern listened by the same trigger into a new Tablet for trigger fire interface. Can be understood as:

```java
Map<PartialPath, List<Trigger>> pathToTriggerListMap => Map<Trigger, Tablet>
```

**Note that currently we do not make any guarantees about the order in which triggers fire.**

Here is an example:

Suppose there are three triggers, and the trigger event of the triggers are all BEFORE INSERT:

- Trigger1 listens on `root.sg.*`
- Trigger2 listens on `root.sg.a`
- Trigger3 listens on `root.sg.b`

Insertion statement:

```sql
insert into root.sg(time, a, b) values (1, 1, 1);
```

The time series `root.sg.a` matches Trigger1 and Trigger2, and the sequence `root.sg.b` matches Trigger1 and Trigger3, then:

- The data of `root.sg.a` and `root.sg.b` will be assembled into a new tablet1, and Trigger1.fire(tablet1) will be executed at the corresponding Trigger Event.
- The data of `root.sg.a` will be assembled into a new tablet2, and Trigger2.fire(tablet2) will be executed at the corresponding Trigger Event.
- The data of `root.sg.b` will be assembled into a new tablet3, and Trigger3.fire(tablet3) will be executed at the corresponding Trigger Event.

#### Listening strategy interface

When the trigger fails to fire, we will take corresponding actions according to the strategy set by the listening strategy interface. You can set `org.apache.iotdb.trigger.api.enums.FailureStrategy`. There are currently two strategies, optimistic and pessimistic:

- Optimistic strategy: The trigger that fails to fire does not affect the firing of subsequent triggers, nor does it affect the writing process, that is, we do not perform additional processing on the sequence involved in the trigger failure, only log the failure to record the failure, and finally inform user that data insertion is successful, but the trigger fire part failed.
- Pessimistic strategy: The failure trigger affects the processing of all subsequent Pipelines, that is, we believe that the firing failure of the trigger will cause all subsequent triggering processes to no longer be carried out. If the trigger event of the trigger is BEFORE INSERT, then the insertion will no longer be performed, and the insertion failure will be returned directly.

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

## Example

If you use [Maven](http://search.maven.org/), you can refer to our sample project **trigger-example**.

You can find it [here](https://github.com/apache/iotdb/tree/master/example/trigger).

Here is the code from one of the sample projects:

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

