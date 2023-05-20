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

## 告警

### 概览
IoTDB 告警功能预计支持两种模式：

* 写入触发：用户写入原始数据到原始时间序列，每插入一条数据都会触发 `Trigger` 的判断逻辑，
若满足告警要求则发送告警到下游数据接收器，
数据接收器再转发告警到外部终端。这种模式：
    * 适合需要即时监控每一条数据的场景。
    * 由于触发器中的运算会影响数据写入性能，适合对原始数据写入性能不敏感的场景。

* 持续查询：用户写入原始数据到原始时间序列，
`ContinousQuery` 定时查询原始时间序列，将查询结果写入新的时间序列，
每一次写入触发 `Trigger` 的判断逻辑，
若满足告警要求则发送告警到下游数据接收器，
数据接收器再转发告警到外部终端。这种模式：
    * 适合需要定时查询数据在某一段时间内的情况的场景。
    * 适合需要将原始数据降采样并持久化的场景。
    * 由于定时查询几乎不影响原始时间序列的写入，适合对原始数据写入性能敏感的场景。

随着 [Trigger](../Trigger/Instructions.md) 模块的引入，可以实现写入触发模式的告警。

### 部署 AlertManager 

#### 安装与运行
##### 二进制文件
预编译好的二进制文件可在 [这里](https://prometheus.io/download/) 下载。

运行方法：
````shell
./alertmanager --config.file=<your_file>
````

##### Docker 镜像
可在  [Quay.io](https://hub.docker.com/r/prom/alertmanager/) 
或 [Docker Hub](https://quay.io/repository/prometheus/alertmanager) 获得。

运行方法：
````shell
docker run --name alertmanager -d -p 127.0.0.1:9093:9093 quay.io/prometheus/alertmanager
````

####  配置

如下是一个示例，可以覆盖到大部分配置规则，详细的配置规则参见 
[这里](https://prometheus.io/docs/alerting/latest/configuration/)。

示例：
``` yaml
# alertmanager.yml

global:
  # The smarthost and SMTP sender used for mail notifications.
  smtp_smarthost: 'localhost:25'
  smtp_from: 'alertmanager@example.org'

# The root route on which each incoming alert enters.
route:
  # The root route must not have any matchers as it is the entry point for
  # all alerts. It needs to have a receiver configured so alerts that do not
  # match any of the sub-routes are sent to someone.
  receiver: 'team-X-mails'

  # The labels by which incoming alerts are grouped together. For example,
  # multiple alerts coming in for cluster=A and alertname=LatencyHigh would
  # be batched into a single group.
  #
  # To aggregate by all possible labels use '...' as the sole label name.
  # This effectively disables aggregation entirely, passing through all
  # alerts as-is. This is unlikely to be what you want, unless you have
  # a very low alert volume or your upstream notification system performs
  # its own grouping. Example: group_by: [...]
  group_by: ['alertname', 'cluster']

  # When a new group of alerts is created by an incoming alert, wait at
  # least 'group_wait' to send the initial notification.
  # This way ensures that you get multiple alerts for the same group that start
  # firing shortly after another are batched together on the first
  # notification.
  group_wait: 30s

  # When the first notification was sent, wait 'group_interval' to send a batch
  # of new alerts that started firing for that group.
  group_interval: 5m

  # If an alert has successfully been sent, wait 'repeat_interval' to
  # resend them.
  repeat_interval: 3h

  # All the above attributes are inherited by all child routes and can
  # overwritten on each.

  # The child route trees.
  routes:
  # This routes performs a regular expression match on alert labels to
  # catch alerts that are related to a list of services.
  - match_re:
      service: ^(foo1|foo2|baz)$
    receiver: team-X-mails

    # The service has a sub-route for critical alerts, any alerts
    # that do not match, i.e. severity != critical, fall-back to the
    # parent node and are sent to 'team-X-mails'
    routes:
    - match:
        severity: critical
      receiver: team-X-pager

  - match:
      service: files
    receiver: team-Y-mails

    routes:
    - match:
        severity: critical
      receiver: team-Y-pager

  # This route handles all alerts coming from a database service. If there's
  # no team to handle it, it defaults to the DB team.
  - match:
      service: database

    receiver: team-DB-pager
    # Also group alerts by affected database.
    group_by: [alertname, cluster, database]

    routes:
    - match:
        owner: team-X
      receiver: team-X-pager

    - match:
        owner: team-Y
      receiver: team-Y-pager

# Inhibition rules allow to mute a set of alerts given that another alert is
# firing.
# We use this to mute any warning-level notifications if the same alert is
# already critical.
inhibit_rules:
- source_match:
    severity: 'critical'
  target_match:
    severity: 'warning'
  # Apply inhibition if the alertname is the same.
  # CAUTION: 
  #   If all label names listed in `equal` are missing 
  #   from both the source and target alerts,
  #   the inhibition rule will apply!
  equal: ['alertname']

receivers:
- name: 'team-X-mails'
  email_configs:
  - to: 'team-X+alerts@example.org, team-Y+alerts@example.org'

- name: 'team-X-pager'
  email_configs:
  - to: 'team-X+alerts-critical@example.org'
  pagerduty_configs:
  - routing_key: <team-X-key>

- name: 'team-Y-mails'
  email_configs:
  - to: 'team-Y+alerts@example.org'

- name: 'team-Y-pager'
  pagerduty_configs:
  - routing_key: <team-Y-key>

- name: 'team-DB-pager'
  pagerduty_configs:
  - routing_key: <team-DB-key>
```

在后面的示例中，我们采用的配置如下：
````yaml
# alertmanager.yml

global: 
  smtp_smarthost: ''
  smtp_from: '' 
  smtp_auth_username: '' 
  smtp_auth_password: '' 
  smtp_require_tls: false

route:
  group_by: ['alertname'] 
  group_wait: 1m
  group_interval: 10m
  repeat_interval: 10h 
  receiver: 'email'

receivers:
  - name: 'email'
    email_configs: 
    - to: '' 

inhibit_rules:
  - source_match:
      severity: 'critical'
    target_match:
      severity: 'warning'
    equal: ['alertname']
````

#### API
`AlertManager` API 分为 `v1` 和 `v2` 两个版本，当前 `AlertManager` API 版本为 `v2` 
（配置参见
[api/v2/openapi.yaml](https://github.com/prometheus/alertmanager/blob/master/api/v2/openapi.yaml))。

默认配置的前缀为 `/api/v1` 或 `/api/v2`，
发送告警的 endpoint 为 `/api/v1/alerts` 或 `/api/v2/alerts`。
如果用户指定了 `--web.route-prefix`，
例如 `--web.route-prefix=/alertmanager/`，
那么前缀将会变为 `/alertmanager/api/v1` 或 `/alertmanager/api/v2`，
发送告警的 endpoint 变为 `/alertmanager/api/v1/alerts` 
或 `/alertmanager/api/v2/alerts`。

### 创建 trigger

#### 编写 trigger 类

用户通过自行创建 Java 类、编写钩子中的逻辑来定义一个触发器。
具体配置流程参见 [Trigger](../Trigger/Implement-Trigger.md)。

下面的示例创建了 `org.apache.iotdb.trigger.ClusterAlertingExample` 类，
其 `alertManagerHandler` 
成员变量可发送告警至地址为 `http://127.0.0.1:9093/` 的 AlertManager 实例。

当 `value > 100.0` 时，发送 `severity` 为 `critical` 的告警；
当  `50.0 < value <= 100.0` 时，发送 `severity` 为 `warning` 的告警。

```java
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

#### 创建 trigger

如下的 sql 语句在 `root.ln.wf01.wt01.temperature` 
时间序列上注册了名为 `root-ln-wf01-wt01-alert`、
运行逻辑由 `org.apache.iotdb.trigger.ClusterAlertingExample` 
类定义的触发器。

``` sql
  CREATE STATELESS TRIGGER `root-ln-wf01-wt01-alert`
  AFTER INSERT
  ON root.ln.wf01.wt01.temperature
  AS "org.apache.iotdb.trigger.ClusterAlertingExample"
  USING URI 'http://jar/ClusterAlertingExample.jar'
```

### 写入数据

当我们完成 AlertManager 的部署和启动、Trigger 的创建，
可以通过向时间序列写入数据来测试告警功能。

``` sql
INSERT INTO root.ln.wf01.wt01(timestamp, temperature) VALUES (1, 0);
INSERT INTO root.ln.wf01.wt01(timestamp, temperature) VALUES (2, 30);
INSERT INTO root.ln.wf01.wt01(timestamp, temperature) VALUES (3, 60);
INSERT INTO root.ln.wf01.wt01(timestamp, temperature) VALUES (4, 90);
INSERT INTO root.ln.wf01.wt01(timestamp, temperature) VALUES (5, 120);
```

执行完上述写入语句后，可以收到告警邮件。由于我们的 `AlertManager` 配置中设定 `severity` 为 `critical` 的告警
会抑制 `severity` 为 `warning` 的告警，我们收到的告警邮件中只包含写入
`(5, 120)` 后触发的告警。                    

<img  alt="alerting" src="https://alioss.timecho.com/docs/img/github/115957896-a9791080-a537-11eb-9962-541412bdcee6.png">
