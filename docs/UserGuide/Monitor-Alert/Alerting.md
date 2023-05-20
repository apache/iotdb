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

# Alerting

## Overview
The alerting of IoTDB is expected to support two modes:

* Writing triggered: the user writes data to the original time series, and every time a piece of data is inserted, the judgment logic of `trigger` will be triggered.
If the alerting requirements are met, an alert is sent to the data sink,
The data sink then forwards the alert to the external terminal. 
    * This mode is suitable for scenarios that need to monitor every piece of data in real time.
    * Since the operation in the trigger will affect the data writing performance, it is suitable for scenarios that are not sensitive to the original data writing performance.

* Continuous query: the user writes data to the original time series,
`ContinousQuery` periodically queries the original time series, and writes the query results into the new time series,
Each write triggers the judgment logic of `trigger`,
If the alerting requirements are met, an alert is sent to the data sink,
The data sink then forwards the alert to the external terminal. 
    * This mode is suitable for scenarios where data needs to be regularly queried within a certain period of time.
    * It is Suitable for scenarios where the original data needs to be down-sampled and persisted.
    * Since the timing query hardly affects the writing of the original time series, it is suitable for scenarios that are sensitive to the performance of the original data writing performance.

With the introduction of the  [Trigger](../Trigger/Instructions.md) into IoTDB,
at present, users can use these two modules with `AlertManager` to realize the writing triggered alerting mode.



## Deploying AlertManager

### Installation
#### Precompiled binaries
The pre-compiled binary file can be downloaded [here](https://prometheus.io/download/).

Running command:
````shell
./alertmanager --config.file=<your_file>
````

#### Docker image
Available at [Quay.io](https://hub.docker.com/r/prom/alertmanager/)
or [Docker Hub](https://quay.io/repository/prometheus/alertmanager).

Running command:
````shell
docker run --name alertmanager -d -p 127.0.0.1:9093:9093 quay.io/prometheus/alertmanager
````

### Configuration

The following is an example, which can cover most of the configuration rules. For detailed configuration rules, see
[here](https://prometheus.io/docs/alerting/latest/configuration/).

Example:
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

In the following example, we used the following configuration:
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


### API
The `AlertManager` API is divided into two versions, `v1` and `v2`. The current `AlertManager` API version is `v2`
(For configuration see
[api/v2/openapi.yaml](https://github.com/prometheus/alertmanager/blob/master/api/v2/openapi.yaml)).

By default, the prefix is `/api/v1` or `/api/v2` and the endpoint for sending alerts is `/api/v1/alerts` or `/api/v2/alerts`.
If the user specifies `--web.route-prefix`,
for example `--web.route-prefix=/alertmanager/`,
then the prefix will become `/alertmanager/api/v1` or `/alertmanager/api/v2`,
and the endpoint that sends the alert becomes `/alertmanager/api/v1/alerts`
or `/alertmanager/api/v2/alerts`.

## Creating trigger

### Writing the trigger class

The user defines a trigger by creating a Java class and writing the logic in the hook.
Please refer to [Trigger](../Trigger/Implement-Trigger.md) for the specific configuration process.

The following example creates the `org.apache.iotdb.trigger.ClusterAlertingExample` class,
Its alertManagerHandler member variables can send alerts to the AlertManager instance 
at the address of `http://127.0.0.1:9093/`.

When `value> 100.0`, send an alert of `critical` severity;
when `50.0 <value <= 100.0`, send an alert of `warning` severity
.
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

### Creating trigger

The following SQL statement registered the trigger 
named `root-ln-wf01-wt01-alert` 
on the `root.ln.wf01.wt01.temperature` time series, 
whose operation logic is defined 
by `org.apache.iotdb.trigger.ClusterAlertingExample` java class.

``` sql
  CREATE STATELESS TRIGGER `root-ln-wf01-wt01-alert`
  AFTER INSERT
  ON root.ln.wf01.wt01.temperature
  AS "org.apache.iotdb.trigger.AlertingExample"
  USING URI 'http://jar/ClusterAlertingExample.jar'
```


## Writing data

When we finish the deployment and startup of AlertManager as well as the creation of Trigger, 
we can test the alerting
by writing data to the time series.

``` sql
INSERT INTO root.ln.wf01.wt01(timestamp, temperature) VALUES (1, 0);
INSERT INTO root.ln.wf01.wt01(timestamp, temperature) VALUES (2, 30);
INSERT INTO root.ln.wf01.wt01(timestamp, temperature) VALUES (3, 60);
INSERT INTO root.ln.wf01.wt01(timestamp, temperature) VALUES (4, 90);
INSERT INTO root.ln.wf01.wt01(timestamp, temperature) VALUES (5, 120);
```

After executing the above writing statements, 
we can receive an alerting email. Because our `AlertManager` configuration above
makes alerts of `critical` severity inhibit those of `warning` severity, 
the alerting email we receive only contains the alert triggered
by the writing of `(5, 120)`.

<img width="669" alt="alerting" src="https://alioss.timecho.com/docs/img/github/115957896-a9791080-a537-11eb-9962-541412bdcee6.png">


