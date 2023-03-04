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

import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.engine.trigger.api.TriggerAttributes;
import org.apache.iotdb.db.engine.trigger.sink.alertmanager.AlertManagerConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.alertmanager.AlertManagerEvent;
import org.apache.iotdb.db.engine.trigger.sink.alertmanager.AlertManagerHandler;

import java.io.IOException;
import java.util.HashMap;

public class AlertingExample implements Trigger {

  private final AlertManagerHandler alertManagerHandler = new AlertManagerHandler();

  private final AlertManagerConfiguration alertManagerConfiguration =
      new AlertManagerConfiguration("http://127.0.0.1:9093/api/v2/alerts");

  private String alertname;

  private final HashMap<String, String> labels = new HashMap<>();

  private final HashMap<String, String> annotations = new HashMap<>();

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    alertManagerHandler.open(alertManagerConfiguration);

    alertname = "alert_test";

    labels.put("series", "root.ln.wf01.wt01.temperature");
    labels.put("value", "");
    labels.put("severity", "");

    annotations.put("summary", "high temperature");
    annotations.put("description", "{{.alertname}}: {{.series}} is {{.value}}");
  }

  @Override
  public void onDrop() throws IOException {
    alertManagerHandler.close();
  }

  @Override
  public void onStart() {
    alertManagerHandler.open(alertManagerConfiguration);
  }

  @Override
  public void onStop() throws Exception {
    alertManagerHandler.close();
  }

  @Override
  public Double fire(long timestamp, Double value) throws Exception {
    if (value > 100.0) {
      labels.put("value", String.valueOf(value));
      labels.put("severity", "critical");
      AlertManagerEvent alertManagerEvent = new AlertManagerEvent(alertname, labels, annotations);
      alertManagerHandler.onEvent(alertManagerEvent);
    } else if (value > 50.0) {
      labels.put("value", String.valueOf(value));
      labels.put("severity", "warning");
      AlertManagerEvent alertManagerEvent = new AlertManagerEvent(alertname, labels, annotations);
      alertManagerHandler.onEvent(alertManagerEvent);
    }

    return value;
  }

  @Override
  public double[] fire(long[] timestamps, double[] values) throws Exception {
    for (double value : values) {
      if (value > 100.0) {
        labels.put("value", String.valueOf(value));
        labels.put("severity", "critical");
        AlertManagerEvent alertManagerEvent = new AlertManagerEvent(alertname, labels, annotations);
        alertManagerHandler.onEvent(alertManagerEvent);
      } else if (value > 50.0) {
        labels.put("value", String.valueOf(value));
        labels.put("severity", "warning");
        AlertManagerEvent alertManagerEvent = new AlertManagerEvent(alertname, labels, annotations);
        alertManagerHandler.onEvent(alertManagerEvent);
      }
    }
    return values;
  }
}
