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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBEvent;
import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBHandler;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTEvent;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTHandler;
import org.apache.iotdb.db.utils.windowing.configuration.SlidingSizeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.handler.SlidingSizeWindowEvaluationHandler;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.fusesource.mqtt.client.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerExample implements Trigger {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerExample.class);

  private static final String TARGET_DEVICE = "root.alerting";

  private final LocalIoTDBHandler localIoTDBHandler = new LocalIoTDBHandler();
  private final MQTTHandler mqttHandler = new MQTTHandler();

  // This field is required when the target MQTT server is current IoTDB.
  // When IoTDB restarts, the registered triggers will be restored before starting the MQTT service.
  // For this trigger, if openSinkHandlers() is called in onCreate(), IoTDB server will be stuck
  // in openSinkHandlers when recovering, because it can't connect to the MQTT server (not started
  // yet).
  // See IOTDB-2274 for more detail.
  private volatile boolean isSinksOpenedAfterCreation = false;

  private SlidingSizeWindowEvaluationHandler windowEvaluationHandler;

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    LOGGER.info("onCreate(TriggerAttributes attributes)");

    double lo = attributes.getDouble("lo");
    double hi = attributes.getDouble("hi");

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
  public Double fire(long timestamp, Double value, PartialPath path) throws Exception {
    tryOpenSinksFirstOnFire();
    windowEvaluationHandler.collect(timestamp, value);
    return value;
  }

  @Override
  public double[] fire(long[] timestamps, double[] values, PartialPath path) throws Exception {
    tryOpenSinksFirstOnFire();
    for (int i = 0; i < timestamps.length; ++i) {
      windowEvaluationHandler.collect(timestamps[i], values[i]);
    }
    return values;
  }

  // See IOTDB-2274 for more detail.
  private void tryOpenSinksFirstOnFire() throws Exception {
    if (!isSinksOpenedAfterCreation) {
      synchronized (this) {
        if (!isSinksOpenedAfterCreation) {
          openSinkHandlers();
          isSinksOpenedAfterCreation = true;
        }
      }
    }
  }

  private void openSinkHandlers() throws Exception {
    localIoTDBHandler.open(
        new LocalIoTDBConfiguration(
            TARGET_DEVICE, new String[] {"local"}, new TSDataType[] {TSDataType.DOUBLE}));
    mqttHandler.open(
        new MQTTConfiguration(
            "127.0.0.1",
            1883,
            "root",
            "root",
            new PartialPath(TARGET_DEVICE),
            new String[] {"remote"}));
  }

  private void closeSinkHandlers() throws Exception {
    localIoTDBHandler.close();
    mqttHandler.close();
  }
}
