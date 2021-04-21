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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.sink.mqtt.MQTTConfiguration;
import org.apache.iotdb.db.sink.mqtt.MQTTEvent;
import org.apache.iotdb.db.sink.mqtt.MQTTHandler;
import org.apache.iotdb.db.sink.ts.TimeSeriesConfiguration;
import org.apache.iotdb.db.sink.ts.TimeSeriesEvent;
import org.apache.iotdb.db.sink.ts.TimeSeriesHandler;
import org.apache.iotdb.db.utils.windowing.configuration.SlidingSizeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.handler.SlidingSizeWindowEvaluationHandler;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.fusesource.mqtt.client.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerExample implements Trigger {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerExample.class);

  private SlidingSizeWindowEvaluationHandler windowEvaluationHandler;

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    LOGGER.info("onCreate(TriggerAttributes attributes)");

    double lo = attributes.getDouble("lo");
    double hi = attributes.getDouble("hi");

    TimeSeriesHandler timeSeriesHandler = new TimeSeriesHandler();
    timeSeriesHandler.open(
        new TimeSeriesConfiguration(
            "root.alerting", new String[] {"local"}, new TSDataType[] {TSDataType.DOUBLE}));

    MQTTHandler mqttHandler = new MQTTHandler();
    mqttHandler.open(
        new MQTTConfiguration(
            "127.0.0.1",
            1883,
            "root",
            "root",
            new PartialPath("root.alerting"),
            new String[] {"remote"}));

    windowEvaluationHandler =
        new SlidingSizeWindowEvaluationHandler(
            new SlidingSizeWindowConfiguration(TSDataType.DOUBLE, 100, 100),
            window -> {
              double avg = 0;
              for (int i = 0; i < window.size(); ++i) {
                avg += window.getDouble(i);
              }
              avg /= window.size();

              if (avg < lo || hi < avg) {
                timeSeriesHandler.onEvent(new TimeSeriesEvent(window.getTime(0), avg));
                mqttHandler.onEvent(
                    new MQTTEvent("test", QoS.EXACTLY_ONCE, false, window.getTime(0), avg));
              }
            });
  }

  @Override
  public void onDrop() {
    LOGGER.info("onDrop()");
  }

  @Override
  public void onStart() {
    LOGGER.info("onStart()");
  }

  @Override
  public void onStop() {
    LOGGER.info("onStop()");
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
}
