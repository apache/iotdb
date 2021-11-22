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

package org.apache.iotdb.db.engine.trigger.builtin;

import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.engine.trigger.api.TriggerAttributes;
import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBEvent;
import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBHandler;
import org.apache.iotdb.db.utils.windowing.api.Evaluator;
import org.apache.iotdb.db.utils.windowing.api.Window;
import org.apache.iotdb.db.utils.windowing.configuration.SlidingSizeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.configuration.SlidingTimeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.handler.SlidingSizeWindowEvaluationHandler;
import org.apache.iotdb.db.utils.windowing.handler.SlidingTimeWindowEvaluationHandler;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class MovingExtremeTrigger implements Trigger {

  private final LocalIoTDBHandler localIoTDBHandler = new LocalIoTDBHandler();

  private boolean isSizeBased;
  private SlidingSizeWindowEvaluationHandler slidingSizeWindowEvaluationHandler;
  private SlidingTimeWindowEvaluationHandler slidingTimeWindowEvaluationHandler;

  private String device;
  private String[] measurements;
  private TSDataType[] dataTypes;

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    device = attributes.getString("device");
    measurements = new String[] {attributes.getString("measurement")};
    dataTypes = new TSDataType[] {TSDataType.DOUBLE};

    openSinkHandlers();

    final Evaluator evaluator =
        new Evaluator() {

          @Override
          public void evaluate(Window window) throws Exception {
            double extreme = 0;
            double[] array = window.getDoubleArray();
            for (int i = 0, n = window.size(); i < n; ++i) {
              extreme = Math.max(extreme, Math.abs(array[i]));
            }

            localIoTDBHandler.onEvent(new LocalIoTDBEvent(window.getTime(0), extreme));
          }

          @Override
          public void onRejection(Window window) {
            double extreme = 0;
            double[] array = window.getDoubleArray();
            for (int i = 0, n = window.size(); i < n; ++i) {
              extreme = Math.max(extreme, Math.abs(array[i]));
            }

            try {
              localIoTDBHandler.onEvent(new LocalIoTDBEvent(window.getTime(0), extreme));
            } catch (Exception e) {
              throw new RuntimeException(e.getMessage());
            }
          }
        };

    int windowSize = attributes.getInt("window_size");
    int slidingStep = attributes.getInt("sliding_step");
    isSizeBased = attributes.getStringOrDefault("strategy", "time").equals("size");
    if (isSizeBased) {
      slidingSizeWindowEvaluationHandler =
          new SlidingSizeWindowEvaluationHandler(
              new SlidingSizeWindowConfiguration(TSDataType.DOUBLE, windowSize, slidingStep),
              evaluator);
    } else {
      slidingTimeWindowEvaluationHandler =
          new SlidingTimeWindowEvaluationHandler(
              new SlidingTimeWindowConfiguration(TSDataType.DOUBLE, windowSize, slidingStep),
              evaluator);
    }
  }

  @Override
  public void onDrop() throws Exception {
    closeSinkHandlers();
  }

  @Override
  public void onStart() throws Exception {
    openSinkHandlers();
  }

  @Override
  public void onStop() throws Exception {
    closeSinkHandlers();
  }

  @Override
  public Double fire(long timestamp, Double value) {
    if (isSizeBased) {
      slidingSizeWindowEvaluationHandler.collect(timestamp, value);
    } else {
      slidingTimeWindowEvaluationHandler.collect(timestamp, value);
    }
    return value;
  }

  @Override
  public double[] fire(long[] timestamps, double[] values) {
    if (isSizeBased) {
      for (int i = 0, n = timestamps.length; i < n; ++i) {
        slidingSizeWindowEvaluationHandler.collect(timestamps[i], values[i]);
      }
    } else {
      for (int i = 0, n = timestamps.length; i < n; ++i) {
        slidingTimeWindowEvaluationHandler.collect(timestamps[i], values[i]);
      }
    }
    return values;
  }

  private void openSinkHandlers() throws Exception {
    localIoTDBHandler.open(new LocalIoTDBConfiguration(device, measurements, dataTypes));
  }

  private void closeSinkHandlers() throws Exception {
    localIoTDBHandler.close();
  }
}
