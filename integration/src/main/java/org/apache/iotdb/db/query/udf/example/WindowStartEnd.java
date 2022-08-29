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

package org.apache.iotdb.db.query.udf.example;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

public class WindowStartEnd implements UDTF {

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    configurations.setOutputDataType(Type.INT64);
    if (ExampleUDFConstant.ACCESS_STRATEGY_SLIDING_SIZE.equals(
        parameters.getString(ExampleUDFConstant.ACCESS_STRATEGY_KEY))) {
      configurations.setAccessStrategy(
          parameters.hasAttribute(ExampleUDFConstant.SLIDING_STEP_KEY)
              ? new SlidingSizeWindowAccessStrategy(
                  parameters.getInt(ExampleUDFConstant.WINDOW_SIZE_KEY),
                  parameters.getInt(ExampleUDFConstant.SLIDING_STEP_KEY))
              : new SlidingSizeWindowAccessStrategy(
                  parameters.getInt(ExampleUDFConstant.WINDOW_SIZE_KEY)));
    } else {
      configurations.setAccessStrategy(
          parameters.hasAttribute(ExampleUDFConstant.SLIDING_STEP_KEY)
                  && parameters.hasAttribute(ExampleUDFConstant.DISPLAY_WINDOW_BEGIN_KEY)
                  && parameters.hasAttribute(ExampleUDFConstant.DISPLAY_WINDOW_END_KEY)
              ? new SlidingTimeWindowAccessStrategy(
                  parameters.getLong(ExampleUDFConstant.TIME_INTERVAL_KEY),
                  parameters.getLong(ExampleUDFConstant.SLIDING_STEP_KEY),
                  parameters.getLong(ExampleUDFConstant.DISPLAY_WINDOW_BEGIN_KEY),
                  parameters.getLong(ExampleUDFConstant.DISPLAY_WINDOW_END_KEY))
              : new SlidingTimeWindowAccessStrategy(
                  parameters.getLong(ExampleUDFConstant.TIME_INTERVAL_KEY)));
    }
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws IOException {
    collector.putLong(rowWindow.windowStartTime(), rowWindow.windowEndTime());
  }
}
