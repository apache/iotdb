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

import java.io.IOException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.OneByOneAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.TumblingWindowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class Counter extends UDTF {

  // for IT
  public static final String ACCESS_STRATEGY_KEY = "access";
  public static final String ACCESS_STRATEGY_ONE_BY_ONE = "one-by-one";
  public static final String ACCESS_STRATEGY_TUMBLING = "tumbling";
  public static final String ACCESS_STRATEGY_SLIDING = "access";

  public static final String WINDOW_SIZE = "windowSize";

  public static final String TIME_INTERVAL_KEY = "timeInterval";
  public static final String SLIDING_STEP_KEY = "slidingStep";
  public static final String DISPLAY_WINDOW_BEGIN = "displayWindowBegin";
  public static final String DISPLAY_WINDOW_END = "displayWindowEnd";

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    System.out.println("Counter#beforeStart");
    configurations.setOutputDataType(TSDataType.INT32);
    switch (parameters.getStringOrDefault(ACCESS_STRATEGY_KEY, ACCESS_STRATEGY_ONE_BY_ONE)) {
      case ACCESS_STRATEGY_TUMBLING:
        configurations.setAccessStrategy(new TumblingWindowAccessStrategy(
            parameters.getInt(WINDOW_SIZE)));
        break;
      case ACCESS_STRATEGY_SLIDING:
        configurations.setAccessStrategy(new SlidingTimeWindowAccessStrategy(
            parameters.getLong(TIME_INTERVAL_KEY),
            parameters.getLong(SLIDING_STEP_KEY),
            parameters.getLong(DISPLAY_WINDOW_BEGIN),
            parameters.getLong(DISPLAY_WINDOW_END)));
        break;
      case ACCESS_STRATEGY_ONE_BY_ONE:
      default:
        configurations.setAccessStrategy(new OneByOneAccessStrategy());
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    collector.putInt(row.getTime(), 1);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws IOException {
    if (rowWindow.windowSize() != 0) {
      collector.putInt(rowWindow.getRow(0).getTime(), rowWindow.windowSize());
    }
  }

  @Override
  public void beforeDestroy() {
    System.out.println("Counter#beforeDestroy");
  }
}
