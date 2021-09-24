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

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.iotdb.db.integration.IoTDBUDFWindowQueryIT.ACCESS_STRATEGY_KEY;
import static org.apache.iotdb.db.integration.IoTDBUDFWindowQueryIT.ACCESS_STRATEGY_ROW_BY_ROW;
import static org.apache.iotdb.db.integration.IoTDBUDFWindowQueryIT.ACCESS_STRATEGY_SLIDING_SIZE;
import static org.apache.iotdb.db.integration.IoTDBUDFWindowQueryIT.ACCESS_STRATEGY_SLIDING_TIME;
import static org.apache.iotdb.db.integration.IoTDBUDFWindowQueryIT.DISPLAY_WINDOW_BEGIN_KEY;
import static org.apache.iotdb.db.integration.IoTDBUDFWindowQueryIT.DISPLAY_WINDOW_END_KEY;
import static org.apache.iotdb.db.integration.IoTDBUDFWindowQueryIT.SLIDING_STEP_KEY;
import static org.apache.iotdb.db.integration.IoTDBUDFWindowQueryIT.TIME_INTERVAL_KEY;
import static org.apache.iotdb.db.integration.IoTDBUDFWindowQueryIT.WINDOW_SIZE_KEY;

public class Accumulator implements UDTF {

  private static final Logger logger = LoggerFactory.getLogger(Accumulator.class);

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator.validateInputSeriesDataType(0, TSDataType.INT32);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    logger.debug("Accumulator#beforeStart");
    configurations.setOutputDataType(TSDataType.INT32);
    switch (parameters.getStringOrDefault(ACCESS_STRATEGY_KEY, ACCESS_STRATEGY_ROW_BY_ROW)) {
      case ACCESS_STRATEGY_SLIDING_SIZE:
        configurations.setAccessStrategy(
            new SlidingSizeWindowAccessStrategy(parameters.getInt(WINDOW_SIZE_KEY)));
        break;
      case ACCESS_STRATEGY_SLIDING_TIME:
        configurations.setAccessStrategy(
            new SlidingTimeWindowAccessStrategy(
                parameters.getLong(TIME_INTERVAL_KEY),
                parameters.getLong(SLIDING_STEP_KEY),
                parameters.getLong(DISPLAY_WINDOW_BEGIN_KEY),
                parameters.getLong(DISPLAY_WINDOW_END_KEY)));
        break;
      case ACCESS_STRATEGY_ROW_BY_ROW:
      default:
        configurations.setAccessStrategy(new RowByRowAccessStrategy());
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws IOException {
    collector.putInt(row.getTime(), row.getInt(0));
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws IOException {
    int accumulator = 0;
    RowIterator rowIterator = rowWindow.getRowIterator();
    while (rowIterator.hasNextRow()) {
      accumulator += rowIterator.next().getInt(0);
    }
    if (rowWindow.windowSize() != 0) {
      collector.putInt(rowWindow.getRow(0).getTime(), accumulator);
    }
  }

  @Override
  public void beforeDestroy() {
    logger.debug("Accumulator#beforeDestroy");
  }
}
