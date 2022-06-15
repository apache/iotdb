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
import org.apache.iotdb.udf.api.access.RowIterator;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SlidingTimeWindowConstructionTester implements UDTF {

  private static final Logger logger =
      LoggerFactory.getLogger(SlidingTimeWindowConstructionTester.class);

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator.validateInputSeriesNumber(1).validateInputSeriesDataType(0, Type.INT32);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    logger.debug("SlidingTimeWindowConstructionTester#beforeStart");
    long timeInterval = parameters.getLong(ExampleUDFConstant.TIME_INTERVAL_KEY);
    configurations
        .setOutputDataType(Type.INT32)
        .setAccessStrategy(new SlidingTimeWindowAccessStrategy(timeInterval));
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
    logger.debug("SlidingTimeWindowConstructionTester#beforeDestroy");
  }
}
