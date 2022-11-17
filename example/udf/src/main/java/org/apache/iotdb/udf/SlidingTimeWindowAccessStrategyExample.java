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

package org.apache.iotdb.udf;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlidingTimeWindowAccessStrategyExample implements UDTF {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SlidingTimeWindowAccessStrategyExample.class);

  public SlidingTimeWindowAccessStrategyExample() {}

  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    LOGGER.info("###### TestSlidingTimeWindow # beforeStart #######");
    LOGGER.info("attributes: {}", parameters.getAttributes().toString());
    if (parameters.hasAttribute("start") && parameters.hasAttribute("end")) {
      if (parameters.hasAttribute("step")) {
        configurations
            .setOutputDataType(Type.INT64)
            .setAccessStrategy(
                new SlidingTimeWindowAccessStrategy(
                    (long) parameters.getInt("interval"),
                    (long) parameters.getInt("step"),
                    parameters.getLong("start"),
                    parameters.getLong("end")));
      } else {
        configurations
            .setOutputDataType(Type.INT64)
            .setAccessStrategy(
                new SlidingTimeWindowAccessStrategy(
                    (long) parameters.getInt("interval"),
                    (long) parameters.getInt("interval"),
                    parameters.getLong("start"),
                    parameters.getLong("end")));
      }
    } else {
      if (parameters.hasAttribute("start") || parameters.hasAttribute("end")) {
        throw new RuntimeException("start and end must be both existed. ");
      }

      if (parameters.hasAttribute("step")) {
        configurations
            .setOutputDataType(Type.INT64)
            .setAccessStrategy(
                new SlidingTimeWindowAccessStrategy(
                    (long) parameters.getInt("interval"), (long) parameters.getInt("step")));
      } else {
        configurations
            .setOutputDataType(Type.INT64)
            .setAccessStrategy(
                new SlidingTimeWindowAccessStrategy((long) parameters.getInt("interval")));
      }
    }
  }

  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    LOGGER.info("######### TestSlidingTimeWindow # [{}] ########", rowWindow.windowSize());
    long result = 0L;

    for (int i = 0; i < rowWindow.windowSize(); ++i) {
      if (!rowWindow.getRow(i).isNull(0)) {
        result += rowWindow.getRow(i).getLong(0);
      }
    }

    collector.putLong(rowWindow.windowStartTime(), result);
  }

  public void beforeDestroy() {
    LOGGER.info("###### TestSlidingTimeWindow # beforeDestroy #######");
  }

  public void validate(UDFParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute("interval");
    validator.validateInputSeriesDataType(0, Type.INT64);
  }
}
