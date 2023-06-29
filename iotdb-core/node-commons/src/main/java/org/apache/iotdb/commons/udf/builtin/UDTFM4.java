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

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

/**
 * For each sliding window, M4 returns the first, last, bottom, top points. The window can be
 * controlled by either point size or time interval length. The aggregated points in the output
 * series has been sorted and deduplicated.
 *
 * <p>SlidingSizeWindow usage Example: "select M4(s1,'windowSize'='10','slidingStep'='10') from
 * root.vehicle.d1" (windowSize is required, slidingStep is optional.)
 *
 * <p>SlidingTimeWindow usage Example: "select
 * M4(s1,'timeInterval'='25','slidingStep'='25','displayWindowBegin'='0','displayWindowEnd'='100')
 * from root.vehicle.d1" (timeInterval is required, slidingStep/displayWindowBegin/displayWindowEnd
 * are optional.)
 */
public class UDTFM4 implements UDTF {

  enum AccessStrategy {
    SIZE_WINDOW,
    TIME_WINDOW
  }

  protected AccessStrategy accessStrategy;
  protected TSDataType dataType;

  public static final String WINDOW_SIZE_KEY = "windowSize";
  public static final String TIME_INTERVAL_KEY = "timeInterval";
  public static final String SLIDING_STEP_KEY = "slidingStep";
  public static final String DISPLAY_WINDOW_BEGIN_KEY = "displayWindowBegin";
  public static final String DISPLAY_WINDOW_END_KEY = "displayWindowEnd";

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);

    if (!validator.getParameters().hasAttribute(WINDOW_SIZE_KEY)
        && !validator.getParameters().hasAttribute(TIME_INTERVAL_KEY)) {
      throw new UDFParameterNotValidException(
          String.format(
              "attribute \"%s\"/\"%s\" is required but was not provided.",
              WINDOW_SIZE_KEY, TIME_INTERVAL_KEY));
    }
    if (validator.getParameters().hasAttribute(WINDOW_SIZE_KEY)
        && validator.getParameters().hasAttribute(TIME_INTERVAL_KEY)) {
      throw new UDFParameterNotValidException(
          String.format(
              "use attribute \"%s\" or \"%s\" only one at a time.",
              WINDOW_SIZE_KEY, TIME_INTERVAL_KEY));
    }
    if (validator.getParameters().hasAttribute(WINDOW_SIZE_KEY)) {
      accessStrategy = AccessStrategy.SIZE_WINDOW;
    } else {
      accessStrategy = AccessStrategy.TIME_WINDOW;
    }

    dataType =
        UDFDataTypeTransformer.transformToTsDataType(validator.getParameters().getDataType(0));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException {
    // set data type
    configurations.setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(dataType));

    // set access strategy
    if (accessStrategy == AccessStrategy.SIZE_WINDOW) {
      int windowSize = parameters.getInt(WINDOW_SIZE_KEY);
      int slidingStep = parameters.getIntOrDefault(SLIDING_STEP_KEY, windowSize);
      configurations.setAccessStrategy(
          new SlidingSizeWindowAccessStrategy(windowSize, slidingStep));
    } else {
      long timeInterval = parameters.getLong(TIME_INTERVAL_KEY);
      long displayWindowBegin =
          parameters.getLongOrDefault(DISPLAY_WINDOW_BEGIN_KEY, Long.MIN_VALUE);
      long displayWindowEnd = parameters.getLongOrDefault(DISPLAY_WINDOW_END_KEY, Long.MAX_VALUE);
      long slidingStep = parameters.getLongOrDefault(SLIDING_STEP_KEY, timeInterval);
      configurations.setAccessStrategy(
          new SlidingTimeWindowAccessStrategy(
              timeInterval, slidingStep, displayWindowBegin, displayWindowEnd));
    }
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector)
      throws UDFException, IOException {
    switch (dataType) {
      case INT32:
        transformInt(rowWindow, collector);
        break;
      case INT64:
        transformLong(rowWindow, collector);
        break;
      case FLOAT:
        transformFloat(rowWindow, collector);
        break;
      case DOUBLE:
        transformDouble(rowWindow, collector);
        break;
      default:
        // This will not happen
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            UDFDataTypeTransformer.transformToUDFDataType(dataType),
            Type.INT32,
            Type.INT64,
            Type.FLOAT,
            Type.DOUBLE);
    }
  }

  public void transformInt(RowWindow rowWindow, PointCollector collector) throws IOException {
    if (rowWindow.windowSize() > 0) { // else empty window do nothing
      int firstValue = rowWindow.getRow(0).getInt(0);
      int lastValue = rowWindow.getRow(rowWindow.windowSize() - 1).getInt(0);

      int minValue = Math.min(firstValue, lastValue);
      int maxValue = Math.max(firstValue, lastValue);
      int minIndex = (firstValue < lastValue) ? 0 : rowWindow.windowSize() - 1;
      int maxIndex = (firstValue > lastValue) ? 0 : rowWindow.windowSize() - 1;

      for (int i = 1; i < rowWindow.windowSize() - 1; i++) {
        int value = rowWindow.getRow(i).getInt(0);
        if (value < minValue) {
          minValue = value;
          minIndex = i;
        }
        if (value > maxValue) {
          maxValue = value;
          maxIndex = i;
        }
      }

      Row row = rowWindow.getRow(0);
      collector.putInt(row.getTime(), row.getInt(0));

      int smallerIndex = Math.min(minIndex, maxIndex);
      int largerIndex = Math.max(minIndex, maxIndex);
      if (smallerIndex > 0) {
        row = rowWindow.getRow(smallerIndex);
        collector.putInt(row.getTime(), row.getInt(0));
      }
      if (largerIndex > smallerIndex) {
        row = rowWindow.getRow(largerIndex);
        collector.putInt(row.getTime(), row.getInt(0));
      }
      if (largerIndex < rowWindow.windowSize() - 1) {
        row = rowWindow.getRow(rowWindow.windowSize() - 1);
        collector.putInt(row.getTime(), row.getInt(0));
      }
    }
  }

  public void transformLong(RowWindow rowWindow, PointCollector collector) throws IOException {
    if (rowWindow.windowSize() > 0) { // else empty window do nothing
      long firstValue = rowWindow.getRow(0).getLong(0);
      long lastValue = rowWindow.getRow(rowWindow.windowSize() - 1).getLong(0);

      long minValue = Math.min(firstValue, lastValue);
      long maxValue = Math.max(firstValue, lastValue);
      int minIndex = (firstValue < lastValue) ? 0 : rowWindow.windowSize() - 1;
      int maxIndex = (firstValue > lastValue) ? 0 : rowWindow.windowSize() - 1;

      for (int i = 1; i < rowWindow.windowSize() - 1; i++) {
        long value = rowWindow.getRow(i).getLong(0);
        if (value < minValue) {
          minValue = value;
          minIndex = i;
        }
        if (value > maxValue) {
          maxValue = value;
          maxIndex = i;
        }
      }

      Row row = rowWindow.getRow(0);
      collector.putLong(row.getTime(), row.getLong(0));

      int smallerIndex = Math.min(minIndex, maxIndex);
      int largerIndex = Math.max(minIndex, maxIndex);
      if (smallerIndex > 0) {
        row = rowWindow.getRow(smallerIndex);
        collector.putLong(row.getTime(), row.getLong(0));
      }
      if (largerIndex > smallerIndex) {
        row = rowWindow.getRow(largerIndex);
        collector.putLong(row.getTime(), row.getLong(0));
      }
      if (largerIndex < rowWindow.windowSize() - 1) {
        row = rowWindow.getRow(rowWindow.windowSize() - 1);
        collector.putLong(row.getTime(), row.getLong(0));
      }
    }
  }

  public void transformFloat(RowWindow rowWindow, PointCollector collector) throws IOException {
    if (rowWindow.windowSize() > 0) { // else empty window do nothing
      float firstValue = rowWindow.getRow(0).getFloat(0);
      float lastValue = rowWindow.getRow(rowWindow.windowSize() - 1).getFloat(0);

      float minValue = Math.min(firstValue, lastValue);
      float maxValue = Math.max(firstValue, lastValue);
      int minIndex = (firstValue < lastValue) ? 0 : rowWindow.windowSize() - 1;
      int maxIndex = (firstValue > lastValue) ? 0 : rowWindow.windowSize() - 1;

      for (int i = 1; i < rowWindow.windowSize() - 1; i++) {
        float value = rowWindow.getRow(i).getFloat(0);
        if (value < minValue) {
          minValue = value;
          minIndex = i;
        }
        if (value > maxValue) {
          maxValue = value;
          maxIndex = i;
        }
      }

      Row row = rowWindow.getRow(0);
      collector.putFloat(row.getTime(), row.getFloat(0));

      int smallerIndex = Math.min(minIndex, maxIndex);
      int largerIndex = Math.max(minIndex, maxIndex);
      if (smallerIndex > 0) {
        row = rowWindow.getRow(smallerIndex);
        collector.putFloat(row.getTime(), row.getFloat(0));
      }
      if (largerIndex > smallerIndex) {
        row = rowWindow.getRow(largerIndex);
        collector.putFloat(row.getTime(), row.getFloat(0));
      }
      if (largerIndex < rowWindow.windowSize() - 1) {
        row = rowWindow.getRow(rowWindow.windowSize() - 1);
        collector.putFloat(row.getTime(), row.getFloat(0));
      }
    }
  }

  public void transformDouble(RowWindow rowWindow, PointCollector collector) throws IOException {
    if (rowWindow.windowSize() > 0) { // else empty window do nothing
      double firstValue = rowWindow.getRow(0).getDouble(0);
      double lastValue = rowWindow.getRow(rowWindow.windowSize() - 1).getDouble(0);

      double minValue = Math.min(firstValue, lastValue);
      double maxValue = Math.max(firstValue, lastValue);
      int minIndex = (firstValue < lastValue) ? 0 : rowWindow.windowSize() - 1;
      int maxIndex = (firstValue > lastValue) ? 0 : rowWindow.windowSize() - 1;

      for (int i = 1; i < rowWindow.windowSize() - 1; i++) {
        double value = rowWindow.getRow(i).getDouble(0);
        if (value < minValue) {
          minValue = value;
          minIndex = i;
        }
        if (value > maxValue) {
          maxValue = value;
          maxIndex = i;
        }
      }

      Row row = rowWindow.getRow(0);
      collector.putDouble(row.getTime(), row.getDouble(0));

      int smallerIndex = Math.min(minIndex, maxIndex);
      int largerIndex = Math.max(minIndex, maxIndex);
      if (smallerIndex > 0) {
        row = rowWindow.getRow(smallerIndex);
        collector.putDouble(row.getTime(), row.getDouble(0));
      }
      if (largerIndex > smallerIndex) {
        row = rowWindow.getRow(largerIndex);
        collector.putDouble(row.getTime(), row.getDouble(0));
      }
      if (largerIndex < rowWindow.windowSize() - 1) {
        row = rowWindow.getRow(rowWindow.windowSize() - 1);
        collector.putDouble(row.getTime(), row.getDouble(0));
      }
    }
  }
}
