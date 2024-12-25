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
import org.apache.iotdb.udf.api.UDTF;
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

import org.apache.tsfile.enums.TSDataType;

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
      case BLOB:
      case DATE:
      case STRING:
      case TIMESTAMP:
      case BOOLEAN:
      case TEXT:
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
      int index = 0;
      int size = rowWindow.windowSize();
      int firstValueIndex = -1;
      int firstValue = 0;

      for (; index < size; index++) {
        if (!rowWindow.getRow(index).isNull(0)) {
          firstValueIndex = index;
          firstValue = rowWindow.getRow(index).getInt(0);
          break;
        }
      }

      if (firstValueIndex != -1) { // else empty window do nothing
        int lastValueIndex = firstValueIndex;
        int lastValue = firstValue;
        int minValueIndex = firstValueIndex;
        int minValue = firstValue;
        int maxValueIndex = firstValueIndex;
        int maxValue = firstValue;

        for (; index < size; index++) {
          if (!rowWindow.getRow(index).isNull(0)) {
            lastValueIndex = index;
            lastValue = rowWindow.getRow(index).getInt(0);
            if (lastValue < minValue) {
              minValue = lastValue;
              minValueIndex = index;
            }
            if (lastValue > maxValue) {
              maxValue = lastValue;
              maxValueIndex = index;
            }
          }
        }
        // first value
        collector.putInt(rowWindow.getRow(firstValueIndex).getTime(), firstValue);
        // min and max value if not duplicate
        // if min/max value is equal to first/last value, we keep first/last value
        int smallerIndex = Math.min(minValueIndex, maxValueIndex);
        int largerIndex = Math.max(minValueIndex, maxValueIndex);
        if (smallerIndex > firstValueIndex
            && rowWindow.getRow(smallerIndex).getInt(0) != lastValue) {
          collector.putInt(
              rowWindow.getRow(smallerIndex).getTime(), rowWindow.getRow(smallerIndex).getInt(0));
        }
        if (largerIndex > smallerIndex && rowWindow.getRow(largerIndex).getInt(0) != lastValue) {
          collector.putInt(
              rowWindow.getRow(largerIndex).getTime(), rowWindow.getRow(largerIndex).getInt(0));
        }
        // last value
        if (lastValueIndex > firstValueIndex) {
          collector.putInt(rowWindow.getRow(lastValueIndex).getTime(), lastValue);
        }
      }
    }
  }

  public void transformLong(RowWindow rowWindow, PointCollector collector) throws IOException {
    if (rowWindow.windowSize() > 0) { // else empty window do nothing
      int index = 0;
      int size = rowWindow.windowSize();
      int firstValueIndex = -1;
      long firstValue = 0;

      for (; index < size; index++) {
        if (!rowWindow.getRow(index).isNull(0)) {
          firstValueIndex = index;
          firstValue = rowWindow.getRow(index).getLong(0);
          break;
        }
      }

      if (firstValueIndex != -1) { // else empty window do nothing
        int lastValueIndex = firstValueIndex;
        long lastValue = firstValue;
        int minValueIndex = firstValueIndex;
        long minValue = firstValue;
        int maxValueIndex = firstValueIndex;
        long maxValue = firstValue;

        for (; index < size; index++) {
          if (!rowWindow.getRow(index).isNull(0)) {
            lastValueIndex = index;
            lastValue = rowWindow.getRow(index).getLong(0);
            if (lastValue < minValue) {
              minValue = lastValue;
              minValueIndex = index;
            }
            if (lastValue > maxValue) {
              maxValue = lastValue;
              maxValueIndex = index;
            }
          }
        }
        // first value
        collector.putLong(rowWindow.getRow(firstValueIndex).getTime(), firstValue);
        // min and max value if not duplicate
        // if min/max value is equal to first/last value, we keep first/last value
        int smallerIndex = Math.min(minValueIndex, maxValueIndex);
        int largerIndex = Math.max(minValueIndex, maxValueIndex);
        if (smallerIndex > firstValueIndex
            && rowWindow.getRow(smallerIndex).getLong(0) != lastValue) {
          collector.putLong(
              rowWindow.getRow(smallerIndex).getTime(), rowWindow.getRow(smallerIndex).getLong(0));
        }
        if (largerIndex > smallerIndex && rowWindow.getRow(largerIndex).getLong(0) != lastValue) {
          collector.putLong(
              rowWindow.getRow(largerIndex).getTime(), rowWindow.getRow(largerIndex).getLong(0));
        }
        // last value
        if (lastValueIndex > firstValueIndex) {
          collector.putLong(rowWindow.getRow(lastValueIndex).getTime(), lastValue);
        }
      }
    }
  }

  public void transformFloat(RowWindow rowWindow, PointCollector collector) throws IOException {
    if (rowWindow.windowSize() > 0) { // else empty window do nothing
      int index = 0;
      int size = rowWindow.windowSize();
      int firstValueIndex = -1;
      float firstValue = 0;

      for (; index < size; index++) {
        if (!rowWindow.getRow(index).isNull(0)) {
          firstValueIndex = index;
          firstValue = rowWindow.getRow(index).getFloat(0);
          break;
        }
      }

      if (firstValueIndex != -1) { // else empty window do nothing
        int lastValueIndex = firstValueIndex;
        float lastValue = firstValue;
        int minValueIndex = firstValueIndex;
        float minValue = firstValue;
        int maxValueIndex = firstValueIndex;
        float maxValue = firstValue;

        for (; index < size; index++) {
          if (!rowWindow.getRow(index).isNull(0)) {
            lastValueIndex = index;
            lastValue = rowWindow.getRow(index).getFloat(0);
            if (lastValue < minValue) {
              minValue = lastValue;
              minValueIndex = index;
            }
            if (lastValue > maxValue) {
              maxValue = lastValue;
              maxValueIndex = index;
            }
          }
        }
        // first value
        collector.putFloat(rowWindow.getRow(firstValueIndex).getTime(), firstValue);
        // min and max value if not duplicate
        // if min/max value is equal to first/last value, we keep first/last value
        int smallerIndex = Math.min(minValueIndex, maxValueIndex);
        int largerIndex = Math.max(minValueIndex, maxValueIndex);
        if (smallerIndex > firstValueIndex
            && rowWindow.getRow(smallerIndex).getFloat(0) != lastValue) {
          collector.putFloat(
              rowWindow.getRow(smallerIndex).getTime(), rowWindow.getRow(smallerIndex).getFloat(0));
        }
        if (largerIndex > smallerIndex && rowWindow.getRow(largerIndex).getFloat(0) != lastValue) {
          collector.putFloat(
              rowWindow.getRow(largerIndex).getTime(), rowWindow.getRow(largerIndex).getFloat(0));
        }
        // last value
        if (lastValueIndex > firstValueIndex) {
          collector.putFloat(rowWindow.getRow(lastValueIndex).getTime(), lastValue);
        }
      }
    }
  }

  public void transformDouble(RowWindow rowWindow, PointCollector collector) throws IOException {
    if (rowWindow.windowSize() > 0) { // else empty window do nothing
      int index = 0;
      int size = rowWindow.windowSize();
      int firstValueIndex = -1;
      double firstValue = 0;

      for (; index < size; index++) {
        if (!rowWindow.getRow(index).isNull(0)) {
          firstValueIndex = index;
          firstValue = rowWindow.getRow(index).getDouble(0);
          break;
        }
      }

      if (firstValueIndex != -1) { // else empty window do nothing
        int lastValueIndex = firstValueIndex;
        double lastValue = firstValue;
        int minValueIndex = firstValueIndex;
        double minValue = firstValue;
        int maxValueIndex = firstValueIndex;
        double maxValue = firstValue;

        for (; index < size; index++) {
          if (!rowWindow.getRow(index).isNull(0)) {
            lastValueIndex = index;
            lastValue = rowWindow.getRow(index).getDouble(0);
            if (lastValue < minValue) {
              minValue = lastValue;
              minValueIndex = index;
            }
            if (lastValue > maxValue) {
              maxValue = lastValue;
              maxValueIndex = index;
            }
          }
        }
        // first value
        collector.putDouble(rowWindow.getRow(firstValueIndex).getTime(), firstValue);
        // min and max value if not duplicate
        // if min/max value is equal to first/last value, we keep first/last value
        int smallerIndex = Math.min(minValueIndex, maxValueIndex);
        int largerIndex = Math.max(minValueIndex, maxValueIndex);
        if (smallerIndex > firstValueIndex
            && rowWindow.getRow(smallerIndex).getDouble(0) != lastValue) {
          collector.putDouble(
              rowWindow.getRow(smallerIndex).getTime(),
              rowWindow.getRow(smallerIndex).getDouble(0));
        }
        if (largerIndex > smallerIndex && rowWindow.getRow(largerIndex).getDouble(0) != lastValue) {
          collector.putDouble(
              rowWindow.getRow(largerIndex).getTime(), rowWindow.getRow(largerIndex).getDouble(0));
        }
        // last value
        if (lastValueIndex > firstValueIndex) {
          collector.putDouble(rowWindow.getRow(lastValueIndex).getTime(), lastValue);
        }
      }
    }
  }
}
