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
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

public class UDTFEqualSizeBucketAggSample extends UDTFEqualSizeBucketSample {

  private String aggMethodType;
  private Aggregator aggregator;

  private interface Aggregator {

    void aggregateInt(RowWindow rowWindow, PointCollector collector) throws IOException;

    void aggregateLong(RowWindow rowWindow, PointCollector collector) throws IOException;

    void aggregateFloat(RowWindow rowWindow, PointCollector collector) throws IOException;

    void aggregateDouble(RowWindow rowWindow, PointCollector collector) throws IOException;
  }

  private static class AvgAggregator implements Aggregator {
    @Override
    public void aggregateInt(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double sum = 0;
      for (int i = 0; i < windowSize; i++) {
        sum += rowWindow.getRow(i).getInt(0);
      }
      collector.putDouble(time, sum / windowSize);
    }

    @Override
    public void aggregateLong(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double sum = 0;
      for (int i = 0; i < windowSize; i++) {
        sum += rowWindow.getRow(i).getLong(0);
      }
      collector.putDouble(time, sum / windowSize);
    }

    @Override
    public void aggregateFloat(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double sum = 0;
      for (int i = 0; i < windowSize; i++) {
        sum += rowWindow.getRow(i).getFloat(0);
      }
      collector.putDouble(time, sum / windowSize);
    }

    @Override
    public void aggregateDouble(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double sum = 0;
      for (int i = 0; i < windowSize; i++) {
        sum += rowWindow.getRow(i).getDouble(0);
      }
      collector.putDouble(time, sum / windowSize);
    }
  }

  private static class MinAggregator implements Aggregator {
    @Override
    public void aggregateInt(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      int minValue = rowWindow.getRow(0).getInt(0);
      for (int i = 1; i < windowSize; i++) {
        int value = rowWindow.getRow(i).getInt(0);
        if (minValue > value) {
          minValue = value;
        }
      }
      collector.putInt(time, minValue);
    }

    @Override
    public void aggregateLong(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      long minValue = rowWindow.getRow(0).getLong(0);
      for (int i = 1; i < windowSize; i++) {
        long value = rowWindow.getRow(i).getLong(0);
        if (minValue > value) {
          minValue = value;
        }
      }
      collector.putLong(time, minValue);
    }

    @Override
    public void aggregateFloat(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      float minValue = rowWindow.getRow(0).getFloat(0);
      for (int i = 1; i < windowSize; i++) {
        float value = rowWindow.getRow(i).getFloat(0);
        if (minValue > value) {
          minValue = value;
        }
      }
      collector.putFloat(time, minValue);
    }

    @Override
    public void aggregateDouble(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double minValue = rowWindow.getRow(0).getDouble(0);
      for (int i = 1; i < windowSize; i++) {
        double value = rowWindow.getRow(i).getDouble(0);
        if (minValue > value) {
          minValue = value;
        }
      }
      collector.putDouble(time, minValue);
    }
  }

  private static class MaxAggregator implements Aggregator {
    @Override
    public void aggregateInt(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      int maxValue = rowWindow.getRow(0).getInt(0);
      for (int i = 1; i < windowSize; i++) {
        int value = rowWindow.getRow(i).getInt(0);
        if (maxValue < value) {
          maxValue = value;
        }
      }
      collector.putInt(time, maxValue);
    }

    @Override
    public void aggregateLong(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      long maxValue = rowWindow.getRow(0).getLong(0);
      for (int i = 1; i < windowSize; i++) {
        long value = rowWindow.getRow(i).getLong(0);
        if (maxValue < value) {
          maxValue = value;
        }
      }
      collector.putLong(time, maxValue);
    }

    @Override
    public void aggregateFloat(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      float maxValue = rowWindow.getRow(0).getFloat(0);
      for (int i = 1; i < windowSize; i++) {
        float value = rowWindow.getRow(i).getFloat(0);
        if (maxValue < value) {
          maxValue = value;
        }
      }
      collector.putFloat(time, maxValue);
    }

    @Override
    public void aggregateDouble(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double maxValue = rowWindow.getRow(0).getDouble(0);
      for (int i = 1; i < windowSize; i++) {
        double value = rowWindow.getRow(i).getDouble(0);
        if (maxValue < value) {
          maxValue = value;
        }
      }
      collector.putDouble(time, maxValue);
    }
  }

  private static class SumAggregator implements Aggregator {
    @Override
    public void aggregateInt(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      int sum = 0;
      for (int i = 0; i < windowSize; i++) {
        sum += rowWindow.getRow(i).getInt(0);
      }
      collector.putInt(time, sum);
    }

    @Override
    public void aggregateLong(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      long sum = 0;
      for (int i = 0; i < windowSize; i++) {
        sum += rowWindow.getRow(i).getLong(0);
      }
      collector.putLong(time, sum);
    }

    @Override
    public void aggregateFloat(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      float sum = 0;
      for (int i = 0; i < windowSize; i++) {
        sum += rowWindow.getRow(i).getFloat(0);
      }
      collector.putFloat(time, sum);
    }

    @Override
    public void aggregateDouble(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double sum = 0;
      for (int i = 0; i < windowSize; i++) {
        sum += rowWindow.getRow(i).getDouble(0);
      }
      collector.putDouble(time, sum);
    }
  }

  private static class ExtremeAggregator implements Aggregator {
    @Override
    public void aggregateInt(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      int extreme = 0;
      for (int i = 0; i < windowSize; i++) {
        int origin = rowWindow.getRow(i).getInt(0);
        int value = origin > 0 ? origin : -origin;
        if (extreme < value) {
          extreme = origin;
        }
      }
      collector.putInt(time, extreme);
    }

    @Override
    public void aggregateLong(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      long extreme = 0;
      for (int i = 0; i < windowSize; i++) {
        long origin = rowWindow.getRow(i).getLong(0);
        long value = origin > 0 ? origin : -origin;
        if (extreme < value) {
          extreme = origin;
        }
      }
      collector.putLong(time, extreme);
    }

    @Override
    public void aggregateFloat(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      float extreme = 0;
      for (int i = 0; i < windowSize; i++) {
        float origin = rowWindow.getRow(i).getFloat(0);
        float value = origin > 0 ? origin : -origin;
        if (extreme < value) {
          extreme = origin;
        }
      }
      collector.putFloat(time, extreme);
    }

    @Override
    public void aggregateDouble(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double extreme = 0;
      for (int i = 0; i < windowSize; i++) {
        double origin = rowWindow.getRow(i).getDouble(0);
        double value = origin > 0 ? origin : -origin;
        if (extreme < value) {
          extreme = origin;
        }
      }
      collector.putDouble(time, extreme);
    }
  }

  private static class VarianceAggregator implements Aggregator {
    @Override
    public void aggregateInt(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double avg = 0, sum = 0;
      for (int i = 0; i < windowSize; i++) {
        avg += rowWindow.getRow(i).getInt(0);
      }
      avg /= windowSize;
      for (int i = 0; i < windowSize; i++) {
        double delta = rowWindow.getRow(i).getInt(0) - avg;
        sum += delta * delta;
      }
      collector.putDouble(time, sum / windowSize);
    }

    @Override
    public void aggregateLong(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double avg = 0, sum = 0;
      for (int i = 0; i < windowSize; i++) {
        avg += rowWindow.getRow(i).getLong(0);
      }
      avg /= windowSize;
      for (int i = 0; i < windowSize; i++) {
        double delta = rowWindow.getRow(i).getLong(0) - avg;
        sum += delta * delta;
      }
      collector.putDouble(time, sum / windowSize);
    }

    @Override
    public void aggregateFloat(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double avg = 0, sum = 0;
      for (int i = 0; i < windowSize; i++) {
        avg += rowWindow.getRow(i).getFloat(0);
      }
      avg /= windowSize;
      for (int i = 0; i < windowSize; i++) {
        double delta = rowWindow.getRow(i).getFloat(0) - avg;
        sum += delta * delta;
      }
      collector.putDouble(time, sum / windowSize);
    }

    @Override
    public void aggregateDouble(RowWindow rowWindow, PointCollector collector) throws IOException {
      long time = rowWindow.getRow(0).getTime();
      int windowSize = rowWindow.windowSize();

      double avg = 0, sum = 0;
      for (int i = 0; i < windowSize; i++) {
        avg += rowWindow.getRow(i).getDouble(0);
      }
      avg /= windowSize;
      for (int i = 0; i < windowSize; i++) {
        double delta = rowWindow.getRow(i).getDouble(0) - avg;
        sum += delta * delta;
      }
      collector.putDouble(time, sum / windowSize);
    }
  }

  @Override
  public void validate(UDFParameterValidator validator) throws MetadataException, UDFException {
    super.validate(validator);
    aggMethodType = validator.getParameters().getStringOrDefault("type", "avg").toLowerCase();
    validator.validate(
        type ->
            "avg".equals(type)
                || "max".equals(type)
                || "min".equals(type)
                || "sum".equals(type)
                || "extreme".equals(type)
                || "variance".equals(type),
        "Illegal aggregation method. Aggregation type should be avg, min, max, sum, extreme, variance.",
        aggMethodType);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws UDFParameterNotValidException {
    // if we use aggregation method on average or variance, the outputDataType may be double.
    // For other scenarios, outputDataType == dataType
    TSDataType outputDataType = dataType;
    if ("avg".equals(aggMethodType) || "variance".equals(aggMethodType)) {
      outputDataType = TSDataType.DOUBLE;
    }
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(bucketSize))
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(outputDataType));
    switch (aggMethodType) {
      case "avg":
        aggregator = new AvgAggregator();
        break;
      case "min":
        aggregator = new MinAggregator();
        break;
      case "max":
        aggregator = new MaxAggregator();
        break;
      case "sum":
        aggregator = new SumAggregator();
        break;
      case "extreme":
        aggregator = new ExtremeAggregator();
        break;
      case "variance":
        aggregator = new VarianceAggregator();
        break;
      default:
        throw new UDFParameterNotValidException(
            "Illegal aggregation method. Aggregation type should be avg, min, max, sum, extreme, variance.");
    }
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector)
      throws IOException, UDFParameterNotValidException {
    switch (dataType) {
      case INT32:
        aggregator.aggregateInt(rowWindow, collector);
        break;
      case INT64:
        aggregator.aggregateLong(rowWindow, collector);
        break;
      case FLOAT:
        aggregator.aggregateFloat(rowWindow, collector);
        break;
      case DOUBLE:
        aggregator.aggregateDouble(rowWindow, collector);
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
}
