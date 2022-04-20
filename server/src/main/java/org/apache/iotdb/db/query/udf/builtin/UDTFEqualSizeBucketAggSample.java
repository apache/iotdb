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

package org.apache.iotdb.db.query.udf.builtin;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.exception.UDFException;
import org.apache.iotdb.db.query.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.db.query.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class UDTFEqualSizeBucketAggSample extends UDTFEqualSizeBucketSample {

  private String aggMethodType;

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
        "Illegal aggregation method.",
        aggMethodType);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    // if we use aggregation method on average or variance, the outputDataType may be double.
    // For other scenarios, outputDataType == dataType
    TSDataType outputDataType = dataType;
    if ("avg".equals(aggMethodType) || "variance".equals(aggMethodType)) {
      outputDataType = TSDataType.DOUBLE;
    }
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(bucketSize))
        .setOutputDataType(outputDataType);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector)
      throws IOException, UDFParameterNotValidException {
    long time = rowWindow.getRow(0).getTime();
    int windowSize = rowWindow.windowSize();
    switch (aggMethodType) {
      case "avg":
        {
          switch (dataType) {
            case INT32:
              transformAvgInt(rowWindow, collector, time, windowSize);
              break;
            case INT64:
              transformAvgLong(rowWindow, collector, time, windowSize);
              break;
            case FLOAT:
              transformAvgFloat(rowWindow, collector, time, windowSize);
              break;
            case DOUBLE:
              transformAvgDouble(rowWindow, collector, time, windowSize);
              break;
            default:
              // This will not happen
              throw new UDFInputSeriesDataTypeNotValidException(
                  0,
                  dataType,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE);
          }
          break;
        }
      case "min":
        {
          switch (dataType) {
            case INT32:
              transformMinInt(rowWindow, collector, time, windowSize);
              break;
            case INT64:
              transformMinLong(rowWindow, collector, time, windowSize);
              break;
            case FLOAT:
              transformMinFloat(rowWindow, collector, time, windowSize);
              break;
            case DOUBLE:
              transformMinDouble(rowWindow, collector, time, windowSize);
              break;
            default:
              // This will not happen
              throw new UDFInputSeriesDataTypeNotValidException(
                  0,
                  dataType,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE);
          }
          break;
        }
      case "max":
        {
          switch (dataType) {
            case INT32:
              transformMaxInt(rowWindow, collector, time, windowSize);
              break;
            case INT64:
              transformMaxLong(rowWindow, collector, time, windowSize);
              break;
            case FLOAT:
              transformMaxFloat(rowWindow, collector, time, windowSize);
              break;
            case DOUBLE:
              transformMaxDouble(rowWindow, collector, time, windowSize);
              break;
            default:
              // This will not happen
              throw new UDFInputSeriesDataTypeNotValidException(
                  0,
                  dataType,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE);
          }
          break;
        }
      case "sum":
        {
          switch (dataType) {
            case INT32:
              transformSumInt(rowWindow, collector, time, windowSize);
              break;
            case INT64:
              transformSumLong(rowWindow, collector, time, windowSize);
              break;
            case FLOAT:
              transformSumFloat(rowWindow, collector, time, windowSize);
              break;
            case DOUBLE:
              transformSumDouble(rowWindow, collector, time, windowSize);
              break;
            default:
              // This will not happen
              throw new UDFInputSeriesDataTypeNotValidException(
                  0,
                  dataType,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE);
          }
          break;
        }
      case "extreme":
        {
          switch (dataType) {
            case INT32:
              transformExtremeInt(rowWindow, collector, time, windowSize);
              break;
            case INT64:
              transformExtremeLong(rowWindow, collector, time, windowSize);
              break;
            case FLOAT:
              transformExtremeFloat(rowWindow, collector, time, windowSize);
              break;
            case DOUBLE:
              transformExtremeDouble(rowWindow, collector, time, windowSize);
              break;
            default:
              // This will not happen
              throw new UDFInputSeriesDataTypeNotValidException(
                  0,
                  dataType,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE);
          }
          break;
        }
      case "variance":
        {
          switch (dataType) {
            case INT32:
              transformVarianceInt(rowWindow, collector, time, windowSize);
              break;
            case INT64:
              transformVarianceLong(rowWindow, collector, time, windowSize);
              break;
            case FLOAT:
              transformVarianceFloat(rowWindow, collector, time, windowSize);
              break;
            case DOUBLE:
              transformVarianceDouble(rowWindow, collector, time, windowSize);
              break;
            default:
              // This will not happen
              throw new UDFInputSeriesDataTypeNotValidException(
                  0,
                  dataType,
                  TSDataType.INT32,
                  TSDataType.INT64,
                  TSDataType.FLOAT,
                  TSDataType.DOUBLE);
          }
          break;
        }
      default:
        throw new UDFParameterNotValidException("Illegal aggregation method.");
    }
  }

  // transform for avg
  public void transformAvgInt(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    double sum = 0;
    for (int i = 0; i < windowSize; i++) {
      sum += rowWindow.getRow(i).getInt(0) * 1.0 / windowSize;
    }
    collector.putDouble(time, sum);
  }

  public void transformAvgLong(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    double sum = 0;
    for (int i = 0; i < windowSize; i++) {
      sum += rowWindow.getRow(i).getLong(0) * 1.0 / windowSize;
    }
    collector.putDouble(time, sum);
  }

  public void transformAvgFloat(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    double sum = 0;
    for (int i = 0; i < windowSize; i++) {
      sum += rowWindow.getRow(i).getFloat(0) * 1.0 / windowSize;
    }
    collector.putDouble(time, sum);
  }

  public void transformAvgDouble(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    double sum = 0;
    for (int i = 0; i < windowSize; i++) {
      sum += rowWindow.getRow(i).getDouble(0) / windowSize;
    }
    collector.putDouble(time, sum);
  }

  // transform for min
  public void transformMinInt(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    int minValue = rowWindow.getRow(0).getInt(0);
    for (int i = 1; i < windowSize; i++) {
      int value = rowWindow.getRow(i).getInt(0);
      if (minValue > value) {
        minValue = value;
      }
    }
    collector.putInt(time, minValue);
  }

  public void transformMinLong(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    long minValue = rowWindow.getRow(0).getLong(0);
    for (int i = 1; i < windowSize; i++) {
      long value = rowWindow.getRow(i).getLong(0);
      if (minValue > value) {
        minValue = value;
      }
    }
    collector.putLong(time, minValue);
  }

  public void transformMinFloat(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    float minValue = rowWindow.getRow(0).getFloat(0);
    for (int i = 1; i < windowSize; i++) {
      float value = rowWindow.getRow(i).getFloat(0);
      if (minValue > value) {
        minValue = value;
      }
    }
    collector.putFloat(time, minValue);
  }

  public void transformMinDouble(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    double minValue = rowWindow.getRow(0).getDouble(0);
    for (int i = 1; i < windowSize; i++) {
      double value = rowWindow.getRow(i).getDouble(0);
      if (minValue > value) {
        minValue = value;
      }
    }
    collector.putDouble(time, minValue);
  }

  // transform for max
  public void transformMaxInt(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    int maxValue = rowWindow.getRow(0).getInt(0);
    for (int i = 1; i < windowSize; i++) {
      int value = rowWindow.getRow(i).getInt(0);
      if (maxValue < value) {
        maxValue = value;
      }
    }
    collector.putInt(time, maxValue);
  }

  public void transformMaxLong(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    long maxValue = rowWindow.getRow(0).getLong(0);
    for (int i = 1; i < windowSize; i++) {
      long value = rowWindow.getRow(i).getLong(0);
      if (maxValue < value) {
        maxValue = value;
      }
    }
    collector.putLong(time, maxValue);
  }

  public void transformMaxFloat(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    float maxValue = rowWindow.getRow(0).getFloat(0);
    for (int i = 1; i < windowSize; i++) {
      float value = rowWindow.getRow(i).getFloat(0);
      if (maxValue < value) {
        maxValue = value;
      }
    }
    collector.putFloat(time, maxValue);
  }

  public void transformMaxDouble(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    double maxValue = rowWindow.getRow(0).getDouble(0);
    for (int i = 1; i < windowSize; i++) {
      double value = rowWindow.getRow(i).getDouble(0);
      if (maxValue < value) {
        maxValue = value;
      }
    }
    collector.putDouble(time, maxValue);
  }

  // transform for sum
  public void transformSumInt(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    int sum = 0;
    for (int i = 0; i < windowSize; i++) {
      sum += rowWindow.getRow(i).getInt(0);
    }
    collector.putInt(time, sum);
  }

  public void transformSumLong(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    long sum = 0;
    for (int i = 0; i < windowSize; i++) {
      sum += rowWindow.getRow(i).getLong(0);
    }
    collector.putLong(time, sum);
  }

  public void transformSumFloat(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    float sum = 0;
    for (int i = 0; i < windowSize; i++) {
      sum += rowWindow.getRow(i).getFloat(0);
    }
    collector.putFloat(time, sum);
  }

  public void transformSumDouble(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    double sum = 0;
    for (int i = 0; i < windowSize; i++) {
      sum += rowWindow.getRow(i).getDouble(0);
    }
    collector.putDouble(time, sum);
  }

  // transform for extreme
  public void transformExtremeInt(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
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

  public void transformExtremeLong(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
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

  public void transformExtremeFloat(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
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

  public void transformExtremeDouble(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
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

  // transform for variance
  public void transformVarianceInt(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    double avg = 0, sum = 0;
    for (int i = 0; i < windowSize; i++) {
      avg += rowWindow.getRow(i).getInt(0) * 1.0 / windowSize;
    }
    for (int i = 0; i < windowSize; i++) {
      sum += (rowWindow.getRow(i).getInt(0) - avg) * (rowWindow.getRow(i).getInt(0) - avg);
    }
    collector.putDouble(time, sum / windowSize);
  }

  public void transformVarianceLong(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    double avg = 0, sum = 0;
    for (int i = 0; i < windowSize; i++) {
      avg += rowWindow.getRow(i).getLong(0) * 1.0 / windowSize;
    }
    for (int i = 0; i < windowSize; i++) {
      sum += (rowWindow.getRow(i).getLong(0) - avg) * (rowWindow.getRow(i).getLong(0) - avg);
    }
    collector.putDouble(time, sum / windowSize);
  }

  public void transformVarianceFloat(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    double avg = 0, sum = 0;
    for (int i = 0; i < windowSize; i++) {
      avg += rowWindow.getRow(i).getFloat(0) / windowSize;
    }
    for (int i = 0; i < windowSize; i++) {
      sum += (rowWindow.getRow(i).getFloat(0) - avg) * (rowWindow.getRow(i).getFloat(0) - avg);
    }
    collector.putDouble(time, sum / windowSize);
  }

  public void transformVarianceDouble(
      RowWindow rowWindow, PointCollector collector, long time, int windowSize) throws IOException {
    double avg = 0, sum = 0;
    for (int i = 0; i < windowSize; i++) {
      avg += rowWindow.getRow(i).getDouble(0) / windowSize;
    }
    for (int i = 0; i < windowSize; i++) {
      sum += (rowWindow.getRow(i).getDouble(0) - avg) * (rowWindow.getRow(i).getDouble(0) - avg);
    }
    collector.putDouble(time, sum / windowSize);
  }
}
