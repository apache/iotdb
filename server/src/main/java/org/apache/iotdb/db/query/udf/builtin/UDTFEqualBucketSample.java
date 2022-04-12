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
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
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
import java.util.Arrays;
import java.util.Random;

public class UDTFEqualBucketSample implements UDTF {

  private TSDataType inputDataType;
  private TSDataType outputDataType;
  private String method;
  private String aggMethodType;
  private double proportion;
  private int bucketSize;
  private int bucketSampleNum;
  private Random random;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    method = validator.getParameters().getStringOrDefault("method", "random").toLowerCase();
    proportion = validator.getParameters().getDoubleOrDefault("proportion", 0.1);
    aggMethodType = validator.getParameters().getStringOrDefault("type", "avg").toLowerCase();
    bucketSampleNum = validator.getParameters().getIntOrDefault("- bucket_sample_num", 10);
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
        .validate(
            method ->
                "random".equals(method)
                    || "aggregation".equals(method)
                    || "m4".equals(method)
                    || "outlier".equals(method),
            "Illegal equal bucket sampling method.",
            method)
        .validate(
            proportion -> (double) proportion > 0 && (double) proportion <= 1,
            "Illegal sample proportion.",
            proportion)
        .validate(
            type -> "avg".equals(type) || "max".equals(type) || "min".equals(type),
            "Illegal aggregation method.",
            aggMethodType)
        .validate(
            bucketSampleNum -> (int) bucketSampleNum >= 1,
            "Illegal aggregation method.",
            bucketSampleNum);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException {
    inputDataType = parameters.getDataType(0);
    random = new Random();
    outputDataType = inputDataType;
    // if we use aggregation method on average, the outputDataType may be double.
    // For other scenarios, outputDataType == inputDataType
    if ("aggregation".equals(method) && "avg".equals(aggMethodType)) {
      outputDataType = TSDataType.DOUBLE;
    }
    bucketSize = (int) (1 / proportion);
    if ("m4".equals(method)) {
      bucketSize *= 4;
    }
    if ("outlier".equals(method)) {
      // TODO do not justify the upper bound of bucketSampleNum
      bucketSize *= bucketSampleNum;
    }
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(bucketSize))
        .setOutputDataType(outputDataType);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector)
      throws UDFParameterNotValidException, IOException {
    switch (method) {
      case "random":
        randomSample(rowWindow, collector);
        break;
      case "outlier":
        outlierSample(rowWindow, collector);
        break;
      case "m4":
        m4Sample(rowWindow, collector);
        break;
      case "aggregation":
        aggregationSample(rowWindow, collector);
        break;
      default:
        // this will not happen
        throw new UDFParameterNotValidException("Illegal equal bucket sampling method.");
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    UDTF.super.terminate(collector);
  }

  public void randomSample(RowWindow rowWindow, PointCollector collector)
      throws IOException, UDFInputSeriesDataTypeNotValidException {
    Row row = rowWindow.getRow(random.nextInt(rowWindow.windowSize()));
    switch (outputDataType) {
      case INT32:
        collector.putInt(row.getTime(), row.getInt(0));
        break;
      case INT64:
        collector.putLong(row.getTime(), row.getLong(0));
        break;
      case FLOAT:
        collector.putFloat(row.getTime(), row.getFloat(0));
        break;
      case DOUBLE:
        collector.putDouble(row.getTime(), row.getDouble(0));
      default:
        // This will not happen
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            outputDataType,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE);
    }
  }

  public void outlierSample(RowWindow rowWindow, PointCollector collector) {
    //    int[] arr = new int[]
  }

  public void m4Sample(RowWindow rowWindow, PointCollector collector)
      throws IOException, UDFInputSeriesDataTypeNotValidException {
    int minIndex = 0, maxIndex = 0;
    switch (outputDataType) {
      case INT32: {
        int maxValue = rowWindow.getRow(0).getInt(0);
        int minValue = rowWindow.getRow(0).getInt(0);
        for (int i = 1; i < rowWindow.windowSize(); i++) {
          int value = rowWindow.getRow(i).getInt(0);
          if (minValue > value) {
            minValue = value;
            minIndex = i;
          }
          if (maxValue < value) {
            maxValue = value;
            maxIndex = i;
          }
        }
        int[] arr = new int[]{0, minIndex, maxIndex, rowWindow.windowSize() - 1};
        // avoid duplicated index
        Arrays.sort(arr);
        Row row = rowWindow.getRow(0);
        collector.putInt(row.getTime(), row.getInt(0));
        for (int i = 1; i < 4; i++) {
          if (arr[i] > arr[i - 1]) {
            row = rowWindow.getRow(arr[i]);
            collector.putInt(row.getTime(), row.getInt(0));
          }
        }
        break;
      }
      case INT64: {
        long maxValue = rowWindow.getRow(0).getLong(0);
        long minValue = rowWindow.getRow(0).getLong(0);
        for (int i = 1; i < rowWindow.windowSize(); i++) {
          long value = rowWindow.getRow(i).getLong(0);
          if (minValue > value) {
            minValue = value;
            minIndex = i;
          }
          if (maxValue < value) {
            maxValue = value;
            maxIndex = i;
          }
        }
        int[] arr = new int[]{0, minIndex, maxIndex, rowWindow.windowSize() - 1};
        // avoid duplicated index
        Arrays.sort(arr);
        Row row = rowWindow.getRow(0);
        collector.putLong(row.getTime(), row.getLong(0));
        for (int i = 1; i < 4; i++) {
          if (arr[i] > arr[i - 1]) {
            row = rowWindow.getRow(arr[i]);
            collector.putLong(row.getTime(), row.getLong(0));
          }
        }
        break;
      }
      case FLOAT: {
        float maxValue = rowWindow.getRow(0).getFloat(0);
        float minValue = rowWindow.getRow(0).getFloat(0);
        for (int i = 1; i < rowWindow.windowSize(); i++) {
          float value = rowWindow.getRow(i).getFloat(0);
          if (minValue > value) {
            minValue = value;
            minIndex = i;
          }
          if (maxValue < value) {
            maxValue = value;
            maxIndex = i;
          }
        }
        int[] arr = new int[]{0, minIndex, maxIndex, rowWindow.windowSize() - 1};
        // avoid duplicated index
        Arrays.sort(arr);
        Row row = rowWindow.getRow(0);
        collector.putFloat(row.getTime(), row.getFloat(0));
        for (int i = 1; i < 4; i++) {
          if (arr[i] > arr[i - 1]) {
            row = rowWindow.getRow(arr[i]);
            collector.putFloat(row.getTime(), row.getFloat(0));
          }
        }
        break;
      }
      case DOUBLE: {
        double maxValue = rowWindow.getRow(0).getDouble(0);
        double minValue = rowWindow.getRow(0).getDouble(0);
        for (int i = 1; i < rowWindow.windowSize(); i++) {
          double value = rowWindow.getRow(i).getDouble(0);
          if (minValue > value) {
            minValue = value;
            minIndex = i;
          }
          if (maxValue < value) {
            maxValue = value;
            maxIndex = i;
          }
        }
        int[] arr = new int[]{0, minIndex, maxIndex, rowWindow.windowSize() - 1};
        // avoid duplicated index
        Arrays.sort(arr);
        Row row = rowWindow.getRow(0);
        collector.putDouble(row.getTime(), row.getDouble(0));
        for (int i = 1; i < 4; i++) {
          if (arr[i] > arr[i - 1]) {
            row = rowWindow.getRow(arr[i]);
            collector.putDouble(row.getTime(), row.getDouble(0));
          }
        }
        break;
      }
      default:
        // This will not happen
        throw new UDFInputSeriesDataTypeNotValidException(
            0,
            outputDataType,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE);
    }
  }

  public void aggregationSample(RowWindow rowWindow, PointCollector collector)
      throws IOException, UDFInputSeriesDataTypeNotValidException {
    long time = rowWindow.getRow(0).getTime();
    int windowSize = rowWindow.windowSize();
    if ("avg".equals(aggMethodType)) {
      double sum = 0;
      switch (outputDataType) {
        case INT32:{
          for (int i = 0; i < windowSize; i++) {
            sum += rowWindow.getRow(i).getInt(0) * 1.0 / windowSize;
          }
          break;
        }
        case INT64:{
          for (int i = 0; i < windowSize; i++) {
            sum += rowWindow.getRow(i).getLong(0) * 1.0 / windowSize;
          }
          break;
        }
        case FLOAT:{
          for (int i = 0; i < windowSize; i++) {
            sum += rowWindow.getRow(i).getFloat(0) / windowSize;
          }
          break;
        }
        case DOUBLE:{
          for (int i = 0; i < windowSize; i++) {
            sum += rowWindow.getRow(i).getDouble(0) / windowSize;
          }
          break;
        }
        default:
          // This will not happen
          throw new UDFInputSeriesDataTypeNotValidException(
              0,
              outputDataType,
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.FLOAT,
              TSDataType.DOUBLE);
      }
      collector.putDouble(time, sum);
    } else if ("max".equals(aggMethodType)) {
      switch (outputDataType) {
        case INT32:{
          int maxValue = rowWindow.getRow(0).getInt(0);
          for (int i = 1; i < windowSize; i++) {
            int value = rowWindow.getRow(i).getInt(0);
            if (maxValue < value) {
              maxValue = value;
            }
          }
          collector.putInt(time, maxValue);
          break;
        }
        case INT64:{
          long maxValue = rowWindow.getRow(0).getLong(0);
          for (int i = 1; i < windowSize; i++) {
            long value = rowWindow.getRow(i).getLong(0);
            if (maxValue < value) {
              maxValue = value;
            }
          }
          collector.putLong(time, maxValue);
          break;
        }
        case FLOAT:{
          float maxValue = rowWindow.getRow(0).getFloat(0);
          for (int i = 1; i < windowSize; i++) {
            float value = rowWindow.getRow(i).getFloat(0);
            if (maxValue < value) {
              maxValue = value;
            }
          }
          collector.putFloat(time, maxValue);
          break;
        }
        case DOUBLE:{
          double maxValue = rowWindow.getRow(0).getDouble(0);
          for (int i = 1; i < windowSize; i++) {
            double value = rowWindow.getRow(i).getDouble(0);
            if (maxValue < value) {
              maxValue = value;
            }
          }
          collector.putDouble(time, maxValue);
          break;
        }
        default:
          // This will not happen
          throw new UDFInputSeriesDataTypeNotValidException(
              0,
              outputDataType,
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.FLOAT,
              TSDataType.DOUBLE);
      }
    } else if ("min".equals(aggMethodType)) {
      switch (outputDataType) {
        case INT32:{
          int minValue = rowWindow.getRow(0).getInt(0);
          for (int i = 1; i < windowSize; i++) {
            int value = rowWindow.getRow(i).getInt(0);
            if (minValue > value) {
              minValue = value;
            }
          }
          collector.putInt(time, minValue);
          break;
        }
        case INT64:{
          long minValue = rowWindow.getRow(0).getLong(0);
          for (int i = 1; i < windowSize; i++) {
            long value = rowWindow.getRow(i).getLong(0);
            if (minValue > value) {
              minValue = value;
            }
          }
          collector.putLong(time, minValue);
          break;
        }
        case FLOAT:{
          float minValue = rowWindow.getRow(0).getFloat(0);
          for (int i = 1; i < windowSize; i++) {
            float value = rowWindow.getRow(i).getFloat(0);
            if (minValue > value) {
              minValue = value;
            }
          }
          collector.putFloat(time, minValue);
          break;
        }
        case DOUBLE:{
          double minValue = rowWindow.getRow(0).getDouble(0);
          for (int i = 1; i < windowSize; i++) {
            double value = rowWindow.getRow(i).getDouble(0);
            if (minValue > value) {
              minValue = value;
            }
          }
          collector.putDouble(time, minValue);
          break;
        }
        default:
          // This will not happen
          throw new UDFInputSeriesDataTypeNotValidException(
              0,
              outputDataType,
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.FLOAT,
              TSDataType.DOUBLE);
      }
    }
  }

  static class Triplet implements Comparable<Triplet> {

    public int left;
    public double middle;
    public int right;

    public Triplet(int l, double m, int r) {
      left = l;
      middle = m;
      right = r;
    }

    @Override
    public int compareTo(Triplet o) {
      return 0;
    }
  }
}
