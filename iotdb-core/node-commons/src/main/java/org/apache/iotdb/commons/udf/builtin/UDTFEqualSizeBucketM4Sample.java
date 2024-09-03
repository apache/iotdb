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

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFInputSeriesDataTypeNotValidException;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

public class UDTFEqualSizeBucketM4Sample extends UDTFEqualSizeBucketSample {

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
    bucketSize *= 4;
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(bucketSize))
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(dataType));
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
      case TIMESTAMP:
      case BOOLEAN:
      case DATE:
      case STRING:
      case TEXT:
      case BLOB:
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
    if (rowWindow.windowSize() <= 4) {
      for (int i = 0; i < rowWindow.windowSize(); i++) {
        Row row = rowWindow.getRow(i);
        collector.putInt(row.getTime(), row.getInt(0));
      }
      return;
    }

    int minIndex = 1, maxIndex = 1;
    int maxValue = rowWindow.getRow(1).getInt(0);
    int minValue = rowWindow.getRow(1).getInt(0);
    for (int i = 2; i < rowWindow.windowSize() - 1; i++) {
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
    if (minIndex == maxIndex) {
      maxIndex = rowWindow.windowSize() - 2;
    }

    Row row = rowWindow.getRow(0);
    collector.putInt(row.getTime(), row.getInt(0));
    if (maxIndex < minIndex) {
      row = rowWindow.getRow(maxIndex);
      collector.putInt(row.getTime(), row.getInt(0));
      row = rowWindow.getRow(minIndex);
    } else {
      row = rowWindow.getRow(minIndex);
      collector.putInt(row.getTime(), row.getInt(0));
      row = rowWindow.getRow(maxIndex);
    }
    collector.putInt(row.getTime(), row.getInt(0));
    row = rowWindow.getRow(rowWindow.windowSize() - 1);
    collector.putInt(row.getTime(), row.getInt(0));
  }

  public void transformLong(RowWindow rowWindow, PointCollector collector) throws IOException {
    if (rowWindow.windowSize() <= 4) {
      for (int i = 0; i < rowWindow.windowSize(); i++) {
        Row row = rowWindow.getRow(i);
        collector.putLong(row.getTime(), row.getLong(0));
      }
      return;
    }

    int minIndex = 1, maxIndex = 1;
    long maxValue = rowWindow.getRow(1).getLong(0);
    long minValue = rowWindow.getRow(1).getLong(0);
    for (int i = 2; i < rowWindow.windowSize() - 1; i++) {
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
    if (minIndex == maxIndex) {
      maxIndex = rowWindow.windowSize() - 2;
    }

    Row row = rowWindow.getRow(0);
    collector.putLong(row.getTime(), row.getLong(0));
    if (maxIndex < minIndex) {
      row = rowWindow.getRow(maxIndex);
      collector.putLong(row.getTime(), row.getLong(0));
      row = rowWindow.getRow(minIndex);
    } else {
      row = rowWindow.getRow(minIndex);
      collector.putLong(row.getTime(), row.getLong(0));
      row = rowWindow.getRow(maxIndex);
    }
    collector.putLong(row.getTime(), row.getLong(0));
    row = rowWindow.getRow(rowWindow.windowSize() - 1);
    collector.putLong(row.getTime(), row.getLong(0));
  }

  public void transformFloat(RowWindow rowWindow, PointCollector collector) throws IOException {
    if (rowWindow.windowSize() <= 4) {
      for (int i = 0; i < rowWindow.windowSize(); i++) {
        Row row = rowWindow.getRow(i);
        collector.putFloat(row.getTime(), row.getFloat(0));
      }
      return;
    }

    int minIndex = 1, maxIndex = 1;
    float maxValue = rowWindow.getRow(1).getFloat(0);
    float minValue = rowWindow.getRow(1).getFloat(0);
    for (int i = 2; i < rowWindow.windowSize() - 1; i++) {
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
    if (minIndex == maxIndex) {
      maxIndex = rowWindow.windowSize() - 2;
    }

    Row row = rowWindow.getRow(0);
    collector.putFloat(row.getTime(), row.getFloat(0));
    if (maxIndex < minIndex) {
      row = rowWindow.getRow(maxIndex);
      collector.putFloat(row.getTime(), row.getFloat(0));
      row = rowWindow.getRow(minIndex);
    } else {
      row = rowWindow.getRow(minIndex);
      collector.putFloat(row.getTime(), row.getFloat(0));
      row = rowWindow.getRow(maxIndex);
    }
    collector.putFloat(row.getTime(), row.getFloat(0));
    row = rowWindow.getRow(rowWindow.windowSize() - 1);
    collector.putFloat(row.getTime(), row.getFloat(0));
  }

  public void transformDouble(RowWindow rowWindow, PointCollector collector) throws IOException {
    if (rowWindow.windowSize() <= 4) {
      for (int i = 0; i < rowWindow.windowSize(); i++) {
        Row row = rowWindow.getRow(i);
        collector.putDouble(row.getTime(), row.getDouble(0));
      }
      return;
    }

    int minIndex = 1, maxIndex = 1;
    double maxValue = rowWindow.getRow(1).getDouble(0);
    double minValue = rowWindow.getRow(1).getDouble(0);
    for (int i = 2; i < rowWindow.windowSize() - 1; i++) {
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
    if (minIndex == maxIndex) {
      maxIndex = rowWindow.windowSize() - 2;
    }

    Row row = rowWindow.getRow(0);
    collector.putDouble(row.getTime(), row.getDouble(0));
    if (maxIndex < minIndex) {
      row = rowWindow.getRow(maxIndex);
      collector.putDouble(row.getTime(), row.getDouble(0));
      row = rowWindow.getRow(minIndex);
    } else {
      row = rowWindow.getRow(minIndex);
      collector.putDouble(row.getTime(), row.getDouble(0));
      row = rowWindow.getRow(maxIndex);
    }
    collector.putDouble(row.getTime(), row.getDouble(0));
    row = rowWindow.getRow(rowWindow.windowSize() - 1);
    collector.putDouble(row.getTime(), row.getDouble(0));
  }
}
