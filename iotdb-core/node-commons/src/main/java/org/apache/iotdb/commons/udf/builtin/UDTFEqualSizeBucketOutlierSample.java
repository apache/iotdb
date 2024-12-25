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
import org.apache.iotdb.udf.api.access.Row;
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

import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

public class UDTFEqualSizeBucketOutlierSample extends UDTFEqualSizeBucketSample {

  private String type;
  private int number;
  private OutlierSampler outlierSampler;

  private interface OutlierSampler {

    void outlierSampleInt(RowWindow rowWindow, PointCollector collector) throws IOException;

    void outlierSampleLong(RowWindow rowWindow, PointCollector collector) throws IOException;

    void outlierSampleFloat(RowWindow rowWindow, PointCollector collector) throws IOException;

    void outlierSampleDouble(RowWindow rowWindow, PointCollector collector) throws IOException;
  }

  private class AvgOutlierSampler implements OutlierSampler {

    @Override
    public void outlierSampleInt(RowWindow rowWindow, PointCollector collector) throws IOException {
      int windowSize = rowWindow.windowSize();
      if (windowSize <= number) {
        for (int i = 0; i < windowSize; i++) {
          Row row = rowWindow.getRow(i);
          collector.putInt(row.getTime(), row.getInt(0));
        }
        return;
      }

      double avg = 0;
      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));
      for (int i = 0; i < windowSize; i++) {
        avg += rowWindow.getRow(i).getInt(0);
      }
      avg /= windowSize;
      for (int i = 0; i < windowSize; i++) {
        double value = Math.abs(rowWindow.getRow(i).getInt(0) - avg);
        addToMinHeap(pq, i, value);
      }

      putPQValueInt(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleLong(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (windowSize <= number) {
        for (int i = 0; i < windowSize; i++) {
          Row row = rowWindow.getRow(i);
          collector.putLong(row.getTime(), row.getLong(0));
        }
        return;
      }

      double avg = 0;
      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));
      for (int i = 0; i < windowSize; i++) {
        avg += rowWindow.getRow(i).getLong(0);
      }
      avg /= windowSize;
      for (int i = 0; i < windowSize; i++) {
        double value = Math.abs(rowWindow.getRow(i).getLong(0) - avg);
        addToMinHeap(pq, i, value);
      }

      putPQValueLong(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleFloat(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (windowSize <= number) {
        for (int i = 0; i < windowSize; i++) {
          Row row = rowWindow.getRow(i);
          collector.putFloat(row.getTime(), row.getFloat(0));
        }
        return;
      }

      double avg = 0;
      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));
      for (int i = 0; i < windowSize; i++) {
        avg += rowWindow.getRow(i).getFloat(0);
      }
      avg /= windowSize;
      for (int i = 0; i < windowSize; i++) {
        double value = Math.abs(rowWindow.getRow(i).getFloat(0) - avg);
        addToMinHeap(pq, i, value);
      }

      putPQValueFloat(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleDouble(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (windowSize <= number) {
        for (int i = 0; i < windowSize; i++) {
          Row row = rowWindow.getRow(i);
          collector.putDouble(row.getTime(), row.getDouble(0));
        }
        return;
      }

      double avg = 0;
      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));
      for (int i = 0; i < windowSize; i++) {
        avg += rowWindow.getRow(i).getDouble(0);
      }
      avg /= windowSize;
      for (int i = 0; i < windowSize; i++) {
        double value = Math.abs(rowWindow.getRow(i).getDouble(0) - avg);
        addToMinHeap(pq, i, value);
      }

      putPQValueDouble(pq, rowWindow, collector);
    }
  }

  private class StendisOutlierSampler implements OutlierSampler {

    @Override
    public void outlierSampleInt(RowWindow rowWindow, PointCollector collector) throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallInt(rowWindow, collector, windowSize)) {
        return;
      }

      long row0x = rowWindow.getRow(0).getTime(),
          row1x = rowWindow.getRow(windowSize - 1).getTime();
      int row0y = rowWindow.getRow(0).getInt(0), row1y = rowWindow.getRow(windowSize - 1).getInt(0);

      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));

      double A = (double) row0y - row1y;
      double B = (double) row1x - row0x;
      double C = (double) row0x * row1y - row1x * row0y;
      double denominator = Math.sqrt(A * A + B * B);

      for (int i = 1; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        double value = Math.abs(A * row.getTime() + B * row.getInt(0) + C) / denominator;
        addToMinHeap(pq, i, value);
      }

      putPQValueInt(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleLong(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallLong(rowWindow, collector, windowSize)) {
        return;
      }

      long row0x = rowWindow.getRow(0).getTime(),
          row1x = rowWindow.getRow(windowSize - 1).getTime();
      long row0y = rowWindow.getRow(0).getLong(0),
          row1y = rowWindow.getRow(windowSize - 1).getLong(0);

      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));

      double A = (double) row0y - row1y;
      double B = (double) row1x - row0x;
      double C = (double) row0x * row1y - row1x * row0y;
      double denominator = Math.sqrt(A * A + B * B);

      for (int i = 1; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        double value = Math.abs(A * row.getTime() + B * row.getLong(0) + C) / denominator;
        addToMinHeap(pq, i, value);
      }

      putPQValueLong(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleFloat(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallFloat(rowWindow, collector, windowSize)) {
        return;
      }

      long row0x = rowWindow.getRow(0).getTime(),
          row1x = rowWindow.getRow(windowSize - 1).getTime();
      float row0y = rowWindow.getRow(0).getFloat(0),
          row1y = rowWindow.getRow(windowSize - 1).getFloat(0);

      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));

      double A = (double) row0y - row1y;
      double B = (double) row1x - row0x;
      double C = (double) row0x * row1y - row1x * row0y;
      double denominator = Math.sqrt(A * A + B * B);

      for (int i = 1; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        double value = Math.abs(A * row.getTime() + B * row.getFloat(0) + C) / denominator;
        addToMinHeap(pq, i, value);
      }

      putPQValueFloat(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleDouble(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallDouble(rowWindow, collector, windowSize)) {
        return;
      }

      long row0x = rowWindow.getRow(0).getTime(),
          row1x = rowWindow.getRow(windowSize - 1).getTime();
      double row0y = rowWindow.getRow(0).getDouble(0),
          row1y = rowWindow.getRow(windowSize - 1).getDouble(0);

      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));

      double A = row0y - row1y;
      double B = (double) row1x - row0x;
      double C = row0x * row1y - row1x * row0y;
      double denominator = Math.sqrt(A * A + B * B);

      for (int i = 1; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        double value = Math.abs(A * row.getTime() + B * row.getDouble(0) + C) / denominator;
        addToMinHeap(pq, i, value);
      }

      putPQValueDouble(pq, rowWindow, collector);
    }
  }

  private class CosOutlierSampler implements OutlierSampler {

    @Override
    public void outlierSampleInt(RowWindow rowWindow, PointCollector collector) throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallInt(rowWindow, collector, windowSize)) {
        return;
      }

      // o -> -o.right, max heap
      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> -o.right));

      long lastTime, currentTime, nextTime, x1, x2;
      int lastValue, currentValue, nextValue, y1, y2;
      double value;

      for (int i = 1; i < windowSize - 1; i++) {
        lastTime = rowWindow.getRow(i - 1).getTime();
        currentTime = rowWindow.getRow(i).getTime();
        nextTime = rowWindow.getRow(i + 1).getTime();

        lastValue = rowWindow.getRow(i - 1).getInt(0);
        currentValue = rowWindow.getRow(i).getInt(0);
        nextValue = rowWindow.getRow(i + 1).getInt(0);

        x1 = currentTime - lastTime;
        x2 = nextTime - currentTime;
        y1 = currentValue - lastValue;
        y2 = nextValue - currentValue;

        value =
            (x1 * x2 + y1 * y2)
                / (Math.sqrt((double) x1 * x1 + y1 * y1) * Math.sqrt((double) x2 * x2 + y2 * y2));

        addToMaxHeap(pq, i, value);
      }

      putPQValueInt(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleLong(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallLong(rowWindow, collector, windowSize)) {
        return;
      }

      // o -> -o.right, max heap
      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> -o.right));

      long lastTime, currentTime, nextTime, x1, x2;
      long lastValue, currentValue, nextValue, y1, y2;
      double value;

      for (int i = 1; i < windowSize - 1; i++) {
        lastTime = rowWindow.getRow(i - 1).getTime();
        currentTime = rowWindow.getRow(i).getTime();
        nextTime = rowWindow.getRow(i + 1).getTime();

        lastValue = rowWindow.getRow(i - 1).getLong(0);
        currentValue = rowWindow.getRow(i).getLong(0);
        nextValue = rowWindow.getRow(i + 1).getLong(0);

        x1 = currentTime - lastTime;
        x2 = nextTime - currentTime;
        y1 = currentValue - lastValue;
        y2 = nextValue - currentValue;

        value =
            (x1 * x2 + y1 * y2)
                / (Math.sqrt((double) x1 * x1 + y1 * y1) * Math.sqrt((double) x2 * x2 + y2 * y2));

        addToMaxHeap(pq, i, value);
      }

      putPQValueLong(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleFloat(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallFloat(rowWindow, collector, windowSize)) {
        return;
      }

      // o -> -o.right, max heap
      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> -o.right));

      long lastTime, currentTime, nextTime, x1, x2;
      float lastValue, currentValue, nextValue, y1, y2;
      double value;

      for (int i = 1; i < windowSize - 1; i++) {
        lastTime = rowWindow.getRow(i - 1).getTime();
        currentTime = rowWindow.getRow(i).getTime();
        nextTime = rowWindow.getRow(i + 1).getTime();

        lastValue = rowWindow.getRow(i - 1).getFloat(0);
        currentValue = rowWindow.getRow(i).getFloat(0);
        nextValue = rowWindow.getRow(i + 1).getFloat(0);

        x1 = currentTime - lastTime;
        x2 = nextTime - currentTime;
        y1 = currentValue - lastValue;
        y2 = nextValue - currentValue;

        value = (x1 * x2 + y1 * y2) / (Math.sqrt(x1 * x1 + y1 * y1) * Math.sqrt(x2 * x2 + y2 * y2));

        addToMaxHeap(pq, i, value);
      }

      putPQValueFloat(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleDouble(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallDouble(rowWindow, collector, windowSize)) {
        return;
      }

      // o -> -o.right, max heap
      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> -o.right));

      long lastTime, currentTime, nextTime, x1, x2;
      double lastValue, currentValue, nextValue, y1, y2;
      double value;

      for (int i = 1; i < windowSize - 1; i++) {
        lastTime = rowWindow.getRow(i - 1).getTime();
        currentTime = rowWindow.getRow(i).getTime();
        nextTime = rowWindow.getRow(i + 1).getTime();

        lastValue = rowWindow.getRow(i - 1).getDouble(0);
        currentValue = rowWindow.getRow(i).getDouble(0);
        nextValue = rowWindow.getRow(i + 1).getDouble(0);

        x1 = currentTime - lastTime;
        x2 = nextTime - currentTime;
        y1 = currentValue - lastValue;
        y2 = nextValue - currentValue;

        value = (x1 * x2 + y1 * y2) / (Math.sqrt(x1 * x1 + y1 * y1) * Math.sqrt(x2 * x2 + y2 * y2));

        addToMaxHeap(pq, i, value);
      }

      putPQValueDouble(pq, rowWindow, collector);
    }
  }

  private class PrenextdisOutlierSampler implements OutlierSampler {

    @Override
    public void outlierSampleInt(RowWindow rowWindow, PointCollector collector) throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallInt(rowWindow, collector, windowSize)) {
        return;
      }

      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));

      long lastTime, currentTime, nextTime, x1, x2;
      int lastValue, currentValue, nextValue, y1, y2;
      double value;

      for (int i = 1; i < windowSize - 1; i++) {
        lastTime = rowWindow.getRow(i - 1).getTime();
        currentTime = rowWindow.getRow(i).getTime();
        nextTime = rowWindow.getRow(i + 1).getTime();

        lastValue = rowWindow.getRow(i - 1).getInt(0);
        currentValue = rowWindow.getRow(i).getInt(0);
        nextValue = rowWindow.getRow(i + 1).getInt(0);

        x1 = Math.abs(currentTime - lastTime);
        x2 = Math.abs(nextTime - currentTime);
        y1 = Math.abs(currentValue - lastValue);
        y2 = Math.abs(nextValue - currentValue);

        value = (double) x1 + y1 + x2 + y2;

        addToMinHeap(pq, i, value);
      }

      putPQValueInt(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleLong(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallLong(rowWindow, collector, windowSize)) {
        return;
      }

      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));

      long lastTime, currentTime, nextTime, x1, x2;
      long lastValue, currentValue, nextValue, y1, y2;
      double value;

      for (int i = 1; i < windowSize - 1; i++) {
        lastTime = rowWindow.getRow(i - 1).getTime();
        currentTime = rowWindow.getRow(i).getTime();
        nextTime = rowWindow.getRow(i + 1).getTime();

        lastValue = rowWindow.getRow(i - 1).getLong(0);
        currentValue = rowWindow.getRow(i).getLong(0);
        nextValue = rowWindow.getRow(i + 1).getLong(0);

        x1 = Math.abs(currentTime - lastTime);
        x2 = Math.abs(nextTime - currentTime);
        y1 = Math.abs(currentValue - lastValue);
        y2 = Math.abs(nextValue - currentValue);

        value = (double) x1 + y1 + x2 + y2;

        addToMinHeap(pq, i, value);
      }

      putPQValueLong(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleFloat(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallFloat(rowWindow, collector, windowSize)) {
        return;
      }

      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));

      long lastTime, currentTime, nextTime, x1, x2;
      float lastValue, currentValue, nextValue, y1, y2;
      double value;

      for (int i = 1; i < windowSize - 1; i++) {
        lastTime = rowWindow.getRow(i - 1).getTime();
        currentTime = rowWindow.getRow(i).getTime();
        nextTime = rowWindow.getRow(i + 1).getTime();

        lastValue = rowWindow.getRow(i - 1).getFloat(0);
        currentValue = rowWindow.getRow(i).getFloat(0);
        nextValue = rowWindow.getRow(i + 1).getFloat(0);

        x1 = Math.abs(currentTime - lastTime);
        x2 = Math.abs(nextTime - currentTime);
        y1 = Math.abs(currentValue - lastValue);
        y2 = Math.abs(nextValue - currentValue);

        value = x1 + y1 + x2 + y2;

        addToMinHeap(pq, i, value);
      }

      putPQValueFloat(pq, rowWindow, collector);
    }

    @Override
    public void outlierSampleDouble(RowWindow rowWindow, PointCollector collector)
        throws IOException {
      int windowSize = rowWindow.windowSize();
      if (isWindowSizeTooSmallDouble(rowWindow, collector, windowSize)) {
        return;
      }

      PriorityQueue<Pair<Integer, Double>> pq =
          new PriorityQueue<>(number, Comparator.comparing(o -> o.right));

      long lastTime, currentTime, nextTime, x1, x2;
      double lastValue, currentValue, nextValue, y1, y2;
      double value;

      for (int i = 1; i < windowSize - 1; i++) {
        lastTime = rowWindow.getRow(i - 1).getTime();
        currentTime = rowWindow.getRow(i).getTime();
        nextTime = rowWindow.getRow(i + 1).getTime();

        lastValue = rowWindow.getRow(i - 1).getDouble(0);
        currentValue = rowWindow.getRow(i).getDouble(0);
        nextValue = rowWindow.getRow(i + 1).getDouble(0);

        x1 = Math.abs(currentTime - lastTime);
        x2 = Math.abs(nextTime - currentTime);
        y1 = Math.abs(currentValue - lastValue);
        y2 = Math.abs(nextValue - currentValue);

        value = x1 + y1 + x2 + y2;

        addToMinHeap(pq, i, value);
      }

      putPQValueDouble(pq, rowWindow, collector);
    }
  }

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException, MetadataException {
    super.validate(validator);
    type = validator.getParameters().getStringOrDefault("type", "avg").toLowerCase();
    number = validator.getParameters().getIntOrDefault("number", 3);
    validator
        .validate(
            type ->
                "avg".equals(type)
                    || "stendis".equals(type)
                    || "cos".equals(type)
                    || "prenextdis".equals(type),
            "Illegal outlier method. Outlier type should be avg, stendis, cos or prenextdis.",
            type)
        .validate(
            number -> (int) number > 0, "Illegal number. Number should be greater than 0.", number);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    bucketSize *= number;
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(bucketSize))
        .setOutputDataType(UDFDataTypeTransformer.transformToUDFDataType(dataType));
    switch (type) {
      case "avg":
        outlierSampler = new AvgOutlierSampler();
        break;
      case "stendis":
        outlierSampler = new StendisOutlierSampler();
        break;
      case "cos":
        outlierSampler = new CosOutlierSampler();
        break;
      case "prenextdis":
        outlierSampler = new PrenextdisOutlierSampler();
        break;
      default:
        throw new UDFParameterNotValidException(
            "Illegal outlier method. Outlier type should be avg, stendis, cos or prenextdis.");
    }
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector)
      throws IOException, UDFParameterNotValidException {
    switch (dataType) {
      case INT32:
        outlierSampler.outlierSampleInt(rowWindow, collector);
        break;
      case INT64:
        outlierSampler.outlierSampleLong(rowWindow, collector);
        break;
      case FLOAT:
        outlierSampler.outlierSampleFloat(rowWindow, collector);
        break;
      case DOUBLE:
        outlierSampler.outlierSampleDouble(rowWindow, collector);
        break;
      case TEXT:
      case BLOB:
      case DATE:
      case STRING:
      case BOOLEAN:
      case TIMESTAMP:
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

  public void addToMinHeap(PriorityQueue<Pair<Integer, Double>> pq, int i, double value) {
    if (pq.size() < number) {
      pq.add(new Pair<>(i, value));
    } else if (value > pq.peek().right) {
      pq.poll();
      pq.add(new Pair<>(i, value));
    }
  }

  public void addToMaxHeap(PriorityQueue<Pair<Integer, Double>> pq, int i, double value) {
    if (pq.size() < number) {
      pq.add(new Pair<>(i, value));
    } else if (value < pq.peek().right) {
      pq.poll();
      pq.add(new Pair<>(i, value));
    }
  }

  public void putPQValueInt(
      PriorityQueue<Pair<Integer, Double>> pq, RowWindow rowWindow, PointCollector collector)
      throws IOException {
    int[] arr = new int[number];
    for (int i = 0; i < number; i++) {
      arr[i] = pq.peek().left;
      pq.poll();
    }
    Arrays.sort(arr);
    for (int i = 0; i < number; i++) {
      collector.putInt(rowWindow.getRow(arr[i]).getTime(), rowWindow.getRow(arr[i]).getInt(0));
    }
  }

  public void putPQValueLong(
      PriorityQueue<Pair<Integer, Double>> pq, RowWindow rowWindow, PointCollector collector)
      throws IOException {
    int[] arr = new int[number];
    for (int i = 0; i < number; i++) {
      arr[i] = pq.peek().left;
      pq.poll();
    }
    Arrays.sort(arr);
    for (int i = 0; i < number; i++) {
      collector.putLong(rowWindow.getRow(arr[i]).getTime(), rowWindow.getRow(arr[i]).getLong(0));
    }
  }

  public void putPQValueFloat(
      PriorityQueue<Pair<Integer, Double>> pq, RowWindow rowWindow, PointCollector collector)
      throws IOException {
    int[] arr = new int[number];
    for (int i = 0; i < number; i++) {
      arr[i] = pq.peek().left;
      pq.poll();
    }
    Arrays.sort(arr);
    for (int i = 0; i < number; i++) {
      collector.putFloat(rowWindow.getRow(arr[i]).getTime(), rowWindow.getRow(arr[i]).getFloat(0));
    }
  }

  public void putPQValueDouble(
      PriorityQueue<Pair<Integer, Double>> pq, RowWindow rowWindow, PointCollector collector)
      throws IOException {
    int[] arr = new int[number];
    for (int i = 0; i < number; i++) {
      arr[i] = pq.peek().left;
      pq.poll();
    }
    Arrays.sort(arr);
    for (int i = 0; i < number; i++) {
      collector.putDouble(
          rowWindow.getRow(arr[i]).getTime(), rowWindow.getRow(arr[i]).getDouble(0));
    }
  }

  public boolean isWindowSizeTooSmallInt(
      RowWindow rowWindow, PointCollector collector, int windowSize) throws IOException {
    if (windowSize <= number) {
      for (int i = 0; i < windowSize; i++) {
        Row row = rowWindow.getRow(i);
        collector.putInt(row.getTime(), row.getInt(0));
      }
      return true;
    } else if (windowSize == number + 1) {
      for (int i = 0; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        collector.putInt(row.getTime(), row.getInt(0));
      }
      return true;
    } else if (windowSize == number + 2) {
      for (int i = 1; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        collector.putInt(row.getTime(), row.getInt(0));
      }
      return true;
    }
    return false;
  }

  public boolean isWindowSizeTooSmallLong(
      RowWindow rowWindow, PointCollector collector, int windowSize) throws IOException {
    if (windowSize <= number) {
      for (int i = 0; i < windowSize; i++) {
        Row row = rowWindow.getRow(i);
        collector.putLong(row.getTime(), row.getLong(0));
      }
      return true;
    } else if (windowSize == number + 1) {
      for (int i = 0; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        collector.putLong(row.getTime(), row.getLong(0));
      }
      return true;
    } else if (windowSize == number + 2) {
      for (int i = 1; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        collector.putLong(row.getTime(), row.getLong(0));
      }
      return true;
    }
    return false;
  }

  public boolean isWindowSizeTooSmallFloat(
      RowWindow rowWindow, PointCollector collector, int windowSize) throws IOException {
    if (windowSize <= number) {
      for (int i = 0; i < windowSize; i++) {
        Row row = rowWindow.getRow(i);
        collector.putFloat(row.getTime(), row.getFloat(0));
      }
      return true;
    } else if (windowSize == number + 1) {
      for (int i = 0; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        collector.putFloat(row.getTime(), row.getFloat(0));
      }
      return true;
    } else if (windowSize == number + 2) {
      for (int i = 1; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        collector.putFloat(row.getTime(), row.getFloat(0));
      }
      return true;
    }
    return false;
  }

  public boolean isWindowSizeTooSmallDouble(
      RowWindow rowWindow, PointCollector collector, int windowSize) throws IOException {
    if (windowSize <= number) {
      for (int i = 0; i < windowSize; i++) {
        Row row = rowWindow.getRow(i);
        collector.putDouble(row.getTime(), row.getDouble(0));
      }
      return true;
    } else if (windowSize == number + 1) {
      for (int i = 0; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        collector.putDouble(row.getTime(), row.getDouble(0));
      }
      return true;
    } else if (windowSize == number + 2) {
      for (int i = 1; i < windowSize - 1; i++) {
        Row row = rowWindow.getRow(i);
        collector.putDouble(row.getTime(), row.getDouble(0));
      }
      return true;
    }
    return false;
  }
}
