/**
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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * Statistics for double type.
 */
public class DoubleStatistics extends Statistics<Double> {

  private double min;
  private double max;
  private double first;
  private double last;
  private double sum;

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    min = BytesUtils.bytesToDouble(minBytes);
    max = BytesUtils.bytesToDouble(maxBytes);
  }

  @Override
  public void updateStats(double value) {
    if (this.isEmpty) {
      initializeStats(value, value, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, value, value, value, value);
    }
  }

  @Override
  public void updateStats(double[] values) {
    for (double value : values) {
      if (this.isEmpty) {
        initializeStats(value, value, value, value, value);
        isEmpty = false;
      } else {
        updateStats(value, value, value, value, value);
      }
    }
  }

  private void updateStats(double minValue, double maxValue, double firstValue, double lastValue,
      double sumValue) {
    if (minValue < min) {
      min = minValue;
    }
    if (maxValue > max) {
      max = maxValue;
    }
    sum += sumValue;
    this.last = lastValue;
  }

  @Override
  public Double getMin() {
    return min;
  }

  @Override
  public Double getMax() {
    return max;
  }

  @Override
  public Double getFirst() {
    return first;
  }

  @Override
  public Double getLast() {
    return last;
  }

  @Override
  public double getSum() {
    return sum;
  }

  @Override
  protected void mergeStatisticsValue(Statistics<?> stats) {
    DoubleStatistics doubleStats = (DoubleStatistics) stats;
    if (this.isEmpty) {
      initializeStats(doubleStats.getMin(), doubleStats.getMax(), doubleStats.getFirst(),
          doubleStats.getLast(), doubleStats.getSum());
      isEmpty = false;
    } else {
      updateStats(doubleStats.getMin(), doubleStats.getMax(), doubleStats.getFirst(),
          doubleStats.getLast(), doubleStats.getSum());
    }

  }

  /**
   * initialize double statistics.
   *
   * @param min min value
   * @param max max value
   * @param first the first value
   * @param last the last value
   * @param sum sum value
   */
  private void initializeStats(double min, double max, double first, double last, double sum) {
    this.min = min;
    this.max = max;
    this.first = first;
    this.last = last;
    this.sum = sum;
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.doubleToBytes(min);
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.doubleToBytes(max);
  }

  @Override
  public byte[] getFirstBytes() {
    return BytesUtils.doubleToBytes(first);
  }

  @Override
  public byte[] getLastBytes() {
    return BytesUtils.doubleToBytes(last);
  }

  @Override
  public byte[] getSumBytes() {
    return BytesUtils.doubleToBytes(sum);
  }

  @Override
  public ByteBuffer getMinBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(min);
  }

  @Override
  public ByteBuffer getMaxBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(max);
  }

  @Override
  public ByteBuffer getFirstBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(first);
  }

  @Override
  public ByteBuffer getLastBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(last);
  }

  @Override
  public ByteBuffer getSumBytebuffer() {
    return ReadWriteIOUtils.getByteBuffer(sum);
  }

  @Override
  public int sizeOfDatum() {
    return 8;
  }

  @Override
  public String toString() {
    return "[min:" + min + ",max:" + max + ",first:" + first + ",last:" + last + ",sum:" + sum
        + "]";
  }

  @Override
  void fill(InputStream inputStream) throws IOException {
    this.min = ReadWriteIOUtils.readDouble(inputStream);
    this.max = ReadWriteIOUtils.readDouble(inputStream);
    this.first = ReadWriteIOUtils.readDouble(inputStream);
    this.last = ReadWriteIOUtils.readDouble(inputStream);
    this.sum = ReadWriteIOUtils.readDouble(inputStream);
  }

  @Override
  void fill(ByteBuffer byteBuffer) throws IOException {
    this.min = ReadWriteIOUtils.readDouble(byteBuffer);
    this.max = ReadWriteIOUtils.readDouble(byteBuffer);
    this.first = ReadWriteIOUtils.readDouble(byteBuffer);
    this.last = ReadWriteIOUtils.readDouble(byteBuffer);
    this.sum = ReadWriteIOUtils.readDouble(byteBuffer);
  }
}
