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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DoubleStatistics extends Statistics<Double> {

  private double minValue;
  private double maxValue;
  private double firstValue;
  private double lastValue;
  private double sumValue;

  static final int DOUBLE_STATISTICS_FIXED_RAM_SIZE = 80;

  @Override
  public TSDataType getType() {
    return TSDataType.DOUBLE;
  }

  /**
   * The output of this method should be identical to the method "serializeStats(OutputStream
   * outputStream)"
   */
  @Override
  public int getStatsSize() {
    return 40;
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
  public void initializeStats(double min, double max, double first, double last, double sum) {
    this.minValue = min;
    this.maxValue = max;
    this.firstValue = first;
    this.lastValue = last;
    this.sumValue = sum;
  }

  private void updateStats(double minValue, double maxValue, double lastValue, double sumValue) {
    if (minValue < this.minValue) {
      this.minValue = minValue;
    }
    if (maxValue > this.maxValue) {
      this.maxValue = maxValue;
    }
    this.sumValue += sumValue;
    this.lastValue = lastValue;
  }

  private void updateStats(
      double minValue,
      double maxValue,
      double firstValue,
      double lastValue,
      double sumValue,
      long startTime,
      long endTime) {
    if (minValue < this.minValue) {
      this.minValue = minValue;
    }
    if (maxValue > this.maxValue) {
      this.maxValue = maxValue;
    }
    this.sumValue += sumValue;
    // only if endTime greater or equals to the current endTime need we update the last value
    // only if startTime less or equals to the current startTime need we update the first value
    // otherwise, just ignore
    if (startTime <= this.getStartTime()) {
      this.firstValue = firstValue;
    }
    if (endTime >= this.getEndTime()) {
      this.lastValue = lastValue;
    }
  }

  @Override
  void updateStats(double value) {
    if (this.isEmpty) {
      initializeStats(value, value, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, value, value, value);
    }
  }

  @Override
  void updateStats(double[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i]);
    }
  }

  @Override
  public long calculateRamSize() {
    return DOUBLE_STATISTICS_FIXED_RAM_SIZE;
  }

  @Override
  public Double getMinValue() {
    return minValue;
  }

  @Override
  public Double getMaxValue() {
    return maxValue;
  }

  @Override
  public Double getFirstValue() {
    return firstValue;
  }

  @Override
  public Double getLastValue() {
    return lastValue;
  }

  @Override
  public double getSumDoubleValue() {
    return sumValue;
  }

  @Override
  public long getSumLongValue() {
    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.DOUBLE, "long sum"));
  }

  @Override
  public Double getCurrentValue() {
    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.DOUBLE, "current"));
  }

  @Override
  protected void mergeStatisticsValue(Statistics<Double> stats) {
    DoubleStatistics doubleStats = (DoubleStatistics) stats;
    if (this.isEmpty) {
      initializeStats(
          doubleStats.getMinValue(),
          doubleStats.getMaxValue(),
          doubleStats.getFirstValue(),
          doubleStats.getLastValue(),
          doubleStats.sumValue);
      isEmpty = false;
    } else {
      updateStats(
          doubleStats.getMinValue(),
          doubleStats.getMaxValue(),
          doubleStats.getFirstValue(),
          doubleStats.getLastValue(),
          doubleStats.sumValue,
          stats.getStartTime(),
          stats.getEndTime());
    }
  }

  @Override
  public int serializeStats(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(minValue, outputStream);
    byteLen += ReadWriteIOUtils.write(maxValue, outputStream);
    byteLen += ReadWriteIOUtils.write(firstValue, outputStream);
    byteLen += ReadWriteIOUtils.write(lastValue, outputStream);
    byteLen += ReadWriteIOUtils.write(sumValue, outputStream);
    return byteLen;
  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {
    this.minValue = ReadWriteIOUtils.readDouble(inputStream);
    this.maxValue = ReadWriteIOUtils.readDouble(inputStream);
    this.firstValue = ReadWriteIOUtils.readDouble(inputStream);
    this.lastValue = ReadWriteIOUtils.readDouble(inputStream);
    this.sumValue = ReadWriteIOUtils.readDouble(inputStream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    this.minValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.maxValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.firstValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.sumValue = ReadWriteIOUtils.readDouble(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    double e = 0.00001;
    DoubleStatistics that = (DoubleStatistics) o;
    return Math.abs(that.minValue - minValue) < e
        && Math.abs(that.maxValue - maxValue) < e
        && Math.abs(that.firstValue - firstValue) < e
        && Math.abs(that.lastValue - lastValue) < e
        && Math.abs(that.sumValue - sumValue) < e;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), minValue, maxValue, firstValue, lastValue, sumValue);
  }

  @Override
  public String toString() {
    return super.toString()
        + " [minValue:"
        + minValue
        + ",maxValue:"
        + maxValue
        + ",firstValue:"
        + firstValue
        + ",lastValue:"
        + lastValue
        + ",sumValue:"
        + sumValue
        + "]";
  }
}
