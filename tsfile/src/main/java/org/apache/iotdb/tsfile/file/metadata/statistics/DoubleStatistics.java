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
import java.util.HashSet;
import java.util.Set;

public class DoubleStatistics extends Statistics<Double> {

  /** @author Yuyuan Kang */
  private MinMaxInfo<Double> minInfo;

  private MinMaxInfo<Double> maxInfo;
  private double firstValue;
  private double lastValue;
  private double sumValue;
  private final TSDataType minMaxDataType = TSDataType.MIN_MAX_DOUBLE;

  static final int DOUBLE_STATISTICS_FIXED_RAM_SIZE = 80;

  /** @author Yuyuan Kang */
  public DoubleStatistics() {
    this.minInfo = new MinMaxInfo<>(Double.MAX_VALUE, new HashSet<>());
    this.maxInfo = new MinMaxInfo<>(Double.MIN_VALUE, new HashSet<>());
  }

  @Override
  public TSDataType getType() {
    return TSDataType.DOUBLE;
  }

  @Override
  public int getStatsSize() {
    int len = 0;
    // min info
    len += 8; // value of min info, double
    len += 4; //  size of bottom timestamps, int
    len += 8 * minInfo.timestamps.size(); // timestamps of min info, double(s)
    // max info
    len += 8; // value of max info, double
    len += 4; // size of top timestamps, int
    len += 8 * maxInfo.timestamps.size(); // timestamps of max info, double(s)
    len += 24; // first value, last value and sum value
    return len;
  }

  /** @author Yuyuan Kang */
  public void initializeStats(
      MinMaxInfo<Double> minInfo,
      MinMaxInfo<Double> maxInfo,
      double firstValue,
      double last,
      double sum) {
    this.minInfo = new MinMaxInfo<>(minInfo);
    this.maxInfo = new MinMaxInfo<>(maxInfo);
    this.firstValue = firstValue;
    this.lastValue = last;
    this.sumValue += sum;
  }

  /** @author Yuyuan Kang */
  public void initializeStats(
      double min,
      long bottomTimestamp,
      double max,
      long topTimestamp,
      double firstValue,
      double last,
      double sum) {
    this.minInfo = new MinMaxInfo<>(min, bottomTimestamp);
    this.maxInfo = new MinMaxInfo<>(max, topTimestamp);
    this.firstValue = firstValue;
    this.lastValue = last;
    this.sumValue += sum;
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateMinInfo(Double val, long timestamp) {
    if (val < this.minInfo.val) {
      this.minInfo.reset(val, timestamp);
    } else if (val.equals(this.minInfo.val)) {
      this.minInfo.timestamps.add(timestamp);
    }
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateMinInfo(Double val, Set<Long> timestamps) {
    if (val < this.minInfo.val) {
      this.minInfo.reset(val, timestamps);
    } else if (val.equals(this.minInfo.val)) {
      this.minInfo.timestamps.addAll(timestamps);
    }
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateMaxInfo(Double val, long timestamp) {
    if (val > this.maxInfo.val) {
      this.maxInfo.reset(val, timestamp);
    } else if (val.equals(this.maxInfo.val)) {
      this.maxInfo.timestamps.add(timestamp);
    }
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateMaxInfo(Double val, Set<Long> timestamps) {
    if (val > this.maxInfo.val) {
      this.maxInfo.reset(val, timestamps);
    } else if (val.equals(this.maxInfo.val)) {
      this.maxInfo.timestamps.addAll(timestamps);
    }
  }

  /** @author Yuyuan Kang */
  private void updateStats(
      double minValue,
      long bottomTimestamp,
      double maxValue,
      long topTimestamp,
      double lastValue,
      double sumValue) {
    updateMinInfo(minValue, bottomTimestamp);
    updateMaxInfo(maxValue, topTimestamp);
    this.sumValue += sumValue;
    this.lastValue = lastValue;
  }

  /** @author Yuyuan Kang */
  private void updateStats(
      MinMaxInfo<Double> minInfo,
      MinMaxInfo<Double> maxInfo,
      double firstValue,
      double lastValue,
      double sumValue,
      long startTime,
      long endTime) {
    updateMinInfo(minInfo.val, minInfo.timestamps);
    updateMaxInfo(maxInfo.val, maxInfo.timestamps);
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

  //  @Override
  //  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
  //    minValue = BytesUtils.bytesToDouble(minBytes);
  //    maxValue = BytesUtils.bytesToDouble(maxBytes);
  //  }

  /** @author Yuyuan Kang */
  @Override
  void updateStats(double value, long timestamp) {
    if (this.isEmpty) {
      initializeStats(value, timestamp, value, timestamp, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, timestamp, value, timestamp, value, value);
    }
  }

  /** @author Yuyuan Kang */
  @Override
  void updateStats(double[] values, long[] timestamps, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i], timestamps[i]);
    }
  }

  @Override
  public long calculateRamSize() {
    return DOUBLE_STATISTICS_FIXED_RAM_SIZE;
  }

  /** @author Yuyuan Kang */
  @Override
  public MinMaxInfo<Double> getMinInfo() {
    return minInfo;
  }

  /** @author Yuyuan Kang */
  @Override
  public MinMaxInfo<Double> getMaxInfo() {
    return maxInfo;
  }

  /** @author Yuyuan Kang */
  @Override
  public Double getMinValue() {
    return this.minInfo.val;
  }

  /** @author Yuyuan Kang */
  @Override
  public Double getMaxValue() {
    return this.maxInfo.val;
  }

  /** @author Yuyuan Kang */
  @Override
  public Set<Long> getBottomTimestamps() {
    return this.minInfo.timestamps;
  }

  /** @author Yuyuan Kang */
  @Override
  public Set<Long> getTopTimestamps() {
    return this.maxInfo.timestamps;
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
    throw new StatisticsClassException("Double statistics does not support: long sum");
  }

  /** @author Yuyuan Kang */
  @Override
  protected void mergeStatisticsValue(Statistics stats) {
    DoubleStatistics doubleStats = (DoubleStatistics) stats;
    if (this.isEmpty) {
      initializeStats(
          doubleStats.getMinInfo(),
          doubleStats.getMaxInfo(),
          doubleStats.getFirstValue(),
          doubleStats.getLastValue(),
          doubleStats.sumValue);
      isEmpty = false;
    } else {
      updateStats(
          doubleStats.getMinInfo(),
          doubleStats.getMaxInfo(),
          doubleStats.getFirstValue(),
          doubleStats.getLastValue(),
          doubleStats.sumValue,
          stats.getStartTime(),
          stats.getEndTime());
    }
  }

  //  @Override
  //  public byte[] getMinInfoBytes() {
  //    return BytesUtils.doubleToBytes(minValue);
  //  }
  //
  //  @Override
  //  public byte[] getMaxInfoBytes() {
  //    return BytesUtils.doubleToBytes(maxValue);
  //  }
  //
  //  @Override
  //  public byte[] getFirstValueBytes() {
  //    return BytesUtils.doubleToBytes(firstValue);
  //  }
  //
  //  @Override
  //  public byte[] getLastValueBytes() {
  //    return BytesUtils.doubleToBytes(lastValue);
  //  }
  //
  //  @Override
  //  public byte[] getSumValueBytes() {
  //    return BytesUtils.doubleToBytes(sumValue);
  //  }
  //
  //  @Override
  //  public ByteBuffer getMinValueBuffer() {
  //    return ReadWriteIOUtils.getByteBuffer(minValue);
  //  }
  //
  //  @Override
  //  public ByteBuffer getMaxValueBuffer() {
  //    return ReadWriteIOUtils.getByteBuffer(maxValue);
  //  }
  //
  //  @Override
  //  public ByteBuffer getFirstValueBuffer() {
  //    return ReadWriteIOUtils.getByteBuffer(firstValue);
  //  }
  //
  //  @Override
  //  public ByteBuffer getLastValueBuffer() {
  //    return ReadWriteIOUtils.getByteBuffer(lastValue);
  //  }
  //
  //  @Override
  //  public ByteBuffer getSumValueBuffer() {
  //    return ReadWriteIOUtils.getByteBuffer(sumValue);
  //  }

  /** @author Yuyuan Kang */
  @Override
  public int serializeStats(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(minInfo, minMaxDataType, outputStream);
    byteLen += ReadWriteIOUtils.write(maxInfo, minMaxDataType, outputStream);
    byteLen += ReadWriteIOUtils.write(firstValue, outputStream);
    byteLen += ReadWriteIOUtils.write(lastValue, outputStream);
    byteLen += ReadWriteIOUtils.write(sumValue, outputStream);
    return byteLen;
  }

  /** @author Yuyuan Kang */
  @Override
  public void deserialize(InputStream inputStream) throws IOException {
    this.minInfo = ReadWriteIOUtils.readMinMaxInfo(inputStream, minMaxDataType);
    this.maxInfo = ReadWriteIOUtils.readMinMaxInfo(inputStream, minMaxDataType);
    this.firstValue = ReadWriteIOUtils.readDouble(inputStream);
    this.lastValue = ReadWriteIOUtils.readDouble(inputStream);
    this.sumValue = ReadWriteIOUtils.readDouble(inputStream);
  }

  /** @author Yuyuan Kang */
  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    this.minInfo = ReadWriteIOUtils.readMinMaxInfo(byteBuffer, minMaxDataType);
    this.maxInfo = ReadWriteIOUtils.readMinMaxInfo(byteBuffer, minMaxDataType);
    this.firstValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.sumValue = ReadWriteIOUtils.readDouble(byteBuffer);
  }

  /** @author Yuyuan Kang */
  @Override
  public String toString() {
    return super.toString()
        + " [minValue:"
        + minInfo
        + ",maxValue:"
        + maxInfo
        + ",firstValue:"
        + firstValue
        + ",lastValue:"
        + lastValue
        + ",sumValue:"
        + sumValue
        + "]";
  }
}
