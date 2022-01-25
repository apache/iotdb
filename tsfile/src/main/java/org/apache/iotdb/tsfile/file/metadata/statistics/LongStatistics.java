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

public class LongStatistics extends Statistics<Long> {

  /** @author Yuyuan Kang */
  private MinMaxInfo<Long> minInfo;

  private MinMaxInfo<Long> maxInfo;
  private long firstValue;
  private long lastValue;
  private double sumValue;
  private final TSDataType minMaxDataType = TSDataType.MIN_MAX_INT64;

  static final int LONG_STATISTICS_FIXED_RAM_SIZE = 80;

  @Override
  public TSDataType getType() {
    return TSDataType.INT64;
  }

  /** @author Yuyuan Kang */
  public LongStatistics() {
    this.minInfo = new MinMaxInfo<>(Long.MAX_VALUE, -1);
    this.maxInfo = new MinMaxInfo<>(Long.MIN_VALUE, -1);
  }

  /** @author Yuyuan Kang */
  @Override
  public int getStatsSize() {
    int len = 0;
    // min info
    len += 8; // value of min info, long
    len += 8; // timestamps of min info, long
    // max info
    len += 8; // value of max info, long
    len += 8; // timestamps of max info, long
    len += 24; // first value, last value and sum value
    return len;
  }

  /** @author Yuyuan Kang */
  public void initializeStats(
      MinMaxInfo<Long> minInfo, MinMaxInfo<Long> maxInfo, long firstValue, long last, double sum) {
    this.minInfo = new MinMaxInfo<>(minInfo);
    this.maxInfo = new MinMaxInfo<>(maxInfo);
    this.firstValue = firstValue;
    this.lastValue = last;
    this.sumValue += sum;
  }

  /** @author Yuyuan Kang */
  public void initializeStats(
      long min,
      long bottomTimestamp,
      long max,
      long topTimestamp,
      long firstValue,
      long last,
      double sum) {
    this.minInfo = new MinMaxInfo<>(min, bottomTimestamp);
    this.maxInfo = new MinMaxInfo<>(max, topTimestamp);
    this.firstValue = firstValue;
    this.lastValue = last;
    this.sumValue += sum;
  }

  /** @author Yuyuan Kang */
  private void updateStats(
      long minValue,
      long bottomTimestamp,
      long maxValue,
      long topTimestamp,
      long lastValue,
      double sumValue) {
    updateMinInfo(minValue, bottomTimestamp);
    updateMaxInfo(maxValue, topTimestamp);
    this.sumValue += sumValue;
    this.lastValue = lastValue;
  }

  /** @author Yuyuan Kang */
  private void updateStats(
      MinMaxInfo<Long> minInfo,
      MinMaxInfo<Long> maxInfo,
      long firstValue,
      long lastValue,
      double sumValue,
      long startTime,
      long endTime) {
    updateMinInfo(minInfo.val, minInfo.timestamp);
    updateMaxInfo(maxInfo.val, maxInfo.timestamp);
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
  //    minValue = BytesUtils.bytesToLong(minBytes);
  //    maxValue = BytesUtils.bytesToLong(maxBytes);
  //  }

  /** @author Yuyuan Kang */
  @Override
  public MinMaxInfo<Long> getMinInfo() {
    return minInfo;
  }

  /** @author Yuyuan Kang */
  @Override
  public MinMaxInfo<Long> getMaxInfo() {
    return maxInfo;
  }

  /** @author Yuyuan Kang */
  @Override
  public Long getMinValue() {
    return this.minInfo.val;
  }

  /** @author Yuyuan Kang */
  @Override
  public Long getMaxValue() {
    return this.maxInfo.val;
  }

  /** @author Yuyuan Kang */
  @Override
  public long getBottomTimestamp() {
    return this.minInfo.timestamp;
  }

  /** @author Yuyuan Kang */
  @Override
  public long getTopTimestamp() {
    return this.maxInfo.timestamp;
  }

  @Override
  public Long getFirstValue() {
    return firstValue;
  }

  @Override
  public Long getLastValue() {
    return lastValue;
  }

  @Override
  public double getSumDoubleValue() {
    return sumValue;
  }

  @Override
  public long getSumLongValue() {
    throw new StatisticsClassException("Long statistics does not support: long sum");
  }

  /** @author Yuyuan Kang */
  @Override
  void updateStats(long value, long timestamp) {
    if (isEmpty) {
      initializeStats(value, timestamp, value, timestamp, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, timestamp, value, timestamp, value, value);
    }
  }

  /** @author Yuyuan Kang */
  @Override
  void updateStats(long[] values, long[] timestamps, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i], timestamps[i]);
    }
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateStats(long minValue, long bottomTimestamp, long maxValue, long topTimestamp) {
    updateMinInfo(minValue, bottomTimestamp);
    updateMaxInfo(maxValue, topTimestamp);
  }

  @Override
  public long calculateRamSize() {
    return LONG_STATISTICS_FIXED_RAM_SIZE;
  }

  /** @author Yuyuan Kang */
  @Override
  protected void mergeStatisticsValue(Statistics stats) {
    LongStatistics longStats = (LongStatistics) stats;
    if (isEmpty) {
      initializeStats(
          longStats.getMinInfo(),
          longStats.getMaxInfo(),
          longStats.getFirstValue(),
          longStats.getLastValue(),
          longStats.sumValue);
      isEmpty = false;
    } else {
      updateStats(
          longStats.getMinInfo(),
          longStats.getMaxInfo(),
          longStats.getFirstValue(),
          longStats.getLastValue(),
          longStats.sumValue,
          stats.getStartTime(),
          stats.getEndTime());
    }
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateMinInfo(Long val, long timestamp) {
    if (val < this.minInfo.val) {
      this.minInfo.reset(val, timestamp);
    }
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateMaxInfo(Long val, long timestamp) {
    if (val > this.maxInfo.val) {
      this.maxInfo.reset(val, timestamp);
    }
  }

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
    this.firstValue = ReadWriteIOUtils.readLong(inputStream);
    this.lastValue = ReadWriteIOUtils.readLong(inputStream);
    this.sumValue = ReadWriteIOUtils.readDouble(inputStream);
  }

  /** @author Yuyuan Kang */
  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    this.minInfo = ReadWriteIOUtils.readMinMaxInfo(byteBuffer, minMaxDataType);
    this.maxInfo = ReadWriteIOUtils.readMinMaxInfo(byteBuffer, minMaxDataType);
    this.firstValue = ReadWriteIOUtils.readLong(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readLong(byteBuffer);
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
