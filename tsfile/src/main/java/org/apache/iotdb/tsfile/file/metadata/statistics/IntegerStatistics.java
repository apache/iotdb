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

/** Statistics for int type. */
public class IntegerStatistics extends Statistics<Integer> {

  /** @author Yuyuan Kang */
  private MinMaxInfo<Integer> minInfo;

  private MinMaxInfo<Integer> maxInfo;
  private int firstValue;
  private int lastValue;
  private long sumValue;
  /** @author Yuyuan Kang */
  private final TSDataType minMaxDataType = TSDataType.MIN_MAX_INT32;

  static final int INTEGER_STATISTICS_FIXED_RAM_SIZE = 64;

  @Override
  public TSDataType getType() {
    return TSDataType.INT32;
  }

  /** @author Yuyuan Kang */
  public IntegerStatistics() {
    minInfo = new MinMaxInfo<>(Integer.MAX_VALUE, -1);
    maxInfo = new MinMaxInfo<>(Integer.MIN_VALUE, -1);
  }

  /** @author Yuyuan Kang */
  @Override
  public int getStatsSize() {
    int len = 0;
    // min info
    len += 4; // value of min info, int
    len += 8; // timestamps of min info, long
    // max info
    len += 4; // value of max info, int
    len += 8; // timestamps of max info, long
    len += 16; // first value, last value and sum value
    return len;
  }

  /** @author Yuyuan Kang */
  public void initializeStats(
      MinMaxInfo<Integer> minInfo,
      MinMaxInfo<Integer> maxInfo,
      int firstValue,
      int last,
      long sum) {
    this.minInfo = new MinMaxInfo<>(minInfo);
    this.maxInfo = new MinMaxInfo<>(maxInfo);
    this.firstValue = firstValue;
    this.lastValue = last;
    this.sumValue += sum;
  }

  /** @author Yuyuan Kang */
  public void initializeStats(
      int min,
      long bottomTimestamp,
      int max,
      long topTimestamp,
      int firstValue,
      int last,
      long sum) {
    this.minInfo = new MinMaxInfo<>(min, bottomTimestamp);
    this.maxInfo = new MinMaxInfo<>(max, topTimestamp);
    this.firstValue = firstValue;
    this.lastValue = last;
    this.sumValue += sum;
  }

  /** @author Yuyuan Kang */
  private void updateStats(
      int minValue,
      long bottomTimestamp,
      int maxValue,
      long topTimestamp,
      int lastValue,
      long sumValue) {
    updateMinInfo(minValue, bottomTimestamp);
    updateMaxInfo(maxValue, topTimestamp);
    this.sumValue += sumValue;
    this.lastValue = lastValue;
  }

  /** @author Yuyuan Kang */
  private void updateStats(
      MinMaxInfo<Integer> minInfo,
      MinMaxInfo<Integer> maxInfo,
      int firstValue,
      int lastValue,
      long sumValue,
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
  //    minValue = BytesUtils.bytesToInt(minBytes);
  //    maxValue = BytesUtils.bytesToInt(maxBytes);
  //  }

  /** @author Yuyuan Kang */
  @Override
  void updateStats(int value, long timestamp) {
    if (isEmpty) {
      initializeStats(value, timestamp, value, timestamp, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, timestamp, value, timestamp, value, value);
    }
  }

  /** @author Yuyuan Kang */
  @Override
  void updateStats(int[] values, long[] timestamps, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i], timestamps[i]);
    }
  }

  @Override
  public long calculateRamSize() {
    return INTEGER_STATISTICS_FIXED_RAM_SIZE;
  }

  /** @author Yuyuan Kang */
  @Override
  public MinMaxInfo<Integer> getMinInfo() {
    return this.minInfo;
  }

  /** @author Yuyuan Kang */
  @Override
  public MinMaxInfo<Integer> getMaxInfo() {
    return this.maxInfo;
  }

  /** @author Yuyuan Kang */
  @Override
  public Integer getMinValue() {
    return this.minInfo.val;
  }

  /** @author Yuyuan Kang */
  @Override
  public Integer getMaxValue() {
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
  public Integer getFirstValue() {
    return firstValue;
  }

  @Override
  public Integer getLastValue() {
    return lastValue;
  }

  @Override
  public double getSumDoubleValue() {
    throw new StatisticsClassException("Integer statistics does not support: double sum");
  }

  @Override
  public long getSumLongValue() {
    return sumValue;
  }

  /** @author Yuyuan Kang */
  @Override
  protected void mergeStatisticsValue(Statistics stats) {
    IntegerStatistics intStats = (IntegerStatistics) stats;
    if (isEmpty) {
      initializeStats(
          intStats.getMinInfo(),
          intStats.getMaxInfo(),
          intStats.getFirstValue(),
          intStats.getLastValue(),
          intStats.sumValue);
      isEmpty = false;
    } else {
      updateStats(
          intStats.getMinInfo(),
          intStats.getMaxInfo(),
          intStats.getFirstValue(),
          intStats.getLastValue(),
          intStats.sumValue,
          stats.getStartTime(),
          stats.getEndTime());
    }
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateMinInfo(Integer val, long timestamp) {
    if (val < this.minInfo.val) {
      this.minInfo.reset(val, timestamp);
    }
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateMaxInfo(Integer val, long timestamp) {
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
    this.firstValue = ReadWriteIOUtils.readInt(inputStream);
    this.lastValue = ReadWriteIOUtils.readInt(inputStream);
    this.sumValue = ReadWriteIOUtils.readLong(inputStream);
  }

  /** @author Yuyuan Kang */
  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    this.minInfo = ReadWriteIOUtils.readMinMaxInfo(byteBuffer, minMaxDataType);
    this.maxInfo = ReadWriteIOUtils.readMinMaxInfo(byteBuffer, minMaxDataType);
    this.firstValue = ReadWriteIOUtils.readInt(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readInt(byteBuffer);
    this.sumValue = ReadWriteIOUtils.readLong(byteBuffer);
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
