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

/** Statistics for float type. */
public class FloatStatistics extends Statistics<Float> {

  /** @author Yuyuan Kang */
  private MinMaxInfo<Float> minInfo;

  private MinMaxInfo<Float> maxInfo;
  private float firstValue;
  private float lastValue;
  private double sumValue;
  private final TSDataType minMaxDataType = TSDataType.MIN_MAX_FLOAT;

  static final int FLOAT_STATISTICS_FIXED_RAM_SIZE = 64;

  /** @author Yuyuan Kang */
  public FloatStatistics() {
    minInfo = new MinMaxInfo<>(Float.MAX_VALUE, -1);
    maxInfo = new MinMaxInfo<>(Float.MIN_VALUE, -1);
  }

  @Override
  public TSDataType getType() {
    return TSDataType.FLOAT;
  }

  /** @author Yuyuan Kang */
  @Override
  public int getStatsSize() {
    int len = 0;
    // min info
    len += 8; // value of min info, float
    len += 8; // timestamps of min info, long
    // max info
    len += 8; // value of max info, float
    len += 8; // timestamps of max info, long
    len += 24; // first value, last value and sum value
    return len;
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateMinInfo(Float val, long timestamp) {
    if (val < this.minInfo.val) {
      this.minInfo.reset(val, timestamp);
    }
  }

  /** @author Yuyuan Kang */
  @Override
  public void updateMaxInfo(Float val, long timestamp) {
    if (val > this.maxInfo.val) {
      this.maxInfo.reset(val, timestamp);
    }
  }

  /** @author Yuyuan Kang */
  public void initializeStats(
      MinMaxInfo<Float> minInfo,
      MinMaxInfo<Float> maxInfo,
      float firstValue,
      float last,
      double sum) {
    this.minInfo = new MinMaxInfo<>(minInfo);
    this.maxInfo = new MinMaxInfo<>(maxInfo);
    this.firstValue = firstValue;
    this.lastValue = last;
    this.sumValue += sum;
  }

  /** @author Yuyuan Kang */
  public void initializeStats(
      float min,
      long bottomTimestamp,
      float max,
      long topTimestamp,
      float firstValue,
      float last,
      double sum) {
    this.minInfo = new MinMaxInfo<>(min, bottomTimestamp);
    this.maxInfo = new MinMaxInfo<>(max, topTimestamp);
    this.firstValue = firstValue;
    this.lastValue = last;
    this.sumValue += sum;
  }

  /** @author Yuyuan Kang */
  private void updateStats(
      float minValue,
      long bottomTimestamp,
      float maxValue,
      long topTimestamp,
      float last,
      double sumValue) {
    updateMinInfo(minValue, bottomTimestamp);
    updateMaxInfo(maxValue, topTimestamp);
    this.sumValue += sumValue;
    this.lastValue = last;
  }

  /** @author Yuyuan Kang */
  private void updateStats(
      MinMaxInfo<Float> minInfo,
      MinMaxInfo<Float> maxInfo,
      float first,
      float last,
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
      this.firstValue = first;
    }
    if (endTime >= this.getEndTime()) {
      this.lastValue = last;
    }
  }

  //  @Override
  //  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
  //    minValue = BytesUtils.bytesToFloat(minBytes);
  //    maxValue = BytesUtils.bytesToFloat(maxBytes);
  //  }

  /** @author Yuyuan Kang */
  @Override
  void updateStats(float value, long timestamp) {
    if (this.isEmpty) {
      initializeStats(value, timestamp, value, timestamp, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, timestamp, value, timestamp, value, value);
    }
  }

  /** @author Yuyuan Kang */
  @Override
  void updateStats(float[] values, long[] timestamps, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i], timestamps[i]);
    }
  }

  @Override
  public long calculateRamSize() {
    return FLOAT_STATISTICS_FIXED_RAM_SIZE;
  }

  /** @author Yuyuan Kang */
  @Override
  public MinMaxInfo<Float> getMinInfo() {
    return minInfo;
  }

  /** @author Yuyuan Kang */
  @Override
  public MinMaxInfo<Float> getMaxInfo() {
    return maxInfo;
  }

  /** @author Yuyuan Kang */
  @Override
  public Float getMinValue() {
    return this.minInfo.val;
  }

  /** @author Yuyuan Kang */
  @Override
  public Float getMaxValue() {
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
  public Float getFirstValue() {
    return firstValue;
  }

  @Override
  public Float getLastValue() {
    return lastValue;
  }

  @Override
  public double getSumDoubleValue() {
    return sumValue;
  }

  @Override
  public long getSumLongValue() {
    throw new StatisticsClassException("Float statistics does not support: long sum");
  }

  /** @author Yuyuan Kang */
  @Override
  protected void mergeStatisticsValue(Statistics stats) {
    FloatStatistics floatStats = (FloatStatistics) stats;
    if (isEmpty) {
      initializeStats(
          floatStats.getMinInfo(),
          floatStats.getMaxInfo(),
          floatStats.getFirstValue(),
          floatStats.getLastValue(),
          floatStats.sumValue);
      isEmpty = false;
    } else {
      updateStats(
          floatStats.getMinInfo(),
          floatStats.getMaxInfo(),
          floatStats.getFirstValue(),
          floatStats.getLastValue(),
          floatStats.sumValue,
          stats.getStartTime(),
          stats.getEndTime());
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
    this.firstValue = ReadWriteIOUtils.readFloat(inputStream);
    this.lastValue = ReadWriteIOUtils.readFloat(inputStream);
    this.sumValue = ReadWriteIOUtils.readDouble(inputStream);
  }

  /** @author Yuyuan Kang */
  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    this.minInfo = ReadWriteIOUtils.readMinMaxInfo(byteBuffer, minMaxDataType);
    this.maxInfo = ReadWriteIOUtils.readMinMaxInfo(byteBuffer, minMaxDataType);
    this.firstValue = ReadWriteIOUtils.readFloat(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readFloat(byteBuffer);
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
