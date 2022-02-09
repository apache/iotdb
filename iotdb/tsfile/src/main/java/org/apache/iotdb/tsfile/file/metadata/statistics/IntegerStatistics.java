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
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/** Statistics for int type. */
public class IntegerStatistics extends Statistics<Integer> {

  private int minValue;
  private int maxValue;
  private int firstValue;
  private int lastValue;
  private long sumValue;

  static final int INTEGER_STATISTICS_FIXED_RAM_SIZE = 64;

  @Override
  public TSDataType getType() {
    return TSDataType.INT32;
  }

  @Override
  public int getStatsSize() {
    return 24;
  }

  public void initializeStats(int min, int max, int first, int last, long sum) {
    this.minValue = min;
    this.maxValue = max;
    this.firstValue = first;
    this.lastValue = last;
    this.sumValue = sum;
  }

  private void updateStats(int minValue, int maxValue, int lastValue, long sumValue) {
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
      int minValue,
      int maxValue,
      int firstValue,
      int lastValue,
      long sumValue,
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
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    minValue = BytesUtils.bytesToInt(minBytes);
    maxValue = BytesUtils.bytesToInt(maxBytes);
  }

  @Override
  void updateStats(int value) {
    if (isEmpty) {
      initializeStats(value, value, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, value, value, value);
    }
  }

  @Override
  void updateStats(int[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i]);
    }
  }

  @Override
  public long calculateRamSize() {
    return INTEGER_STATISTICS_FIXED_RAM_SIZE;
  }

  @Override
  public Integer getMinValue() {
    return minValue;
  }

  @Override
  public Integer getMaxValue() {
    return maxValue;
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

  @Override
  protected void mergeStatisticsValue(Statistics<Integer> stats) {
    IntegerStatistics intStats = (IntegerStatistics) stats;
    if (isEmpty) {
      initializeStats(
          intStats.getMinValue(),
          intStats.getMaxValue(),
          intStats.getFirstValue(),
          intStats.getLastValue(),
          intStats.sumValue);
      isEmpty = false;
    } else {
      updateStats(
          intStats.getMinValue(),
          intStats.getMaxValue(),
          intStats.getFirstValue(),
          intStats.getLastValue(),
          intStats.sumValue,
          stats.getStartTime(),
          stats.getEndTime());
    }
  }

  @Override
  public ByteBuffer getMinValueBuffer() {
    return ReadWriteIOUtils.getByteBuffer(minValue);
  }

  @Override
  public ByteBuffer getMaxValueBuffer() {
    return ReadWriteIOUtils.getByteBuffer(maxValue);
  }

  @Override
  public ByteBuffer getFirstValueBuffer() {
    return ReadWriteIOUtils.getByteBuffer(firstValue);
  }

  @Override
  public ByteBuffer getLastValueBuffer() {
    return ReadWriteIOUtils.getByteBuffer(lastValue);
  }

  @Override
  public ByteBuffer getSumValueBuffer() {
    return ReadWriteIOUtils.getByteBuffer(sumValue);
  }

  @Override
  public byte[] getMinValueBytes() {
    return BytesUtils.intToBytes(minValue);
  }

  @Override
  public byte[] getMaxValueBytes() {
    return BytesUtils.intToBytes(maxValue);
  }

  @Override
  public byte[] getFirstValueBytes() {
    return BytesUtils.intToBytes(firstValue);
  }

  @Override
  public byte[] getLastValueBytes() {
    return BytesUtils.intToBytes(lastValue);
  }

  @Override
  public byte[] getSumValueBytes() {
    return BytesUtils.longToBytes(sumValue);
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
    this.minValue = ReadWriteIOUtils.readInt(inputStream);
    this.maxValue = ReadWriteIOUtils.readInt(inputStream);
    this.firstValue = ReadWriteIOUtils.readInt(inputStream);
    this.lastValue = ReadWriteIOUtils.readInt(inputStream);
    this.sumValue = ReadWriteIOUtils.readLong(inputStream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    this.minValue = ReadWriteIOUtils.readInt(byteBuffer);
    this.maxValue = ReadWriteIOUtils.readInt(byteBuffer);
    this.firstValue = ReadWriteIOUtils.readInt(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readInt(byteBuffer);
    this.sumValue = ReadWriteIOUtils.readLong(byteBuffer);
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
