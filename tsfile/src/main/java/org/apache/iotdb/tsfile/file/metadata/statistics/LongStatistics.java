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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * Statistics for long type.
 */
public class LongStatistics extends Statistics<Long> {

  private long min;
  private long max;
  private long first;
  private long last;
  private double sum;

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    min = BytesUtils.bytesToLong(minBytes);
    max = BytesUtils.bytesToLong(maxBytes);
  }

  @Override
  public Long getMin() {
    return min;
  }

  @Override
  public Long getMax() {
    return max;
  }

  @Override
  public Long getFirst() {
    return first;
  }

  @Override
  public Long getLast() {
    return last;
  }

  @Override
  public double getSum() {
    return sum;
  }

  @Override
  public void updateStats(long value) {
    if (isEmpty) {
      initializeStats(value, value, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, value, value, value, value);
    }
  }

  @Override
  public void updateStats(long[] values) {
    for (long value : values) {
      if (isEmpty) {
        initializeStats(value, value, value, value, value);
        isEmpty = false;
      } else {
        updateStats(value, value, value, value, value);
      }
    }
  }

  private void updateStats(long minValue, long maxValue, long firstValue, long lastValue,
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
  public void updateStats(long minValue, long maxValue) {
    if (minValue < min) {
      min = minValue;
    }
    if (maxValue > max) {
      max = maxValue;
    }
  }

  @Override
  protected void mergeStatisticsValue(Statistics<?> stats) {
    LongStatistics longStats = (LongStatistics) stats;
    if (isEmpty) {
      initializeStats(longStats.getMin(), longStats.getMax(), longStats.getFirst(),
          longStats.getLast(), longStats.getSum());
      isEmpty = false;
    } else {
      updateStats(longStats.getMin(), longStats.getMax(), longStats.getFirst(), longStats.getLast(),
          longStats.getSum());
    }

  }

  private void initializeStats(long min, long max, long firstValue, long last, double sum) {
    this.min = min;
    this.max = max;
    this.first = firstValue;
    this.last = last;
    this.sum += sum;
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.longToBytes(min);
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.longToBytes(max);
  }

  @Override
  public byte[] getFirstBytes() {
    return BytesUtils.longToBytes(first);
  }

  @Override
  public byte[] getLastBytes() {
    return BytesUtils.longToBytes(last);
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
  public String toString() {
    return "[min:" + min + ",max:" + max + ",first:" + first + ",last:" + last + ",sum:" + sum
        + "]";
  }

  @Override
  public int sizeOfDatum() {
    return 8;
  }

  @Override
  void deserialize(InputStream inputStream) throws IOException {
    this.min = ReadWriteIOUtils.readLong(inputStream);
    this.max = ReadWriteIOUtils.readLong(inputStream);
    this.first = ReadWriteIOUtils.readLong(inputStream);
    this.last = ReadWriteIOUtils.readLong(inputStream);
    this.sum = ReadWriteIOUtils.readDouble(inputStream);
  }

  @Override
  void deserialize(ByteBuffer byteBuffer) throws IOException {
    this.min = ReadWriteIOUtils.readLong(byteBuffer);
    this.max = ReadWriteIOUtils.readLong(byteBuffer);
    this.first = ReadWriteIOUtils.readLong(byteBuffer);
    this.last = ReadWriteIOUtils.readLong(byteBuffer);
    this.sum = ReadWriteIOUtils.readDouble(byteBuffer);
  }

}
