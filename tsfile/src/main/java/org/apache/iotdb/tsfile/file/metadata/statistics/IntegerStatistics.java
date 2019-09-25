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
 * Statistics for int type.
 */
public class IntegerStatistics extends Statistics<Integer> {

  private int min;
  private int max;
  private int first;
  private int last;
  private double sum;

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    min = BytesUtils.bytesToInt(minBytes);
    max = BytesUtils.bytesToInt(maxBytes);
  }

  @Override
  public void updateStats(int value) {
    if (isEmpty) {
      initializeStats(value, value, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, value, value, value, value);
      isEmpty = false;
    }
  }

  @Override
  public void updateStats(int[] values) {
    for (int value : values) {
      if (isEmpty) {
        initializeStats(value, value, value, value, value);
        isEmpty = false;
      } else {
        updateStats(value, value, value, value, value);
        isEmpty = false;
      }
    }
  }

  private void updateStats(int minValue, int maxValue, int firstValue, int lastValue,
      double sumValue) {
    // TODO: unused parameter
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
  public Integer getMin() {
    return min;
  }

  @Override
  public Integer getMax() {
    return max;
  }

  @Override
  public Integer getFirst() {
    return first;
  }

  @Override
  public Integer getLast() {
    return last;
  }

  @Override
  public double getSum() {
    return sum;
  }

  @Override
  protected void mergeStatisticsValue(Statistics<?> stats) {
    IntegerStatistics intStats = (IntegerStatistics) stats;
    if (isEmpty) {
      initializeStats(intStats.getMin(), intStats.getMax(), intStats.getFirst(), intStats.getLast(),
          intStats.getSum());
      isEmpty = false;
    } else {
      updateStats(intStats.getMin(), intStats.getMax(), intStats.getFirst(), intStats.getLast(),
          intStats.getSum());
    }

  }

  private void initializeStats(int min, int max, int first, int last, double sum) {
    this.min = min;
    this.max = max;
    this.first = first;
    this.last = last;
    this.sum = sum;
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
  public byte[] getMinBytes() {
    return BytesUtils.intToBytes(min);
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.intToBytes(max);
  }

  @Override
  public byte[] getFirstBytes() {
    return BytesUtils.intToBytes(first);
  }

  @Override
  public byte[] getLastBytes() {
    return BytesUtils.intToBytes(last);
  }

  @Override
  public byte[] getSumBytes() {
    return BytesUtils.doubleToBytes(sum);
  }

  @Override
  public int sizeOfDatum() {
    return 4;
  }

  @Override
  public String toString() {
    return "[min:" + min + ",max:" + max + ",first:" + first + ",last:" + last + ",sum:" + sum
        + "]";
  }

  @Override
  void fill(InputStream inputStream) throws IOException {
    this.min = ReadWriteIOUtils.readInt(inputStream);
    this.max = ReadWriteIOUtils.readInt(inputStream);
    this.first = ReadWriteIOUtils.readInt(inputStream);
    this.last = ReadWriteIOUtils.readInt(inputStream);
    this.sum = ReadWriteIOUtils.readDouble(inputStream);
  }

  @Override
  void fill(ByteBuffer byteBuffer) throws IOException {
    this.min = ReadWriteIOUtils.readInt(byteBuffer);
    this.max = ReadWriteIOUtils.readInt(byteBuffer);
    this.first = ReadWriteIOUtils.readInt(byteBuffer);
    this.last = ReadWriteIOUtils.readInt(byteBuffer);
    this.sum = ReadWriteIOUtils.readDouble(byteBuffer);
  }
}
