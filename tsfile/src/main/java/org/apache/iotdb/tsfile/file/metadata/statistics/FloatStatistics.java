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
 * Statistics for float type.
 */
public class FloatStatistics extends Statistics<Float> {

  private float min;
  private float max;
  private float first;
  private double sum;
  private float last;

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    min = BytesUtils.bytesToFloat(minBytes);
    max = BytesUtils.bytesToFloat(maxBytes);
  }

  @Override
  public void updateStats(float value) {
    if (this.isEmpty) {
      initializeStats(value, value, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, value, value, value, value);
    }
  }

  @Override
  public void updateStats(float[] values) {
    for (float value : values) {
      if (this.isEmpty) {
        initializeStats(value, value, value, value, value);
        isEmpty = false;
      } else {
        updateStats(value, value, value, value, value);
      }
    }
  }

  private void updateStats(float minValue, float maxValue, float firstValue, float last,
      double sumValue) {
    if (minValue < min) {
      min = minValue;
    }
    if (maxValue > max) {
      max = maxValue;
    }
    sum += sumValue;
    this.last = last;
  }

  @Override
  public Float getMin() {
    return min;
  }

  @Override
  public Float getMax() {
    return max;
  }

  @Override
  public Float getFirst() {
    return first;
  }

  @Override
  public Float getLast() {
    return last;
  }

  @Override
  public double getSum() {
    return sum;
  }

  @Override
  protected void mergeStatisticsValue(Statistics<?> stats) {
    FloatStatistics floatStats = (FloatStatistics) stats;
    if (isEmpty) {
      initializeStats(floatStats.getMin(), floatStats.getMax(), floatStats.getFirst(),
          floatStats.getLast(), floatStats.getSum());
      isEmpty = false;
    } else {
      updateStats(floatStats.getMin(), floatStats.getMax(), floatStats.getFirst(),
          floatStats.getLast(), floatStats.getSum());
    }

  }

  private void initializeStats(float min, float max, float first, float last, double sum) {
    this.min = min;
    this.max = max;
    this.first = first;
    this.last = last;
    this.sum = sum;
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.floatToBytes(min);
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.floatToBytes(max);
  }

  @Override
  public byte[] getFirstBytes() {
    return BytesUtils.floatToBytes(first);
  }

  @Override
  public byte[] getLastBytes() {
    return BytesUtils.floatToBytes(last);
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
    return 4;
  }

  @Override
  public String toString() {
    return "[min:" + min + ",max:" + max + ",first:" + first + ",last:" + last + ",sum:" + sum
        + "]";
  }

  @Override
  void deserialize(InputStream inputStream) throws IOException {
    this.min = ReadWriteIOUtils.readFloat(inputStream);
    this.max = ReadWriteIOUtils.readFloat(inputStream);
    this.first = ReadWriteIOUtils.readFloat(inputStream);
    this.last = ReadWriteIOUtils.readFloat(inputStream);
    this.sum = ReadWriteIOUtils.readDouble(inputStream);
  }

  @Override
  void deserialize(ByteBuffer byteBuffer) throws IOException {
    this.min = ReadWriteIOUtils.readFloat(byteBuffer);
    this.max = ReadWriteIOUtils.readFloat(byteBuffer);
    this.first = ReadWriteIOUtils.readFloat(byteBuffer);
    this.last = ReadWriteIOUtils.readFloat(byteBuffer);
    this.sum = ReadWriteIOUtils.readDouble(byteBuffer);
  }
}
