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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class DoubleStatistics extends Statistics<Double> {

  private double minValue;
  private double maxValue;
  private double sumValue;

  @Override
  public TSDataType getType() {
    return TSDataType.DOUBLE;
  }

  @Override
  public int getStatsSize() {
    return 40;
  }

  /**
   * initialize double statistics.
   *
   * @param min min value
   * @param max max value
   * @param sum sum value
   */
  private void initializeStats(double min, double max, double sum) {
    this.minValue = min;
    this.maxValue = max;
    this.sumValue = sum;
  }

  private void updateStats(double minValue, double maxValue, double sumValue) {
    if (minValue < this.minValue) {
      this.minValue = minValue;
    }
    if (maxValue > this.maxValue) {
      this.maxValue = maxValue;
    }
    this.sumValue += sumValue;
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    minValue = BytesUtils.bytesToDouble(minBytes);
    maxValue = BytesUtils.bytesToDouble(maxBytes);
  }

  @Override
  void updateStats(double value) {
    if (count == 0) {
      initializeStats(value, value, value);
    } else {
      updateStats(value, value, value);
    }
    count++;
  }

  @Override
  void updateStats(double[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i]);
    }
    count += batchSize;
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
  public double getSumValue() {
    return sumValue;
  }

  @Override
  protected void mergeStatisticsValue(Statistics stats) {
    DoubleStatistics doubleStats = (DoubleStatistics) stats;
    if (count == 0) {
      initializeStats(doubleStats.getMinValue(), doubleStats.getMaxValue(), doubleStats.getSumValue());
    } else {
      updateStats(doubleStats.getMinValue(), doubleStats.getMaxValue(), doubleStats.getSumValue());
    }
    count += stats.count;
  }

  @Override
  public byte[] getMinValueBytes() {
    return BytesUtils.doubleToBytes(minValue);
  }

  @Override
  public byte[] getMaxValueBytes() {
    return BytesUtils.doubleToBytes(maxValue);
  }

  @Override
  public byte[] getSumValueBytes() {
    return BytesUtils.doubleToBytes(sumValue);
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
  public ByteBuffer getSumValueBuffer() {
    return ReadWriteIOUtils.getByteBuffer(sumValue);
  }

  @Override
  public int serializeStats(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(minValue, outputStream);
    byteLen += ReadWriteIOUtils.write(maxValue, outputStream);
    byteLen += ReadWriteIOUtils.write(sumValue, outputStream);
    return byteLen;
  }

  @Override
  void deserialize(InputStream inputStream) throws IOException {
    this.minValue = ReadWriteIOUtils.readDouble(inputStream);
    this.maxValue = ReadWriteIOUtils.readDouble(inputStream);
    this.sumValue = ReadWriteIOUtils.readDouble(inputStream);
  }

  @Override
  void deserialize(ByteBuffer byteBuffer) {
    this.minValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.maxValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.sumValue = ReadWriteIOUtils.readDouble(byteBuffer);
  }

  @Override
  public String getString() {
    return "[minValue:" + minValue + ",maxValue:" + maxValue + ",sumValue:" + sumValue + "]";
  }
}
