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
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Statistics for string type. */
public class BinaryStatistics extends Statistics<Binary> {

  private Binary firstValue = new Binary("");
  private Binary lastValue = new Binary("");
  private static final String BINARY = "Binary";
  static final int BINARY_STATISTICS_FIXED_RAM_SIZE = 32;

  @Override
  public TSDataType getType() {
    return TSDataType.TEXT;
  }

  /**
   * The output of this method should be identical to the method "serializeStats(OutputStream outputStream)"
   */
  @Override
  public int getStatsSize() {
    return 4 + firstValue.getValues().length + 4 + lastValue.getValues().length;
  }

  /**
   * initialize Statistics.
   *
   * @param first the first value
   * @param last the last value
   */
  public void initializeStats(Binary first, Binary last) {
    this.firstValue = first;
    this.lastValue = last;
  }

  private void updateStats(Binary firstValue, Binary lastValue) {
    this.lastValue = lastValue;
  }

  private void updateStats(Binary firstValue, Binary lastValue, long startTime, long endTime) {
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
  public Binary getMinValue() {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG,BINARY, "min"));
  }

  @Override
  public Binary getMaxValue() {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG,BINARY, "max"));
  }

  @Override
  public Binary getFirstValue() {
    return firstValue;
  }

  @Override
  public Binary getLastValue() {
    return lastValue;
  }

  @Override
  public double getSumDoubleValue() {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG,BINARY, "double sum"));
  }

  @Override
  public long getSumLongValue() {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG,BINARY, "long sum"));
  }

  @Override
  protected void mergeStatisticsValue(Statistics<Binary> stats) {
    BinaryStatistics stringStats = (BinaryStatistics) stats;
    if (isEmpty) {
      initializeStats(stringStats.getFirstValue(), stringStats.getLastValue());
      isEmpty = false;
    } else {
      updateStats(
          stringStats.getFirstValue(),
          stringStats.getLastValue(),
          stats.getStartTime(),
          stats.getEndTime());
    }
  }

  @Override
  void updateStats(Binary value) {
    if (isEmpty) {
      initializeStats(value, value);
      isEmpty = false;
    } else {
      updateStats(value, value);
    }
  }

  @Override
  void updateStats(Binary[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i]);
    }
  }

  @Override
  public long calculateRamSize() {
    return RamUsageEstimator.sizeOf(this);
  }

  @Override
  public int serializeStats(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(firstValue, outputStream);
    byteLen += ReadWriteIOUtils.write(lastValue, outputStream);
    return byteLen;
  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {
    this.firstValue = ReadWriteIOUtils.readBinary(inputStream);
    this.lastValue = ReadWriteIOUtils.readBinary(inputStream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    this.firstValue = ReadWriteIOUtils.readBinary(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readBinary(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    BinaryStatistics that = (BinaryStatistics) o;
    return Objects.equals(firstValue, that.firstValue) && Objects.equals(lastValue, that.lastValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), firstValue, lastValue);
  }

  @Override
  public String toString() {
    return super.toString() + " [firstValue:" + firstValue + ",lastValue:" + lastValue + "]";
  }
}
