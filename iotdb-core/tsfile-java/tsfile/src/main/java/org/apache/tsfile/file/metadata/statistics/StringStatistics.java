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

package org.apache.tsfile.file.metadata.statistics;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.filter.StatisticsClassException;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfCharArray;

public class StringStatistics extends Statistics<Binary> {
  public static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(StringStatistics.class)
          + 4 * RamUsageEstimator.shallowSizeOfInstance(Binary.class);

  private static final Binary EMPTY_VALUE = new Binary("", TSFileConfig.STRING_CHARSET);

  private Binary firstValue = EMPTY_VALUE;
  private Binary lastValue = EMPTY_VALUE;
  private Binary minValue = EMPTY_VALUE;
  private Binary maxValue = EMPTY_VALUE;

  @Override
  public TSDataType getType() {
    return TSDataType.STRING;
  }

  /** The output of this method should be identical to the method "serializeStats(outputStream)". */
  @Override
  public int getStatsSize() {
    return 4 * 4
        + firstValue.getValues().length
        + lastValue.getValues().length
        + minValue.getValues().length
        + maxValue.getValues().length;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE
        + sizeOfCharArray(firstValue.getLength())
        + sizeOfCharArray(lastValue.getLength())
        + sizeOfCharArray(minValue.getLength())
        + sizeOfCharArray(maxValue.getLength());
  }

  public void initializeStats(Binary first, Binary last, Binary min, Binary max) {
    this.firstValue = first;
    this.lastValue = last;
    this.minValue = min;
    this.maxValue = max;
  }

  private void updateStats(Binary minValue, Binary maxValue, Binary lastValue) {
    if (this.minValue.compareTo(minValue) > 0) {
      this.minValue = minValue;
    }
    if (this.maxValue.compareTo(maxValue) < 0) {
      this.maxValue = maxValue;
    }
    this.lastValue = lastValue;
  }

  private void updateStats(
      Binary firstValue,
      Binary lastValue,
      Binary minValue,
      Binary maxValue,
      long startTime,
      long endTime) {
    // only if endTime greater or equals to the current endTime need we update the last value
    // only if startTime less or equals to the current startTime need we update the first value
    // otherwise, just ignore
    if (this.minValue.compareTo(minValue) > 0) {
      this.minValue = minValue;
    }
    if (this.maxValue.compareTo(maxValue) < 0) {
      this.maxValue = maxValue;
    }
    if (startTime <= this.getStartTime()) {
      this.firstValue = firstValue;
    }
    if (endTime >= this.getEndTime()) {
      this.lastValue = lastValue;
    }
  }

  @Override
  public Binary getMinValue() {
    return minValue;
  }

  @Override
  public Binary getMaxValue() {
    return maxValue;
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
    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.STRING, "double sum"));
  }

  @Override
  public long getSumLongValue() {
    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.STRING, "long sum"));
  }

  @Override
  protected void mergeStatisticsValue(Statistics<Binary> stats) {
    StringStatistics stringStats = (StringStatistics) stats;
    if (isEmpty) {
      initializeStats(
          stringStats.getFirstValue(),
          stringStats.getLastValue(),
          stringStats.getMinValue(),
          stringStats.getMaxValue());
      isEmpty = false;
    } else {
      updateStats(
          stringStats.getFirstValue(),
          stringStats.getLastValue(),
          stringStats.getMinValue(),
          stringStats.getMaxValue(),
          stats.getStartTime(),
          stats.getEndTime());
    }
  }

  @Override
  void updateStats(Binary value) {
    if (isEmpty) {
      initializeStats(value, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, value, value);
    }
  }

  @Override
  void updateStats(Binary[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i]);
    }
  }

  @Override
  public int serializeStats(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(firstValue, outputStream);
    byteLen += ReadWriteIOUtils.write(lastValue, outputStream);
    byteLen += ReadWriteIOUtils.write(minValue, outputStream);
    byteLen += ReadWriteIOUtils.write(maxValue, outputStream);
    return byteLen;
  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {
    this.firstValue = ReadWriteIOUtils.readBinary(inputStream);
    this.lastValue = ReadWriteIOUtils.readBinary(inputStream);
    this.minValue = ReadWriteIOUtils.readBinary(inputStream);
    this.maxValue = ReadWriteIOUtils.readBinary(inputStream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    this.firstValue = ReadWriteIOUtils.readBinary(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readBinary(byteBuffer);
    this.minValue = ReadWriteIOUtils.readBinary(byteBuffer);
    this.maxValue = ReadWriteIOUtils.readBinary(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    StringStatistics that = (StringStatistics) o;
    return firstValue.equals(that.firstValue)
        && lastValue.equals(that.lastValue)
        && minValue.equals(that.minValue)
        && maxValue.equals(that.maxValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), firstValue, lastValue, minValue, maxValue);
  }

  @Override
  public String toString() {
    return super.toString()
        + " [firstValue:"
        + firstValue
        + ", lastValue:"
        + lastValue
        + ", minValue:"
        + minValue
        + ", maxValue:"
        + maxValue
        + "]";
  }
}
