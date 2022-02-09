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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class TimeStatistics extends Statistics<Long> {

  static final int TIME_STATISTICS_FIXED_RAM_SIZE = 40;

  @Override
  public TSDataType getType() {
    return TSDataType.VECTOR;
  }

  @Override
  public int getStatsSize() {
    return 0;
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    throw new StatisticsClassException("Time statistics does not support: set min max from bytes");
  }

  @Override
  public Long getMinValue() {
    throw new StatisticsClassException("Time statistics does not support: min value");
  }

  @Override
  public Long getMaxValue() {
    throw new StatisticsClassException("Time statistics does not support: max value");
  }

  @Override
  public Long getFirstValue() {
    throw new StatisticsClassException("Time statistics does not support: first value");
  }

  @Override
  public Long getLastValue() {
    throw new StatisticsClassException("Time statistics does not support: last value");
  }

  @Override
  public double getSumDoubleValue() {
    throw new StatisticsClassException("Time statistics does not support: double sum");
  }

  @Override
  public long getSumLongValue() {
    throw new StatisticsClassException("Time statistics does not support: long sum");
  }

  @Override
  void updateStats(long value) {
    throw new StatisticsClassException("Time statistics does not support: update stats");
  }

  @Override
  void updateStats(long[] values, int batchSize) {
    throw new StatisticsClassException("Time statistics does not support: update stats");
  }

  @Override
  public void updateStats(long minValue, long maxValue) {
    throw new StatisticsClassException("Time statistics does not support: update stats");
  }

  @Override
  public long calculateRamSize() {
    return TIME_STATISTICS_FIXED_RAM_SIZE;
  }

  @Override
  protected void mergeStatisticsValue(Statistics<Long> stats) {}

  @Override
  public byte[] getMinValueBytes() {
    throw new StatisticsClassException("Time statistics does not support: get min value bytes");
  }

  @Override
  public byte[] getMaxValueBytes() {
    throw new StatisticsClassException("Time statistics does not support: get max value bytes");
  }

  @Override
  public byte[] getFirstValueBytes() {
    throw new StatisticsClassException("Time statistics does not support: get first value bytes");
  }

  @Override
  public byte[] getLastValueBytes() {
    throw new StatisticsClassException("Time statistics does not support: get last value bytes");
  }

  @Override
  public byte[] getSumValueBytes() {
    throw new StatisticsClassException("Time statistics does not support: get sum value bytes");
  }

  @Override
  public ByteBuffer getMinValueBuffer() {
    throw new StatisticsClassException("Time statistics does not support: get min value bytes");
  }

  @Override
  public ByteBuffer getMaxValueBuffer() {
    throw new StatisticsClassException("Time statistics does not support: get max value buffer");
  }

  @Override
  public ByteBuffer getFirstValueBuffer() {
    throw new StatisticsClassException("Time statistics does not support: get first value buffer");
  }

  @Override
  public ByteBuffer getLastValueBuffer() {
    throw new StatisticsClassException("Time statistics does not support: get last value buffer");
  }

  @Override
  public ByteBuffer getSumValueBuffer() {
    throw new StatisticsClassException("Time statistics does not support: get sum value buffer");
  }

  @Override
  public int serializeStats(OutputStream outputStream) {
    return 0;
  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {}

  @Override
  public void deserialize(ByteBuffer byteBuffer) {}
}
