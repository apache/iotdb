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
  private static final String TIME = "Time";

  @Override
  public TSDataType getType() {
    return TSDataType.VECTOR;
  }

  /**
   * The output of this method should be identical to the method "serializeStats(OutputStream
   * outputStream)"
   */
  @Override
  public int getStatsSize() {
    return 0;
  }

  @Override
  public void update(long time) {
    super.update(time);
    setEmpty(false);
  }

  @Override
  public void update(long[] time, int batchSize) {
    super.update(time, batchSize);
    if (batchSize > 0) {
      setEmpty(false);
    }
  }

  @Override
  public void update(long[] time, int batchSize, int arrayOffset) {
    super.update(time, batchSize, arrayOffset);
    if (batchSize > 0) {
      setEmpty(false);
    }
  }

  @Override
  public Long getMinValue() {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG, TIME, "min value"));
  }

  @Override
  public Long getMaxValue() {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG, TIME, "max value"));
  }

  @Override
  public Long getFirstValue() {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG, TIME, "first value"));
  }

  @Override
  public Long getLastValue() {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG, TIME, "last value"));
  }

  @Override
  public double getSumDoubleValue() {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG, TIME, "double sum"));
  }

  @Override
  public long getSumLongValue() {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG, TIME, "long sum"));
  }

  @Override
  void updateStats(long value) {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG, TIME, "update stats"));
  }

  @Override
  void updateStats(long[] values, int batchSize) {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG, TIME, "update stats"));
  }

  @Override
  public void updateStats(long minValue, long maxValue) {
    throw new StatisticsClassException(String.format(STATS_UNSUPPORTED_MSG, TIME, "update stats"));
  }

  @Override
  public long calculateRamSize() {
    return TIME_STATISTICS_FIXED_RAM_SIZE;
  }

  @Override
  protected void mergeStatisticsValue(Statistics<Long> stats) {}

  @Override
  public int serializeStats(OutputStream outputStream) {
    return 0;
  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {}

  @Override
  public void deserialize(ByteBuffer byteBuffer) {}
}
