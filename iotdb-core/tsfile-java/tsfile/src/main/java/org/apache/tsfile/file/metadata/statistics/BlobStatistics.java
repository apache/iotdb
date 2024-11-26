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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.filter.StatisticsClassException;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BlobStatistics extends Statistics<Binary> {

  public static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(BlobStatistics.class);

  // no statistics for blob data type

  @Override
  public TSDataType getType() {
    return TSDataType.BLOB;
  }

  /** The output of this method should be identical to the method "serializeStats(outputStream)". */
  @Override
  public int getStatsSize() {
    return 0;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE;
  }

  @Override
  int serializeStats(OutputStream outputStream) throws IOException {
    return 0;
  }

  public void updateStats(Binary value) {
    if (isEmpty) {
      isEmpty = false;
    }
  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {
    // do nothing
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    // do nothing
  }

  @Override
  public Binary getMinValue() {
    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.BLOB, "min"));
  }

  @Override
  public Binary getMaxValue() {
    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.BLOB, "max"));
  }

  @Override
  public Binary getFirstValue() {
    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.BLOB, "first"));
  }

  @Override
  public Binary getLastValue() {
    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.BLOB, "last"));
  }

  @Override
  public double getSumDoubleValue() {
    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.BLOB, "sum"));
  }

  @Override
  public long getSumLongValue() {

    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.BLOB, "sum"));
  }

  @Override
  protected void mergeStatisticsValue(Statistics<Binary> stats) {
    // do nothing
    if (isEmpty) {
      isEmpty = false;
    }
  }

  @Override
  public String toString() {
    return "BlobStatistics{}";
  }
}
