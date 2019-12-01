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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

/**
 * Statistics for string type.
 */
public class BinaryStatistics extends Statistics<Binary> {

  @Override
  public TSDataType getType() {
    return TSDataType.TEXT;
  }

  @Override
  public int getStatsSize() {
    return 0;
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
  }

  @Override
  public Binary getMinValue() {
    throw new StatisticsClassException("Binary statistics does not support: min");
  }

  @Override
  public Binary getMaxValue() {
    throw new StatisticsClassException("Binary statistics does not support: max");
  }

  @Override
  public double getSumValue() {
    throw new StatisticsClassException("Binary statistics does not support: sum");
  }

  @Override
  protected void mergeStatisticsValue(Statistics stats) {
    count += stats.count;
  }

  @Override
  void updateStats(Binary value) {
    count++;
  }

  @Override
  void updateStats(Binary[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i]);
    }
    count += batchSize;
  }

  @Override
  public byte[] getMinValueBytes() {
    throw new StatisticsClassException("Binary statistics does not support: min");
  }

  @Override
  public byte[] getMaxValueBytes() {
    throw new StatisticsClassException("Binary statistics does not support: max");
  }

  @Override
  public byte[] getSumValueBytes() {
    throw new StatisticsClassException("Binary statistics does not support: sum");
  }

  @Override
  public ByteBuffer getMinValueBuffer() {
    throw new StatisticsClassException("Binary statistics does not support: min");
  }

  @Override
  public ByteBuffer getMaxValueBuffer() {
    throw new StatisticsClassException("Binary statistics does not support: max");
  }

  @Override
  public ByteBuffer getSumValueBuffer() {
    throw new StatisticsClassException("Binary statistics does not support: sum");
  }

  @Override
  public int serializeStats(OutputStream outputStream) {
    return 0;
  }

  @Override
  void deserialize(InputStream inputStream) {
  }

  @Override
  void deserialize(ByteBuffer byteBuffer) {
  }

  @Override
  public String getString() {
    return "";
  }

}
