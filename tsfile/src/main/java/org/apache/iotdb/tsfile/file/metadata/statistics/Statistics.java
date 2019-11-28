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
import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for recording statistic information of each measurement in a delta file. While
 * writing processing, the processor records the statistics information. Statistics includes maximum,
 * minimum and null value count up to version 0.0.1.<br> Each data type extends this Statistic as
 * super class.<br>
 *
 * @param <T> data type for Statistics
 */
public abstract class Statistics<T> {

  private static final Logger LOG = LoggerFactory.getLogger(Statistics.class);
  /**
   * isEmpty being false means this statistic has been initialized and the max and min is not null;
   */
  protected boolean isEmpty = true;

  /**
   * static method providing statistic instance for respective data type.
   *
   * @param type - data type
   * @return Statistics
   */
  public static Statistics<?> getStatsByType(TSDataType type) {
    switch (type) {
      case INT32:
        return new IntegerStatistics();
      case INT64:
        return new LongStatistics();
      case TEXT:
        return new BinaryStatistics();
      case BOOLEAN:
        return new BooleanStatistics();
      case DOUBLE:
        return new DoubleStatistics();
      case FLOAT:
        return new FloatStatistics();
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  abstract TSDataType getType();

  public abstract int getSerializedSize();

  public abstract int serialize(OutputStream outputStream) throws IOException;

  /**
   * read data from the inputStream.
   */
  abstract void deserialize(InputStream inputStream) throws IOException;

  abstract void deserialize(ByteBuffer byteBuffer);

  public abstract void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes);

  public abstract T getMinValue();

  public abstract T getMaxValue();

  public abstract T getFirstValue();

  public abstract T getLastValue();

  public abstract double getSumValue();

  public abstract byte[] getMinValueBytes();

  public abstract byte[] getMaxValueBytes();

  public abstract byte[] getFirstValueBytes();

  public abstract byte[] getLastValueBytes();

  public abstract byte[] getSumValueBytes();

  public abstract ByteBuffer getMinValueBuffer();

  public abstract ByteBuffer getMaxValueBuffer();

  public abstract ByteBuffer getFirstValueBuffer();

  public abstract ByteBuffer getLastValueBuffer();

  public abstract ByteBuffer getSumValueBuffer();

  /**
   * merge parameter to this statistic
   *
   * @param stats input statistics
   * @throws StatisticsClassException cannot merge statistics
   */
  public void mergeStatistics(Statistics<?> stats) {
    if (stats == null) {
      LOG.warn("tsfile-file parameter stats is null");
      return;
    }
    if (this.getClass() == stats.getClass()) {
      if (!stats.isEmpty) {
        mergeStatisticsValue(stats);
        isEmpty = false;
      }
    } else {
      String thisClass = this.getClass().toString();
      String statsClass = stats.getClass().toString();
      LOG.warn("Statistics classes mismatched,no merge: {} v.s. {}",
          thisClass, statsClass);

      throw new StatisticsClassException(this.getClass(), stats.getClass());
    }
  }

  protected abstract void mergeStatisticsValue(Statistics stats);

  public boolean isEmpty() {
    return isEmpty;
  }

  public void setEmpty(boolean empty) {
    isEmpty = empty;
  }

  public void updateStats(boolean value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(int value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(long value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(float value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(double value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(Binary value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(boolean[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(int[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(long[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(float[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(double[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(Binary[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  /**
   * This method with two parameters is only used by {@code unsequence} which
   * updates/inserts/deletes timestamp.
   *
   * @param min min timestamp
   * @param max max timestamp
   */
  public void updateStats(long min, long max) {
    throw new UnsupportedOperationException();
  }

  public static Statistics deserialize(InputStream inputStream, TSDataType dataType)
      throws IOException {
    Statistics statistics = getStatsByType(dataType);
    statistics.deserialize(inputStream);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics deserialize(ByteBuffer buffer, TSDataType dataType) {
    Statistics statistics = getStatsByType(dataType);
    statistics.deserialize(buffer);
    statistics.isEmpty = false;
    return statistics;
  }

  public void reset() {
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o != null && getClass() == o.getClass();
  }

}
