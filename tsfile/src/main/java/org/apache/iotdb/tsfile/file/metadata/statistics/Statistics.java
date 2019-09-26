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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for recording statistic information of each measurement in a delta file. While
 * writing processing, the processor records the digest information. Statistics includes maximum,
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

  public static Statistics deserialize(InputStream inputStream, TSDataType dataType)
      throws IOException {
    Statistics statistics = getStatsByType(dataType);
    statistics.fill(inputStream);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics deserialize(ByteBuffer buffer, TSDataType dataType) throws IOException {
    Statistics statistics = getStatsByType(dataType);
    statistics.fill(buffer);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics deserialize(TsFileInput input, long offset, TSDataType dataType)
      throws IOException {
    Statistics statistics = getStatsByType(dataType);
    statistics.fill(input, offset);
    statistics.isEmpty = false;
    return statistics;
  }

  public abstract void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes);

  public abstract T getMin();

  public abstract T getMax();

  public abstract T getFirst();

  public abstract T getLast();

  public abstract double getSum();

  public abstract byte[] getMinBytes();

  public abstract byte[] getMaxBytes();

  public abstract byte[] getFirstBytes();

  public abstract byte[] getLastBytes();

  public abstract byte[] getSumBytes();

  public abstract ByteBuffer getMinBytebuffer();

  public abstract ByteBuffer getMaxBytebuffer();

  public abstract ByteBuffer getFirstBytebuffer();

  public abstract ByteBuffer getLastBytebuffer();

  public abstract ByteBuffer getSumBytebuffer();

  /**
   * merge parameter to this statistic. Including
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
      LOG.warn("tsfile-file Statistics classes mismatched,no merge: {} v.s. {}",
          thisClass, statsClass);

      throw new StatisticsClassException(this.getClass(), stats.getClass());
    }
  }

  protected abstract void mergeStatisticsValue(Statistics<?> stats);

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

  public void updateStats(float value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(double value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(BigDecimal value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(Binary value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(boolean[] values) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(int[] values) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(long[] values) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(float[] values) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(double[] values) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(BigDecimal[] values) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(Binary[] values) {
    throw new UnsupportedOperationException();
  }

  public void reset() {
  }

  /**
   * @return the size of one field of this class.<br> int, float - 4<br> double, long, bigDecimal -
   * 8 <br> boolean - 1 <br> No - 0 <br> binary - -1 which means uncertainty </>
   */
  public abstract int sizeOfDatum();

  /**
   * read data from the inputStream.
   */
  abstract void fill(InputStream inputStream) throws IOException;

  abstract void fill(ByteBuffer byteBuffer) throws IOException;

  protected void fill(TsFileInput input, long offset) throws IOException {
    int size = getSerializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    ReadWriteIOUtils.readAsPossible(input, offset, buffer);
    buffer.flip();
    fill(buffer);
  }

  public int getSerializedSize() {
    if (sizeOfDatum() == 0) {
      return 0;
    } else if (sizeOfDatum() != -1) {
      return sizeOfDatum() * 4 + 8;
    } else {
      return 4 * Integer.BYTES + getMinBytes().length + getMaxBytes().length
          + getFirstBytes().length
          + getLastBytes().length + getSumBytes().length;
    }
  }

  public int serialize(OutputStream outputStream) throws IOException {
    int length = 0;
    if (sizeOfDatum() == 0) {
      return 0;
    } else if (sizeOfDatum() != -1) {
      length = sizeOfDatum() * 4 + 8;
      outputStream.write(getMinBytes());
      outputStream.write(getMaxBytes());
      outputStream.write(getFirstBytes());
      outputStream.write(getLastBytes());
      outputStream.write(getSumBytes());
    } else {
      byte[] tmp = getMinBytes();
      length += tmp.length;
      length += ReadWriteIOUtils.write(tmp.length, outputStream);
      outputStream.write(tmp);
      tmp = getMaxBytes();
      length += tmp.length;
      length += ReadWriteIOUtils.write(tmp.length, outputStream);
      outputStream.write(tmp);
      tmp = getFirstBytes();
      length += tmp.length;
      length += ReadWriteIOUtils.write(tmp.length, outputStream);
      outputStream.write(tmp);
      tmp = getLastBytes();
      length += tmp.length;
      length += ReadWriteIOUtils.write(tmp.length, outputStream);
      outputStream.write(tmp);
      outputStream.write(getSumBytes());
      length += 8;
    }
    return length;
  }
}
