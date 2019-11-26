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
import java.util.Arrays;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
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

  private ByteBuffer[] statistics;

  /**
   * size of valid values in statistics. Note that some values in statistics can be null and thus
   * invalid.
   */
  private int validSizeOfArray = 0;

  private int digestSerializedSize = Integer.BYTES; // initialize for number of statistics

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
    statistics.deserialize(inputStream);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics deserialize(ByteBuffer buffer, TSDataType dataType) throws IOException {
    Statistics statistics = getStatsByType(dataType);
    statistics.deserialize(buffer);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics deserialize(TsFileInput input, long offset, TSDataType dataType)
      throws IOException {
    Statistics statistics = getStatsByType(dataType);
    statistics.deserialize(input, offset);
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
  abstract void deserialize(InputStream inputStream) throws IOException;

  abstract void deserialize(ByteBuffer byteBuffer) throws IOException;

  protected void deserialize(TsFileInput input, long offset) throws IOException {
    int size = getSerializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    ReadWriteIOUtils.readAsPossible(input, offset, buffer);
    buffer.flip();
    deserialize(buffer);
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

  public static int getNullDigestSize() {
    return Integer.BYTES;
  }

  public static int serializeNullTo(OutputStream outputStream) throws IOException {
    return ReadWriteIOUtils.write(0, outputStream);
  }

  public static int serializeNullTo(ByteBuffer buffer) {
    return ReadWriteIOUtils.write(0, buffer);
  }

  /**
   * use given input stream to deserialize.
   *
   * @param inputStream -given input stream
   * @return -an instance of TsDigest
   */
  public static Statistics deserializeFrom(InputStream inputStream, TSDataType dataType) throws IOException {
    Statistics digest = getStatsByType(dataType);
    int size = ReadWriteIOUtils.readInt(inputStream);
    digest.validSizeOfArray = size;
    digest.digestSerializedSize = Integer.BYTES;
    if (size > 0) {
      digest.statistics = new ByteBuffer[StatisticType.getTotalTypeNum()];
      ByteBuffer value;
      // check if it's an old version of TsFile
      String key = "";
      if (TSFileDescriptor.getInstance().getConfig().getEndian().equals(EndianType.LITTLE_ENDIAN.toString())) {
        for (int i = 0; i < size; i++) {
          key = ReadWriteIOUtils.readString(inputStream);
          value = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(inputStream);
          short n;
          switch (key) {
            case "min_value":
              n = 0;
              break;
            case "max_value":
              n = 1;
              break;
            case "first":
              n = 2;
              break;
            case "last":
              n = 3;
              break;
            case "sum":
              n = 4;
              break;
            default:
              n = -1;
          }
          digest.statistics[n] = value;
          digest.digestSerializedSize += Short.BYTES + Integer.BYTES + value.remaining();
        }
      }
      else {
        for (int i = 0; i < size; i++) {
          short n = ReadWriteIOUtils.readShort(inputStream);
          value = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(inputStream);
          digest.statistics[n] = value;
          digest.digestSerializedSize += Short.BYTES + Integer.BYTES + value.remaining();
        }
      }
    } // else left digest.statistics as null
    return digest;
  }

  /**
   * use given buffer to deserialize.
   *
   * @param buffer -given buffer
   * @return -an instance of TsDigest
   */
  public static Statistics deserializeFrom(ByteBuffer buffer, TSDataType dataType) {
    Statistics digest = getStatsByType(dataType);
    int size = ReadWriteIOUtils.readInt(buffer);
    digest.validSizeOfArray = size;
    digest.digestSerializedSize = Integer.BYTES;
    if (size > 0) {
      digest.statistics = new ByteBuffer[StatisticType.getTotalTypeNum()];
      ByteBuffer value;
      // check if it's old version of TsFile
      buffer.mark();
      String key = ReadWriteIOUtils.readString(buffer);
      if (key.equals("min_value") || key.equals("max_value") || key.equals("first")
        || key.equals("last") || key.equals("sum")) {
        buffer.reset();
        for (int i = 0; i < size; i++) {
          key = ReadWriteIOUtils.readString(buffer);
          value = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
          short n;
          switch (key) {
            case "min_value":
              n = 0;
              break;
            case "max_value":
              n = 1;
              break;
            case "first":
              n = 2;
              break;
            case "last":
              n = 3;
              break;
            case "sum":
              n = 4;
              break;
            default:
              n = -1;
          }
          digest.statistics[n] = value;
          digest.digestSerializedSize += Short.BYTES + Integer.BYTES + value.remaining();
        }
      }
      else {
        buffer.reset();
        for (int i = 0; i < size; i++) {
          short n = ReadWriteIOUtils.readShort(buffer);
          value = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
          digest.statistics[n] = value;
          digest.digestSerializedSize += Short.BYTES + Integer.BYTES + value.remaining();
        }
      }
    } // else left digest.statistics as null

    return digest;
  }

  private void reCalculate() {
    validSizeOfArray = 0;
    digestSerializedSize = Integer.BYTES;
    if (statistics != null) {
      for (ByteBuffer value : statistics) {
        if (value != null) {
          // StatisticType serialized value, byteBuffer.capacity and byteBuffer.array
          digestSerializedSize += Short.BYTES + Integer.BYTES + value.remaining();
          validSizeOfArray++;
        }
      }
    }
  }

  /**
   * get statistics of the current object.
   */
  public ByteBuffer[] getStatistics() {
    return statistics; //TODO unmodifiable
  }

  public void setStatistics(ByteBuffer[] statistics) throws IOException {
    if (statistics != null && statistics.length != StatisticType.getTotalTypeNum()) {
      throw new IOException(String.format(
        "The length of array of statistics doesn't equal StatisticType.getTotalTypeNum() %d",
        StatisticType.getTotalTypeNum()));
    }
    this.statistics = statistics;
    reCalculate(); // DO NOT REMOVE THIS
  }

  @Override
  public String toString() {
    return statistics != null ? Arrays.toString(statistics) : "";
  }

  /**
   * use given outputStream to serialize.
   *
   * @param outputStream -given outputStream
   * @return -byte length
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    if (validSizeOfArray == 0) {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    } else {
      byteLen += ReadWriteIOUtils.write(validSizeOfArray, outputStream);
      for (int i = 0; i < statistics.length; i++) {
        if (statistics[i] != null) {
          byteLen += ReadWriteIOUtils.write((short) i, outputStream);
          byteLen += ReadWriteIOUtils.write(statistics[i], outputStream);
        }
      }
    }
    return byteLen;
  }

  /**
   * use given buffer to serialize.
   *
   * @param buffer -given buffer
   * @return -byte length
   */
  public int serializeTo(ByteBuffer buffer) {
    int byteLen = 0;
    if (validSizeOfArray == 0) {
      byteLen += ReadWriteIOUtils.write(0, buffer);
    } else {
      byteLen += ReadWriteIOUtils.write(validSizeOfArray, buffer);
      for (int i = 0; i < statistics.length; i++) {
        if (statistics[i] != null) {
          byteLen += ReadWriteIOUtils.write((short) i, buffer);
          byteLen += ReadWriteIOUtils.write(statistics[i], buffer);
        }
      }
    }
    return byteLen;
  }

  /**
   * get the serializedSize of the current object.
   *
   * @return -serializedSize
   */
  public int getDigestSerializedSize() {
    return digestSerializedSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Statistics digest = (Statistics) o;
    if (digestSerializedSize != digest.digestSerializedSize || validSizeOfArray != digest.validSizeOfArray
      || ((statistics == null) ^ (digest.statistics == null))) {
      return false;
    }

    if (statistics != null) {
      for (int i = 0; i < statistics.length; i++) {
        if ((statistics[i] == null) ^ (digest.statistics[i] == null)) {
          // one is null and the other is not null
          return false;
        }
        if (statistics[i] != null) {
          if (!statistics[i].equals(digest.statistics[i])) {
            return false;
          }
        }
      }
    }
    return true;
  }

  public enum StatisticType {
    min_value, max_value, first_value, last_value, sum_value;

    public static int getTotalTypeNum() {
      return StatisticType.values().length;
    }

    public static StatisticType deserialize(short i) {
      return StatisticType.values()[i];
    }

    public static int getSerializedSize() {
      return Short.BYTES;
    }

    public short serialize() {
      return (short) this.ordinal();
    }
  }
}
