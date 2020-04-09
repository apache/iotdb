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
import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.file.metadata.OldChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDigest;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.oldstatistics.*;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This class is used for recording statistic information of each measurement in a delta file. While
 * writing processing, the processor records the statistics information. Statistics includes
 * maximum, minimum and null value count up to version 0.0.1.<br> Each data type extends this
 * Statistic as super class.<br>
 */
public abstract class Statistics<T> {

  private static final Logger LOG = LoggerFactory.getLogger(Statistics.class);
  /**
   * isEmpty being false means this statistic has been initialized and the max and min is not null;
   */
  protected boolean isEmpty = true;

  /**
   * number of time-value points
   */
  private long count = 0;

  private long startTime = Long.MAX_VALUE;
  private long endTime = Long.MIN_VALUE;

  /**
   * If the statistics has been modified, it can't be used.
   */
  private boolean canUseStatistics = true;

  /**
   * static method providing statistic instance for respective data type.
   *
   * @param type - data type
   * @return Statistics
   */
  public static Statistics getStatsByType(TSDataType type) {
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

  public abstract TSDataType getType();

  public int getSerializedSize() {
    return 24 // count, startTime, endTime
        + getStatsSize();
  }

  public abstract int getStatsSize();

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(count, outputStream);
    byteLen += ReadWriteIOUtils.write(startTime, outputStream);
    byteLen += ReadWriteIOUtils.write(endTime, outputStream);
    // value statistics of different data type
    byteLen += serializeStats(outputStream);
    return byteLen;
  }

  abstract int serializeStats(OutputStream outputStream) throws IOException;

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
   * @throws StatisticsClassException cannot merge statistics
   */
  public void mergeStatistics(Statistics stats) {
    if (this.getClass() == stats.getClass()) {
      if (stats.startTime < this.startTime) {
        this.startTime = stats.startTime;
      }
      if (stats.endTime > this.endTime) {
        this.endTime = stats.endTime;
      }
      // must be sure no overlap between two statistics
      this.count += stats.count;
      mergeStatisticsValue(stats);
      isEmpty = false;
    } else {
      String thisClass = this.getClass().toString();
      String statsClass = stats.getClass().toString();
      LOG.warn("Statistics classes mismatched,no merge: {} v.s. {}", thisClass, statsClass);

      throw new StatisticsClassException(this.getClass(), stats.getClass());
    }
  }

  public void update(long time, boolean value) {
    if (time < this.startTime) {
      startTime = time;
    }
    if (time > this.endTime) {
      endTime = time;
    }
    count++;
    updateStats(value);
  }

  public void update(long time, int value) {
    if (time < this.startTime) {
      startTime = time;
    }
    if (time > this.endTime) {
      endTime = time;
    }
    count++;
    updateStats(value);
  }

  public void update(long time, long value) {
    if (time < this.startTime) {
      startTime = time;
    }
    if (time > this.endTime) {
      endTime = time;
    }
    count++;
    updateStats(value);
  }

  public void update(long time, float value) {
    if (time < this.startTime) {
      startTime = time;
    }
    if (time > this.endTime) {
      endTime = time;
    }
    count++;
    updateStats(value);
  }

  public void update(long time, double value) {
    if (time < this.startTime) {
      startTime = time;
    }
    if (time > this.endTime) {
      endTime = time;
    }
    count++;
    updateStats(value);
  }

  public void update(long time, Binary value) {
    if (time < startTime) {
      startTime = time;
    }
    if (time > endTime) {
      endTime = time;
    }
    count++;
    updateStats(value);
  }

  public void update(long[] time, boolean[] values, int batchSize) {
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    count += batchSize;
    updateStats(values, batchSize);
  }

  public void update(long[] time, int[] values, int batchSize) {
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    count += batchSize;
    updateStats(values, batchSize);
  }

  public void update(long[] time, long[] values, int batchSize) {
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    count += batchSize;
    updateStats(values, batchSize);
  }

  public void update(long[] time, float[] values, int batchSize) {
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    count += batchSize;
    updateStats(values, batchSize);
  }

  public void update(long[] time, double[] values, int batchSize) {
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    count += batchSize;
    updateStats(values, batchSize);
  }

  public void update(long[] time, Binary[] values, int batchSize) {
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    count += batchSize;
    updateStats(values, batchSize);
  }

  protected abstract void mergeStatisticsValue(Statistics stats);

  public boolean isEmpty() {
    return isEmpty;
  }

  public void setEmpty(boolean empty) {
    isEmpty = empty;
  }

  void updateStats(boolean value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(int value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(long value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(float value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(double value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(Binary value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(boolean[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(int[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(long[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(float[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(double[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(Binary[] values, int batchSize) {
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
    statistics.setCount(ReadWriteIOUtils.readLong(inputStream));
    statistics.setStartTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.setEndTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.deserialize(inputStream);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics deserialize(ByteBuffer buffer, TSDataType dataType) {
    Statistics statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteIOUtils.readLong(buffer));
    statistics.setStartTime(ReadWriteIOUtils.readLong(buffer));
    statistics.setEndTime(ReadWriteIOUtils.readLong(buffer));
    statistics.deserialize(buffer);
    statistics.isEmpty = false;
    return statistics;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public long getCount() {
    return count;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public boolean canUseStatistics() {
    return canUseStatistics;
  }

  public void setCanUseStatistics(boolean canUseStatistics) {
    this.canUseStatistics = canUseStatistics;
  }
  
  public static Statistics upgradeOldStatistics(OldStatistics oldstatistics, 
      TSDataType dataType, int numOfValues, long maxTimestamp, long minTimestamp) {
    Statistics statistics = Statistics.getStatsByType(dataType);
    statistics.setStartTime(minTimestamp);
    statistics.setEndTime(maxTimestamp);
    statistics.setCount(numOfValues);
    statistics.setEmpty(false);
    
    switch (dataType) {
      case INT32:
        ((IntegerStatistics) statistics)
        .initializeStats(((OldIntegerStatistics) oldstatistics).getMin(), 
            ((OldIntegerStatistics) oldstatistics).getMax(), 
            ((OldIntegerStatistics) oldstatistics).getFirst(),
            ((OldIntegerStatistics) oldstatistics).getLast(),
            ((OldIntegerStatistics) oldstatistics).getSum());
        break;
      case INT64:
        ((LongStatistics) statistics)
        .initializeStats(((OldLongStatistics) oldstatistics).getMin(), 
            ((OldLongStatistics) oldstatistics).getMax(), 
            ((OldLongStatistics) oldstatistics).getFirst(),
            ((OldLongStatistics) oldstatistics).getLast(),
            ((OldLongStatistics) oldstatistics).getSum());
        break;
      case TEXT:
        ((BinaryStatistics) statistics)
        .initializeStats(((OldBinaryStatistics) oldstatistics).getFirst(),
            ((OldBinaryStatistics) oldstatistics).getLast());
        break;
      case BOOLEAN:
        ((BooleanStatistics) statistics)
        .initializeStats(((OldBooleanStatistics) oldstatistics).getFirst(),
            ((OldBooleanStatistics) oldstatistics).getLast());
        break;
      case DOUBLE:
        ((DoubleStatistics) statistics)
        .initializeStats(((OldDoubleStatistics) oldstatistics).getMin(), 
            ((OldDoubleStatistics) oldstatistics).getMax(), 
            ((OldDoubleStatistics) oldstatistics).getFirst(),
            ((OldDoubleStatistics) oldstatistics).getLast(),
            ((OldDoubleStatistics) oldstatistics).getSum());
        break;
      case FLOAT:
        ((FloatStatistics) statistics)
        .initializeStats(((OldFloatStatistics) oldstatistics).getMin(), 
            ((OldFloatStatistics) oldstatistics).getMax(), 
            ((OldFloatStatistics) oldstatistics).getFirst(),
            ((OldFloatStatistics) oldstatistics).getLast(),
            ((OldFloatStatistics) oldstatistics).getSum());
        break;
      default:
        throw new UnknownColumnTypeException(statistics.getType()
            .toString());
    }
    return statistics;
  }

  public static Statistics constructStatisticsFromOldChunkMetadata(OldChunkMetadata oldChunkMetadata) {
    Statistics statistics;
    statistics = Statistics.getStatsByType(oldChunkMetadata.getTsDataType());
    statistics.setStartTime(oldChunkMetadata.getStartTime());
    statistics.setEndTime(oldChunkMetadata.getEndTime());
    statistics.setCount(oldChunkMetadata.getNumOfPoints());
    statistics.setEmpty(false);
    TsDigest tsDigest = oldChunkMetadata.getDigest();
    ByteBuffer[] buffers = tsDigest.getStatistics();
    
    switch (statistics.getType()) {
      case INT32:
        ((IntegerStatistics) statistics)
        .initializeStats(ReadWriteIOUtils.readInt(buffers[0]), 
            ReadWriteIOUtils.readInt(buffers[1]), 
            ReadWriteIOUtils.readInt(buffers[2]),
            ReadWriteIOUtils.readInt(buffers[3]),
            ReadWriteIOUtils.readInt(buffers[4]));
        break;
      case INT64:
        ((LongStatistics) statistics)
        .initializeStats(ReadWriteIOUtils.readLong(buffers[0]), 
            ReadWriteIOUtils.readLong(buffers[1]), 
            ReadWriteIOUtils.readLong(buffers[2]),
            ReadWriteIOUtils.readLong(buffers[3]),
            ReadWriteIOUtils.readLong(buffers[4]));
        break;
      case TEXT:
        ((BinaryStatistics) statistics)
        .initializeStats(ReadWriteIOUtils.readBinary(buffers[2]),
            ReadWriteIOUtils.readBinary(buffers[3]));
        break;
      case BOOLEAN:
        ((BooleanStatistics) statistics)
        .initializeStats(ReadWriteIOUtils.readBool(buffers[2]),
            ReadWriteIOUtils.readBool(buffers[3]));
        break;
      case DOUBLE:
        ((DoubleStatistics) statistics)
        .initializeStats(ReadWriteIOUtils.readDouble(buffers[0]), 
            ReadWriteIOUtils.readDouble(buffers[1]), 
            ReadWriteIOUtils.readDouble(buffers[2]),
            ReadWriteIOUtils.readDouble(buffers[3]),
            ReadWriteIOUtils.readDouble(buffers[4]));
        break;
      case FLOAT:
        ((FloatStatistics) statistics)
        .initializeStats(ReadWriteIOUtils.readFloat(buffers[0]), 
            ReadWriteIOUtils.readFloat(buffers[1]), 
            ReadWriteIOUtils.readFloat(buffers[2]),
            ReadWriteIOUtils.readFloat(buffers[3]),
            ReadWriteIOUtils.readFloat(buffers[4]));
        break;
      default:
        throw new UnknownColumnTypeException(statistics.getType()
            .toString());
    }
    return statistics;
  }

  @Override
  public String toString() {
    return "startTime: " + startTime + " endTime: " + endTime + " count: " + count;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o != null && getClass() == o.getClass();
  }

}
