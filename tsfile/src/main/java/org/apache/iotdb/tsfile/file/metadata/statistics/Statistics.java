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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.hash.BloomFilter;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * This class is used for recording statistic information of each measurement in a delta file. While
 * writing processing, the processor records the statistics information. Statistics includes
 * maximum, minimum and null value count up to version 0.0.1.<br>
 * Each data type extends this Statistic as super class.<br>
 * <br>
 * For the statistics in the Unseq file TimeSeriesMetadata, only firstValue, lastValue, startTime
 * and endTime can be used.</br>
 */
public abstract class Statistics<T extends Serializable> {

  private static final Logger LOG = LoggerFactory.getLogger(Statistics.class);
  /**
   * isEmpty being false means this statistic has been initialized and the max and min is not null;
   */
  protected boolean isEmpty = true;

  /** number of time-value points */
  private int count = 0;

  private long startTime = Long.MAX_VALUE;
  private long endTime = Long.MIN_VALUE;

  static final String STATS_UNSUPPORTED_MSG = "%s statistics does not support: %s";

  public static Boolean ENABLE_SYNOPSIS = false;
  public static Boolean ENABLE_BLOOM_FILTER = false;
  public static int STATISTICS_PAGE_MAXSIZE = 8000;
  public static int SYNOPSIS_SIZE_IN_BYTE = 1024;
  public static int BLOOM_FILTER_BITS_PER_KEY = 8;
  public static int BLOOM_FILTER_SIZE = 0;
  public static int PAGE_SIZE_IN_BYTE = 65536;
  public static int SUMMARY_TYPE = 0;

  protected static double getFPP(double bitsPerKey) {
    return Math.exp(-1 * bitsPerKey * Math.pow(Math.log(2.0D), 2));
  }

  protected static long optimalNumOfBits(long n, double p) {
    return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  protected static int getBFArrayLength(long bits) {
    return Ints.checkedCast(LongMath.divide(bits, 64, RoundingMode.CEILING));
  }

  protected static int calcBFSize(long n, double p) {
    return getBFArrayLength(optimalNumOfBits(n, p)) * 8 + 6;
  }

  /**
   * static method providing statistic instance for respective data type.
   *
   * @param type - data type
   * @return Statistics
   */
  public static Statistics<? extends Serializable> getStatsByType(TSDataType type) {
    ENABLE_SYNOPSIS = TSFileDescriptor.getInstance().getConfig().isEnableSynopsis();
    ENABLE_BLOOM_FILTER = TSFileDescriptor.getInstance().getConfig().isEnableBloomFilter();
    SYNOPSIS_SIZE_IN_BYTE = TSFileDescriptor.getInstance().getConfig().getSynopsisSizeInByte();
    BLOOM_FILTER_BITS_PER_KEY =
        TSFileDescriptor.getInstance().getConfig().getBloomFilterBitsPerKey();
    STATISTICS_PAGE_MAXSIZE =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    BLOOM_FILTER_SIZE = calcBFSize(STATISTICS_PAGE_MAXSIZE, getFPP(BLOOM_FILTER_BITS_PER_KEY));
    PAGE_SIZE_IN_BYTE = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    SUMMARY_TYPE = TSFileDescriptor.getInstance().getConfig().getSummaryType();
    //    System.out.println(
    //        "\t[DEBUG][Statistics] enable kll/bf:"
    //            + ENABLE_SYNOPSIS
    //            + "/"
    //            + ENABLE_BLOOM_FILTER
    //            + "  kllSize:"
    //            + SYNOPSIS_SIZE_IN_BYTE
    //            + "  bitsPerKey:"
    //            + BLOOM_FILTER_BITS_PER_KEY);

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
      case VECTOR:
        return new TimeStatistics();
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  public static int getSizeByType(TSDataType type) {
    switch (type) {
      case INT32:
        return IntegerStatistics.INTEGER_STATISTICS_FIXED_RAM_SIZE;
      case INT64:
        return LongStatistics.LONG_STATISTICS_FIXED_RAM_SIZE;
      case TEXT:
        return BinaryStatistics.BINARY_STATISTICS_FIXED_RAM_SIZE;
      case BOOLEAN:
        return BooleanStatistics.BOOLEAN_STATISTICS_FIXED_RAM_SIZE;
      case DOUBLE:
        return DoubleStatistics.DOUBLE_STATISTICS_FIXED_RAM_SIZE;
      case FLOAT:
        return FloatStatistics.FLOAT_STATISTICS_FIXED_RAM_SIZE;
      case VECTOR:
        return TimeStatistics.TIME_STATISTICS_FIXED_RAM_SIZE;
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  public abstract TSDataType getType();

  public int getSerializedSize() {
    return getSerializedSize(false);
  }

  public int getSerializedSize(boolean isChunkMetaData) {
    return ReadWriteForEncodingUtils.uVarIntSize(count) // count
        + 16 // startTime, endTime
        + (!isChunkMetaData ? getStatsSize() : getChunkMetaDataStatsSize());
  }

  public abstract int getStatsSize();

  public int getChunkMetaDataStatsSize() {
    return getStatsSize();
  }

  public int serialize(OutputStream outputStream) throws IOException {
    return serialize(outputStream, false);
  }

  public int serialize(OutputStream outputStream, boolean isChunkMetaData) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(count, outputStream);
    byteLen += ReadWriteIOUtils.write(startTime, outputStream);
    byteLen += ReadWriteIOUtils.write(endTime, outputStream);
    // value statistics of different data type
    if (!isChunkMetaData) byteLen += serializeStats(outputStream);
    else byteLen += serializeChunkMetadataStat(outputStream);
    return byteLen;
  }

  abstract int serializeStats(OutputStream outputStream) throws IOException;

  int serializeChunkMetadataStat(OutputStream outputStream) throws IOException {
    return serializeStats(outputStream);
  }

  /** read data from the inputStream. */
  public abstract void deserialize(InputStream inputStream) throws IOException;

  public abstract void deserialize(ByteBuffer byteBuffer) throws IOException;

  public void setPageStatFromChunkMetaDataStat(
      Statistics<? extends Serializable> stat, int pageID) {
    // no-op
  }

  public abstract T getMinValue();

  public abstract T getMaxValue();

  public abstract T getFirstValue();

  public abstract T getLastValue();

  public abstract double getSumDoubleValue();

  public abstract long getSumLongValue();

  /**
   * merge parameter to this statistic
   *
   * @throws StatisticsClassException cannot merge statistics
   */
  @SuppressWarnings("unchecked")
  public void mergeStatistics(Statistics<? extends Serializable> stats) {
    if (this.getClass() == stats.getClass()) {
      if (stats.startTime < this.startTime) {
        this.startTime = stats.startTime;
      }
      if (stats.endTime > this.endTime) {
        this.endTime = stats.endTime;
      }
      // must be sure no overlap between two statistics
      this.count += stats.count;
      mergeStatisticsValue((Statistics<T>) stats);
      isEmpty = false;
    } else {
      Class<?> thisClass = this.getClass();
      Class<?> statsClass = stats.getClass();
      LOG.warn("Statistics classes mismatched,no merge: {} v.s. {}", thisClass, statsClass);

      throw new StatisticsClassException(thisClass, statsClass);
    }
  }

  public void mergeChunkMetadataStatValue(Statistics<T> stats) {
    mergeStatisticsValue((Statistics<T>) stats);
  }

  public void mergeChunkMetadataStat(Statistics<? extends Serializable> stats) {
    if (this.getClass() == stats.getClass()) {
      if (stats.startTime < this.startTime) {
        this.startTime = stats.startTime;
      }
      if (stats.endTime > this.endTime) {
        this.endTime = stats.endTime;
      }
      // must be sure no overlap between two statistics
      this.count += stats.count;
      mergeChunkMetadataStatValue((Statistics<T>) stats);
      isEmpty = false;
    } else {
      Class<?> thisClass = this.getClass();
      Class<?> statsClass = stats.getClass();
      LOG.warn("Statistics classes mismatched,no merge: {} v.s. {}", thisClass, statsClass);

      throw new StatisticsClassException(thisClass, statsClass);
    }
  }

  public void update(long time, boolean value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, int value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, long value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, float value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, double value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, Binary value) {
    update(time);
    updateStats(value);
  }

  public void update(long time) {
    if (time < startTime) {
      startTime = time;
    }
    if (time > endTime) {
      endTime = time;
    }
    count++;
    if (ENABLE_BLOOM_FILTER) updateBF(time);
  }

  public void updateBF(long time) {
    ; // no-op
  }

  public void update(long[] time, boolean[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, int[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, long[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, float[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, double[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, Binary[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, int batchSize) {
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    count += batchSize;
    if (ENABLE_BLOOM_FILTER) for (int i = 0; i < batchSize; i++) updateBF(time[i]);
  }

  protected abstract void mergeStatisticsValue(Statistics<T> stats);

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

  public static Statistics<? extends Serializable> deserialize(
      InputStream inputStream, TSDataType dataType) throws IOException {
    Statistics<? extends Serializable> statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream));
    statistics.setStartTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.setEndTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.deserialize(inputStream);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics<? extends Serializable> deserialize(
      ByteBuffer buffer, TSDataType dataType) throws IOException {
    Statistics<? extends Serializable> statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
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

  public void setCount(int count) {
    this.count = count;
  }

  public abstract long calculateRamSize();

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

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), count, startTime, endTime);
  }

  public boolean hasBf() {
    return false;
  }

  public int getBfNum() {
    return 0;
  }

  public List<BloomFilter<Long>> getBfList() {
    return null;
  }

  public BloomFilter<Long> getBf(int bfId) {
    return null;
  }

  public long getBfMinTime(int bfId) {
    return Long.MAX_VALUE;
  }

  public long getBfMaxTime(int bfId) {
    return Long.MIN_VALUE;
  }

  public long getBfCount(int bfId) {
    return 0;
  }
}
