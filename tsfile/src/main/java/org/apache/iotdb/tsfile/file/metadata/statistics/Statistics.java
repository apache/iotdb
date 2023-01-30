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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

  static final int maxp = 11; // AR orders + 1 = maxp

  private long timeInterval = 0;
  private double[] covariances = new double[maxp];
  private double[] firstPoints = new double[maxp];
  private double[] lastPoints = new double[maxp];
  private List<Long> timeWindow = new ArrayList<>();
  private List<Double> valueWindow = new ArrayList<>();
  private long startTime = Long.MAX_VALUE;
  private long endTime = Long.MIN_VALUE;

  static final String STATS_UNSUPPORTED_MSG = "%s statistics does not support: %s";

  /**
   * static method providing statistic instance for respective data type.
   *
   * @param type - data type
   * @return Statistics
   */
  public static Statistics<? extends Serializable> getStatsByType(TSDataType type) {
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
    return ReadWriteForEncodingUtils.uVarIntSize(count) // count
        + 16 // startTime, endTime
        + 8 // time interval
        + 8 * maxp * 3 // covariances, firstPoints, lastPoints
        + getStatsSize();
  }

  public abstract int getStatsSize();

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    System.out.println("========================");
    System.out.println(this.timeWindow.size());
    System.out.println("========================");
    if (this.timeWindow.size() >= maxp) {
      updateStatistics();
    }
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(count, outputStream);
    byteLen += ReadWriteIOUtils.write(startTime, outputStream);
    byteLen += ReadWriteIOUtils.write(endTime, outputStream);
    byteLen += ReadWriteIOUtils.write(timeInterval, outputStream);
    for (int i = 0; i < maxp; i++) {
      byteLen += ReadWriteIOUtils.write(covariances[i], outputStream);
    }
    for (int i = 0; i < maxp; i++) {
      byteLen += ReadWriteIOUtils.write(firstPoints[i], outputStream);
    }
    for (int i = 0; i < maxp; i++) {
      byteLen += ReadWriteIOUtils.write(lastPoints[i], outputStream);
    }
    // value statistics of different data type
    byteLen += serializeStats(outputStream);
    return byteLen;
  }

  public void updateStatistics() {

    int length = timeWindow.size();
    timeInterval = calTimeInterval();

    for (int i = 0; i < maxp; i++) {
      covariances[i] = 0;
      for (int j = 0; j < length - i; j++) {
        if (j + i < length) covariances[i] += valueWindow.get(j) * valueWindow.get(j + i);
      }
      System.out.print(covariances[i] + ", ");
    }
    System.out.print("\n");

    if (valueWindow.size() < maxp) {
      firstPoints = new double[maxp];
      lastPoints = new double[maxp];
      for (int i = 0; i < valueWindow.size(); i++) {
        firstPoints[i] = valueWindow.get(i);
        lastPoints[i] = valueWindow.get(i);
      }
    } else {
      for (int j = 0; j < maxp; j++) firstPoints[j] = valueWindow.get(j);
      for (int j = length - maxp; j < length; j++)
        lastPoints[j - length + maxp] = valueWindow.get(j);
    }
    for (int j = 0; j < firstPoints.length; j++) System.out.print(firstPoints[j] + ", ");
    System.out.print("\n");
    for (int j = 0; j < lastPoints.length; j++) System.out.print(lastPoints[j] + ", ");
    System.out.print("\n");
  }

  private long calTimeInterval() {
    int count = 0;
    long maxFreqInterval = timeWindow.get(1) - timeWindow.get(0);
    for (int i = 2; i < timeWindow.size(); i++) {
      if (maxFreqInterval == timeWindow.get(i) - timeWindow.get(i - 1)) count++;
      else {
        count--;
        if (count == 0) {
          maxFreqInterval = timeWindow.get(i) - timeWindow.get(i - 1);
          count++;
        }
      }
    }
    return maxFreqInterval;
  }

  abstract int serializeStats(OutputStream outputStream) throws IOException;

  /** read data from the inputStream. */
  public abstract void deserialize(InputStream inputStream) throws IOException;

  public abstract void deserialize(ByteBuffer byteBuffer);

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
    // TODO: merge covariances and coefficients
    if (this.getClass() == stats.getClass()) {
      this.timeInterval = stats.timeInterval;
      this.timeWindow = stats.timeWindow;
      this.valueWindow = stats.valueWindow;
      this.firstPoints = stats.firstPoints;
      this.lastPoints = stats.lastPoints;

      if (!stats.isEmpty) {
        if (stats.startTime < this.startTime) {
          this.startTime = stats.startTime;
        }
        if (stats.endTime > this.endTime) {
          this.endTime = stats.endTime;
        }
        //        for (int i = 0; i < maxp; i++){
        //          this.covariances[i] += stats.covariances[i];
        //          for (int k = 0; k < lastPoints.length; k++)
        //            if (k + i - lastPoints.length >= 0 && k + i -lastPoints.length <
        // stats.firstPoints.length )
        //              this.covariances[i] += lastPoints[k] * stats.firstPoints[k + i -
        // lastPoints.length];
        //        }
        //        if (stats.endTime < this.startTime){
        //          this.firstPoints = stats.firstPoints;
        //        }
        //        if (stats.startTime > this.endTime){
        //          this.lastPoints = stats.lastPoints;
        //        }

        // must be sure no overlap between two statistics
        this.count += stats.count;
        mergeStatisticsValue((Statistics<T>) stats);
        isEmpty = false;
      }
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
    this.timeWindow.add(time);
    this.valueWindow.add(value);
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
  }

  public void updateTimeAndValueWindow(long time, double value) {
    timeWindow.add(time);
    valueWindow.add(value);
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
    statistics.setTimeInterval(ReadWriteIOUtils.readLong(inputStream));
    for (int i = 0; i < maxp; i++) {
      statistics.setCovariances(ReadWriteIOUtils.readDouble(inputStream), i);
    }
    for (int i = 0; i < maxp; i++) {
      statistics.setFirstPoints(ReadWriteIOUtils.readDouble(inputStream), i);
    }
    for (int i = 0; i < maxp; i++) {
      statistics.setLastPoints(ReadWriteIOUtils.readDouble(inputStream), i);
    }
    statistics.deserialize(inputStream);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics<? extends Serializable> deserialize(
      ByteBuffer buffer, TSDataType dataType) {
    Statistics<? extends Serializable> statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
    statistics.setStartTime(ReadWriteIOUtils.readLong(buffer));
    statistics.setEndTime(ReadWriteIOUtils.readLong(buffer));
    statistics.setTimeInterval(ReadWriteIOUtils.readLong(buffer));
    for (int i = 0; i < maxp; i++) {
      statistics.setCovariances(ReadWriteIOUtils.readDouble(buffer), i);
    }
    for (int i = 0; i < maxp; i++) {
      statistics.setFirstPoints(ReadWriteIOUtils.readDouble(buffer), i);
    }
    for (int i = 0; i < maxp; i++) {
      statistics.setLastPoints(ReadWriteIOUtils.readDouble(buffer), i);
    }
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

  public int getCount() {
    return count;
  }

  public List<Long> getTimeWindow() {
    return timeWindow;
  }

  public List<Double> getValueWindow() {
    return valueWindow;
  }

  public double[] getFirstPoints() {
    return firstPoints;
  }

  public double[] getLastPoints() {
    return lastPoints;
  }

  public double[] getCovariances() {
    return covariances;
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

  public void setTimeInterval(long interval) {
    this.timeInterval = interval;
  }

  public void setFirstPoints(double firstPoint, int i) {
    this.firstPoints[i] = firstPoint;
  }

  public void setFirstPoints(List<Double> firstPoints) {
    this.firstPoints = firstPoints.stream().mapToDouble(Double::valueOf).toArray();
  }

  public void setLastPoints(double lastPoint, int i) {
    this.lastPoints[i] = lastPoint;
  }

  public void setLastPoints(List<Double> lastPoints) {
    this.lastPoints = lastPoints.stream().mapToDouble(Double::valueOf).toArray();
  }

  public void setCovariances(double covariance, int i) {
    this.covariances[i] = covariance;
  }

  public void setCovariances(double[] covariances) {
    for (int i = 0; i < covariances.length; i++) {
      this.covariances[i] = covariances[i];
    }
  }

  public long getTimeInterval() {
    return this.timeInterval;
  }

  public void clearTimeAndValueWindow() {
    this.timeWindow.clear();
    this.valueWindow.clear();
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
}
