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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.KMeans;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.KShape;
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
import java.util.Arrays;
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

  private static final TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();

  private static final Logger LOG = LoggerFactory.getLogger(Statistics.class);
  /**
   * isEmpty being false means this statistic has been initialized and the max and min is not null;
   */
  protected boolean isEmpty = true;

  /** number of time-value points */
  private int count = 0;

  private long startTime = Long.MAX_VALUE;
  private long endTime = Long.MIN_VALUE;
  private List<Long> timeWindow = new ArrayList<>();
  private List<Double> valueWindow = new ArrayList<>();
  private static final int k = tsFileConfig.getClusterNum(); // cluster num
  private static final int l = tsFileConfig.getSeqLength(); // subsequence length
  private static final int edK = k + 2; // cluster num for euclidean
  public double[][][] sumMatrices = new double[k][l][l];
  public double[][] centroids = new double[k][l];
  public double[] deltas = new double[k];
  public int[] idx = new int[1000];
  public double[] headExtraPoints = new double[l];
  public double[] tailExtraPoints = new double[l];

  // pre-computed metadata for K-shape-M
  public double[][] edCentroids = new double[edK][l];
  public double[] edDeltas = new double[edK];
  public int[] edCounts = new int[edK];
  public int[] edIdx = new int[1000];

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

  public Statistics clone() {
    Statistics newInstance = new DoubleStatistics();
    newInstance.setCount((int) this.count);
    newInstance.setStartTime(this.startTime);
    newInstance.setEndTime(this.endTime);
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++)
        for (int p = 0; p < l; p++) newInstance.setSumMatrices(this.sumMatrices[i][j][p], i, j, p);
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++) newInstance.setCentroids(this.centroids[i][j], i, j);
    for (int i = 0; i < k; i++) newInstance.setDeltas(this.deltas[i], i);
    for (int i = 0; i < 1000; i++) newInstance.setIdx(this.idx[i], i);
    for (int i = 0; i < l; i++) newInstance.setHeadExtraPoints(this.headExtraPoints[i], i);
    for (int i = 0; i < l; i++) newInstance.setTailExtraPoints(this.tailExtraPoints[i], i);

    for (int i = 0; i < edK; i++)
      for (int j = 0; j < l; j++) newInstance.setEdCentroids(this.edCentroids[i][j], i, j);
    for (int i = 0; i < edK; i++) newInstance.setEdDelta(this.edDeltas[i], i);
    for (int i = 0; i < edK; i++) newInstance.setEdCounts(this.edCounts[i], i);
    for (int i = 0; i < 1000; i++) newInstance.setEdIdx(this.edIdx[i], i);
    return newInstance;
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
        + 8 * k * l * l // sumMatrices
        + 8 * k * l // centroids
        + 8 * k // deltas
        + 4 * 1000 // idx
        + 8 * edK * l // edCentroids
        + 8 * edK // edDeltas
        + 4 * edK // edCounts
        + 4 * 1000 // edIdx
        + 8 * l * 2 // headExtraPoints, tailExtraPoints
        + getStatsSize();
  }

  public abstract int getStatsSize();

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    updateStatistics();
    System.out.println("========================");
    System.out.println("k=" + k + ", l=" + l);
    System.out.println("Start time = " + startTime + ", end time = " + endTime);
    System.out.println("========================");

    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(count, outputStream);
    byteLen += ReadWriteIOUtils.write(startTime, outputStream);
    byteLen += ReadWriteIOUtils.write(endTime, outputStream);

    for (int i = 0; i < k; i++) {
      double[][] _matrix = sumMatrices[i];
      for (int j = 0; j < l; j++)
        for (int p = 0; p < l; p++) byteLen += ReadWriteIOUtils.write(_matrix[j][p], outputStream);
    }
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++) byteLen += ReadWriteIOUtils.write(centroids[i][j], outputStream);
    for (int i = 0; i < k; i++) byteLen += ReadWriteIOUtils.write(deltas[i], outputStream);
    for (int i = 0; i < 1000; i++) byteLen += ReadWriteIOUtils.write(idx[i], outputStream);

    // metadata for K-Shape-M
    for (int i = 0; i < edK; i++)
      for (int j = 0; j < l; j++)
        byteLen += ReadWriteIOUtils.write(edCentroids[i][j], outputStream);
    for (int i = 0; i < edK; i++) byteLen += ReadWriteIOUtils.write(edDeltas[i], outputStream);
    for (int i = 0; i < edK; i++) byteLen += ReadWriteIOUtils.write(edCounts[i], outputStream);
    for (int i = 0; i < 1000; i++) byteLen += ReadWriteIOUtils.write(edIdx[i], outputStream);

    for (int i = 0; i < l; i++) byteLen += ReadWriteIOUtils.write(headExtraPoints[i], outputStream);
    for (int i = 0; i < l; i++) byteLen += ReadWriteIOUtils.write(tailExtraPoints[i], outputStream);

    // value statistics of different data type
    byteLen += serializeStats(outputStream);
    return byteLen;
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
    if (this.getClass() == stats.getClass()) {
      if (!stats.isEmpty) {
        this.timeWindow = stats.timeWindow;
        this.valueWindow = stats.valueWindow;

        this.sumMatrices = stats.sumMatrices;
        this.centroids = stats.centroids;
        this.deltas = stats.deltas;
        this.idx = stats.idx;

        this.edCentroids = stats.edCentroids;
        this.edDeltas = stats.edDeltas;
        this.edCounts = stats.edCounts;
        this.edIdx = stats.edIdx;

        this.headExtraPoints = stats.headExtraPoints;
        this.tailExtraPoints = stats.tailExtraPoints;

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
    // metadata for K-Shape
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++)
        for (int p = 0; p < l; p++)
          statistics.setSumMatrices(ReadWriteIOUtils.readDouble(inputStream), i, j, p);
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++)
        statistics.setCentroids(ReadWriteIOUtils.readDouble(inputStream), i, j);
    for (int i = 0; i < k; i++) statistics.setDeltas(ReadWriteIOUtils.readDouble(inputStream), i);
    for (int i = 0; i < 1000; i++) statistics.setIdx(ReadWriteIOUtils.readInt(inputStream), i);

    // metadata for K-Shape-M
    for (int i = 0; i < edK; i++)
      for (int j = 0; j < l; j++)
        statistics.setEdCentroids(ReadWriteIOUtils.readDouble(inputStream), i, j);
    for (int i = 0; i < edK; i++)
      statistics.setEdDelta(ReadWriteIOUtils.readDouble(inputStream), i);
    for (int i = 0; i < edK; i++) statistics.setEdCounts(ReadWriteIOUtils.readInt(inputStream), i);
    for (int i = 0; i < 1000; i++) statistics.setEdIdx(ReadWriteIOUtils.readInt(inputStream), i);

    for (int i = 0; i < l; i++)
      statistics.setHeadExtraPoints(ReadWriteIOUtils.readDouble(inputStream), i);
    for (int i = 0; i < l; i++)
      statistics.setTailExtraPoints(ReadWriteIOUtils.readDouble(inputStream), i);
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
    // metadata for K-Shape
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++)
        for (int p = 0; p < l; p++)
          statistics.setSumMatrices(ReadWriteIOUtils.readDouble(buffer), i, j, p);
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++)
        statistics.setCentroids(ReadWriteIOUtils.readDouble(buffer), i, j);
    for (int i = 0; i < k; i++) statistics.setDeltas(ReadWriteIOUtils.readDouble(buffer), i);
    for (int i = 0; i < 1000; i++) statistics.setIdx(ReadWriteIOUtils.readInt(buffer), i);

    // metadata for K-Shape-M
    for (int i = 0; i < edK; i++)
      for (int j = 0; j < l; j++)
        statistics.setEdCentroids(ReadWriteIOUtils.readDouble(buffer), i, j);
    for (int i = 0; i < edK; i++) statistics.setEdDelta(ReadWriteIOUtils.readDouble(buffer), i);
    for (int i = 0; i < edK; i++) statistics.setEdCounts(ReadWriteIOUtils.readInt(buffer), i);
    for (int i = 0; i < 1000; i++) statistics.setEdIdx(ReadWriteIOUtils.readInt(buffer), i);

    for (int i = 0; i < l; i++)
      statistics.setHeadExtraPoints(ReadWriteIOUtils.readDouble(buffer), i);
    for (int i = 0; i < l; i++)
      statistics.setTailExtraPoints(ReadWriteIOUtils.readDouble(buffer), i);

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

  public void setSumMatrices(double v, int i, int j, int p) {
    this.sumMatrices[i][j][p] = v;
  }

  public void setCentroids(double v, int i, int j) {
    this.centroids[i][j] = v;
  }

  public void setDeltas(double v, int i) {
    this.deltas[i] = v;
  }

  public void setIdx(int v, int i) {
    this.idx[i] = v;
  }

  public void setHeadExtraPoints(double v, int i) {
    this.headExtraPoints[i] = v;
  }

  public void setTailExtraPoints(double v, int i) {
    this.tailExtraPoints[i] = v;
  }

  public void setEdCentroids(double v, int i, int j) {
    this.edCentroids[i][j] = v;
  }

  public void setEdDelta(double v, int i) {
    this.edDeltas[i] = v;
  }

  public void setEdIdx(int v, int i) {
    this.edIdx[i] = v;
  }

  public void setEdCounts(int v, int i) {
    this.edCounts[i] = v;
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

  private void updateStatistics() {
    if (this.timeWindow.size() < l) {
      return;
    }
    int n = this.timeWindow.size(); // all points
    //    double timeInterval = (double) calTimeInterval();

    long seqStartTime = (long) (Math.ceil(this.startTime * 1.0 / l) * l);
    long seqEndTime = (long) (Math.floor(this.endTime * 1.0 / l) * l);
    Arrays.fill(this.idx, -1);

    List<List<Double>> tmpSeqs = new ArrayList<>();
    List<Double> tmpSeq = new ArrayList<>();
    int _i = 0;
    while (_i < n) {
      if (this.timeWindow.get(_i) < seqStartTime) {
        this.headExtraPoints[_i] = this.valueWindow.get(_i);
        _i++;
        continue;
      }
      tmpSeq.add(this.valueWindow.get(_i));
      if (tmpSeq.size() == l) {
        tmpSeqs.add(tmpSeq);
        tmpSeq = new ArrayList<>();
      }
      _i++;
    }
    if (tmpSeq.size() > 0)
      for (int i = 0; i < tmpSeq.size(); i++)
        this.tailExtraPoints[l - tmpSeq.size() + i] = tmpSeq.get(i);

    double[][] X = new double[tmpSeqs.size()][l];
    for (int i = 0; i < tmpSeqs.size(); i++) {
      X[i] = tmpSeqs.get(i).stream().mapToDouble(Double::doubleValue).toArray();
      X[i] = normalize(X[i]);
    }

    int seqNum = X.length;
    int l = X[0].length;

    // pre-compute metadata for K-Shape
    KShape kshape = new KShape(k, 30);
    kshape.fit(X);
    for (int i = 0; i < k; i++)
      this.sumMatrices[i] = kshape.getSumMatrices()[i].getArray(); // k * l * l
    this.centroids = kshape.getCentroids(); // k * l
    this.deltas = kshape.getDeltas(); // k
    for (int i = 0; i < kshape.getIdx().length; i++) this.idx[i] = kshape.getIdx()[i];

    // pre-compute metadata for K-Shape-M
    Arrays.fill(this.edIdx, -1);
    Arrays.fill(this.edCounts, 0);
    KMeans clustering = new KMeans.Builder(edK, X).iterations(30).ifPlusInitial(true).build();
    this.edCentroids = clustering.getCentroids();
    this.edDeltas = clustering.getDeltas();
    for (int i = 0; i < clustering.getIdx().length; i++) this.edIdx[i] = clustering.getIdx()[i];
    for (int idx : this.edIdx) {
      if (idx == -1) break;
      this.edCounts[idx]++;
    }
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

  private double[] normalize(double[] x) {
    double sum = 0.0;
    for (double v : x) sum += v;
    double mean = sum / x.length;
    double std = 0.0;
    for (double v : x) std += (v - mean) * (v - mean);
    std = Math.sqrt(std / (x.length - 1));
    double[] res = new double[x.length];
    for (int i = 0; i < x.length; i++) res[i] = (x[i] - mean) / std;
    return res;
  }
}
