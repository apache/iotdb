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

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder.IntDeltaDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DoublePrecisionDecoderV2;
import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.ValuePoint;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
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
public abstract class Statistics<T> {

  private static final Logger LOG = LoggerFactory.getLogger(Statistics.class);
  /**
   * isEmpty being false means this statistic has been initialized and the max and min is not null;
   */
  protected boolean isEmpty = true;

  /** number of time-value points */
  private int count = 0;

  private long startTime = Long.MAX_VALUE;
  private long endTime = Long.MIN_VALUE;

  private StepRegress stepRegress = new StepRegress();

  public ValueIndex valueIndex = new ValueIndex();

  /** @author Yuyuan Kang */
  final String OPERATION_NOT_SUPPORT_FORMAT = "%s statistics does not support operation: %s";

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
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  public abstract TSDataType getType();

  @Deprecated
  public int getSerializedSize() {
    return ReadWriteForEncodingUtils.uVarIntSize(count) // count
        + 16 // startTime, endTime
        + getStatsSize();
    // stepRegress not counted
    // value index not counted
  }

  public abstract int getStatsSize();

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(count, outputStream);
    byteLen += ReadWriteIOUtils.write(startTime, outputStream);
    byteLen += ReadWriteIOUtils.write(endTime, outputStream);
    // value statistics of different data type
    byteLen += serializeStats(outputStream);
    // serialize stepRegress
    byteLen += serializeStepRegress(outputStream);
    // serialize value index
    byteLen += serializeValueIndex(outputStream);
    return byteLen;
  }

  int serializeValueIndex(OutputStream outputStream) throws IOException {
    valueIndex.learn(); // ensure executed once and only once
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(valueIndex.idxOut.size(), outputStream);
    // NOTE use size instead of buffer is important
    outputStream.write(valueIndex.idxOut.getBuf(), 0, valueIndex.idxOut.size());
    byteLen += valueIndex.idxOut.size();

    byteLen += ReadWriteIOUtils.write(valueIndex.valueOut.size(), outputStream);
    // NOTE use size instead of buffer is important
    outputStream.write(valueIndex.valueOut.getBuf(), 0, valueIndex.valueOut.size());
    byteLen += valueIndex.valueOut.size();

    byteLen += ReadWriteIOUtils.write(valueIndex.errorBound, outputStream);
    LOG.info("value_index_serialize_byteLen,{}", byteLen);
    return byteLen;
  }

  /**
   * slope, m: the number of segment keys, m-2 segment keys in between when m>=2. The first and the
   * last segment keys are not serialized here, because they are minTime and endTime respectively.
   */
  int serializeStepRegress(OutputStream outputStream) throws IOException {
    stepRegress.learn(); // ensure executed once and only once
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(stepRegress.getSlope(), outputStream); // K
    DoubleArrayList segmentKeys = stepRegress.getSegmentKeys();
    // t1 is startTime, tm is endTime, so no need serialize t1 and tm
    byteLen += ReadWriteIOUtils.write(segmentKeys.size(), outputStream); // m
    for (int i = 1; i < segmentKeys.size() - 1; i++) { // t2,t3,...,tm-1
      byteLen += ReadWriteIOUtils.write(segmentKeys.get(i), outputStream);
    }
    LOG.info("time_index_serialize_byteLen,{}", byteLen);
    return byteLen;
  }

  abstract int serializeStats(OutputStream outputStream) throws IOException;

  /** read data from the inputStream. */
  public abstract void deserialize(InputStream inputStream) throws IOException;

  public abstract void deserialize(ByteBuffer byteBuffer);

  //  public abstract void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes);

  /** @author Yuyuan Kang */
  public abstract MinMaxInfo<T> getMinInfo();

  /** @author Yuyuan Kang */
  public abstract MinMaxInfo<T> getMaxInfo();

  public abstract T getMinValue();

  public abstract void setMinInfo(MinMaxInfo minInfo);

  public abstract void setMaxInfo(MinMaxInfo maxInfo);

  public abstract T getMaxValue();

  /** @author Yuyuan Kang */
  public abstract long getBottomTimestamp();

  /** @author Yuyuan Kang */
  public abstract long getTopTimestamp();

  public abstract T getFirstValue();

  public abstract T getLastValue();

  public abstract double getSumDoubleValue();

  public abstract long getSumLongValue();

  //  public abstract byte[] getMinInfoBytes();
  //
  //  public abstract byte[] getMaxInfoBytes();
  //
  //  public abstract byte[] getFirstValueBytes();
  //
  //  public abstract byte[] getLastValueBytes();
  //
  //  public abstract byte[] getSumValueBytes();

  //  public abstract ByteBuffer getMinValueBuffer();
  //
  //  public abstract ByteBuffer getMaxValueBuffer();
  //
  //  public abstract ByteBuffer getFirstValueBuffer();
  //
  //  public abstract ByteBuffer getLastValueBuffer();
  //
  //  public abstract ByteBuffer getSumValueBuffer();

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
      // TODO M4-LSM assumes that there is always only one page in a chunk
      // TODO M4-LSM if there are more than one chunk in a time series, then access each
      // chunkMetadata anyway
      this.stepRegress = stats.stepRegress;
      // TODO
      this.valueIndex = stats.valueIndex;

      isEmpty = false;
    } else {
      String thisClass = this.getClass().toString();
      String statsClass = stats.getClass().toString();
      LOG.warn("Statistics classes mismatched,no merge: {} v.s. {}", thisClass, statsClass);

      throw new StatisticsClassException(this.getClass(), stats.getClass());
    }
  }

  @Deprecated
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

  /** @author Yuyuan Kang */
  public void update(long time, int value) {
    count++;
    if (time < this.startTime) {
      startTime = time;
    }
    if (time > this.endTime) {
      endTime = time;
    }
    // update time index
    updateStepRegress(time);
    updateValueIndex(value);
    updateStats(value, time);
  }

  /** @author Yuyuan Kang */
  public void update(long time, long value) {
    count++;
    if (time < this.startTime) {
      startTime = time;
    }
    if (time > this.endTime) {
      endTime = time;
    }
    updateStepRegress(time);
    updateValueIndex(value);
    updateStats(value, time);
  }

  /** @author Yuyuan Kang */
  public void update(long time, float value) {
    count++;
    if (time < this.startTime) {
      startTime = time;
    }
    if (time > this.endTime) {
      endTime = time;
    }
    updateStepRegress(time);
    updateValueIndex(value);
    updateStats(value, time);
  }

  /** @author Yuyuan Kang */
  public void update(long time, double value) {
    count++;
    if (time < this.startTime) {
      startTime = time;
    }
    if (time > this.endTime) {
      endTime = time;
    }
    updateStepRegress(time);
    updateValueIndex(value);
    updateStats(value, time);
  }

  @Deprecated
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

  @Deprecated
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

  /** @author Yuyuan Kang */
  public void update(long[] time, int[] values, int batchSize) {
    count += batchSize;
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    updateStepRegress(time, batchSize);
    updateValueIndex(values, batchSize);
    updateStats(values, time, batchSize);
  }

  /** @author Yuyuan Kang */
  public void update(long[] time, long[] values, int batchSize) {
    count += batchSize;
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    updateStepRegress(time, batchSize);
    updateValueIndex(values, batchSize);
    updateStats(values, time, batchSize);
  }

  /** @author Yuyuan Kang */
  public void update(long[] time, float[] values, int batchSize) {
    count += batchSize;
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    updateStepRegress(time, batchSize);
    updateValueIndex(values, batchSize);
    updateStats(values, time, batchSize);
  }

  /** @author Yuyuan Kang */
  public void update(long[] time, double[] values, int batchSize) {
    count += batchSize;
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    updateStepRegress(time, batchSize);
    updateValueIndex(values, batchSize);
    updateStats(values, time, batchSize);
  }

  @Deprecated
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

  /** @author Yuyuan Kang */
  public abstract void updateMinInfo(T val, long timestamp);

  /** @author Yuyuan Kang */
  public abstract void updateMaxInfo(T val, long timestamp);

  void updateStats(boolean value) {
    throw new UnsupportedOperationException();
  }

  void updateStepRegress(long timestamp) {
    stepRegress.insert(timestamp);
  }

  void updateStepRegress(long[] timestamps, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStepRegress(timestamps[i]);
    }
  }

  void updateValueIndex(int value) {
    valueIndex.insert(value);
  }

  void updateValueIndex(long value) {
    valueIndex.insert(value);
  }

  void updateValueIndex(float value) {
    valueIndex.insert(value);
  }

  void updateValueIndex(double value) {
    valueIndex.insert(value);
  }

  void updateValueIndex(int[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateValueIndex(values[i]);
    }
  }

  void updateValueIndex(long[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateValueIndex(values[i]);
    }
  }

  void updateValueIndex(float[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateValueIndex(values[i]);
    }
  }

  void updateValueIndex(double[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateValueIndex(values[i]);
    }
  }

  /** @author Yuyuan Kang */
  public void updateStats(int value, long timestamp) {
    throw new UnsupportedOperationException();
  }

  /** @author Yuyuan Kang */
  public void updateStats(long value, long timestamp) {
    throw new UnsupportedOperationException();
  }

  /** @author Yuyuan Kang */
  public void updateStats(float value, long timestamp) {
    throw new UnsupportedOperationException();
  }

  /** @author Yuyuan Kang */
  public void updateStats(double value, long timestamp) {
    throw new UnsupportedOperationException();
  }

  void updateStats(Binary value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(boolean[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  /** @author Yuyuan Kang */
  void updateStats(int[] values, long[] timestamps, int batchSize) {
    throw new UnsupportedOperationException();
  }

  /** @author Yuyuan Kang */
  void updateStats(long[] values, long[] timestamps, int batchSize) {
    throw new UnsupportedOperationException();
  }

  /** @author Yuyuan Kang */
  void updateStats(float[] values, long[] timestamps, int batchSize) {
    throw new UnsupportedOperationException();
  }

  /** @author Yuyuan Kang */
  void updateStats(double[] values, long[] timestamps, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(Binary[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  /**
   * @param min min timestamp
   * @param max max timestamp
   * @author Yuyuan Kang This method with two parameters is only used by {@code unsequence} which
   *     updates/inserts/deletes timestamp.
   */
  public void updateStats(long min, long bottomTimestamp, long max, long topTimestamp) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  public static Statistics deserialize(InputStream inputStream, TSDataType dataType)
      throws IOException {
    Statistics statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream));
    statistics.setStartTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.setEndTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.deserialize(inputStream);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics deserialize(ByteBuffer buffer, TSDataType dataType) throws IOException {
    Statistics statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
    statistics.setStartTime(ReadWriteIOUtils.readLong(buffer));
    statistics.setEndTime(ReadWriteIOUtils.readLong(buffer));
    statistics.deserialize(buffer);
    statistics.deserializeStepRegress(buffer);
    statistics.deserializeValueIndex(buffer);
    statistics.isEmpty = false;
    return statistics;
  }

  void deserializeValueIndex(ByteBuffer buffer) throws IOException {
    // add the first point
    valueIndex.modelPointIdx_list.add(1);
    switch (getType()) {
        //      case INT32:
        //        int intV = (int) getFirstValue();
        //        valueIndex.modelPointVal_list.add((double) intV);
        //        valueIndex.sortedModelPoints.add(new ValuePoint(1, (double) intV));
        //        break;
      case INT64:
        long longV = (Long) getFirstValue();
        valueIndex.modelPointVal_list.add((double) longV);
        valueIndex.sortedModelPoints.add(new ValuePoint(1, (double) longV));
        break;
        //      case FLOAT:
        //        float floatV = (float) getFirstValue();
        //        valueIndex.modelPointVal_list.add((double) floatV);
        //        valueIndex.sortedModelPoints.add(new ValuePoint(1, (double) floatV));
        //        break;
      case DOUBLE:
        double doubleV = (Double) getFirstValue();
        valueIndex.modelPointVal_list.add(doubleV);
        valueIndex.sortedModelPoints.add(new ValuePoint(1, doubleV));
        break;
      default:
        throw new IOException("unsupported data type");
    }

    // sdt point idx of the points in between the first and the last points
    int idxSize = ReadWriteIOUtils.readInt(buffer);
    if (idxSize > 0) {
      ByteBuffer idxBuffer = buffer.slice();
      idxBuffer.limit(idxSize);
      Decoder idxDecoder = new IntDeltaDecoder();
      while (idxDecoder.hasNext(idxBuffer)) {
        int idx = idxDecoder.readInt(idxBuffer);
        valueIndex.modelPointIdx_list.add(idx);
        valueIndex.sortedModelPoints.add(new ValuePoint(idx, 0));
      }
    }

    // sdt point value of the points in between the first and the last points
    buffer.position(buffer.position() + idxSize);
    int valueSize = ReadWriteIOUtils.readInt(buffer);
    if (valueSize > 0) {
      ByteBuffer valueBuffer = buffer.slice();
      valueBuffer.limit(valueSize);
      Decoder valueDecoder = new DoublePrecisionDecoderV2();
      int n = 0;
      while (valueDecoder.hasNext(valueBuffer)) {
        double value = valueDecoder.readDouble(valueBuffer);
        valueIndex.modelPointVal_list.add(value);
        // NOTE: n+1 because the first point already added
        valueIndex.sortedModelPoints.get(n + 1).value = value;
        n++;
      }
    }

    // add the last point except the first point
    if (count >= 2) { // otherwise only one point no need to store again
      valueIndex.modelPointIdx_list.add(count);
      switch (getType()) {
          //        case INT32:
          //          int intV = (int) getLastValue();
          //          valueIndex.modelPointVal_list.add((double) intV);
          //          valueIndex.sortedModelPoints.add(new ValuePoint(count, (double) intV));
          //          break;
        case INT64:
          long longV = (Long) getLastValue();
          valueIndex.modelPointVal_list.add((double) longV);
          valueIndex.sortedModelPoints.add(new ValuePoint(count, (double) longV));
          break;
          //        case FLOAT:
          //          float floatV = (float) getLastValue();
          //          valueIndex.modelPointVal_list.add((double) floatV);
          //          valueIndex.sortedModelPoints.add(new ValuePoint(count, (double) floatV));
          //          break;
        case DOUBLE:
          double doubleV = (Double) getLastValue();
          valueIndex.modelPointVal_list.add(doubleV);
          valueIndex.sortedModelPoints.add(new ValuePoint(count, doubleV));
          break;
        default:
          throw new IOException("unsupported data type");
      }
    }

    // error bound
    buffer.position(buffer.position() + valueSize);
    valueIndex.errorBound = ReadWriteIOUtils.readDouble(buffer);

    // sort by value from small to big
    Collections.sort(valueIndex.sortedModelPoints);
  }

  void deserializeStepRegress(ByteBuffer byteBuffer) {
    this.stepRegress.setSlope(ReadWriteIOUtils.readDouble(byteBuffer)); // K
    int m = ReadWriteIOUtils.readInt(byteBuffer); // m
    DoubleArrayList segmentKeys = new DoubleArrayList();
    segmentKeys.add(this.startTime); // t1
    if (m > 1) { // TODO DEBUG
      for (int i = 0; i < m - 2; i++) { // t2,t3,...,tm-1
        segmentKeys.add(ReadWriteIOUtils.readDouble(byteBuffer));
      }
      segmentKeys.add(this.endTime);
    }
    this.stepRegress.setSegmentKeys(segmentKeys);
    if (m > 1) {
      // don't forget this, execute once and only once when
      this.stepRegress.inferInterceptsFromSegmentKeys();
    }
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public StepRegress getStepRegress() {
    return stepRegress;
  }

  public int getCount() {
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
}
