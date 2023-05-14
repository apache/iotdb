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

package org.apache.iotdb.db.query.aggregation.impl;

import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.utils.ValueIterator;
import org.apache.iotdb.db.utils.quantiles.KLLDoublesForMedian;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.iotdb.tsfile.file.metadata.statistics.Statistics.STATISTICS_PAGE_MAXSIZE;

public class KLLDebugFullReadingAggrResult extends AggregateResult {
  private final long deltaForUnsignedCompare = 1L << 63;
  private TSDataType seriesDataType;
  private long n, K1, K2;
  private int bitsOfDataType, iteration; // bitsOfDataType == bitsCounted + bitsConcerned
  private double L, R, lastL;
  private KLLDoublesForMedian worker;
  long FULL_READ = 0, DEMAND_READ = 0;
  boolean FULL_READING_PAGE = false;

  private boolean hasFinalResult;

  private int getBitsOfDataType() {
    switch (seriesDataType) {
      case INT32:
      case FLOAT:
        return 32;
      case INT64:
      case DOUBLE:
        return 64;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation MEDIAN : %s", seriesDataType));
    }
  }

  private boolean hasTwoMedians() {
    return (n & 1) == 0;
  }

  public KLLDebugFullReadingAggrResult(TSDataType seriesDataType)
      throws UnSupportedDataTypeException {
    super(TSDataType.DOUBLE, AggregationType.EXACT_MEDIAN_KLL_DEBUG_FULL_READING);
    this.seriesDataType = seriesDataType;
    reset();
  }

  // turn FLOAT/INT32 to unsigned long keeping relative order
  private long dataToLongBits(Object data) throws UnSupportedDataTypeException {
    long longBits;
    switch (seriesDataType) {
      case INT32:
        return (int) data + (1L << 31);
      case FLOAT:
        longBits = Float.floatToIntBits((float) data) + (1L << 31);
        return (float) data >= 0f ? longBits : longBits ^ 0x7F800000L;
      case INT64:
        return (long) data + (1L << 63);
      case DOUBLE:
        longBits = Double.doubleToLongBits((double) data) + (1L << 63);
        return (double) data >= 0d ? longBits : longBits ^ 0x7FF0000000000000L;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation MEDIAN : %s", seriesDataType));
    }
  }

  private double dataToDouble(Object data) throws UnSupportedDataTypeException {
    switch (seriesDataType) {
      case INT32:
        return (int) data;
      case FLOAT:
        return (float) data;
      case INT64:
        return (long) data;
      case DOUBLE:
        return (double) data;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation MEDIAN : %s", seriesDataType));
    }
  }

  private double longBitsToResult(long longBits) throws UnSupportedDataTypeException {
    switch (seriesDataType) {
      case INT32:
        return (double) (longBits - (1L << 31));
      case FLOAT:
        longBits = (longBits >>> 31) > 0 ? longBits : longBits ^ 0x7F800000L;
        return Float.intBitsToFloat((int) (longBits - (1L << 31)));
      case INT64:
        return (double) (longBits - (1L << 63));
      case DOUBLE:
        longBits = (longBits >>> 63) > 0 ? longBits : longBits ^ 0x7FF0000000000000L;
        return Double.longBitsToDouble(longBits - (1L << 63));
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation MEDIAN : %s", seriesDataType));
    }
  }

  //  private void updateStatusFromData(Object data, long times) {
  //    long signedLongBits = dataToLongBits(data) ^ deltaForUnsignedCompare;
  //    if (iteration == 0) n += times;
  //    if (hasTwoDividedMedians) {
  //      if (L1 <= signedLongBits && signedLongBits <= R1)
  //        maxInInterval1 = Math.max(maxInInterval1, signedLongBits);
  //
  //      if (L2 <= signedLongBits && signedLongBits <= R2)
  //        minInInterval2 = Math.min(minInInterval2, signedLongBits);
  //      return;
  //    }
  //    if (lastL1 <= signedLongBits && signedLongBits < L1) K1 -= times;
  //    else if (L1 <= signedLongBits && signedLongBits <= R1) worker.add(signedLongBits, times);
  //  }

  private void updateStatusFromData(Object data) {
    FULL_READ++;
    double dataF = dataToDouble(data);
    if (iteration == 0) n++;
    if (lastL <= dataF && dataF < L) K1--;
    else if (L <= dataF && dataF <= R) worker.add(dataF);
  }

  @Override
  public void startIteration() {
    if (iteration == 0) {
      worker = new KLLDoublesForMedian();
    } else {
      worker.reset();
    }
  }

  @Override
  public void finishIteration() {
    iteration++;
    if (n == 0) {
      hasFinalResult = true;
      return;
    }
    lastL = L;
    if (iteration == 1) { // first iteration over
      K1 = (n + 1) >> 1;
    }
    K2 = hasTwoMedians() ? (K1 + 1) : K1;
    if (worker.isExactResult()) {
      //      System.out.println("[kll floats DEBUG] "+worker.sketch.getN()+" ???
      // "+worker.sketch.getK());
      //      System.out.println("[kll floats DEBUG] "+"!!! "+n);
      double v1 = worker.sketch.getQuantile(1.0 * (K1 - 1) / worker.sketch.getN());
      double v2 = worker.sketch.getQuantile(1.0 * (K2 - 1) / worker.sketch.getN());
      setDoubleValue(FULL_READ);
      hasFinalResult = true;
      System.out.println("[kll floats DEBUG] calc over  answer:" + getDoubleValue());
      return;
    }
    List<Double> iterationResult = worker.findResultRange(K1, K2);
    L = iterationResult.get(0);
    R = iterationResult.get(1);
    System.out.println(
        "[kll floats DEBUG] "
            + L
            + "~"
            + R
            + "  K1,K2:"
            + K1
            + ","
            + K2
            + " cntN:"
            + worker.sketch.getN()
            + " eps:"
            + worker.sketch.getNormalizedRankError(false));
    if (L == R) {
      setDoubleValue(FULL_READ);
      hasFinalResult = true;
    }
  }

  @Override
  protected boolean hasCandidateResult() {
    return hasFinalResult && n > 0;
  }

  @Override
  public Double getResult() {
    return hasCandidateResult() ? getDoubleValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    switch (statistics.getType()) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in aggregation MEDIAN : %s", statistics.getType()));
    }
    double minVal = dataToDouble(statistics.getMinValue());
    double maxVal = dataToDouble(statistics.getMaxValue());
    if (maxVal < L) {
      K1 -= statistics.getCount();
    } else if (minVal > R) return;
    if (minVal == maxVal) {
      for (long i = statistics.getCount(); i > 0; i--) updateStatusFromData(minVal);
    } /*else
      throw new QueryProcessException("Failed to update median aggregation result from statistics.");*/
  }

  @Override
  public void updateResultFromPageData(IBatchDataIterator batchIterator) {
    updateResultFromPageData(batchIterator, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void updateResultFromPageData(
      IBatchDataIterator batchIterator, long minBound, long maxBound) {
    while (batchIterator.hasNext()) {
      if (batchIterator.currentTime() >= maxBound || batchIterator.currentTime() < minBound) {
        break;
      }
      updateStatusFromData(batchIterator.currentValue());
      batchIterator.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        updateStatusFromData(values[i]);
      }
    }
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, ValueIterator valueIterator) {
    //    List<Object> tmp = new ArrayList<>();
    while (valueIterator.hasNext()) {
      updateStatusFromData(valueIterator.next());
      //      Object tmpObj = valueIterator.next();
      //      updateStatusFromData(tmpObj);
      //      tmp.add(tmpObj);
    }
    //
    // System.out.println("\t\t[MEDIAN]"+this.hashCode()+"[updateResultUsingValues]"+tmp.toString());
  }

  @Override
  public int maxIteration() {
    return bitsOfDataType / 16;
  }

  @Override
  public boolean hasFinalResult() {
    return hasFinalResult;
  }

  @Override
  public void merge(AggregateResult another) {
    //    System.out.println("[DEBUG] [merge] " + this.getResult() + "  " + another.getResult());
    // merge not supported
    //        throw new QueryProcessException("Can't merge MedianAggregateResult");
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {
    this.seriesDataType = TSDataType.deserialize(buffer.get());
    this.n = buffer.getLong();
    // TODO
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(seriesDataType, outputStream);
    ReadWriteIOUtils.write(n, outputStream);
    // TODO
  }

  public long getN() {
    return n;
  }

  @Override
  public void reset() {
    super.reset();
    n = 0;
    bitsOfDataType = getBitsOfDataType();
    hasFinalResult = false;
    L = -Double.MAX_VALUE;
    R = Double.MAX_VALUE;
    iteration = 0;
    FULL_READ = 0;
    FULL_READING_PAGE = false;
    DEMAND_READ = 0;
  }

  @Override
  public boolean canUpdateFromStatistics(Statistics statistics) {
    FULL_READING_PAGE = false;
    //    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();
    //    Comparable<Object> maxVal = (Comparable<Object>) statistics.getMaxValue();
    double minVal = dataToDouble(statistics.getMinValue());
    double maxVal = dataToDouble(statistics.getMaxValue());
    if (minVal == maxVal) return true;
    if (maxVal < L) {
      return true;
    } else if (minVal > R) return true;

    if (statistics.getCount() <= STATISTICS_PAGE_MAXSIZE) {
      FULL_READING_PAGE = true;
    }
    return false;
  }

  @Override
  public boolean groupByLevelBeforeAggregation() {
    return true;
  }
}
