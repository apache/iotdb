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
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.HeapLongKLLSketch;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class KLLFloatsMedianAggrResult extends AggregateResult {
  private final long deltaForUnsignedCompare = 1L << 63;
  private TSDataType seriesDataType;
  private long n, K1, K2;
  private int bitsOfDataType, iteration; // bitsOfDataType == bitsCounted + bitsConcerned
  private long L, R, lastL;
  private HeapLongKLLSketch worker;

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

  public KLLFloatsMedianAggrResult(TSDataType seriesDataType) throws UnSupportedDataTypeException {
    super(TSDataType.DOUBLE, AggregationType.EXACT_MEDIAN_KLL_FLOATS);
    this.seriesDataType = seriesDataType;
    reset();
  }

  private long dataToLong(Object data) throws UnSupportedDataTypeException {
    long result;
    switch (seriesDataType) {
      case INT32:
        return (int) data;
      case FLOAT:
        result = Float.floatToIntBits((float) data);
        return (float) data >= 0f ? result : result ^ Long.MAX_VALUE;
      case INT64:
        return (long) data;
      case DOUBLE:
        result = Double.doubleToLongBits((double) data);
        return (double) data >= 0d ? result : result ^ Long.MAX_VALUE;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation MEDIAN : %s", seriesDataType));
    }
  }

  private double longToResult(long result) throws UnSupportedDataTypeException {
    switch (seriesDataType) {
      case INT32:
        return (double) (result);
      case FLOAT:
        result = (result >>> 31) == 0 ? result : result ^ Long.MAX_VALUE;
        return Float.intBitsToFloat((int) (result));
      case INT64:
        return (double) (result);
      case DOUBLE:
        result = (result >>> 63) == 0 ? result : result ^ Long.MAX_VALUE;
        return Double.longBitsToDouble(result);
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
    long dataL = dataToLong(data);
    if (iteration == 0) n++;
    if (lastL <= dataL && dataL < L) K1--;
    else if (L <= dataL && dataL <= R) worker.update(dataL);
  }

  @Override
  public void startIteration() {
    worker = new HeapLongKLLSketch(maxMemoryByte);
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
    if (worker.exactResult()) {
      long v1 = worker.getExactResult((int) K1 - 1), v2 = worker.getExactResult((int) K2 - 1);
      double ans = 0.5 * (longToResult(v1) + longToResult(v2));
      setDoubleValue(ans);
      hasFinalResult = true;
      System.out.println("[kll POINT DEBUG] calc over  answer:" + getDoubleValue());
      return;
    }
    long err = (long) (1.5 * worker.getN() / worker.getNumLen());
    long minK = K1 - 1 - err * 3, maxK = K2 - 1 + err * 3;
    L = worker.findMinValueWithRank(minK);
    R = worker.findMaxValueWithRank(maxK);
    if (L == R + 1) {
      if (worker.getApproxRank(L) < K1) R = L;
      else L = R;
    }
    //    System.out.println("\t[KLL STAT DEBUG] cntLR found.");
    System.out.println("\t[KLL POINT DEBUG] avg_err:" + err + "\tcntL:" + L + "\tcntR:" + R + "\n");

    if (L == R) {
      double ans = longToResult(L);
      setDoubleValue(ans);
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
    long minVal = dataToLong(statistics.getMinValue());
    long maxVal = dataToLong(statistics.getMaxValue());
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
    L = Long.MIN_VALUE;
    R = Long.MAX_VALUE;
    iteration = 0;
  }

  @Override
  public boolean canUpdateFromStatistics(Statistics statistics) {
    //    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();
    //    Comparable<Object> maxVal = (Comparable<Object>) statistics.getMaxValue();
    long minVal = dataToLong(statistics.getMinValue());
    long maxVal = dataToLong(statistics.getMaxValue());
    if (minVal == maxVal) return true;
    if (maxVal < L) {
      return true;
    } else if (minVal > R) return true;
    return false;
  }

  @Override
  public boolean groupByLevelBeforeAggregation() {
    return true;
  }
}
