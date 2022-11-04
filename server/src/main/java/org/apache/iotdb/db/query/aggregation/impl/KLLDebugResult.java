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
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.HeapLongKLLSketch;
import org.apache.iotdb.tsfile.utils.KLLSketchForQuantile;
import org.apache.iotdb.tsfile.utils.LongKLLSketch;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;
import static org.apache.iotdb.tsfile.file.metadata.statistics.Statistics.SYNOPSIS_SIZE_IN_BYTE;

public class KLLDebugResult extends AggregateResult {
  private TSDataType seriesDataType;
  private int iteration;
  private long pageKLLNum;
  private long cntL, cntR, lastL;
  private long n, K1, heapN;
  private HeapLongKLLSketch heapKLL;
  private boolean hasFinalResult;
  private final int maxMemoryByte = 1 << 19; // half of the limit
  private final int pageAvgError = 50, pageMaxError = 127;
  private final int pageKLLMemoryByte = (68 + 15) * 8,
      pageKLLNumMemoryByte = SYNOPSIS_SIZE_IN_BYTE * Long.BYTES;
  //  private List<HeapLongKLLSketch> pageKLL;
  private List<KLLSketchForQuantile> pageKLL;
  private int pageKLLIndex;
  private final int pageKLLMaxSize = (int) Math.floor((1.0 * maxMemoryByte / pageKLLMemoryByte));
  long DEBUG = 0;

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

  private long approximateDataAvgError() {
    long dataAvgError = (long) Math.ceil(2.0 * heapN / heapKLL.getMaxMemoryNum()) + 1;
    return dataAvgError;
  }

  private long approximateStatAvgError() {
    double rate = 1.0 * pageKLLNumMemoryByte * pageKLLNum / maxMemoryByte;
    long pageStatAvgError;
    if (rate < 1.0) {
      pageStatAvgError = (long) Math.ceil(pageAvgError * Math.pow(pageKLLNum, 0.5));
      if (pageKLLNum <= 10) pageStatAvgError += pageMaxError;
    } else {
      int memKLLNum = maxMemoryByte / pageKLLNumMemoryByte;
      long memErr = (long) Math.ceil(pageAvgError * Math.pow(memKLLNum, 0.5));
      pageStatAvgError = (long) Math.ceil(rate * 0.5 * memErr + 0.5 * memErr);
    }
    return pageStatAvgError;
  }

  private long approximateMaxError() {
    return 0;
  }

  private boolean hasTwoMedians() {
    return (n & 1) == 0;
  }

  public KLLDebugResult(TSDataType seriesDataType) throws UnSupportedDataTypeException {
    super(DOUBLE, AggregationType.EXACT_MEDIAN_KLL_DEBUG);
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

  private void updateStatusFromData(Object data) {
    long dataL = dataToLong(data);
    if (iteration == 0) n++;
    if (cntL <= dataL && dataL <= cntR) {
      heapKLL.update(dataL);
      heapN++;
    } else if (lastL <= dataL && dataL < cntL) K1--;
  }

  private long getRankInKLL(long val) {
    //    long rank = 0;
    //    if (pageKLL != null) {
    //      for (HeapLongKLLSketch heapLongKLLSketch : pageKLL)
    //        if (heapLongKLLSketch != null) rank += heapLongKLLSketch.getApproxRank(val);
    //    }
    //    rank += heapKLL.getApproxRank(val);
    //    return rank;
    return heapKLL.getApproxRank(val);
  }

  public long findMaxValueWithRankLE(long K) {
    long L = Long.MIN_VALUE, R = Long.MAX_VALUE, mid;
    while (L < R) {
      mid = L + ((R - L) >>> 1);
      if (mid == L) mid++;
      //      System.out.println(
      //          "\t\t\t" + L + "\t" + R + "\t\t\tmid:" + mid + "\trank:" + getRankInKLL(mid));
      if (getRankInKLL(mid) <= K) L = mid;
      else R = mid - 1;
      //      System.out.println("\t mid:"+mid+"  mid_rank:"+getRankInKLL(mid));
    }
    return L;
  }

  public long findMinValueWithRankGE(long K) {
    long L = Long.MIN_VALUE, R = Long.MAX_VALUE, mid;
    while (L < R) {
      mid = L + ((R - L) >>> 1);
      if (mid == R) mid--;
      //      System.out.println(
      //          "\t\t\t" + L + "\t" + R + "\t\t\tmid:" + mid + "\trank:" + getRankInKLL(mid));
      if (getRankInKLL(mid) >= K) R = mid;
      else L = mid + 1;
    }
    return L;
  }

  @Override
  public void startIteration() {
    heapN = 0;
    if (iteration == 0) { // first iteration
      heapKLL = new HeapLongKLLSketch(maxMemoryByte);
      lastL = cntL = Long.MIN_VALUE;
      cntR = Long.MAX_VALUE;
      n = 0;
      pageKLLNum = 0;
      pageKLL = new ArrayList<>(pageKLLMaxSize);
      for (int i = 0; i < pageKLLMaxSize; i++) pageKLL.add(null);
      pageKLLIndex = 0;
    } else {
      heapKLL = new HeapLongKLLSketch(maxMemoryByte);
      pageKLLNum = 0;
      pageKLL = null;
      System.out.println(
          "\t[KLL STAT DEBUG] start iteration "
              + iteration
              + " cntL,R:"
              + "["
              + cntL
              + ","
              + cntR
              + "]"
              + "\tlastL:"
              + lastL
              + "\tK1:"
              + K1);
    }
  }

  @Override
  public void finishIteration() {
    System.out.println(
        "\t[KLL STAT DEBUG]"
            + "finish iteration "
            + iteration
            + " cntL,R:"
            + "["
            + cntL
            + ","
            + cntR
            + "]"
            + "\tlastL:"
            + lastL
            + "\tK1:"
            + K1);
    System.out.println(
        "\t[KLL STAT DEBUG]" + " statNum:" + pageKLLNum + " heapN:" + heapKLL.getN());
    iteration++;
    if (n == 0) {
      hasFinalResult = true;
      return;
    }
    lastL = cntL;

    if (iteration == 1) { // first iteration over
      K1 = (n + 1) >> 1;
    }
    long K2 = hasTwoMedians() ? (K1 + 1) : K1;

    System.out.println("\t[KLL STAT DEBUG]" + " K1,K2:" + K1 + ", " + K2);
    if (pageKLLNum == 0) { // all in heapKLL
      System.out.println("\t[KLL STAT DEBUG]" + " calc by heap only. N:" + heapKLL.getN());
      heapKLL.show();

      double v1 = longToResult(heapKLL.findMinValueWithRank(K1 - 1));
      //      System.out.println("\t[KLL STAT DEBUG]" + "v1:" + v1);
      double v2 = longToResult(heapKLL.findMinValueWithRank(K2 - 1));
      double ans = 0.5 * (v1 + v2);
      setDoubleValue(ans);
      hasFinalResult = true;
      return;
    }
    // iteration = 0 && there are page KLL statistics
    //    heapKLL.show();
    //    System.out.println("\t[KLL STAT DEBUG] remaining pageKLLSize:" + pageKLLIndex);
    mergePageKLL();
    heapKLL.show();
    System.out.println("\t[KLL STAT DEBUG] heapN:" + heapKLL.getN() + "\tn_true:" + n);
    double v1 = longToResult(heapKLL.findMinValueWithRank(K1 - 1));
    double v2 = longToResult(heapKLL.findMinValueWithRank(K2 - 1));
    double ans = 0.5 * (v1 + v2);
    setDoubleValue(ans);
    hasFinalResult = true;
  }

  @Override
  protected boolean hasCandidateResult() {
    return hasFinalResult && n > 0;
  }

  @Override
  public Double getResult() {
    return hasCandidateResult() ? getDoubleValue() : null;
  }

  //  private void addSketch(KLLSketchForQuantile sketch, List<HeapLongKLLSketch> a, int baseByte) {
  //    int pos0 = 0;
  //    while (pos0 < pageKLLMaxLen && a.get(pos0) != null) pos0++;
  //    HeapLongKLLSketch bigger_sketch = new HeapLongKLLSketch(baseByte << pos0);
  //    bigger_sketch.mergeWithTempSpace(sketch);
  //    for (int i = 0; i < pos0; i++) {
  //      bigger_sketch.mergeWithTempSpace(a.get(i));
  //      a.set(i, null);
  //    }
  //    if (pos0 == pageKLLMaxLen) { // mem of pageKLL list is too large.
  //      heapKLL.mergeWithTempSpace(bigger_sketch);
  //    } else a.set(pos0, bigger_sketch);
  //  }
  private void addSketch(KLLSketchForQuantile sketch) {
    if (pageKLLIndex < pageKLLMaxSize) pageKLL.set(pageKLLIndex++, sketch);
    else {
      heapKLL.mergeWithTempSpace(pageKLL);
      for (int i = 0; i < pageKLLMaxSize; i++) pageKLL.set(i, null);
      pageKLLIndex = 0;
      pageKLL.set(pageKLLIndex++, sketch);
      //      System.out.println(
      //          "\t[KLL STAT DEBUG]\theapKLL merge pageKLLList. newN: "
      //              + heapKLL.getN()
      //              + "   n_true:"
      //              + n);
      //      heapKLL.show();
    }
  }

  private void mergePageKLL() {
    HeapLongKLLSketch tmpSketch = heapKLL;
    heapKLL = new HeapLongKLLSketch(maxMemoryByte * 2);
    heapKLL.mergeWithTempSpace(tmpSketch);
    heapKLL.mergeWithTempSpace(pageKLL);
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
    if (iteration == 0) {
      n += statistics.getCount();
      pageKLLNum++;
      //      if (statistics.getType() == DOUBLE) {
      //      }
      if (statistics.getType() == DOUBLE) {
        DoubleStatistics stat = (DoubleStatistics) statistics;
        if (stat.getKllSketchNum() > 0) {
          for (LongKLLSketch sketch : stat.getKllSketchList()) addSketch(sketch);
          return;
        } else System.out.println("\t\t\t\t!!!!!![ERROR!] no KLL in stat!");
      }
    }
    long minVal = dataToLong(statistics.getMinValue());
    long maxVal = dataToLong(statistics.getMaxValue());
    System.out.println(
        "\t[KLL STAT DEBUG] no KLL in stat. update from statistics:\t"
            + "min,max:"
            + minVal
            + ","
            + maxVal
            + " n:"
            + statistics.getCount());
    // out of range
    if (minVal > cntR || maxVal < lastL) return;
    if (lastL <= minVal && maxVal < cntL) {
      K1 -= statistics.getCount();
      return;
    }
    if (minVal == maxVal) { // min == max
      for (int i = 0; i < statistics.getCount(); i++) updateStatusFromData(minVal);
      return;
    }
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
      //      updateStatusFromData(tmpObj, 1);
      //      tmp.add(tmpObj);
    }
    //
    // System.out.println("\t\t[MEDIAN]"+this.hashCode()+"[updateResultUsingValues]"+tmp.toString());
  }

  @Override
  public int maxIteration() {
    return 1;
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
    // TODO
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(seriesDataType, outputStream);
    // TODO
  }

  public long getN() {
    return n;
  }

  @Override
  public void reset() {
    super.reset();
    heapKLL = null;
    lastL = cntL = Long.MIN_VALUE;
    cntR = Long.MAX_VALUE;
    n = 0;
    iteration = 0;
    hasFinalResult = false;
  }

  @Override
  public boolean canUpdateFromStatistics(Statistics statistics) {
    if ((seriesDataType == DOUBLE) && iteration == 0) {
      DoubleStatistics doubleStats = (DoubleStatistics) statistics;
      if (doubleStats.getKllSketchNum() > 0) return true;
    }
    if (iteration > 0) {
      long minVal = dataToLong(statistics.getMinValue());
      long maxVal = dataToLong(statistics.getMaxValue());
      if (minVal > cntR || maxVal < lastL) return true;
      if (lastL <= minVal && maxVal < cntL) return true;
    }
    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();
    Comparable<Object> maxVal = (Comparable<Object>) statistics.getMaxValue();
    return (minVal.compareTo(maxVal) == 0); // min==max
  }

  @Override
  public boolean groupByLevelBeforeAggregation() {
    return true;
  }
}
