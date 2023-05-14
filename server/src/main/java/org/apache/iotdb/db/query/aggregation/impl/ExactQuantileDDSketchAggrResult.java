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
import org.apache.iotdb.tsfile.utils.KLLSketchLazyExact;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;

public class ExactQuantileDDSketchAggrResult extends AggregateResult {
  private TSDataType seriesDataType;
  private int iteration;
  private long cntL, cntR, detL, detR;
  private long n, K1, K2, countOfLessThanCntL;
  private KLLSketchLazyExact heapKLL;
  private boolean hasFinalResult;
  //  double PR=1.0;
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

  public ExactQuantileDDSketchAggrResult(TSDataType seriesDataType)
      throws UnSupportedDataTypeException {
    super(DOUBLE, AggregationType.EXACT_QUANTILE_BASELINE_KLL);
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
    } else if (dataL < cntL) {
      countOfLessThanCntL++;
    }
  }

  @Override
  public void startIteration() {
    countOfLessThanCntL = 0;
    if (iteration == 0) { // first iteration
      heapKLL = new KLLSketchLazyExact(maxMemoryByte);
      cntL = Long.MIN_VALUE;
      cntR = Long.MAX_VALUE;
      detL = cntL;
      detR = cntR;
      n = 0;
    } else {
      heapKLL = new KLLSketchLazyExact(maxMemoryByte);
      System.out.println(
          "\t[ExactQuantile DEBUG] start iteration "
              + iteration
              + " cntL,R:"
              + "["
              + longToResult(cntL)
              + ","
              + longToResult(cntR)
              + "]"
              + "\tK1,2:"
              + K1
              + ","
              + K2);
    }
  }

  @Override
  public void finishIteration() {
    //    System.out.println(
    //        "\t[ExactQuantile DEBUG]"
    //            + "finish iteration "
    //            + iteration
    //            + " cntL,R:"
    //            + "["
    //            + cntL
    //            + ","
    //            + cntR
    //            + "]"
    //            + "\tK1,2:"
    //            + K1+","+K2);
    iteration++;
    if (n == 0) {
      hasFinalResult = true;
      return;
    }

    if (iteration == 1) { // first iteration over
      K1 = (int) Math.floor(QUANTILE * (n - 1) + 1);
      K2 = (int) Math.ceil(QUANTILE * (n - 1) + 1);
    }
    long cntK1 = K1 - countOfLessThanCntL, cntK2 = K2 - countOfLessThanCntL;
    System.out.println(
        "\t[ExactQuantile baselineKLL DEBUG]finish iter."
            + " cntK1,2:"
            + cntK1
            + ","
            + cntK2
            + "\t\tkllN:"
            + heapKLL.getN());
    if (cntK1 <= 0 || cntK2 > heapKLL.getN()) { // iteration failed.
      if (cntK1 <= 0) {
        cntR = cntL;
        cntL = detL;
      } else {
        cntL = cntR;
        cntR = detR;
      }
      detL = cntL;
      detR = cntR;
      if (detL >= detR) {
        double ans = (longToResult(detL) + longToResult(detR)) * 0.5;
        setDoubleValue(ans);
        hasFinalResult = true;
      }
      return;
    }
    if (cntL == cntR) {
      double ans = (longToResult(cntL) + longToResult(cntR)) * 0.5;
      setDoubleValue(ans);
      hasFinalResult = true;
      return;
    }
    if (heapKLL.exactResult()) {
      long valL = heapKLL.getExactResult((int) cntK1 - 1);
      long valR = heapKLL.getExactResult((int) cntK2 - 1);
      double ans = (longToResult(valL) + longToResult(valR)) * 0.5;
      setDoubleValue(ans);
      hasFinalResult = true;
      return;
    }

    detL = cntL;
    detR = cntR;
    //    heapKLL.show();
    double query_q1 = 1.0 * cntK1 / heapKLL.getN(), query_q2 = 1.0 * cntK2 / heapKLL.getN();

    double esti_rel_err = 0;
    for (int level = 2; level <= 100; level++) {
      int[] capacity = KLLSketchLazyExact.calcLevelMaxSize(maxMemoryByte / 8, level);
      long allCap = 0;
      for (int i = 0; i < level; i++) allCap += (long) capacity[i] << i;
      if (allCap < heapKLL.getN()) continue;
      //      System.out.println("\t\t\t\t!! level:"+level);
      int K = capacity[level - 1];
      double CDF_COEF = 2.296, CDF_EXP = 0.9723;
      esti_rel_err = CDF_COEF / Math.pow(K, CDF_EXP);
      break;
    }

    double bound_q1 = Math.max(0, query_q1 - esti_rel_err),
        bound_q2 = Math.min(1.0, query_q2 + esti_rel_err);
    if (bound_q1 == 0) cntL = heapKLL.getMin();
    else
      cntL =
          Math.max(
              heapKLL.getMin(), heapKLL.findMinValueWithRank((int) (bound_q1 * heapKLL.getN())));
    if (bound_q2 == 1) cntR = heapKLL.getMax();
    else
      cntR =
          Math.min(
              heapKLL.getMax(),
              heapKLL.findMaxValueWithRank((int) (bound_q2 * heapKLL.getN())) + 1);

    double[] deterministic_result = heapKLL.findResultRange(cntK1, cntK2, 1.0);
    if (deterministic_result.length == 3 || deterministic_result[0] >= deterministic_result[1]) {
      double ans = (deterministic_result[0] + deterministic_result[1]) * 0.5;
      setDoubleValue(ans);
      hasFinalResult = true;
      return;
    }
    cntL = Math.max(cntL, dataToLong(deterministic_result[0]));
    cntR = Math.min(cntR, dataToLong(deterministic_result[1]));
    //    System.out.println("\t\t\titeration over.
    // cntL,R:"+longToResult(cntL)+","+longToResult(cntR));

    //    double[] deterministic_result = heapKLL.findResultRange(cntK1, cntK2, 1.0);
    //    if (deterministic_result.length == 3 || deterministic_result[0] ==
    // deterministic_result[1]) {
    //      double ans = (deterministic_result[0] + deterministic_result[1]) * 0.5;
    //      setDoubleValue(ans);
    //      hasFinalResult = true;
    //      return;
    //    }
    //    cntL = dataToLong(deterministic_result[0]);
    //    cntR = dataToLong(deterministic_result[1]);
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
    //    System.out.println(
    //        "\t[ExactQuantile DEBUG] update from statistics:\t"
    //            + "min,max:"
    //            + minVal
    //            + ","
    //            + maxVal
    //            + " n:"
    //            + statistics.getCount());
    // out of range
    if (minVal > cntR) return;
    if (maxVal < cntL) {
      countOfLessThanCntL += statistics.getCount();
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
    return 20;
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
    cntL = Long.MIN_VALUE;
    cntR = Long.MAX_VALUE;
    n = 0;
    iteration = 0;
    countOfLessThanCntL = 0;
    hasFinalResult = false;
  }

  @Override
  public boolean canUpdateFromStatistics(Statistics statistics) {
    if (iteration > 0) {
      long minVal = dataToLong(statistics.getMinValue());
      long maxVal = dataToLong(statistics.getMaxValue());
      if (minVal > cntR || maxVal < cntL) return true;
    }
    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();
    Comparable<Object> maxVal = (Comparable<Object>) statistics.getMaxValue();
    return (minVal.compareTo(maxVal) == 0); // min==max
  }

  @Override
  public boolean groupByLevelBeforeAggregation() {
    return true;
  }

  @Override
  public boolean useStatisticsIfPossible() {
    return true;
  }

  @Override
  public void setAttributes(Map<String, String> attrs) {
    if (attrs.containsKey("memory")) {
      String mem = attrs.get("memory");
      if (mem.contains("KB"))
        this.maxMemoryByte = Integer.parseInt(mem.substring(0, mem.length() - 2)) * 1024;
      else if (mem.contains("B"))
        this.maxMemoryByte = Integer.parseInt(mem.substring(0, mem.length() - 1));
    }
    if (attrs.containsKey("quantile")) {
      String q = attrs.get("quantile");
      this.QUANTILE = Double.parseDouble(q);
    }
    //    if (attrs.containsKey("pr")) {
    //      String pr = attrs.get("pr");
    //      this.PR = Double.parseDouble(pr);
    //    }
    System.out.println(
        "  [setAttributes DEBUG]\t\t\tmaxMemoryByte:" + maxMemoryByte + "\t\tquantile:" + QUANTILE);
  }
}
