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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TDigestForExact;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;

public class ExactQuantileTDigestAggrResult extends AggregateResult {
  int mergingBuffer = 1;
  private String returnType = "iteration_num";
  private TSDataType seriesDataType;
  private int iteration;
  private double cntL, cntR, detL, detR;
  private long n, K1, K2, countOfLessThanCntL, countOfCntL, countOfCntR;
  private TDigestForExact sketch;
  DoubleArrayList lastPassData;
  boolean lastPass = false;
  long lastN = Long.MAX_VALUE;
  private ObjectArrayList<TDigestForExact> preComputedSketch;
  private LongArrayList preComputedSketchMinV, preComputedSketchMaxV;
  private int preComputedSketchSize;
  private boolean hasFinalResult;
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

  public ExactQuantileTDigestAggrResult(TSDataType seriesDataType)
      throws UnSupportedDataTypeException {
    super(DOUBLE, AggregationType.EXACT_QUANTILE_TDIGEST);
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
    double dataD = (double) data;
    if (iteration == 0) n++;
    if (lastPass) {
      if (cntL <= dataD && dataD <= cntR) lastPassData.add(dataD);
      else if (dataD < cntL) countOfLessThanCntL++;
    } else {
      if (cntL < dataD && dataD < cntR) {
        sketch.update(dataD);
      } else if (dataD < cntL) {
        countOfLessThanCntL++;
      } else if (cntL == dataD) countOfCntL++;
      else if (cntR == dataD) countOfCntR++;
    }
  }

  @Override
  public void startIteration() {
    countOfLessThanCntL = countOfCntL = countOfCntR = 0;
    if (iteration == 0) { // first iteration
      sketch = new TDigestForExact(maxMemoryByte, mergingBuffer);
      cntL = -Double.MAX_VALUE;
      cntR = Double.MAX_VALUE;
      n = 0;
    } else {
      if (lastN <= maxMemoryByte / 8) {
        lastPass = true;
        lastPassData = new DoubleArrayList(maxMemoryByte / 8);
        sketch = null;
      } else {
        lastPass = false;
        sketch = new TDigestForExact(maxMemoryByte, mergingBuffer);
        System.out.println(
            "\t[ExactQuantile DEBUG TDigest] start iteration "
                + iteration
                + " cntL,R:"
                + "["
                + cntL
                + ","
                + cntR
                + "]"
                + "\tK1,2:"
                + K1
                + ","
                + K2);
      }
    }
  }

  @Override
  public void setDoubleValue(double doubleValue) {
    this.hasCandidateResult = true;
    if (this.returnType.equals("value")) this.doubleValue = doubleValue;
    else if (this.returnType.equals("iteration_num")) this.doubleValue = iteration;
  }

  @Override
  public void finishIteration() {
    //    System.out.println(
    //        "\t[ExactQuantile DEBUG]"
    //            + "finish iteration "
    //            + iteration
    //            + " cntL,R:"
    //            + "["
    //            + longToResult(cntL)
    //            + ","
    //            + longToResult(cntR)
    //            + "]"
    //            + "\tK1,2:"
    //            + K1+","+K2);
    iteration++;
    if (n == 0) {
      setDoubleValue(0);
      hasFinalResult = true;
      return;
    }

    if (preComputedSketch.size() > 0) {
      mergePrecomputedWhenFinish();
    }
    if (iteration == 1) { // first iteration over
      K1 = (int) Math.floor(QUANTILE * (n - 1) + 1);
      K2 = (int) Math.ceil(QUANTILE * (n - 1) + 1);
    }
    long cntK1 = K1 - countOfLessThanCntL, cntK2 = K2 - countOfLessThanCntL;

    if (lastPass) {
      System.out.println(
          "\t[ExactQuantile DEBUG TDigest]\tLast Pass.\tdataN:\t" + lastPassData.size());
      lastPassData.sort(Double::compare);
      double ans = 0;
      //          ((lastPassData.getDouble((int) cntK1 - 1)) + (lastPassData.getDouble((int) cntK2 -
      // 1)))
      //              * 0.5;
      setDoubleValue(ans);
      hasFinalResult = true;
      return;
    }

    //    System.out.println(
    //        "\t[ExactQuantile DEBUG]\tfinish iter."
    //            + " cntK1,2:"
    //            + cntK1
    //            + ","
    //            + cntK2
    //            + "\ttdigestN:"
    //            + sketch.getN());
    double lastCntL = cntL, lastCntR = cntR;
    if (cntK1 <= 0 || cntK2 > countOfCntL + countOfCntR + sketch.totN) { // iteration failed.

      System.out.println(
          "\t[ExactQuantile DEBUG TDigest]\tIter Failed."
              + (cntK1 <= 0 ? "\tans smaller than cntL." : "\tans larger than cntL.")
              + "\tcntL,R:"
              + (cntL)
              + ","
              + (cntR)
              + "\tdetL,R:"
              + (detL)
              + ","
              + (detR));
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
        System.out.println(
            "\t[ExactQuantile DEBUG TDigest]\tFail but Answer Found.\tdetL,R:"
                + (detL)
                + ","
                + (detR));
        double ans = ((detL) + (detR)) * 0.5;
        setDoubleValue(ans);
        hasFinalResult = true;
        return;
      }
      return;
    }
    // iteration success.
    //    sketch.show();
    //    double[] deterministic_result = sketch.findResultRange(cntK1, cntK2, 1.0);
    double[] deterministic_result =
        sketch.getFilter(countOfCntL, countOfCntR, cntL, cntR, cntK1, cntK2);
    if (deterministic_result.length == 3 || deterministic_result[0] >= deterministic_result[1]) {
      double ans = (deterministic_result[0] + deterministic_result[1]) * 0.5;
      setDoubleValue(ans);
      hasFinalResult = true;
      System.out.println(
          "\t[ExactQuantile DEBUG TDigest]\tAnswer Found."
              + " det_result:"
              + deterministic_result[0]
              + "..."
              + deterministic_result[1]
              + "\ttdigestN:"
              + sketch.totN);
      return;
    }

    //    double[] iterate_result = sketch.findResultRange(cntK1, cntK2, fixPr);
    double[] iterate_result = deterministic_result;
    cntL = (iterate_result[0]);
    cntR = (iterate_result[1]);
    detL = (deterministic_result[0]);
    detR = (deterministic_result[1]);

    if (cntL == lastCntL && cntR == lastCntR) {
      double ans = (deterministic_result[0] + deterministic_result[1]) * 0.5;
      setDoubleValue(ans);
      hasFinalResult = true;
      System.out.println(
          "\t[ExactQuantile DEBUG TDigest]\tDANGER!! range_not_updated_after_an_iter.");
      return;
    }

    System.out.println(
        "\t[ExactQuantile DEBUG TDigest]\tfinish iter"
            + (iteration - 1)
            + "."
            + " cntK1,2:"
            + cntK1
            + ","
            + cntK2
            + "\ttdigestN:"
            + sketch.totN);
    lastN = sketch.findMaxNumberInRange(iterate_result[0], iterate_result[1]);
  }

  @Override
  protected boolean hasCandidateResult() {
    return hasFinalResult && n > 0;
  }

  @Override
  public Double getResult() {
    return hasCandidateResult() ? getDoubleValue() : null;
  }

  public void addSketch(TDigestForExact sketch, double minV, double maxV) {
    n += sketch.totN;
  }

  private void mergePrecomputedWhenFinish() {
    //    sketch.mergeWithTempSpace(preComputedSketch, preComputedSketchMinV,
    // preComputedSketchMaxV);
    preComputedSketch.clear();
    preComputedSketchMinV.clear();
    preComputedSketchMaxV.clear();
    preComputedSketchSize = 0;
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
      if (statistics.getType() == DOUBLE) {
        DoubleStatistics stat = (DoubleStatistics) statistics;
        if (stat.getSummaryNum() > 0) {
          //          for (KLLSketchForQuantile sketch : stat.getKllSketchList()) {
          //            ((LongKLLSketch) sketch).deserializeFromBuffer();
          //            addSketch(sketch, stat.getMinValue(), stat.getMaxValue());
          //          }
          return;
        } // else System.out.println("\t\t\t\t!!!!!![ERROR!] no KLL in stat!");
      }
    }
    double minVal = (double) (statistics.getMinValue());
    double maxVal = (double) (statistics.getMaxValue());
    //    System.out.println(
    //        "\t[ExactQuantile DEBUG TDigest] update from statistics:\t"
    //            + "min,max:"
    //            + minVal
    //            + ","
    //            + maxVal
    //            + " statN:"
    //            + statistics.getCount());
    // out of range
    if (minVal > cntR) return;
    if (maxVal < cntL) {
      countOfLessThanCntL += statistics.getCount();
      return;
    }
    if (minVal == maxVal) { // min == max
      if (minVal == cntL) countOfCntL += statistics.getCount();
      else if (minVal == cntR) countOfCntR += statistics.getCount();
      else for (int i = 0; i < statistics.getCount(); i++) updateStatusFromData(minVal);
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
    sketch = null;
    cntL = -Double.MAX_VALUE;
    cntR = Double.MAX_VALUE;
    n = 0;
    iteration = 0;
    countOfLessThanCntL = 0;
    hasFinalResult = false;
    preComputedSketch = new ObjectArrayList<>();
    preComputedSketchMinV = new LongArrayList();
    preComputedSketchMaxV = new LongArrayList();
    preComputedSketchSize = 0;
  }

  @Override
  public boolean canUpdateFromStatistics(Statistics statistics) {
    if ((seriesDataType == DOUBLE) && iteration == 0) {
      DoubleStatistics doubleStats = (DoubleStatistics) statistics;
      if (doubleStats.getSummaryNum() > 0) return true;
    }
    if (iteration > 0) {
      double minVal = (double) (statistics.getMinValue());
      double maxVal = (double) (statistics.getMaxValue());
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
    return false;
  }

  @Override
  public void setAttributes(Map<String, String> attrs) {
    if (attrs.containsKey("memory")) {
      String mem = attrs.get("memory");
      if (mem.contains("KB"))
        this.maxMemoryByte = Integer.parseInt(mem.substring(0, mem.length() - 2)) * 1024;
      else if (mem.contains("MB"))
        this.maxMemoryByte = Integer.parseInt(mem.substring(0, mem.length() - 2)) * 1024 * 1024;
      else if (mem.contains("B"))
        this.maxMemoryByte = Integer.parseInt(mem.substring(0, mem.length() - 1));
    }
    if (attrs.containsKey("quantile")) {
      String q = attrs.get("quantile");
      this.QUANTILE = Double.parseDouble(q);
    }
    if (attrs.containsKey("return_type")) {
      String q = attrs.get("return_type");
      this.returnType = q;
    }
    if (attrs.containsKey("param")) { // 0 means don't use summary.
      String r = attrs.get("param");
      this.mergingBuffer = Integer.parseInt(r);
    }
    System.out.println(
        "  [setAttributes DEBUG]\t\t\tmaxMemoryByte:"
            + maxMemoryByte
            + "\t\tquantile:"
            + QUANTILE
            + "\t\tparam:"
            + mergingBuffer);
  }
}
