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
import org.apache.iotdb.tsfile.utils.KLLSketchLazyEmptyForSimuCompact;
import org.apache.iotdb.tsfile.utils.KLLSketchLazyExact;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;

public class ExactQuantilePrKLLOptFilterAggrResult extends AggregateResult {
  private String returnType = "value";
  private TSDataType seriesDataType;
  private int iteration;
  private long cntL, cntR, detL, detR;
  private long n, K1, K2, countOfLessThanCntL;
  private KLLSketchLazyExact heapKLL;
  private boolean hasFinalResult;
  static KLLSketchLazyEmptyForSimuCompact simuWorker;
  static DoubleArrayList prList;
  long DEBUG = 0;
  int lastLtCountFromPageData;

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

  public ExactQuantilePrKLLOptFilterAggrResult(TSDataType seriesDataType)
      throws UnSupportedDataTypeException {
    super(DOUBLE, AggregationType.EXACT_QUANTILE_PR_KLL_OPT_FILTER);
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
      lastLtCountFromPageData = Integer.MIN_VALUE;
    } else if (dataL < cntL) {
      countOfLessThanCntL++;
      lastLtCountFromPageData++;
    }
  }

  @Override
  public void startIteration() {
    countOfLessThanCntL = 0;
    if (iteration == 0) { // first iteration
      heapKLL = new KLLSketchLazyExact(maxMemoryByte);
      if (seriesDataType == DOUBLE) {
        cntL = dataToLong(-Double.MAX_VALUE);
        cntR = dataToLong(Double.MAX_VALUE);
        //        System.out.println("\t\t\tstartIteration   cntL,R:"+cntL+"  "+cntR);
      } else {
        cntL = Long.MIN_VALUE;
        cntR = Long.MAX_VALUE;
      }
      n = 0;
    } else {
      heapKLL = new KLLSketchLazyExact(maxMemoryByte);
      System.out.println(
          "\t[ExactQuantile DEBUG pr_kll_opt_filter] start iteration "
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

  private double[] simulateIteration(
      double casePr, double lastPr, int depth, int n, int maxMemoryNum, int[] compactNum) {
    if (n <= 0) return new double[] {0, 1.0};
    if (n <= maxMemoryNum)
      return new double[] {
        //            Math.max(0.75, Math.log(n) / Math.log(maxMemoryNum)),
        1.0, 1.0
      };
    int maxERR = 0;
    for (int i = 0; i < compactNum.length; i++) maxERR += compactNum[i] << i;
    double bestSimuIter = 1e3, bestSimuPr = 1.0;

    double pr = bestSimuPr = lastPr;
    int prERR = KLLSketchLazyExact.queryRankErrBound(compactNum, pr);
    int successN = prERR * 2;
    int failN = (Math.min(n, maxERR * 2) - prERR) / 2;
    //            KLLSketchLazyEmptyForSimuCompact simuWorker = new
    // KLLSketchLazyEmptyForSimuCompact(/*n, */maxMemoryNum);
    int[] succComNum, failComNum;
    if (failN < successN) {
      failComNum = simuWorker.simulateCompactNumGivenN(failN);
      succComNum = simuWorker.simulateCompactNumGivenN(successN);
    } else {
      succComNum = simuWorker.simulateCompactNumGivenN(successN);
      failComNum = simuWorker.simulateCompactNumGivenN(failN);
    }
    bestSimuIter =
        1
            + pr
                * simulateIteration(casePr * pr, pr, depth + 1, successN, maxMemoryNum, succComNum)[
                    0]
            + (1 - pr)
                * (1
                    + simulateIteration(
                        casePr * (1 - pr), pr, depth + 1, failN, maxMemoryNum, failComNum)[0]);

    return new double[] {bestSimuIter, bestSimuPr};
  }

  private double[] evaluatePr(
      int maxMemoryNum, double Pr, int succN, int failN, int[] succComNum, int[] failComNum) {
    double[] simulateResult = new double[3];
    double[] successResult = simulateIteration(Pr, Pr, 1, succN, maxMemoryNum, succComNum);
    double[] failResult = simulateIteration(0 * (1 - Pr), Pr, 1, failN, maxMemoryNum, failComNum);
    simulateResult[0] = Pr * successResult[0] + (1 - Pr) * (1 + failResult[0]);
    simulateResult[1] = successResult[1];
    simulateResult[2] = failResult[1];
    //
    // if(DEBUG_PRINT)System.out.println("\t\t\t\t\t\t\t\tcntPR:"+Pr+"\tsuccessN:\t"+succN+"\t\tfailN:\t"+failN+/*"\t\testi_iter:\t"+estimateIterationNum+*/"\t\tsimu_iter:\t"+simulateResult[0]+"\tsimu_nextSuccessPr:"+simulateResult[1]);
    return simulateResult;
  }

  private double findBestPr(
      KLLSketchLazyExact sketch,
      int maxMemoryNum,
      long rk1,
      long rk2,
      double[] deterministic_result) {
    double bestEstiNum = 1e9, nextSuccessPr = 1.0;
    int bestPrId = 0;
    int[] successN = new int[prList.size()], failN = new int[prList.size()];
    int[][] succComNum = new int[prList.size()][], failComNum = new int[prList.size()][];
    for (int i = 0; i < prList.size(); i++) {
      double pr = prList.getDouble(i);
      double[] cntResult = sketch.findResultRange(rk1, rk2, pr);
      int rkValL = (int) cntResult[2],
          rkValR = (int) cntResult[3],
          prErrL = (int) cntResult[4],
          prErrR = (int) cntResult[5];
      int tmpSuccessN = Math.max(rkValR - rkValL, prErrL + prErrR);
      tmpSuccessN += (prErrL + prErrR) / 16;
      if (tmpSuccessN <= maxMemoryNum) tmpSuccessN += (prErrL + prErrR) / 16;
      int tmpFailN =
          (((int) deterministic_result[3] - (int) deterministic_result[2]) - tmpSuccessN) / 2;
      successN[i] = tmpSuccessN;
      failN[i] = Math.max(tmpSuccessN, tmpFailN);
    }
    // KLLSketchLazyEmptyForSimuCompact
    simuWorker = new KLLSketchLazyEmptyForSimuCompact(/*(int)sketch.getN()/2, */ maxMemoryNum);
    for (int i = 0; i < prList.size(); i++)
      succComNum[i] = simuWorker.simulateCompactNumGivenN(successN[i]);
    for (int i = prList.size() - 1; i >= 0; i--)
      failComNum[i] = simuWorker.simulateCompactNumGivenN(failN[i]);

    for (int i = 0; i < prList.size(); i++) {
      double[] cntPrResult =
          evaluatePr(
              maxMemoryNum,
              prList.getDouble(i),
              successN[i],
              failN[i],
              succComNum[i],
              failComNum[i]);
      if (cntPrResult[0] <= bestEstiNum) {
        bestEstiNum = cntPrResult[0];
        bestPrId = i;
        nextSuccessPr = cntPrResult[1];
      }
      //
      // System.out.println("\t\t\t\t\tcntPR:"+prList.getDouble(i)+"\tsuccessN:\t"+successN[i]+"\t\tfailN:\t"+failN[i]+"\t\tsimu_iter:\t"+cntNum);
    }
    return prList.getDouble(bestPrId);
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
      hasFinalResult = true;
      return;
    }

    if (iteration == 1) { // first iteration over
      K1 = (int) Math.floor(QUANTILE * (n - 1) + 1);
      K2 = (int) Math.ceil(QUANTILE * (n - 1) + 1);
    }
    long cntK1 = K1 - countOfLessThanCntL, cntK2 = K2 - countOfLessThanCntL;
    System.out.println(
        "\t\t[ExactQuantile DEBUG]\tfinish iter."
            + " cntK1,2:"
            + cntK1
            + ","
            + cntK2
            + "\tkllN:"
            + heapKLL.getN());
    long lastCntL = cntL, lastCntR = cntR;
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
    // iteration success.
    //    heapKLL.show();
    double[] deterministic_result = heapKLL.findResultRange(cntK1, cntK2, 1.0);
    if (deterministic_result.length == 3 || deterministic_result[0] >= deterministic_result[1]) {
      double ans = (deterministic_result[0] + deterministic_result[1]) * 0.5;
      setDoubleValue(ans);
      hasFinalResult = true;
      System.out.println(
          "\t[ExactQuantile DEBUG pr_kll_opt_filter]\tAnswer Found."
              + " det_result:"
              + deterministic_result[0]
              + "..."
              + deterministic_result[1]
              + "\tkllN:"
              + heapKLL.getN());
      return;
    }

    double bestPr = findBestPr(heapKLL, maxMemoryByte / 8, cntK1, cntK2, deterministic_result);

    double[] iterate_result = heapKLL.findResultRange(cntK1, cntK2, bestPr);
    cntL = dataToLong(iterate_result[0]);
    cntR = dataToLong(iterate_result[1]);
    detL = dataToLong(deterministic_result[0]);
    detR = dataToLong(deterministic_result[1]);

    if (cntL == lastCntL && cntR == lastCntR) {
      double ans = (deterministic_result[0] + deterministic_result[1]) * 0.5;
      setDoubleValue(ans);
      hasFinalResult = true;
      System.out.println(
          "\t[ExactQuantile DEBUG pr_kll_opt_filter]\tDANGER!! range_not_updated_after_an_iter.");
      return;
    }

    System.out.println(
        "\t[ExactQuantile DEBUG pr_kll_opt_filter]\tfinish iter"
            + (iteration - 1)
            + "."
            + " cntK1,2:"
            + cntK1
            + ","
            + cntK2
            + "\tkllN:"
            + heapKLL.getN()
            + "\tbestPr:"
            + bestPr);
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
    //    System.out.println(
    //        "\t[ExactQuantile DEBUG pr_kll_opt_filter] update from statistics:\t"
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
      lastLtCountFromPageData = 0;
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
    lastLtCountFromPageData = 0;
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
    lastLtCountFromPageData = 0;
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        updateStatusFromData(values[i]);
      }
    }
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, ValueIterator valueIterator) {
    //    List<Object> tmp = new ArrayList<>();
    lastLtCountFromPageData = 0;
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
    //    System.out.println(
    //        "\t\t\t\tcanUpdateFromStatistics?\t\t\t"
    //            + "tMinMax:"
    //            + statistics.getStartTime()
    //            + ".."
    //            + statistics.getEndTime()
    //            + "\t\tstatMinMax:"
    //            + statistics.getMinValue()
    //            + ".."
    //            + statistics.getMaxValue()
    //            + "\t\tcntRange:"
    //            + longToResult(cntL)
    //            + "..."
    //            + longToResult(cntR));
    if (iteration > 0) {
      long minVal = dataToLong(statistics.getMinValue());
      long maxVal = dataToLong(statistics.getMaxValue());
      if (minVal > cntR || maxVal < cntL) {
        //        System.out.println("\t\t\t!!!TRUE!!");
        return true;
      }
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
    if (attrs.containsKey("return_type")) {
      String q = attrs.get("return_type");
      this.returnType = q;
    }
    System.out.println(
        "  [setAttributes DEBUG]\t\t\tmaxMemoryByte:" + maxMemoryByte + "\t\tquantile:" + QUANTILE);
    if (prList == null) {
      prList = new DoubleArrayList();
      for (double tmp = 0.70; tmp < 0.9 - 1e-6; tmp += 0.02) prList.add(tmp);
      for (double tmp = 0.90; tmp < 0.99 - 1e-6; tmp += 0.01) prList.add(tmp);
      prList.add(0.99);
      prList.add(0.995);
    }
    simuWorker = new KLLSketchLazyEmptyForSimuCompact(maxMemoryByte / 8);
  }

  @Override
  public double getCntL() {
    /*System.out.println("\t\t????\t\t\t"+cntL);*/
    return longToResult(cntL);
  }

  @Override
  public double getCntR() {
    return longToResult(cntR);
  }

  @Override
  public int getLastLtCountFromPageData() {
    int tmp = lastLtCountFromPageData;
    lastLtCountFromPageData = Integer.MIN_VALUE;
    return tmp;
  }

  @Override
  public boolean canUpdateFromLtCount() {
    return true;
  }

  @Override
  public void updateFromLtCount(int ltCount) {
    countOfLessThanCntL += ltCount;
  }
}
