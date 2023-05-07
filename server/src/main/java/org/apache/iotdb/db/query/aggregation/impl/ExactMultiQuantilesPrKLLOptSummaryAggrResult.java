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
import org.apache.iotdb.tsfile.utils.*;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;

public class ExactMultiQuantilesPrKLLOptSummaryAggrResult extends AggregateResult {
  int mergeBufferRatio = 5;
  int MULTI_QUANTILES = 1, batchN;
  double[] query_q;
  long[] query_rank1, query_rank2;
  long[][] deterministic_result, iterate_result;
  private String returnType = "value";
  private TSDataType seriesDataType;
  private int iteration;
  private long n;
  private KLLSketchLazyExact heapKLL;
  private ObjectArrayList<KLLSketchForQuantile> preComputedSketch;
  private LongArrayList preComputedSketchMinV, preComputedSketchMaxV;
  private int preComputedSketchSize;
  private boolean hasFinalResult;
  static KLLSketchLazyEmptyForSimuCompact simuWorker;
  static DoubleArrayList prList, singlePrList;
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

  public ExactMultiQuantilesPrKLLOptSummaryAggrResult(TSDataType seriesDataType)
      throws UnSupportedDataTypeException {
    super(DOUBLE, AggregationType.EXACT_MULTI_QUANTILES_PR_KLL_OPT_SUMMARY);
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

  private void processValBuf() {
    Arrays.sort(valBuf, 0, bufSize);
    int index = 0;
    for (int bufID = 0; bufID < bufSize; bufID++) {
      long dataL = valBuf[bufID];
      while (index < rest_multi_quantiles && dataL > rest_iterate_result[index][1]) index++;
      if (index == rest_multi_quantiles) break;
      int i = index;
      for (; i < rest_multi_quantiles && dataL >= (rest_iterate_result[i][0]); i++)
        if (dataL < (rest_iterate_result[i][0])) {
          CountOfLessThanValL[i]++;
          CountOfLessThanValL[i + 1]--;
        } else if ((rest_iterate_result[i][0]) <= dataL && dataL <= (rest_iterate_result[i][1]))
          cntWorker[i].update(dataL);
      CountOfLessThanValL[i]++;
    }
    bufSize = 0;
  }

  private void updateStatusFromDataFirstIter(Object data) {
    long dataL = dataToLong(data);
    n++;
    heapKLL.update(dataL);
  }

  private void updateStatusFromDataLaterIter(Object data) {
    long dataL = dataToLong(data);
    valBuf[bufSize++] = dataL;
    if (bufSize == batchN) processValBuf();
  }

  int rest_multi_quantiles, bufSize;
  int[] rest_query_id;
  long[] rest_query_rank1, rest_query_rank2;
  long[][] rest_deterministic_result, rest_iterate_result;
  long minValL, maxValR;
  long[] CountOfLessThanValL;
  long[] valBuf;
  KLLSketchLazyExact[] cntWorker;

  @Override
  public void startIteration() {
    if (iteration == 0) { // first iteration
      if (mergeBufferRatio > 0)
        heapKLL = new KLLSketchLazyExact(maxMemoryByte * (mergeBufferRatio - 1) / mergeBufferRatio);
      else heapKLL = new KLLSketchLazyExact(maxMemoryByte);
      n = 0;
    } else {
      rest_multi_quantiles = 0;
      rest_query_id = new int[MULTI_QUANTILES];
      rest_query_rank1 = new long[MULTI_QUANTILES];
      rest_query_rank2 = new long[MULTI_QUANTILES];
      rest_deterministic_result = new long[MULTI_QUANTILES][];
      rest_iterate_result = new long[MULTI_QUANTILES][];
      for (int qid = 0; qid < MULTI_QUANTILES; qid++)
        if (deterministic_result[qid][0] < deterministic_result[qid][1]
            && deterministic_result[qid].length != 3) {
          rest_query_rank1[rest_multi_quantiles] = query_rank1[qid];
          rest_query_rank2[rest_multi_quantiles] = query_rank2[qid];
          rest_deterministic_result[rest_multi_quantiles] = deterministic_result[qid];
          rest_iterate_result[rest_multi_quantiles] = iterate_result[qid];
          rest_query_id[rest_multi_quantiles] = qid;
          rest_multi_quantiles++;
        }
      CountOfLessThanValL = new long[rest_multi_quantiles + 1];
      valBuf = new long[rest_multi_quantiles];
      bufSize = 0;
      batchN = rest_multi_quantiles;

      cntWorker = new KLLSketchLazyExact[rest_multi_quantiles];

      minValL = rest_iterate_result[0][0];
      maxValR = rest_iterate_result[0][1];
      for (int i = 0; i < rest_multi_quantiles; i++) {
        cntWorker[i] = new KLLSketchLazyExact(maxMemoryByte / rest_multi_quantiles);
        minValL = Math.min(minValL, rest_iterate_result[i][0]);
        maxValR = Math.max(maxValR, rest_iterate_result[i][1]);
      }
    }
  }

  private int findBestPrID(
      KLLSketchLazyExact sketch,
      int maxMemoryNum,
      long rk1,
      long rk2,
      long[] deterministic_result,
      int MULTI_QUANTILES) {
    double bestEstiNum = 1e9, nextSuccessPr = 1.0;
    int bestPrId = 0;
    int[] successN = new int[prList.size()], failN = new int[prList.size()];
    int[][] succComNum = new int[prList.size()][], failComNum = new int[prList.size()][];
    for (int i = 0; i < prList.size(); i++) {
      double totPr = prList.getDouble(i), singlePr = singlePrList.getDouble(i);
      double[] cntResult = sketch.findResultRange(rk1, rk2, singlePr);
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
      double cntPrResult =
          evaluatePrForMultiQuantiles(
              maxMemoryNum,
              i,
              successN[i],
              failN[i],
              succComNum[i],
              failComNum[i],
              MULTI_QUANTILES);
      if (cntPrResult <= bestEstiNum) {
        bestEstiNum = cntPrResult;
        bestPrId = i;
      }
      //
      // System.out.println("\t\t\t\t\tcntPR:"+prList.getDouble(i)+"\tsuccessN:\t"+successN[i]+"\t\tfailN:\t"+failN[i]+"\t\testiIter:\t"+bestEstiNum);
    }
    //
    // if(DEBUG_PRINT)System.out.println("\tbestTotPr:\t"+prList.getDouble(bestPrId)+"\t\tbestSinglePr:\t"+singlePrList.getDouble(bestPrId)+"\t\testiIter:"+bestEstiNum+"\tnextSuccessN:\t"+successN[bestPrId]);
    return bestPrId;
  }

  private double simulateIterationForMultiQuantiles(
      double casePr, int prID, int depth, int n, int maxMemoryNum, int[] compactNum) {
    if (n <= 0) return 0;
    if (n <= maxMemoryNum)
      return
      //            Math.max(0.75, Math.log(n) / Math.log(maxMemoryNum)),
      1.0;
    int maxERR = 0;
    for (int i = 0; i < compactNum.length; i++) maxERR += compactNum[i] << i;
    double bestSimuIter = 1e3;

    double totPr = prList.getDouble(prID), singlePr = singlePrList.getDouble(prID);
    int prERR = KLLSketchLazyExact.queryRankErrBound(compactNum, singlePr);
    int successN = prERR * 2;
    int failN = (Math.min(n, maxERR * 2) - prERR) / 2;
    failN = Math.max(failN, successN);

    //            KLLSketchLazyEmptyForSimuCompact simuWorker = new
    // KLLSketchLazyEmptyForSimuCompact(/*n, */maxMemoryNum);
    int[] succComNum, failComNum;
    succComNum = simuWorker.simulateCompactNumGivenN(successN);
    failComNum = simuWorker.simulateCompactNumGivenN(failN);
    bestSimuIter =
        1
            + totPr
                * simulateIterationForMultiQuantiles(
                    casePr * totPr, prID, depth + 1, successN, maxMemoryNum, succComNum)
            + (1 - totPr)
                * (1
                    + simulateIterationForMultiQuantiles(
                        casePr * (1 - totPr), prID, depth + 1, failN, maxMemoryNum, failComNum));

    //    DEBUG_COUNT_SIMULATION++;
    return bestSimuIter;
  }

  private double evaluatePrForMultiQuantiles(
      int maxMemoryNum,
      int prID,
      int succN,
      int failN,
      int[] succComNum,
      int[] failComNum,
      int MULTI_QUANTILES) {
    double totPr = prList.getDouble(prID), singlePr = singlePrList.getDouble(prID);
    double successResult =
        simulateIterationForMultiQuantiles(totPr, prID, 1, succN, maxMemoryNum, succComNum);
    double failResult =
        simulateIterationForMultiQuantiles(
            0 * (1 - totPr), prID, 1, failN, maxMemoryNum, failComNum);
    double simulateResult = totPr * successResult + (1 - totPr) * (1 + failResult);
    //
    // if(DEBUG_PRINT)System.out.println("\t\t\t\t\t\t\t\tcntPR:"+Pr+"\tsuccessN:\t"+succN+"\t\tfailN:\t"+failN+/*"\t\testi_iter:\t"+estimateIterationNum+*/"\t\tsimu_iter:\t"+simulateResult[0]+"\tsimu_nextSuccessPr:"+simulateResult[1]);
    return simulateResult;
  }

  private int findBestPrIDForMultiQuantiles(
      KLLSketchLazyExact sketch, int maxMemoryNum, int MULTI_QUANTILES) {
    for (int i = 0; i < prList.size(); i++)
      singlePrList.set(i, Math.pow(prList.getDouble(i), 1.0 / MULTI_QUANTILES));
    double bestEstiNum = 1e9;
    int bestPrId = 0, prNum = prList.size();
    int[] successN = new int[prNum], failN = new int[prNum];
    int[][] succComNum = new int[prNum][], failComNum = new int[prNum][];
    int n = (int) sketch.getN(), maxERR = sketch.getMaxErr();
    for (int i = 0; i < prNum; i++) {
      int prERR = sketch.queryRankErrBound(singlePrList.getDouble(i));
      successN[i] = prERR * 2;
      failN[i] = (Math.min(n, maxERR * 2) - prERR) / 2;
      failN[i] = Math.max(failN[i], successN[i]);
    }
    // KLLSketchLazyEmptyForSimuCompact
    simuWorker = new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum);
    for (int i = 0; i < prNum; i++)
      succComNum[i] = simuWorker.simulateCompactNumGivenN(successN[i]);
    for (int i = prNum - 1; i >= 0; i--)
      failComNum[i] = simuWorker.simulateCompactNumGivenN(failN[i]);

    for (int i = 0; i < prNum; i++) {
      double cntPrResult =
          evaluatePrForMultiQuantiles(
              maxMemoryNum,
              i,
              successN[i],
              failN[i],
              succComNum[i],
              failComNum[i],
              MULTI_QUANTILES);
      if (cntPrResult <= bestEstiNum) {
        bestEstiNum = cntPrResult;
        bestPrId = i;
      }
      //
      // System.out.println("\t\t\t\t\tcntPR:"+prList.getDouble(i)+"\tsuccessN:\t"+successN[i]+"\t\tfailN:\t"+failN[i]+"\t\tsimu_iter:\t"+cntNum);
    }
    //
    // if(DEBUG_PRINT)System.out.println("bestTotPr:\t"+prList.getDouble(bestPrId)+"\t\tbestSinglePr:\t"+singlePrList.getDouble(bestPrId)+"\t\testiIter:"+bestEstiNum+"\tnextSuccessN:\t"+successN[bestPrId]);
    return bestPrId;
  }

  @Override
  public void setDoubleValue(double doubleValue) {
    this.hasCandidateResult = true;
    if (this.returnType.equals("value")) this.doubleValue = doubleValue;
    else if (this.returnType.equals("iteration_num")) this.doubleValue = iteration;
  }

  @Override
  public void finishIteration() {
    //            System.out.println(
    //                "\n\t[ExactMultiQuantiles DEBUG]"
    //                    + "finish iteration "
    //                    + iteration);
    iteration++;
    if (n == 0) {
      hasFinalResult = true;
      return;
    }

    if (preComputedSketch.size() > 0) {
      mergePrecomputedWhenFinish();
    }
    if (iteration > 1 && bufSize > 0) processValBuf();
    if (iteration
        == 1) { // first iteration overint[] query_rank1=new int[MULTI_QUANTILES],query_rank2=new
      // int[MULTI_QUANTILES];
      for (int qid = 0; qid < MULTI_QUANTILES; qid++) {
        query_rank1[qid] = (int) Math.floor(query_q[qid] * (n - 1) + 1);
        query_rank2[qid] = (int) Math.ceil(query_q[qid] * (n - 1) + 1);
        deterministic_result[qid] =
            new long[] {dataToLong(-Double.MAX_VALUE), dataToLong(Double.MAX_VALUE)};
        iterate_result[qid] =
            new long[] {dataToLong(-Double.MAX_VALUE), dataToLong(Double.MAX_VALUE)};
      }

      for (int qid = 0; qid < MULTI_QUANTILES; qid++)
        deterministic_result[qid] =
            heapKLL.findLongResultRange(query_rank1[qid], query_rank2[qid], 1.0);

      int bestPrID =
          findBestPrIDForMultiQuantiles(
              heapKLL, maxMemoryByte / MULTI_QUANTILES / 8, MULTI_QUANTILES);
      for (int qid = 0; qid < MULTI_QUANTILES; qid++)
        iterate_result[qid] =
            heapKLL.findLongResultRange(
                query_rank1[qid], query_rank2[qid], singlePrList.getDouble(bestPrID));
    } else {
      for (int i = 1; i < rest_multi_quantiles; i++)
        CountOfLessThanValL[i] += CountOfLessThanValL[i - 1];

      int[] toEstimateID = new int[rest_multi_quantiles];
      int toEstimate = 0;
      for (int i = 0; i < rest_multi_quantiles; i++) {
        //                        if(DEBUG_PRINT)cntWorker[i].show();
        int qid = rest_query_id[i];
        long cntRank1 = rest_query_rank1[i] - CountOfLessThanValL[i];
        long cntRank2 = rest_query_rank2[i] - CountOfLessThanValL[i];
        //                    System.out.println("\t\t\t\t\t\tcntRank:"+cntRank1+" "+cntRank2);
        if (cntRank1 <= 0 || cntRank2 > cntWorker[i].getN()) { // iteration failed.
          System.out.println(
              "\t\t\t\t\t\titerate fail."
                  + "\t\tcntIter:"
                  + iteration
                  + "\t\tcntQ:"
                  + query_q[qid]);
          if (cntRank1 <= 0)
            iterate_result[qid] =
                new long[] {rest_deterministic_result[i][0], rest_iterate_result[i][0]};
          else
            iterate_result[qid] =
                new long[] {rest_iterate_result[i][1], rest_deterministic_result[i][1]};
          deterministic_result[qid] = iterate_result[qid];
          if (deterministic_result[qid][0] == deterministic_result[qid][1]) continue;
          //          FailCount += 1;
          //                        IterCount-=1;
          continue;
        }
        //        if (DEBUG_PRINT)
        //          System.out.println("\t\t\t\t\t\titerate success." + "\t\tcntN:" +
        // cntWorker[i].getN() + "\t\tcntIter:" + MMP);

        deterministic_result[qid] = cntWorker[i].findLongResultRange(cntRank1, cntRank2, 1.0);
        if (deterministic_result[qid].length == 3) {
          iterate_result[qid] = deterministic_result[qid];
          continue;
        }
        toEstimateID[toEstimate++] = i;
      }
      if (toEstimate > 0) {

        for (int i = 0; i < prList.size(); i++)
          singlePrList.set(i, Math.pow(prList.getDouble(i), 1.0 / toEstimate));

        int worstID = toEstimateID[0];
        for (int i : toEstimateID) if (cntWorker[i].getN() > cntWorker[worstID].getN()) worstID = i;
        //        System.out.println("\t\tfinish iter >=2.\tdet:"+
        // Arrays.toString(deterministic_result[rest_query_id[worstID]]));
        int bestPrID;
        if (toEstimate > 1)
          bestPrID =
              findBestPrIDForMultiQuantiles(
                  cntWorker[worstID], maxMemoryByte / toEstimate / 8, toEstimate);
        else
          bestPrID =
              findBestPrID(
                  cntWorker[worstID],
                  maxMemoryByte / 8,
                  rest_query_rank1[worstID] - CountOfLessThanValL[worstID],
                  query_rank2[worstID] - CountOfLessThanValL[worstID],
                  deterministic_result[rest_query_id[worstID]],
                  1);
        for (int i : toEstimateID) { // todo use normal for. there are 0 in toEstimateID
          cntWorker[i].show();
          int qid = rest_query_id[i];
          iterate_result[qid] =
              cntWorker[i].findLongResultRange(
                  rest_query_rank1[i] - CountOfLessThanValL[i],
                  rest_query_rank2[i] - CountOfLessThanValL[i],
                  singlePrList.getDouble(bestPrID));
        }
      }
    }
    boolean calc_finished = true;
    for (int qid = 0; qid < MULTI_QUANTILES; qid++)
      if (deterministic_result[qid][0] < deterministic_result[qid][1]
          && deterministic_result[qid].length != 3) calc_finished = false;
    if (calc_finished) {
      if (returnType.equals("iteration_num")) setDoubleValue(iteration);
      else {
        double ansSum = 0;
        for (int qid = 0; qid < MULTI_QUANTILES; qid++) {
          double exact_calc_v =
              (longToResult(deterministic_result[qid][0])
                      + longToResult(deterministic_result[qid][1]))
                  * 0.5;
          ansSum += exact_calc_v;
        }
        setDoubleValue(ansSum);
      }
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

  public void addSketch(KLLSketchForQuantile sketch, long minV, long maxV) {
    //    sketch.show();
    n += sketch.getN();
    preComputedSketch.add(sketch);
    preComputedSketchMinV.add(minV);
    preComputedSketchMaxV.add(maxV);
    preComputedSketchSize += sketch.getNumLen() * 8;
    if (preComputedSketchSize >= maxMemoryByte / mergeBufferRatio) {
      heapKLL.mergeWithTempSpace(preComputedSketch, preComputedSketchMinV, preComputedSketchMaxV);
      preComputedSketch.clear();
      preComputedSketchMinV.clear();
      preComputedSketchMaxV.clear();
      preComputedSketchSize = 0;
    }
  }

  private void mergePrecomputedWhenFinish() {
    //    KLLSketchLazyExact tmpSketch = heapKLL;
    //    heapKLL = new KLLSketchLazyExact(maxMemoryByte);
    //    heapKLL.mergeWithTempSpace(tmpSketch, tmpSketch.getMin(), tmpSketch.getMax());
    heapKLL.mergeWithTempSpace(preComputedSketch, preComputedSketchMinV, preComputedSketchMaxV);
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
    if (iteration == 0 && mergeBufferRatio > 0) {
      if (statistics.getType() == DOUBLE) {
        DoubleStatistics stat = (DoubleStatistics) statistics;
        if (stat.getSummaryNum() > 0) {
          for (KLLSketchForQuantile sketch : stat.getKllSketchList()) {
            ((LongKLLSketch) sketch).deserializeFromBuffer();
            addSketch(
                sketch, /*dataToLong(stat.getMinValue()), dataToLong(stat.getMaxValue())*/
                dataToLong(-Double.MAX_VALUE),
                dataToLong(Double.MAX_VALUE));
          }
          return;
        } // else System.out.println("\t\t\t\t!!!!!![ERROR!] no KLL in stat!");
      }
    }
    long minVal = dataToLong(statistics.getMinValue());
    long maxVal = dataToLong(statistics.getMaxValue());
    //    System.out.println(
    //        "\t[ExactQuantile DEBUG pr_kll_opt_summary] update from statistics:\t"
    //            + "min,max:"
    //            + minVal
    //            + ","
    //            + maxVal
    //            + " statN:"
    //            + statistics.getCount());
    // out of range
    if (minVal > maxValR) return;
    if (maxVal < minValL) {
      CountOfLessThanValL[0] += statistics.getCount();
      return;
    }
    if (minVal == maxVal) { // min == max
      if (iteration == 0)
        for (int i = 0; i < statistics.getCount(); i++) updateStatusFromDataFirstIter(minVal);
      else for (int i = 0; i < statistics.getCount(); i++) updateStatusFromDataLaterIter(minVal);
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
    if (iteration == 0)
      while (batchIterator.hasNext()) {
        if (batchIterator.currentTime() >= maxBound || batchIterator.currentTime() < minBound) {
          break;
        }
        updateStatusFromDataFirstIter(batchIterator.currentValue());
        batchIterator.next();
      }
    else {
      //      System.out.println("\t\t??!iter:\t" + iteration + "\t\tbatchN:" + batchN + "\t" +
      // valBuf.length);
      while (batchIterator.hasNext()) {
        if (batchIterator.currentTime() >= maxBound || batchIterator.currentTime() < minBound) {
          break;
        }
        updateStatusFromDataLaterIter(batchIterator.currentValue());
        batchIterator.next();
      }
    }
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    if (iteration == 0)
      for (int i = 0; i < length; i++) {
        if (values[i] != null) {
          updateStatusFromDataFirstIter(values[i]);
        }
      }
    else
      for (int i = 0; i < length; i++) {
        if (values[i] != null) {
          updateStatusFromDataLaterIter(values[i]);
        }
      }
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, ValueIterator valueIterator) {
    if (iteration == 0)
      while (valueIterator.hasNext()) {
        updateStatusFromDataFirstIter(valueIterator.next());
      }
    else
      while (valueIterator.hasNext()) {
        updateStatusFromDataLaterIter(valueIterator.next());
      }
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
    minValL = Long.MIN_VALUE;
    maxValR = Long.MAX_VALUE;
    n = 0;
    iteration = 0;
    hasFinalResult = false;
    preComputedSketch = new ObjectArrayList<>();
    preComputedSketchMinV = new LongArrayList();
    preComputedSketchMaxV = new LongArrayList();
    preComputedSketchSize = 0;
  }

  @Override
  public boolean canUpdateFromStatistics(Statistics statistics) {
    if ((seriesDataType == DOUBLE) && iteration == 0 && mergeBufferRatio > 0) {
      DoubleStatistics doubleStats = (DoubleStatistics) statistics;
      if (doubleStats.getSummaryNum() > 0) return true;
    }
    if (iteration > 0) {
      long minVal = dataToLong(statistics.getMinValue());
      long maxVal = dataToLong(statistics.getMaxValue());
      if (minVal > maxValR || maxVal < minValL) return true;
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
    return mergeBufferRatio > 0;
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
    if (attrs.containsKey("merge_buffer_ratio")) { // 0 means don't use summary.
      String r = attrs.get("merge_buffer_ratio");
      this.mergeBufferRatio = Integer.parseInt(r);
    }
    if (attrs.containsKey("multi_quantiles")) {
      String mq = attrs.get("multi_quantiles");
      this.MULTI_QUANTILES = Integer.parseInt(mq);
    }
    System.out.println(
        "  [setAttributes DEBUG]\t\t\tmaxMemoryByte:"
            + maxMemoryByte
            + "\t\tMULTI_QUANTILES:"
            + MULTI_QUANTILES);
    prList = new DoubleArrayList();
    for (double tmp = 0.70; tmp < 0.9 - 1e-6; tmp += 0.02) prList.add(tmp);
    for (double tmp = 0.90; tmp < 0.99 - 1e-6; tmp += 0.01) prList.add(tmp);
    prList.add(0.99);
    prList.add(0.995);
    singlePrList = new DoubleArrayList(prList);

    this.batchN = MULTI_QUANTILES;
    query_q = new double[MULTI_QUANTILES];
    final double query_delta_q = 1.0 / (MULTI_QUANTILES + 1);
    for (int i = 0; i < MULTI_QUANTILES; i++) query_q[i] = query_delta_q * (i + 1);
    query_rank1 = new long[MULTI_QUANTILES];
    query_rank2 = new long[MULTI_QUANTILES];
    deterministic_result = new long[MULTI_QUANTILES][];
    iterate_result = new long[MULTI_QUANTILES][];
    //    batchN=MULTI_QUANTILES;
    //    valBuf=new long[bufSize];
    //    bufSize=0;
    simuWorker = new KLLSketchLazyEmptyForSimuCompact(maxMemoryByte / 8);
  }
}
