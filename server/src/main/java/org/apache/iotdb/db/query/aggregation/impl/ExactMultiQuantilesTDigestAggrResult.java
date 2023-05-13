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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;

public class ExactMultiQuantilesTDigestAggrResult extends AggregateResult {
  int mergingBuffer = 1;
  int MULTI_QUANTILES = 1, batchN;
  double[] query_q;
  long[] query_rank1, query_rank2;
  double[][] deterministic_result, iterate_result;
  boolean lastPass;
  long lastN;
  DoubleArrayList[] lastPassData;
  private String returnType = "value";
  private TSDataType seriesDataType;
  private int iteration;
  private long n;
  private TDigestForExact sketch;
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

  public ExactMultiQuantilesTDigestAggrResult(TSDataType seriesDataType)
      throws UnSupportedDataTypeException {
    super(DOUBLE, AggregationType.EXACT_MULTI_QUANTILES_TDIGEST);
    this.seriesDataType = seriesDataType;
    reset();
  }

  private void processValBuf() {
    Arrays.sort(valBuf, 0, bufSize);
    int index = 0;
    for (int bufID = 0; bufID < bufSize; bufID++) {
      double dataD = valBuf[bufID];
      while (index < rest_multi_quantiles && dataD > (rest_iterate_result[index][1])) index++;
      if (index == rest_multi_quantiles) break;
      int i = index;
      for (; i < rest_multi_quantiles && dataD >= (rest_iterate_result[i][0]); i++)
        if (dataD < (rest_iterate_result[i][0])) {
          CountOfLessThanValL[i]++;
          CountOfLessThanValL[i + 1]--;
        } else if ((rest_iterate_result[i][0]) <= dataD && dataD <= (rest_iterate_result[i][1])) {
          if (lastPass) lastPassData[i].add(dataD);
          else {
            if (dataD == rest_iterate_result[i][0]) CountOfValL[i]++;
            else if (dataD == rest_iterate_result[i][1]) CountOfValR[i]++;
            else cntWorker[i].update(dataD);
          }
        }
      CountOfLessThanValL[i]++;
    }
    bufSize = 0;
  }

  private void updateStatusFromDataFirstIter(Object data) {
    //    long dataL = dataToLong(data);
    n++;
    sketch.update((double) data);
  }

  private void updateStatusFromDataLaterIter(Object data) {
    valBuf[bufSize++] = (double) data;
    if (bufSize == batchN) processValBuf();
  }

  int rest_multi_quantiles, bufSize;
  int[] rest_query_id;
  long[] rest_query_rank1, rest_query_rank2;
  double[][] rest_deterministic_result, rest_iterate_result;
  double minValL, maxValR;
  long[] CountOfLessThanValL, CountOfValL, CountOfValR;
  double[] valBuf;
  TDigestForExact[] cntWorker;

  @Override
  public void startIteration() {
    if (iteration == 0) { // first iteration
      sketch = new TDigestForExact(maxMemoryByte, mergingBuffer);
      n = 0;
    } else {

      rest_multi_quantiles = 0;
      rest_query_id = new int[MULTI_QUANTILES];
      rest_query_rank1 = new long[MULTI_QUANTILES];
      rest_query_rank2 = new long[MULTI_QUANTILES];
      rest_deterministic_result = new double[MULTI_QUANTILES][];
      rest_iterate_result = new double[MULTI_QUANTILES][];
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
      CountOfValL = new long[rest_multi_quantiles + 1];
      CountOfValR = new long[rest_multi_quantiles + 1];
      valBuf = new double[rest_multi_quantiles];
      bufSize = 0;
      batchN = rest_multi_quantiles;

      if (lastN <= maxMemoryByte / 8 / rest_multi_quantiles) {
        lastPass = true;
        cntWorker = null;
        lastPassData = new DoubleArrayList[rest_multi_quantiles];
        for (int i = 0; i < rest_multi_quantiles; i++) lastPassData[i] = new DoubleArrayList();
        return;
      }

      lastPass = false;
      lastPassData = null;

      cntWorker = new TDigestForExact[rest_multi_quantiles];
      minValL = rest_iterate_result[0][0];
      maxValR = rest_iterate_result[0][1];
      for (int i = 0; i < rest_multi_quantiles; i++) {
        cntWorker[i] = new TDigestForExact(maxMemoryByte / rest_multi_quantiles, mergingBuffer);
        minValL = Math.min(minValL, rest_iterate_result[i][0]);
        maxValR = Math.max(maxValR, rest_iterate_result[i][1]);
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
    //                System.out.println(
    //                    "\t[DEBUG multi_TDigest]"
    //                        + "finishing iteration "
    //                        + iteration);
    iteration++;
    if (n == 0) {
      hasFinalResult = true;
      return;
    }

    if (preComputedSketch.size() > 0) {
      mergePrecomputedWhenFinish();
    }
    if (iteration > 1 && bufSize > 0) processValBuf();
    if (lastPass) {
      for (int i = 1; i < rest_multi_quantiles; i++)
        CountOfLessThanValL[i] += CountOfLessThanValL[i - 1];
      System.out.println("\t[DEBUG multi_TDigest]\tLast Pass.\tlastN:\t" + lastN);
      double sum = 0;
      for (int i = 0; i < rest_multi_quantiles; i++) {
        lastPassData[i].sort(Double::compare);
        double ans = 0;
        //            ((lastPassData[i].getDouble((int) (rest_query_rank1[i] -
        // CountOfLessThanValL[i]) - 1))
        //                    + (lastPassData[i].getDouble(
        //                        (int) (rest_query_rank2[i] - CountOfLessThanValL[i]) - 1)))
        //                * 0.5;
        deterministic_result[rest_query_id[i]][0] = deterministic_result[rest_query_id[i]][1] = ans;
      }
      for (int i = 0; i < MULTI_QUANTILES; i++)
        sum += (deterministic_result[i][0] + deterministic_result[i][0]) * 0.5;
      setDoubleValue(sum);
      hasFinalResult = true;
      return;
    }
    if (iteration
        == 1) { // first iteration overint[] query_rank1=new int[MULTI_QUANTILES],query_rank2=new
      // int[MULTI_QUANTILES];
      //      System.out.println("\t\tfirst-pass start to get bounds.");
      //      sketch.show();
      for (int qid = 0; qid < MULTI_QUANTILES; qid++) {
        query_rank1[qid] = (int) Math.floor(query_q[qid] * (n - 1) + 1);
        query_rank2[qid] = (int) Math.ceil(query_q[qid] * (n - 1) + 1);
        deterministic_result[qid] =
            sketch.getFilter(0, 0, 0, 0, query_rank1[qid], query_rank2[qid]);
      }

      lastN = 0;
      for (int qid = 0; qid < MULTI_QUANTILES; qid++) {

        iterate_result[qid] = sketch.getFilter(0, 0, 0, 0, query_rank1[qid], query_rank2[qid]);
        lastN =
            Math.max(
                lastN, sketch.findMaxNumberInRange(iterate_result[qid][0], iterate_result[qid][1]));
      }
      //      System.out.println("\t\tfirst-pass got bounds.");
    } else {
      for (int i = 1; i < rest_multi_quantiles; i++)
        CountOfLessThanValL[i] += CountOfLessThanValL[i - 1];

      IntArrayList toEstimateID = new IntArrayList(rest_multi_quantiles);
      for (int i = 0; i < rest_multi_quantiles; i++) {
        //                        if(DEBUG_PRINT)cntWorker[i].show();
        int qid = rest_query_id[i];
        long cntRank1 = rest_query_rank1[i] - CountOfLessThanValL[i];
        long cntRank2 = rest_query_rank2[i] - CountOfLessThanValL[i];
        //                    System.out.println("\t\t\t\t\t\tcntRank:"+cntRank1+" "+cntRank2);
        if (cntRank1 <= 0
            || cntRank2
                > CountOfValL[i] + CountOfValR[i] + cntWorker[i].totN) { // iteration failed.
          System.out.println(
              "\t\t\t\t\t\titerate fail."
                  + "\t\tcntIter:"
                  + iteration
                  + "\t\tcntQ:"
                  + query_q[qid]);
          if (cntRank1 <= 0)
            iterate_result[qid] =
                new double[] {rest_deterministic_result[i][0], rest_iterate_result[i][0]};
          else
            iterate_result[qid] =
                new double[] {rest_iterate_result[i][1], rest_deterministic_result[i][1]};
          deterministic_result[qid] = iterate_result[qid];
          if (deterministic_result[qid][0] == deterministic_result[qid][1]) continue;
          //          FailCount += 1;
          //                        IterCount-=1;
          continue;
        }
        //        if (DEBUG_PRINT)
        //          System.out.println("\t\t\t\t\t\titerate success." + "\t\tcntN:" +
        // cntWorker[i].getN() + "\t\tcntIter:" + MMP);

        deterministic_result[qid] =
            cntWorker[i].getFilter(
                CountOfValL[i],
                CountOfValR[i],
                iterate_result[i][0],
                iterate_result[i][1],
                cntRank1,
                cntRank2);
        if (deterministic_result[qid].length == 3
            || deterministic_result[qid][0] == deterministic_result[qid][1]) {
          iterate_result[qid] = deterministic_result[qid];
          continue;
        }
        toEstimateID.add(i);
      }
      if (toEstimateID.size() > 0) {
        int worstID = toEstimateID.getInt(0);
        for (int i : toEstimateID) if (cntWorker[i].getN() > cntWorker[worstID].getN()) worstID = i;
        //        System.out.println("\t\tfinish iter >=2.\tdet:"+
        // Arrays.toString(deterministic_result[rest_query_id[worstID]]));
        System.out.println(
            "\t\t[DEBUG multi_TDigest]:\t iteration:"
                + iteration
                + "\tworstSketch_N:\t"
                + cntWorker[worstID].getN());
        //        cntWorker[worstID].show();
        lastN = 0;
        for (int i : toEstimateID) {
          //          cntWorker[i].show();
          int qid = rest_query_id[i];
          iterate_result[qid] =
              cntWorker[i].getFilter(
                  CountOfValL[i],
                  CountOfValR[i],
                  iterate_result[i][0],
                  iterate_result[i][1],
                  rest_query_rank1[i] - CountOfLessThanValL[i],
                  rest_query_rank2[i] - CountOfLessThanValL[i]);
          lastN =
              Math.max(
                  lastN,
                  cntWorker[i].findMaxNumberInRange(iterate_result[i][0], iterate_result[i][1]));
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
              ((deterministic_result[qid][0]) + (deterministic_result[qid][1])) * 0.5;
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

  public void addSketch(TDigestForExact sketch, double minV, double maxV) {
    //    sketch.show();
    n += sketch.getN();
  }

  private void mergePrecomputedWhenFinish() {
    //    TDigestForExact tmpSketch = heapKLL;
    //    heapKLL = new TDigestForExact(maxMemoryByte);
    //    heapKLL.mergeWithTempSpace(tmpSketch, tmpSketch.getMin(), tmpSketch.getMax());
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
          //          for (TDigestForExact sketch : stat.getKllSketchList()) {
          //            ((LongKLLSketch) sketch).deserializeFromBuffer();
          //            addSketch(
          //                sketch, stat.getMinValue(),stat.getMaxValue());
          //          }
          return;
        } // else System.out.println("\t\t\t\t!!!!!![ERROR!] no KLL in stat!");
      }
    }
    double minVal = (double) (statistics.getMinValue());
    double maxVal = (double) (statistics.getMaxValue());
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
    sketch = null;
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
    if ((seriesDataType == DOUBLE) && iteration == 0) {
      DoubleStatistics doubleStats = (DoubleStatistics) statistics;
      if (doubleStats.getSummaryNum() > 0) return true;
    }
    if (iteration > 0) {
      double minVal = (double) (statistics.getMinValue());
      double maxVal = (double) (statistics.getMaxValue());
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
    return false;
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
    if (attrs.containsKey("param")) { // 0 means don't use summary.
      String r = attrs.get("param");
      this.mergingBuffer = Integer.parseInt(r);
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

    this.batchN = MULTI_QUANTILES;
    query_q = new double[MULTI_QUANTILES];
    final double query_delta_q = 1.0 / (MULTI_QUANTILES + 1);
    for (int i = 0; i < MULTI_QUANTILES; i++) query_q[i] = query_delta_q * (i + 1);
    query_rank1 = new long[MULTI_QUANTILES];
    query_rank2 = new long[MULTI_QUANTILES];
    deterministic_result = new double[MULTI_QUANTILES][];
    iterate_result = new double[MULTI_QUANTILES][];
    lastPass = false;
    lastN = Long.MAX_VALUE;
  }
}
