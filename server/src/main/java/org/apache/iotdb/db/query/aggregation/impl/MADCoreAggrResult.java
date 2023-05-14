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
import org.apache.iotdb.tsfile.utils.CORESketch;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;

public class MADCoreAggrResult extends AggregateResult {
  private String returnType = "value";
  private TSDataType seriesDataType;
  private int iteration, BUCKET = 2333, card = 0, pre_card = 0;
  private double DataMinV, DataMaxV, median, memory = 0, mad, EPSILON;
  private double[] useful_range;
  private DoubleArrayList queue;
  private long n;
  private CORESketch sketch, finestSketch;
  DoubleArrayList lastPassData;
  boolean lastPass = false;
  long lastN = Long.MAX_VALUE;
  private int preComputedSketchSize;
  private boolean hasFinalResult, FindingMedian = true, is_integer = false;
  public static XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom();
  long DEBUG = 0;

  public static class SpacePair {
    public double thread_memory;
    public CORESketch sketch;

    public SpacePair(double thread_memory, CORESketch sketch) {
      this.thread_memory = thread_memory;
      this.sketch = sketch;
    }
  }

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

  public MADCoreAggrResult(TSDataType seriesDataType) throws UnSupportedDataTypeException {
    super(DOUBLE, AggregationType.MAD_CORE);
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
    if (iteration == 0) {
      n++;
      DataMinV = Math.min(DataMinV, dataD);
      DataMaxV = Math.max(DataMaxV, dataD);
    } else if (FindingMedian) {
      sketch.insert(dataD - DataMinV + 1);
    } else {
      sketch.insert_mid(dataD - DataMinV + 1);
      if (sketch.in_range(dataD - DataMinV + 1, useful_range)) {
        queue.add(dataD - DataMinV + 1);
      }
    }
  }

  private static int calculate_card(double max_value, double min_value, int space_limit) {
    int card = 0;
    while (
    /*true*/ card <= 30) {
      if (Math.ceil(Math.pow(2, card) * Math.log(max_value) / Math.log(2))
              - Math.floor(Math.pow(2, card) * Math.log(min_value) / Math.log(2))
          > space_limit) {
        return card - 1;
      }
      card += 1;
    }
    return card;
  }

  private static int calculate_card(double[] real_range, int space_limit) {
    int card = 0;
    while (
    /*true*/ card <= 30) {
      int space_expected = 0;
      for (int i = 0; i < real_range[6]; ++i) {
        space_expected +=
            Math.ceil(Math.pow(2, card) * Math.log(real_range[2 * i + 1]) / Math.log(2))
                - Math.floor(Math.pow(2, card) * Math.log(real_range[2 * i]) / Math.log(2));
      }
      if (space_expected > space_limit) {
        return card - 1;
      }
      card += 1;
    }
    return card;
  }

  @Override
  public void startIteration() {
    if (iteration == 0) { // first iteration
      DataMinV = Double.MAX_VALUE;
      DataMaxV = -Double.MAX_VALUE;
      n = 0;
    } else {
      if (iteration == 1) {
        double[] range =
            new double[] {
              1, DataMaxV - DataMinV + 1, 1, DataMaxV - DataMinV + 1, 1, DataMaxV - DataMinV + 1, 1
            };
        card = calculate_card(range, BUCKET);
        sketch = new CORESketch(card, BUCKET, range);
        pre_card = sketch.card;
      } else if (FindingMedian) {
        pre_card = sketch.card;
      } else {
        useful_range = sketch.get_range();
        sketch = new CORESketch(1);
        queue = new DoubleArrayList();
        sketch.set_range(useful_range);
      }
    }
  }

  @Override
  public void setDoubleValue(double doubleValue) {
    this.hasCandidateResult = true;
    if (this.returnType.equals("mad")) this.doubleValue = mad;
    else if (this.returnType.equals("memory")) this.doubleValue = memory;
    else if (this.returnType.equals("median")) this.doubleValue = median + DataMinV - 1;
    else if (this.returnType.equals("iteration"))
      this.doubleValue = iteration - 1; // ignore first pass for MinMaxV
  }

  @Override
  public void finishIteration() {
    iteration++;
    if (n == 0) {
      setDoubleValue(0);
      hasFinalResult = true;
      return;
    }

    if (iteration == 1) { // first iteration over
      System.out.println(
          "\t[MAD DEBUG CORESketch]\tfirst Pass for MinV,MaxV:\t"
              + DataMinV
              + "\t"
              + DataMaxV
              + "\tn:\t"
              + n);
      return;
    }
    if (iteration >= 2 && FindingMedian) { // find median
      memory = Math.max(memory, sketch.get_bucket_size() * 48.13 / 1024);

      if (sketch.data_read_norm()) {
        finestSketch = sketch;
        FindingMedian = false;
        return;
      }
      int m = sketch.mid_half_count_bucket();
      int[] lr = sketch.edge_half_count_bucket(m);
      if (is_integer && sketch.bucket_finest(m, lr[0], lr[1])) {
        finestSketch = sketch;
        FindingMedian = false;
        return;
      }
      double[] next_range = sketch.generate_useful_range(m, lr[0], lr[1]);
      memory = Math.max(sketch.get_bucket_size() * 48.13 / 1024, memory);
      sketch = new CORESketch(0, BUCKET, next_range);
      double[] real_range = sketch.real_range();
      card = calculate_card(real_range, BUCKET);
      if (card == pre_card) {
        this.hasCandidateResult = true;
        this.hasFinalResult = true;
        this.doubleValue = -233;
        System.out.println(
            "\t\t[MAD ERROR CORESketch]\tLarger bucket limit needed!!!!!!!!!!!!!!!!!!");
        return;
      }
      sketch.set_card(card);
      return;
    }
    if (!FindingMedian) {
      memory = Math.max(memory, ((double) queue.size()) * 8 / 1024);
      //        System.out.println("\t\t\t\tafter in range check. |queue|="+queue.size());
      long[] mid_num = sketch.get_gap();
      long n_queue = queue.size(), median_rank = (n - 1) / 2 - mid_num[1] - mid_num[0];
      if (median_rank < 0) median_rank += mid_num[1];
      assert median_rank >= 0;
      //        System.out.println("\t\t\tmid_rank:"+median_rank);
      median = getKth(queue, 0, (int) n_queue, (int) median_rank);
      queue.replaceAll(aDouble -> Math.abs(aDouble - median));
      double min_mad = Math.max(median - useful_range[1], useful_range[4] - median);
      long mad_rank = (n - 1) / 2 - mid_num[1] - mid_num[2];
      if (mad_rank < 0) mad_rank += mid_num[1] + mid_num[2];
      mad = getKth(queue, 0, (int) n_queue, (int) mad_rank);
      setDoubleValue(mad);
      hasFinalResult = true;
    }
  }

  public static double getKth(DoubleArrayList data, int L, int R, int K) {
    if (L >= R) return data.getDouble(L);
    int pos = L + random.nextInt(R - L);
    double pivot_v = data.getDouble(pos), swap_v;

    int leP = L, eqR = R;
    data.set(pos, data.set(--eqR, pivot_v)); //   [L,leP): < pivot_v ;    [eqR,R): == pivot_v ;

    for (int i = L; i < eqR; i++)
      if ((swap_v = data.getDouble(i)) < pivot_v) data.set(i, data.set(leP++, swap_v));
      else if (swap_v == pivot_v) {
        data.set(i--, data.set(--eqR, swap_v));
      }

    //        if(R-eqR>1)System.out.println("\t\t\t\tk_select. same pivot v.  count:"+(R-eqR));
    if (K < leP - L) return getKth(data, L, leP, K);
    if (K >= (leP - L) + (R - eqR)) return getKth(data, leP, eqR, K - (leP - L) - (R - eqR));
    return pivot_v;
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
    n += statistics.getCount();
    DataMinV = Math.min(DataMinV, (double) statistics.getMinValue());
    DataMaxV = Math.max(DataMaxV, (double) statistics.getMaxValue());
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
    return 10;
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
    mad = median = memory = 0;
    n = 0;
    iteration = 0;
    hasFinalResult = false;
    preComputedSketchSize = 0;
  }

  @Override
  public boolean canUpdateFromStatistics(Statistics statistics) {
    return iteration == 0;
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
    if (attrs.containsKey("bucket")) {
      String b = attrs.get("bucket");
      this.BUCKET = Integer.parseInt(b);
    }
    if (attrs.containsKey("is_integer")) {
      String ii = attrs.get("is_integer");
      this.is_integer = Boolean.parseBoolean(ii);
    }
    if (attrs.containsKey("return_type")) {
      String q = attrs.get("return_type");
      this.returnType = q;
    }
    System.out.println(
        "  [setAttributes DEBUG]\t\t\tbucket:" + BUCKET + "\t\tis_integer:" + is_integer);
  }
}
