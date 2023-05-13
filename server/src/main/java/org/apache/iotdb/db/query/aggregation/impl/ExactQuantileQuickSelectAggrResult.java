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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;

public class ExactQuantileQuickSelectAggrResult extends AggregateResult {
  int mergeBufferRatio = 5;
  private String returnType = "value";
  private TSDataType seriesDataType;
  private long n;
  private boolean hasFinalResult;
  LongArrayList data;
  long DEBUG = 0;
  public static XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom();

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

  public ExactQuantileQuickSelectAggrResult(TSDataType seriesDataType)
      throws UnSupportedDataTypeException {
    super(DOUBLE, AggregationType.EXACT_QUANTILE_QUICK_SELECT);
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
    this.data.add(dataL);
    //    if(this.data.size()%1000==0)System.out.println("\t\t\t\t\t"+(double)data);
  }

  @Override
  public void startIteration() {
    n = 0;
  }

  @Override
  public void setDoubleValue(double doubleValue) {
    this.hasCandidateResult = true;
    if (this.returnType.equals("value")) this.doubleValue = doubleValue;
    else if (this.returnType.equals("space")) this.doubleValue = data.size() * Long.BYTES;
  }

  public long getKth(int L, int R, int K) {
    int pos = L + random.nextInt(R - L);
    long pivot_v = data.getLong(pos), swap_v;

    int leP = L, eqR = R;
    data.set(pos, data.set(--eqR, pivot_v)); //   [L,leP): < pivot_v ;    [eqR,R): == pivot_v ;

    for (int i = L; i < eqR; i++)
      if ((swap_v = data.getLong(i)) < pivot_v) data.set(i, data.set(leP++, swap_v));
      else if (swap_v == pivot_v) {
        data.set(i--, data.set(--eqR, swap_v));
      }

    //        if(R-eqR>1)System.out.println("\t\t\t\tk_select. same pivot v.  count:"+(R-eqR));
    if (K < leP - L) return getKth(L, leP, K);
    if (K >= (leP - L) + (R - eqR)) return getKth(leP, eqR, K - (leP - L) - (R - eqR));
    return pivot_v;
  }

  @Override
  public void finishIteration() {

    n = data.size();
    if (n == 0) {
      hasFinalResult = true;
      return;
    }

    int K1 = (int) Math.floor(QUANTILE * (n - 1) + 1);
    int K2 = (int) Math.ceil(QUANTILE * (n - 1) + 1);
    System.out.println(
        "\t\t[Quick Select DEBUG]\tfinish iter.\tn:" + data.size() + "\t\tK1,2:" + K1 + "," + K2);
    K1--;
    K2--;
    long val1 = getKth(0, (int) n, K1), val2 = Long.MAX_VALUE;
    if (K2 > K1) {
      boolean occur = false;
      for (long d : data)
        if (d < val2) {
          if (d > val1) val2 = d;
          else if (d == val1) {
            if (occur) {
              val2 = d;
              break;
            }
            occur = true;
          }
        }
    } else val2 = val1;
    double ans = 0.5 * (longToResult(val1) + longToResult(val2));
    System.out.println("\t\t\tval1,2:" + longToResult(val1) + "," + longToResult(val2));
    setDoubleValue(ans);
    hasFinalResult = true;
    //    data.sort(Long::compare);
    //    System.out.println("\t\t\tsort...
    // val1,2:"+longToResult(data.getLong(K1))+","+longToResult(data.getLong(K2)));
    //    System.out.println("\t\t\tsort...
    // min,max:"+longToResult(data.getLong(0))+","+longToResult(data.getLong((int)n-1)));
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
    System.out.println("\t\t\t???????!!!!!!!!!!!!!!!!!!!CAN't use stat");
    // no-op
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
    n = 0;
    hasFinalResult = false;
    data = new LongArrayList();
  }

  @Override
  public boolean canUpdateFromStatistics(Statistics statistics) {
    return false;
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
    if (attrs.containsKey("merge_buffer_ratio")) {
      String r = attrs.get("merge_buffer_ratio");
      this.mergeBufferRatio = Integer.parseInt(r);
    }
    System.out.println(
        "  [setAttributes DEBUG]\t\t\tmaxMemoryByte:" + maxMemoryByte + "\t\tquantile:" + QUANTILE);
  }
}
