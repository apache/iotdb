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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

public class OptimizedMedianAggrResult extends AggregateResult {
  private TSDataType seriesDataType;
  private long cnt;
  private int bitsCounted;
  private long prefixOfMedian1, prefixOfMedian2; // prefix of n/2, n/2+1 th number.
  private long K1, K2; //  n/2, n/2+1 th number is the K1,K2 smallest with the two prefix
  private long[] bucket1, bucket2;

  private boolean isSmallAmount;
  private HashSet<Long> valueSet;
  private HashMap<Long, Long> valueCount;
  private boolean hasFinalResult;
  static int smallAmount = 65536; // calc directly when size of valueSet <= smallAmount

  public int bitsOfBucket() {
    return 16;
  }

  private int sizeOfBucket() {
    return 1 << bitsOfBucket();
  }

  private int maskOfBucket() {
    return (1 << bitsOfBucket()) - 1;
  }

  private long maskOfPrefix() {
    return bitsCounted == 0 ? (0) : (((1L << bitsCounted) - 1) << (bitsOfDataType() - bitsCounted));
  }

  private int bitsOfDataType() {
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

  private boolean needsTwoBuckets() {
    return K2 != -1;
  }

  public OptimizedMedianAggrResult(TSDataType seriesDataType) throws UnSupportedDataTypeException {
    super(TSDataType.DOUBLE, AggregationType.EXACT_MEDIAN_OPT);
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

  private int longBitsToIndex(long longBits) {
    return (int)
        ((longBits >>> (bitsOfDataType() - bitsOfBucket() - bitsCounted)) & maskOfBucket());
  }

  private void updateBucketFromData(Object data, long times) {
    long longBits = dataToLongBits(data);
    int index = longBitsToIndex(longBits);
    if ((longBits & maskOfPrefix()) == prefixOfMedian1) bucket1[index] += times;
    if (needsTwoBuckets() && (longBits & maskOfPrefix()) == prefixOfMedian2)
      bucket2[index] += times;
    if (bitsCounted == 0) {
      longBits ^= 1L << 63; // unsigned......
      cnt += times;
      if (isSmallAmount) {
        if (!valueSet.add(longBits)) {
          valueCount.put(longBits, valueCount.get(longBits) + times);
        } else {
          if (valueSet.size() > smallAmount) {
            isSmallAmount = false;
          }
          valueCount.put(longBits, times);
        }
      }
    }
  }

  private Map<String, Long> updateFromBucket(long[] bucket, long K, long prefixOfMedian) {
    long tmpCnt = 0;
    int p = 0;
    for (int i = 0; i < sizeOfBucket(); i++) {
      tmpCnt += bucket[i];
      if (tmpCnt >= K) {
        p = i;
        break;
      }
    }
    //    System.out.println("[updateFromBucket] p:" + p);
    Map<String, Long> result = new HashMap<>();
    result.put("K", K - (tmpCnt - bucket[p]));
    result.put(
        "prefixOfMedian",
        prefixOfMedian | ((long) p << (bitsOfDataType() - bitsOfBucket() - bitsCounted)));
    //    System.out.println(
    //        "[updateFromBucket] prefixOfMedian:" + ((prefixOfMedian << bitsOfBucket()) | p));
    return result;
  }

  @Override
  public void startIteration() {
    System.out.println(
        "[DEBUG 1]:startIteration value:"
            + getValue()
            + " bitsCounted:"
            + bitsCounted
            + " 1,2:"
            + prefixOfMedian1
            + " "
            + prefixOfMedian2);
    if (bucket1 != null) Arrays.fill(bucket1, 0);
    else this.bucket1 = new long[sizeOfBucket()];
    if (bucket2 != null) Arrays.fill(bucket2, 0);
  }

  @Override
  public void finishIteration() {
    if (cnt == 0) return;

    if (bitsCounted == 0) {
      K1 = (cnt + 1) >> 1;
      if ((cnt & 1) == 0) {
        K2 = (cnt >> 1) + 1;
      } else K2 = -1;

      // TODO : just sort when the amount of data is small enough
      if (isSmallAmount) {
        List<Long> valueList = new ArrayList<>(valueSet);
        Collections.sort(valueList);
        long value1 = 0, value2 = 0, valueSum = 0;
        for (Long value : valueList) {
          if (valueSum < K1 && valueSum + valueCount.get(value) >= K1) value1 = value;
          if (valueSum < K2 && valueSum + valueCount.get(value) >= K2) value2 = value;
          valueSum += valueCount.get(value);
        }
        value1 ^= 1L << 63;
        value2 ^= 1L << 63; // unsigned...
        //        System.out.println("[DEBUG] index1:"+value1+"  "+longBitsToResult(value1));
        if (!needsTwoBuckets()) setDoubleValue(longBitsToResult(value1));
        else setDoubleValue(0.5 * (longBitsToResult(value1) + longBitsToResult(value2)));
        hasFinalResult = true;
        //        System.out.println(
        //            "[DEBUG] cnt:" + cnt + " valueSet_size:" + valueSet.size() + " ans:" +
        //                getDoubleValue());
        //        for (Long value : valueList){
        //          System.out.println("[DEBUG]..."+value+"  "+longBitsToResult(value^(1L<<63)));
        //        }
      }

      if (K2 != -1) bucket2 = Arrays.copyOf(bucket1, sizeOfBucket());
    }
    Map<String, Long> result1 = updateFromBucket(bucket1, K1, prefixOfMedian1);
    K1 = result1.get("K");
    prefixOfMedian1 = result1.get("prefixOfMedian");
    //    Arrays.fill(bucket1, 0);

    // TODO: optimize. unnecessary when prefixOfMedian1==prefixOfMedian2
    if (needsTwoBuckets()) {
      Map<String, Long> result2 = updateFromBucket(bucket2, K2, prefixOfMedian2);
      K2 = result2.get("K");
      prefixOfMedian2 = result2.get("prefixOfMedian");
      //      Arrays.fill(bucket2, 0);
    }

    bitsCounted += bitsOfBucket();
    if (bitsCounted == bitsOfDataType()) {
      if (!needsTwoBuckets()) setDoubleValue(longBitsToResult(prefixOfMedian1));
      else
        setDoubleValue(
            0.5 * (longBitsToResult(prefixOfMedian1) + longBitsToResult(prefixOfMedian2)));
      hasFinalResult = true;
      //      System.out.println(
      //          "median1: "
      //              + longBitsToResult(prefixOfMedian1)
      //              + "   median2: "
      //              + longBitsToResult(prefixOfMedian2));
    }

    //    System.out.println("\t\t[MEDIAN]"+this.hashCode()+"  finishIteration "+bitsCounted+"
    // "+bitsOfDataType());
    //        System.out.println(
    //            "K1: "
    //                + K1
    //                + " K2: "
    //                + K2
    //                + "    cnt:"
    //                + cnt
    //                + "|| prefixOfMedian1:"
    //                + prefixOfMedian1
    //                + "  prefixOfMedian2:"
    //                + prefixOfMedian2);
  }

  @Override
  protected boolean hasCandidateResult() {
    return bitsCounted == bitsOfDataType() || hasFinalResult;
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
    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();
    Comparable<Object> maxVal = (Comparable<Object>) statistics.getMaxValue();
    if (minVal.compareTo(maxVal) == 0) {
      updateBucketFromData(minVal, statistics.getCount());
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
      updateBucketFromData(batchIterator.currentValue(), 1);
      batchIterator.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(
      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        updateBucketFromData(values[i], 1);
      }
    }
  }

  @Override
  public void updateResultUsingValues(long[] timestamps, int length, ValueIterator valueIterator) {
    //    List<Object> tmp = new ArrayList<>();
    while (valueIterator.hasNext()) {
      updateBucketFromData(valueIterator.next(), 1);
      //      Object tmpObj = valueIterator.next();
      //      updateBucketFromData(tmpObj, 1);
      //      tmp.add(tmpObj);
    }
    //
    // System.out.println("\t\t[MEDIAN]"+this.hashCode()+"[updateResultUsingValues]"+tmp.toString());
  }

  @Override
  public int maxIteration() {
    return bitsOfDataType() / bitsOfBucket();
  }

  @Override
  public boolean hasFinalResult() {
    return hasFinalResult;
  }

  @Override
  public void merge(AggregateResult another) {
    System.out.println("[DEBUG] [merge] " + this.getResult() + "  " + another.getResult());
    // merge not supported
    //        throw new QueryProcessException("Can't merge MedianAggregateResult");
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {
    this.seriesDataType = TSDataType.deserialize(buffer.get());
    this.cnt = buffer.getLong();
    this.bitsCounted = buffer.getInt();
    this.K1 = buffer.getLong();
    this.K2 = buffer.getLong();
    this.prefixOfMedian1 = buffer.getLong();
    this.prefixOfMedian2 = buffer.getLong();

    this.bucket1 = new long[sizeOfBucket()];
    for (int i = 0; i < sizeOfBucket(); i++) this.bucket1[i] = buffer.getLong();
    if (needsTwoBuckets()) {
      this.bucket2 = new long[sizeOfBucket()];
      for (int i = 0; i < sizeOfBucket(); i++) this.bucket2[i] = buffer.getLong();
    }
  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(seriesDataType, outputStream);
    ReadWriteIOUtils.write(cnt, outputStream);
    ReadWriteIOUtils.write(bitsCounted, outputStream);
    ReadWriteIOUtils.write(K1, outputStream);
    ReadWriteIOUtils.write(K2, outputStream);
    ReadWriteIOUtils.write(prefixOfMedian1, outputStream);
    ReadWriteIOUtils.write(prefixOfMedian2, outputStream);
    for (int i = 0; i < sizeOfBucket(); i++) ReadWriteIOUtils.write(bucket1[i], outputStream);
    if (needsTwoBuckets())
      for (int i = 0; i < sizeOfBucket(); i++) ReadWriteIOUtils.write(bucket2[i], outputStream);
  }

  public long getCnt() {
    return cnt;
  }

  @Override
  public void reset() {
    super.reset();
    cnt = 0;
    bitsCounted = 0;
    prefixOfMedian1 = prefixOfMedian2 = 0;
    K1 = K2 = -1;
    bucket1 = bucket2 = null;
    isSmallAmount = true;
    valueSet = new HashSet<>();
    valueCount = new HashMap<>();
    hasFinalResult = false;
  }

  @Override
  public boolean canUpdateFromStatistics(Statistics statistics) {
    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();
    Comparable<Object> maxVal = (Comparable<Object>) statistics.getMaxValue();
    return (minVal.compareTo(maxVal) == 0);
  }

  @Override
  public boolean groupByLevelBeforeAggregation() {
    return true;
  }
}
