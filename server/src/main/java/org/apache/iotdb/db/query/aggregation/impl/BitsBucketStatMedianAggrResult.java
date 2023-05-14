/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
// package org.apache.iotdb.db.query.aggregation.impl;
//
// import org.apache.iotdb.db.query.aggregation.AggregateResult;
// import org.apache.iotdb.db.query.aggregation.AggregationType;
// import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
// import org.apache.iotdb.db.utils.ValueIterator;
// import org.apache.iotdb.db.utils.quantiles.EclipseCollectionsHashMapForQuantile;
// import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
// import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
// import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
// import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
// import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
// import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
// import org.apache.iotdb.tsfile.utils.GSHashMapForStat;
// import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
//
// import org.eclipse.collections.api.tuple.primitive.LongLongPair;
//
// import java.io.IOException;
// import java.io.OutputStream;
// import java.nio.ByteBuffer;
// import java.util.List;
//
// import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.DOUBLE;
// import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.INT64;
//
// public class BitsBucketStatMedianAggrResult extends AggregateResult {
//  private TSDataType seriesDataType;
//  private long cnt; // = n after iteration.
//  private int bitsOfDataType,
//      bitsCounted,
//      bitsConcerned; // bitsOfDataType == bitsCounted + bitsConcerned
//  private long maskConcerned;
//  private long K1, K2;
//  private long prefixOfMedian1, prefixOfMedian2; // needs prefixOfMedian2 when n is even
//  private long maxWithPrefix1, minWithPrefix2; // when two medians divided
//  private EclipseCollectionsHashMapForQuantile hashMap;
//  private GSHashMapForStat statHashMap;
//  private boolean usingStatistics = false;
//
//  private boolean hasFinalResult;
//  private boolean hasTwoDividedMedians;
//
//  private long maskOfPrefix() {
//    return bitsCounted == 0 ? (0) : (((1L << bitsCounted) - 1) << (bitsOfDataType - bitsCounted));
//  }
//
//  private int getBitsOfDataType() {
//    switch (seriesDataType) {
//      case INT32:
//      case FLOAT:
//        return 32;
//      case INT64:
//      case DOUBLE:
//        return 64;
//      default:
//        throw new UnSupportedDataTypeException(
//            String.format("Unsupported data type in aggregation MEDIAN : %s", seriesDataType));
//    }
//  }
//
//  private boolean hasTwoMedians() {
//    return (cnt & 1) == 0;
//  }
//
//  private boolean hasTwoDividedMedians() {
//    return hasTwoMedians() && prefixOfMedian1 != prefixOfMedian2;
//  }
//
//  public BitsBucketStatMedianAggrResult(TSDataType seriesDataType)
//      throws UnSupportedDataTypeException {
//    super(DOUBLE, AggregationType.EXACT_MEDIAN_BITS_BUCKET_STAT);
//    this.seriesDataType = seriesDataType;
//    reset();
//  }
//
//  // turn FLOAT/INT32 to unsigned long keeping relative order
//  private long dataToLongBits(Object data) throws UnSupportedDataTypeException {
//    long longBits;
//    switch (seriesDataType) {
//      case INT32:
//        return (int) data + (1L << 31);
//      case FLOAT:
//        longBits = Float.floatToIntBits((float) data) + (1L << 31);
//        return (float) data >= 0f ? longBits : longBits ^ 0x7F800000L;
//      case INT64:
//        return (long) data + (1L << 63);
//      case DOUBLE:
//        longBits = Double.doubleToLongBits((double) data) + (1L << 63);
//        return (double) data >= 0d ? longBits : longBits ^ 0x7FF0000000000000L;
//      default:
//        throw new UnSupportedDataTypeException(
//            String.format("Unsupported data type in aggregation MEDIAN : %s", seriesDataType));
//    }
//  }
//
//  private double longBitsToResult(long longBits) throws UnSupportedDataTypeException {
//    switch (seriesDataType) {
//      case INT32:
//        return (double) (longBits - (1L << 31));
//      case FLOAT:
//        longBits = (longBits >>> 31) > 0 ? longBits : longBits ^ 0x7F800000L;
//        return Float.intBitsToFloat((int) (longBits - (1L << 31)));
//      case INT64:
//        return (double) (longBits - (1L << 63));
//      case DOUBLE:
//        longBits = (longBits >>> 63) > 0 ? longBits : longBits ^ 0x7FF0000000000000L;
//        return Double.longBitsToDouble(longBits - (1L << 63));
//      default:
//        throw new UnSupportedDataTypeException(
//            String.format("Unsupported data type in aggregation MEDIAN : %s", seriesDataType));
//    }
//  }
//
//  private void updateStatusFromData(Object data, long times) {
//    long longBits = dataToLongBits(data);
//    if (bitsCounted > 0
//        && (longBits & maskOfPrefix()) != prefixOfMedian1
//        && (longBits & maskOfPrefix()) != prefixOfMedian2) return;
//    if (bitsCounted == 0) cnt += times;
//    if (usingStatistics) {
//      statHashMap.insertLongBits(64, longBits, times);
//      return;
//    }
//    if (hasTwoDividedMedians) {
//      if ((longBits & maskOfPrefix()) == prefixOfMedian1)
//        maxWithPrefix1 = Math.max(maxWithPrefix1, longBits);
//      else minWithPrefix2 = Math.min(minWithPrefix2, longBits);
//      return;
//    }
//    //    if(hashMap.getHashMapSize()==0)
//    //      System.out.println("???!!!!");
//    //    if(bitsConcerned==2)
//    //      System.out.println("\t\t\t"+(long)data+"    "+(longBits & maskConcerned));
//    long dataConcerned = longBits & maskConcerned;
//    hashMap.insert(dataConcerned, times);
//  }
//
//  @Override
//  public void startIteration() {
//    bitsConcerned = bitsOfDataType - bitsCounted;
//    maskConcerned = bitsConcerned == 64 ? -1L : ((1L << bitsConcerned) - 1);
//
//    hasTwoDividedMedians = hasTwoDividedMedians();
//    //    System.out.println("[DEBUG]:startIteration value:"+getValue()
//    //        + " bitsCounted:"+bitsCounted+" prefix1,2:"+prefixOfMedian1+" "+prefixOfMedian2
//    //        + " divided:"+hasTwoDividedMedians
//    //        + " K1,2:" + K1+" "+K2);
//    //    System.out.println("[DEBUG]\t\t result1:"+longBitsToResult(prefixOfMedian1));
//    if (hasTwoDividedMedians) {
//      maxWithPrefix1 = Long.MIN_VALUE;
//      minWithPrefix2 = Long.MAX_VALUE;
//      return;
//    }
//    if (bitsCounted == 0) { // first iteration
//      if (bitsOfDataType == 32)
//        hashMap = new EclipseCollectionsHashMapForQuantile(bitsOfDataType, 31);
//      else hashMap = new EclipseCollectionsHashMapForQuantile(bitsOfDataType, 16);
//    } else {
//      if (bitsConcerned <= 16) hashMap.reset(bitsConcerned, 16);
//      else if (bitsConcerned <= 32) hashMap.reset(bitsConcerned, bitsConcerned - 1);
//      else hashMap.reset(bitsConcerned, 16);
//    }
//  }
//
//  @Override
//  public void finishIteration() {
//    System.out.println(
//        "[BitsBucketStat DEBUG] finishIteration hashMapBits:"
//            + hashMap.getRemainingBits()
//            + "   bitsCounted:"
//            + bitsCounted
//            + "??"
//            + usingStatistics);
//    if (cnt == 0) {
//      hasFinalResult = true;
//      return;
//    }
//    if (hasTwoDividedMedians) {
//      //            System.out.println("[DEBUG]hasTwoDividedMedians");
//      setDoubleValue(0.5 * (longBitsToResult(maxWithPrefix1) + longBitsToResult(minWithPrefix2)));
//      hasFinalResult = true;
//      return;
//    }
//    if (bitsCounted == 0) {
//      K1 = (cnt + 1) >> 1;
//      K2 = hasTwoMedians() ? (K1 + 1) : K1;
//    }
//    if (usingStatistics) {
//      hashMap.reset(statHashMap.remainingBits, 0);
//      long tmpTOT = 0;
//      for (LongLongPair p : statHashMap.getKeyValuesView()) {
//        hashMap.insert(p.getOne(), p.getTwo());
//        tmpTOT += p.getTwo();
//      }
//      //      System.out.println("\t\t[DEBUG] BitsBucketStat finishIteration  stat size:"
//      //          +hashMap.getHashMapSize()+"  remainingBits:"+hashMap.getRemainingBits()+"
//      // TOT:"+tmpTOT);
//    }
//    List<Long> iterationResult = hashMap.findResultIndex(K1, K2);
//    int bitsCountedInIteration = Math.min(bitsConcerned, hashMap.getRemainingBits());
//    prefixOfMedian1 |= iterationResult.get(0) << (bitsConcerned - bitsCountedInIteration);
//    K1 -= iterationResult.get(1);
//    prefixOfMedian2 |= iterationResult.get(2) << (bitsConcerned - bitsCountedInIteration);
//    K2 -= iterationResult.get(3);
//    bitsCounted += bitsCountedInIteration;
//    if (bitsCounted == bitsOfDataType) {
//      if (!hasTwoMedians()) setDoubleValue(longBitsToResult(prefixOfMedian1));
//      else
//        setDoubleValue(
//            0.5 * (longBitsToResult(prefixOfMedian1) + longBitsToResult(prefixOfMedian2)));
//      hasFinalResult = true;
//      //      System.out.println("[opt_4 DEBUG] calc over  answer:" + getDoubleValue());
//    }
//    if (usingStatistics) {
//      usingStatistics = false;
//      statHashMap = null;
//    }
//    //    System.out.println("\t\t[BitsBucketStat]" + this.hashCode()
//    //        + "  finishIteration " + bitsCounted + " " + bitsOfDataType);
//    //    System.out.println(
//    //        "K1: "
//    //            + K1
//    //            + " K2: "
//    //            + K2
//    //            + "    cnt:"
//    //            + cnt
//    //            + "|| prefixOfMedian1:"
//    //            + prefixOfMedian1
//    //            + "  prefixOfMedian2:"
//    //            + prefixOfMedian2);
//  }
//
//  @Override
//  protected boolean hasCandidateResult() {
//    return hasFinalResult && cnt > 0;
//  }
//
//  @Override
//  public Double getResult() {
//    return hasCandidateResult() ? getDoubleValue() : null;
//  }
//
//  @Override
//  public void updateResultFromStatistics(Statistics statistics) {
//    switch (statistics.getType()) {
//      case INT32:
//      case INT64:
//      case FLOAT:
//      case DOUBLE:
//        break;
//      case TEXT:
//      case BOOLEAN:
//      default:
//        throw new UnSupportedDataTypeException(
//            String.format(
//                "Unsupported data type in aggregation MEDIAN : %s", statistics.getType()));
//    }
//    cnt += statistics.getCount();
//    if (statistics.getType() == DOUBLE) {
//      DoubleStatistics doubleStat = (DoubleStatistics) statistics;
//      //    System.out.println("\t\t????"+doubleStat.getGSHashMap().getHashMapSize()+"
//      // "+doubleStat.hashCode()
//      //        +"  time:"+doubleStat.getStartTime()+"...."+doubleStat.getEndTime());
//      //    System.out.println("\t\t????"+doubleStat.getGSHashMap().getRemainingBits()+"
//      // self:"+statHashMap.getRemainingBits());
//      statHashMap.merge(doubleStat.getGSHashMap());
//    }
//    if (statistics.getType() == INT64) {
//      LongStatistics longStat = (LongStatistics) statistics;
//      statHashMap.merge(longStat.getGSHashMap());
//    }
//    //    System.out.println("\t\t???!"+statHashMap.getHashMapSize());
//    //    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();
//    //    Comparable<Object> maxVal = (Comparable<Object>) statistics.getMaxValue();
//    //    if (minVal.compareTo(maxVal) == 0) {
//    //      updateStatusFromData(minVal, statistics.getCount());
//    //    } /*else
//    //      throw new QueryProcessException("Failed to update median aggregation result from
//    // statistics.");*/
//  }
//
//  @Override
//  public void updateResultFromPageData(IBatchDataIterator batchIterator) {
//    updateResultFromPageData(batchIterator, Long.MIN_VALUE, Long.MAX_VALUE);
//  }
//
//  @Override
//  public void updateResultFromPageData(
//      IBatchDataIterator batchIterator, long minBound, long maxBound) {
//    while (batchIterator.hasNext()) {
//      if (batchIterator.currentTime() >= maxBound || batchIterator.currentTime() < minBound) {
//        break;
//      }
//      updateStatusFromData(batchIterator.currentValue(), 1);
//      batchIterator.next();
//    }
//  }
//
//  @Override
//  public void updateResultUsingTimestamps(
//      long[] timestamps, int length, IReaderByTimestamp dataReader) throws IOException {
//    Object[] values = dataReader.getValuesInTimestamps(timestamps, length);
//    for (int i = 0; i < length; i++) {
//      if (values[i] != null) {
//        updateStatusFromData(values[i], 1);
//      }
//    }
//  }
//
//  @Override
//  public void updateResultUsingValues(long[] timestamps, int length, ValueIterator valueIterator)
// {
//    //    List<Object> tmp = new ArrayList<>();
//    while (valueIterator.hasNext()) {
//      updateStatusFromData(valueIterator.next(), 1);
//      //      Object tmpObj = valueIterator.next();
//      //      updateStatusFromData(tmpObj, 1);
//      //      tmp.add(tmpObj);
//    }
//    //
//    //
// System.out.println("\t\t[MEDIAN]"+this.hashCode()+"[updateResultUsingValues]"+tmp.toString());
//  }
//
//  @Override
//  public int maxIteration() {
//    return bitsOfDataType / 16 + 1;
//  }
//
//  @Override
//  public boolean hasFinalResult() {
//    return hasFinalResult;
//  }
//
//  @Override
//  public void merge(AggregateResult another) {
//    //    System.out.println("[DEBUG] [merge] " + this.getResult() + "  " + another.getResult());
//    // merge not supported
//    //        throw new QueryProcessException("Can't merge MedianAggregateResult");
//  }
//
//  @Override
//  protected void deserializeSpecificFields(ByteBuffer buffer) {
//    this.seriesDataType = TSDataType.deserialize(buffer.get());
//    this.cnt = buffer.getLong();
//    this.bitsCounted = buffer.getInt();
//    this.prefixOfMedian1 = buffer.getLong();
//    this.prefixOfMedian2 = buffer.getLong();
//    // TODO
//  }
//
//  @Override
//  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {
//    ReadWriteIOUtils.write(seriesDataType, outputStream);
//    ReadWriteIOUtils.write(cnt, outputStream);
//    ReadWriteIOUtils.write(bitsCounted, outputStream);
//    ReadWriteIOUtils.write(prefixOfMedian1, outputStream);
//    ReadWriteIOUtils.write(prefixOfMedian2, outputStream);
//    // TODO
//  }
//
//  public long getCnt() {
//    return cnt;
//  }
//
//  @Override
//  public void reset() {
//    super.reset();
//    cnt = 0;
//    bitsCounted = 0;
//    bitsOfDataType = getBitsOfDataType();
//    prefixOfMedian1 = prefixOfMedian2 = 0;
//    hasTwoDividedMedians = false;
//    hasFinalResult = false;
//    statHashMap = null;
//    usingStatistics = false;
//  }
//
//  @Override
//  public boolean canUpdateFromStatistics(Statistics statistics) {
//    if ((seriesDataType == DOUBLE) && bitsCounted == 0) {
//      DoubleStatistics doubleStats = (DoubleStatistics) statistics;
//      if (doubleStats.hasSerializeHashMap()) {
//        //        System.out.println("\t\t[DEBUG] bitsBucketStat trying to use stat with count + "
//        //            + doubleStats.getCount());
//        if (!usingStatistics) {
//          statHashMap = new GSHashMapForStat((byte) 64, 65535);
//          usingStatistics = true;
//          if (hashMap.getHashMapSize() > 0) {
//            for (LongLongPair p : hashMap.getKeyValuesView()) {
//              statHashMap.remainingBits = (byte) hashMap.getRemainingBits();
//              statHashMap.insertLongBits(hashMap.getRemainingBits(), p.getOne(), p.getTwo());
//            }
//          }
//        }
//        return true;
//      }
//    }
//    if ((seriesDataType == TSDataType.INT64) && bitsCounted == 0) {
//      LongStatistics longStats = (LongStatistics) statistics;
//      if (longStats.hasSerializeHashMap()) {
//        if (!usingStatistics) {
//          statHashMap = new GSHashMapForStat((byte) 64, 65535);
//          usingStatistics = true;
//          if (hashMap.getHashMapSize() > 0) {
//            for (LongLongPair p : hashMap.getKeyValuesView()) {
//              statHashMap.remainingBits = (byte) hashMap.getRemainingBits();
//              statHashMap.insertLongBits(hashMap.getRemainingBits(), p.getOne(), p.getTwo());
//            }
//          }
//        }
//        return true;
//      }
//    }
//    return false;
//    //    Comparable<Object> minVal = (Comparable<Object>) statistics.getMinValue();
//    //    Comparable<Object> maxVal = (Comparable<Object>) statistics.getMaxValue();
//    //    return (minVal.compareTo(maxVal) == 0);
//  }
//
//  @Override
//  public boolean groupByLevelBeforeAggregation() {
//    return true;
//  }
// }
