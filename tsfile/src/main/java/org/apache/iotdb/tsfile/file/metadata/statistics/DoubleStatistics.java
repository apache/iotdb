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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.LongKLLSketch;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.SamplingHeapForStatMerge;
import org.apache.iotdb.tsfile.utils.TDigestForStatMerge;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.io.*;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DoubleStatistics extends Statistics<Double> {

  private double minValue;
  private double maxValue;
  private double firstValue;
  private double lastValue;
  private double sumValue;
  private int summaryNum = 0;
  private LongKLLSketch kllSketch = null;
  private List<LongKLLSketch> kllSketchList = null;
  private TDigestForStatMerge tDigest = null;
  private List<TDigestForStatMerge> tDigestList = null;
  private SamplingHeapForStatMerge sampling = null;
  private List<SamplingHeapForStatMerge> samplingList = null;
  private int bfNum = 0;
  private BloomFilter<Long> bf = null;
  private List<BloomFilter<Long>> bfList = null;
  private MutableLongList MinTimeMaxTimeCountList = null;

  static final int DOUBLE_STATISTICS_FIXED_RAM_SIZE = 90;

  @Override
  public TSDataType getType() {
    return TSDataType.DOUBLE;
  }

  /**
   * The output of this method should be identical to the method "serializeStats(OutputStream
   * outputStream)"
   */
  @Override
  public int getStatsSize() { // only used for page stat.
    //    System.out.println("\t\t\t\t[DouStat]getStatsSize");
    return 40 + 4 /* + summaryNum * SYNOPSIS_SIZE_IN_BYTE*/;
  }

  @Override
  public int getChunkMetaDataStatsSize() { // never used..
    return 40 + 4 + summaryNum * SYNOPSIS_SIZE_IN_BYTE + bfNum * BLOOM_FILTER_SIZE;
  }

  private long dataToLong(double data) {
    long result;
    result = Double.doubleToLongBits(data);
    return data >= 0d ? result : result ^ Long.MAX_VALUE;
  }
  /**
   * initialize double statistics.
   *
   * @param min min value
   * @param max max value
   * @param first the first value
   * @param last the last value
   * @param sum sum value
   */
  public void initializeStats(double min, double max, double first, double last, double sum) {
    this.minValue = min;
    this.maxValue = max;
    this.firstValue = first;
    this.lastValue = last;
    this.sumValue = sum;
  }

  private void updateStats(double minValue, double maxValue, double lastValue, double sumValue) {
    if (minValue < this.minValue) {
      this.minValue = minValue;
    }
    if (maxValue > this.maxValue) {
      this.maxValue = maxValue;
    }
    this.sumValue += sumValue;
    this.lastValue = lastValue;
  }

  private void updateStats(
      double minValue,
      double maxValue,
      double firstValue,
      double lastValue,
      double sumValue,
      long startTime,
      long endTime) {
    if (minValue < this.minValue) {
      this.minValue = minValue;
    }
    if (maxValue > this.maxValue) {
      this.maxValue = maxValue;
    }
    this.sumValue += sumValue;
    // only if endTime greater or equals to the current endTime need we update the last value
    // only if startTime less or equals to the current startTime need we update the first value
    // otherwise, just ignore
    if (startTime <= this.getStartTime()) {
      this.firstValue = firstValue;
    }
    if (endTime >= this.getEndTime()) {
      this.lastValue = lastValue;
    }
  }

  //  public boolean isSingleSketch() {
  //    return isSingleSketch;
  //  }

  public int getSummaryNum() {
    return summaryNum;
  }

  public int getKllSketchNum() {
    return summaryNum;
  }

  public int getBfNum() {
    return bfNum;
  }

  public long getBFMinTime(int bfID) {
    return MinTimeMaxTimeCountList.get(bfID * 3);
  }

  public long getBFMaxTime(int bfID) {
    return MinTimeMaxTimeCountList.get(bfID * 3 + 1);
  }

  public long getBFCount(int bfID) {
    return MinTimeMaxTimeCountList.get(bfID * 3 + 2);
  }

  //  public LongKLLSketch getKllSketch() {
  //    return kllSketch;
  //  }
  public List<LongKLLSketch> getKllSketchList() {
    return kllSketchList;
  }

  public List<TDigestForStatMerge> getTDigestList() {
    return tDigestList;
  }

  public List<SamplingHeapForStatMerge> getSamplingList() {
    return samplingList;
  }

  protected static double getFPP(double bitsPerKey) {
    return Math.exp(-1 * bitsPerKey * Math.pow(Math.log(2.0D), 2));
  }

  protected static long optimalNumOfBits(long n, double p) {
    return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  protected static int getBFArrayLength(long bits) {
    return Ints.checkedCast(LongMath.divide(bits, 64, RoundingMode.CEILING));
  }

  protected static int calcBFSize(long n, double p) {
    return getBFArrayLength(optimalNumOfBits(n, p)) * 8 + 6;
  }

  @Override
  public void updateBF(long time) {
    if (ENABLE_BLOOM_FILTER) {
      if (bfNum == 0) {
        this.bfList = new ArrayList<>(this.bfNum = 1);
        this.bfList.add(
            this.bf =
                BloomFilter.create(
                    Funnels.longFunnel(),
                    STATISTICS_PAGE_MAXSIZE,
                    getFPP(BLOOM_FILTER_BITS_PER_KEY)));
        this.bf.put(time);
        this.MinTimeMaxTimeCountList =
            LongLists.mutable.withInitialCapacity(3); // new LongArrayList(3);
        this.MinTimeMaxTimeCountList.add(time);
        this.MinTimeMaxTimeCountList.add(time);
        this.MinTimeMaxTimeCountList.add(1);
      } else if (bfNum == 1) {
        this.bf.put(time);
        this.MinTimeMaxTimeCountList.set(0, getStartTime());
        this.MinTimeMaxTimeCountList.set(1, getEndTime());
        this.MinTimeMaxTimeCountList.set(2, getCount());
      } else
        System.out.println(
            "\t[ERROR][DoubleStatistics updateStats]" + " add time in a chunk stat with > 1 bf.");
    }
  }

  @Override
  void updateStats(double value) {
    if (this.isEmpty) {
      initializeStats(value, value, value, value, value);
      isEmpty = false;
      if (ENABLE_SYNOPSIS) {
        this.summaryNum = 1;
        if (SUMMARY_TYPE == 0) {
          this.kllSketchList = new ArrayList<>(1);
          this.kllSketchList.add(
              this.kllSketch =
                  new LongKLLSketch(
                      STATISTICS_PAGE_MAXSIZE, PAGE_SIZE_IN_BYTE, SYNOPSIS_SIZE_IN_BYTE));
          this.kllSketch.update(dataToLong(value));
        }
        if (SUMMARY_TYPE == 1) {
          this.tDigestList = new ArrayList<>(1);
          this.tDigestList.add(
              this.tDigest = new TDigestForStatMerge(PAGE_SIZE_IN_BYTE, SYNOPSIS_SIZE_IN_BYTE));
          this.tDigest.update(value);
        }
        if (SUMMARY_TYPE == 2) {
          this.samplingList = new ArrayList<>(1);
          this.samplingList.add(
              this.sampling =
                  new SamplingHeapForStatMerge(PAGE_SIZE_IN_BYTE, SYNOPSIS_SIZE_IN_BYTE));
          this.sampling.update(dataToLong(value));
        }
      }
    } else {
      updateStats(value, value, value, value);
      if (ENABLE_SYNOPSIS) {
        if (SUMMARY_TYPE == 0) {
          if (this.summaryNum == 1) {
            if (this.kllSketch.getN() >= STATISTICS_PAGE_MAXSIZE) {
              if (this.kllSketch.getN() % 1000 == 0)
                System.out.println(
                    "\t[DoubleStatistics updateStats]"
                        + " add %1000=0 value in a TOO big sketch."
                        + this.kllSketch.getN());
            } else {
              this.kllSketch.update(dataToLong(value));
            }
          }
        }
        if (SUMMARY_TYPE == 1) {
          this.tDigest.update(value);
        }
        if (SUMMARY_TYPE == 2) {
          this.sampling.update(dataToLong(value));
        }
        if (this.summaryNum > 1)
          System.out.println(
              "\t[ERROR][DoubleStatistics updateStats]"
                  + " add value in a chunk stat with > 1 sketch.");
      }
    }
  }

  @Override
  void updateStats(double[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i]);
    }
  }

  @Override
  public long calculateRamSize() {
    return DOUBLE_STATISTICS_FIXED_RAM_SIZE
        + (long) summaryNum * SYNOPSIS_SIZE_IN_BYTE
        + (long) bfNum * BLOOM_FILTER_SIZE;
  }

  @Override
  public Double getMinValue() {
    return minValue;
  }

  @Override
  public Double getMaxValue() {
    return maxValue;
  }

  @Override
  public Double getFirstValue() {
    return firstValue;
  }

  @Override
  public Double getLastValue() {
    return lastValue;
  }

  @Override
  public double getSumDoubleValue() {
    return sumValue;
  }

  @Override
  public long getSumLongValue() {
    throw new StatisticsClassException(
        String.format(STATS_UNSUPPORTED_MSG, TSDataType.DOUBLE, "long sum"));
  }

  @Override
  public void setPageStatFromChunkMetaDataStat(
      Statistics<? extends Serializable> stat, int pageID) {
    DoubleStatistics doubleStat = ((DoubleStatistics) stat);
    if (doubleStat.summaryNum > 0) {
      this.summaryNum = 1;
      if (SUMMARY_TYPE == 0) {
        this.kllSketch = doubleStat.kllSketchList.get(pageID);
        this.kllSketchList = new ArrayList<>(1);
        this.kllSketchList.add(this.kllSketch);
      }
      if (SUMMARY_TYPE == 1) {
        this.tDigest = doubleStat.tDigestList.get(pageID);
        this.tDigestList = new ArrayList<>(1);
        this.tDigestList.add(this.tDigest);
      }
      if (SUMMARY_TYPE == 2) {
        this.sampling = doubleStat.samplingList.get(pageID);
        this.samplingList = new ArrayList<>(1);
        this.samplingList.add(this.sampling);
      }
      //      System.out.println(
      //          "\t[DEBUG][DoubleStat setPageStat] pageID="
      //              + pageID
      //              + "\tn:"
      //              + this.getCount()
      //              + "\tkllN:"
      //              + kllSketch.getN());
    }
    if (doubleStat.bfNum > 0) {
      this.bfNum = 1;
      this.bf = doubleStat.bfList.get(pageID);
      this.bfList = new ArrayList<>(1);
      this.bfList.add(this.bf);
    }
  }

  public void mergeChunkMetadataStatValue(Statistics<Double> stats) {
    DoubleStatistics doubleStats = (DoubleStatistics) stats;
    if (this.isEmpty) {
      initializeStats(
          doubleStats.getMinValue(),
          doubleStats.getMaxValue(),
          doubleStats.getFirstValue(),
          doubleStats.getLastValue(),
          doubleStats.sumValue);
      isEmpty = false;
      if (ENABLE_SYNOPSIS) {
        this.summaryNum = doubleStats.summaryNum;
        if (SUMMARY_TYPE == 0) {
          this.kllSketch = doubleStats.kllSketch;
          this.kllSketchList = doubleStats.kllSketchList;
        }
        if (SUMMARY_TYPE == 1) {
          this.tDigest = doubleStats.tDigest;
          this.tDigestList = doubleStats.tDigestList;
        }
        if (SUMMARY_TYPE == 2) {
          this.sampling = doubleStats.sampling;
          this.samplingList = doubleStats.samplingList;
        }
        //        System.out.println(
        //            "\t[DEBUG][DoubleStat MERGE_STAT] from EMPTY. now N="
        //                + this.getCount()
        //                + "  summaryNum:"
        //                + summaryNum);
      }
      if (ENABLE_BLOOM_FILTER) {
        this.bf = doubleStats.bf;
        this.bfList = doubleStats.bfList;
        this.bfNum = doubleStats.bfNum;
        this.MinTimeMaxTimeCountList = doubleStats.MinTimeMaxTimeCountList;
      }
    } else {
      updateStats(
          doubleStats.getMinValue(),
          doubleStats.getMaxValue(),
          doubleStats.getFirstValue(),
          doubleStats.getLastValue(),
          doubleStats.sumValue,
          stats.getStartTime(),
          stats.getEndTime());
      if (ENABLE_SYNOPSIS) {
        this.summaryNum += doubleStats.summaryNum;
        if (SUMMARY_TYPE == 0) this.kllSketchList.addAll(doubleStats.kllSketchList);
        if (SUMMARY_TYPE == 1) this.tDigestList.addAll(doubleStats.tDigestList);
        if (SUMMARY_TYPE == 2) this.samplingList.addAll(doubleStats.samplingList);
        //        System.out.println(
        //            "\t[DEBUG][DoubleStat MERGE_STAT] from another."
        //                + "\tanother: N="
        //                + doubleStats.getCount()
        //                + " statNum:"
        //                + doubleStats.getsummaryNum()
        //                + "\tnow N="
        //                + this.getCount()
        //                + "  summaryNum:"
        //                + summaryNum);
      }
      if (ENABLE_BLOOM_FILTER) {
        this.bfList.addAll(doubleStats.bfList);
        this.bfNum += doubleStats.bfNum;
        this.MinTimeMaxTimeCountList.addAll(doubleStats.MinTimeMaxTimeCountList);
      }
    }
  }

  @Override
  protected void mergeStatisticsValue(Statistics<Double> stats) {
    DoubleStatistics doubleStats = (DoubleStatistics) stats;
    if (this.isEmpty) {
      initializeStats(
          doubleStats.getMinValue(),
          doubleStats.getMaxValue(),
          doubleStats.getFirstValue(),
          doubleStats.getLastValue(),
          doubleStats.sumValue);
      isEmpty = false;
      //      if (ENABLE_SYNOPSIS) {
      //        this.kllSketch = doubleStats.kllSketch;
      //        this.kllSketchList = doubleStats.kllSketchList;
      //        this.summaryNum = doubleStats.summaryNum;
      //        //        System.out.println(
      //        //            "\t[DEBUG][DoubleStat MERGE_STAT] from EMPTY. now N="
      //        //                + this.getCount()
      //        //                + "  summaryNum:"
      //        //                + summaryNum);
      //      }
      //      if (ENABLE_BLOOM_FILTER) {
      //        this.bf = doubleStats.bf;
      //        this.bfList = doubleStats.bfList;
      //        this.bfNum = doubleStats.bfNum;
      //      }
    } else {
      updateStats(
          doubleStats.getMinValue(),
          doubleStats.getMaxValue(),
          doubleStats.getFirstValue(),
          doubleStats.getLastValue(),
          doubleStats.sumValue,
          stats.getStartTime(),
          stats.getEndTime());
      //      if (ENABLE_SYNOPSIS) {
      //        this.kllSketchList.addAll(doubleStats.kllSketchList);
      //        this.summaryNum += doubleStats.summaryNum;
      //        //        System.out.println(
      //        //            "\t[DEBUG][DoubleStat MERGE_STAT] from another."
      //        //                + "\tanother: N="
      //        //                + doubleStats.getCount()
      //        //                + " statNum:"
      //        //                + doubleStats.getsummaryNum()
      //        //                + "\tnow N="
      //        //                + this.getCount()
      //        //                + "  summaryNum:"
      //        //                + summaryNum);
      //      }
      //      if (ENABLE_BLOOM_FILTER) {
      //        this.bfList.addAll(doubleStats.bfList);
      //        this.bfNum += doubleStats.bfNum;
      //      }
    }
  }

  @Override
  int serializeChunkMetadataStat(OutputStream outputStream) throws IOException {
    //    System.out.println("\t\t\t\t\t[DEBUG DOUBLE stat] serializeStats
    // hashmap:"+serializeHashMap);
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(minValue, outputStream);
    byteLen += ReadWriteIOUtils.write(maxValue, outputStream);
    byteLen += ReadWriteIOUtils.write(firstValue, outputStream);
    byteLen += ReadWriteIOUtils.write(lastValue, outputStream);
    byteLen += ReadWriteIOUtils.write(sumValue, outputStream);
    byteLen += ReadWriteIOUtils.write(summaryNum, outputStream);
    byteLen += ReadWriteIOUtils.write(bfNum, outputStream);
    if (summaryNum > 0) {
      if (SUMMARY_TYPE == 0)
        for (LongKLLSketch sketch : kllSketchList) {
          int tmp = sketch.serialize(outputStream);
          byteLen += tmp;
          //        System.out.println("\t[DEBUG][DoubleStat serializeStats]:\tbytes:" + tmp);
          //        sketch.show();
        }
      if (SUMMARY_TYPE == 1)
        for (TDigestForStatMerge summary : tDigestList) {
          int tmp = summary.serialize(outputStream);
          byteLen += tmp;
          //        System.out.println("\t[DEBUG][DoubleStat serializeStats]:\tbytes:" + tmp);
          //        sketch.show();
        }
      if (SUMMARY_TYPE == 2)
        for (SamplingHeapForStatMerge summary : samplingList) {
          int tmp = summary.serialize(outputStream);
          byteLen += tmp;
          //        System.out.println("\t[DEBUG][DoubleStat serializeStats]:\tbytes:" + tmp);
          //        sketch.show();
        }
    }
    if (bfNum > 0) {
      byteLen += ReadWriteIOUtils.write(BLOOM_FILTER_SIZE, outputStream);
      for (BloomFilter<Long> bf : bfList) {
        bf.writeTo(outputStream);
        byteLen += BLOOM_FILTER_SIZE;
        //        System.out.println("\t[DEBUG][DoubleStat serializeStats]:\tbfBytes:" +
        // BLOOM_FILTER_SIZE);
      }
      for (int i = 0; i < bfNum * 3; i++)
        byteLen += ReadWriteIOUtils.write(MinTimeMaxTimeCountList.get(i), outputStream);
    }

    return byteLen;
  }

  @Override
  public int serializeStats(OutputStream outputStream) throws IOException {
    //    System.out.println("\t\t\t\t\t[DEBUG DOUBLE stat] serializeStats
    // hashmap:"+serializeHashMap);
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(minValue, outputStream);
    byteLen += ReadWriteIOUtils.write(maxValue, outputStream);
    byteLen += ReadWriteIOUtils.write(firstValue, outputStream);
    byteLen += ReadWriteIOUtils.write(lastValue, outputStream);
    byteLen += ReadWriteIOUtils.write(sumValue, outputStream);
    byteLen += ReadWriteIOUtils.write(0, outputStream);
    byteLen += ReadWriteIOUtils.write(0, outputStream);
    return byteLen;
  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {
    this.minValue = ReadWriteIOUtils.readDouble(inputStream);
    this.maxValue = ReadWriteIOUtils.readDouble(inputStream);
    this.firstValue = ReadWriteIOUtils.readDouble(inputStream);
    this.lastValue = ReadWriteIOUtils.readDouble(inputStream);
    this.sumValue = ReadWriteIOUtils.readDouble(inputStream);
    this.summaryNum = ReadWriteIOUtils.readInt(inputStream);
    this.bfNum = ReadWriteIOUtils.readInt(inputStream);
    if (this.summaryNum > 0) {
      if (SUMMARY_TYPE == 0) {
        this.kllSketchList = new ArrayList<>(summaryNum);
        for (int i = 0; i < summaryNum; i++) this.kllSketchList.add(new LongKLLSketch(inputStream));
      }
      if (SUMMARY_TYPE == 1) {
        this.tDigestList = new ArrayList<>(summaryNum);
        for (int i = 0; i < summaryNum; i++)
          this.tDigestList.add(new TDigestForStatMerge(inputStream));
      }
      if (SUMMARY_TYPE == 2) {
        this.samplingList = new ArrayList<>(summaryNum);
        for (int i = 0; i < summaryNum; i++)
          this.samplingList.add(new SamplingHeapForStatMerge(inputStream));
      }
    }
    if (this.bfNum > 0) {
      this.bfList = new ArrayList<>(bfNum);
      for (int i = 0; i < bfNum; i++)
        this.bfList.add(BloomFilter.readFrom(inputStream, Funnels.longFunnel()));
      this.MinTimeMaxTimeCountList = LongLists.mutable.withInitialCapacity(bfNum * 3);
      for (int i = 0; i < bfNum * 3; i++)
        this.MinTimeMaxTimeCountList.add(ReadWriteIOUtils.readLong(inputStream));
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) throws IOException {
    this.minValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.maxValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.firstValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.sumValue = ReadWriteIOUtils.readDouble(byteBuffer);
    this.summaryNum = ReadWriteIOUtils.readInt(byteBuffer);
    this.bfNum = ReadWriteIOUtils.readInt(byteBuffer);
    if (this.summaryNum > 0) {
      if (SUMMARY_TYPE == 0) {
        this.kllSketchList = new ArrayList<>(summaryNum);
        for (int i = 0; i < summaryNum; i++) this.kllSketchList.add(new LongKLLSketch(byteBuffer));
      }
      if (SUMMARY_TYPE == 1) {
        this.tDigestList = new ArrayList<>(summaryNum);
        for (int i = 0; i < summaryNum; i++)
          this.tDigestList.add(new TDigestForStatMerge(byteBuffer));
      }
      if (SUMMARY_TYPE == 2) {
        this.samplingList = new ArrayList<>(summaryNum);
        for (int i = 0; i < summaryNum; i++)
          this.samplingList.add(new SamplingHeapForStatMerge(byteBuffer));
      }
    }
    if (this.bfNum > 0) {
      //      System.out.println(
      //          "\t[DEBUG][DoubleStat Deserialize]\tbfNum:"
      //              + bfNum
      //              + "\tbyteBuffer: offset="
      //              + byteBuffer.arrayOffset()
      //              + " Limit="
      //              + byteBuffer.limit()
      //              + " pos="
      //              + byteBuffer.position()
      //              + " length="
      //              + byteBuffer.remaining()
      //              + " allBFSize:"
      //              + (bfNum * BLOOM_FILTER_SIZE));
      this.bfList = new ArrayList<>(bfNum);
      int tmp_size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < bfNum; i++) {
        //        System.out.println(
        //            "\t[DEBUG][DoubleStat Deserialize]\tbfID:"
        //                + i
        //                + "\t"
        //                + ReadWriteIOUtils.readByte(byteBuffer)
        //                + "\t"
        //                + UnsignedBytes.toInt(ReadWriteIOUtils.readByte(byteBuffer))
        //                + "\t"
        //                + ReadWriteIOUtils.readInt(byteBuffer));
        //        byteBuffer.position(byteBuffer.position() - 6);
        byte[] bfBytes = new byte[tmp_size];
        byteBuffer.get(bfBytes);
        //        System.out.println(
        //            "\t[DEBUG][DoubleStat Deserialize]\t???:"
        //                + bfBytes[0]
        //                + "\t"
        //                + UnsignedBytes.toInt(bfBytes[1]));
        this.bfList.add(
            BloomFilter.readFrom(new ByteArrayInputStream(bfBytes), Funnels.longFunnel()));
        //        byteBuffer.position(byteBuffer.position() + BLOOM_FILTER_SIZE);
      }
      this.MinTimeMaxTimeCountList = LongLists.mutable.withInitialCapacity(bfNum * 3);
      for (int i = 0; i < bfNum * 3; i++)
        this.MinTimeMaxTimeCountList.add(ReadWriteIOUtils.readLong(byteBuffer));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    double e = 0.00001;
    DoubleStatistics that = (DoubleStatistics) o;
    return Math.abs(that.minValue - minValue) < e
        && Math.abs(that.maxValue - maxValue) < e
        && Math.abs(that.firstValue - firstValue) < e
        && Math.abs(that.lastValue - lastValue) < e
        && Math.abs(that.sumValue - sumValue) < e;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), minValue, maxValue, firstValue, lastValue, sumValue);
  }

  @Override
  public String toString() {
    return super.toString()
        + " [minValue:"
        + minValue
        + ",maxValue:"
        + maxValue
        + ",firstValue:"
        + firstValue
        + ",lastValue:"
        + lastValue
        + ",sumValue:"
        + sumValue
        + "]";
  }

  @Override
  public boolean hasBf() {
    return bfNum > 0;
  }

  @Override
  public List<BloomFilter<Long>> getBfList() {
    return bfList;
  }

  @Override
  public BloomFilter<Long> getBf(int bfId) {
    return bfList.get(bfId);
  }

  @Override
  public long getBfMinTime(int bfId) {
    return MinTimeMaxTimeCountList.get(bfId * 3);
  }

  @Override
  public long getBfMaxTime(int bfId) {
    return MinTimeMaxTimeCountList.get(bfId * 3 + 1);
  }

  @Override
  public long getBfCount(int bfId) {
    return MinTimeMaxTimeCountList.get(bfId * 3 + 2);
  }
}
