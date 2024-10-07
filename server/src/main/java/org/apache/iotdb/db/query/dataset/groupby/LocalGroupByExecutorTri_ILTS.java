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

package org.apache.iotdb.db.query.dataset.groupby;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.impl.MinValueAggrResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.MinMaxInfo;
import org.apache.iotdb.tsfile.file.metadata.statistics.QuickHullPoint;
import org.apache.iotdb.tsfile.read.common.ChunkSuit4Tri;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LocalGroupByExecutorTri_ILTS implements GroupByExecutor {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  // Aggregate result buffer of this path
  private final List<AggregateResult> results = new ArrayList<>();

  // keys: 0,1,...,(int) Math.floor((endTime * 1.0 - startTime) / interval)-1
  private Map<Integer, List<ChunkSuit4Tri>> splitChunkList = new HashMap<>();

  private long p1t;
  private double p1v;
  private long pnt;
  private double pnv;

  private long lt;
  private double lv;

  private final int N1;

  private final int numIterations = CONFIG.getNumIterations(); // do not make it static

  private Filter timeFilter;

  public LocalGroupByExecutorTri_ILTS(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      Filter timeFilter,
      TsFileFilter fileFilter,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    //    long start = System.nanoTime();

    // get all data sources
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(path, context, this.timeFilter);

    // update filter by TTL
    this.timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    SeriesReader seriesReader =
        new SeriesReader(
            path,
            allSensors,
            // fix bug: here use the aggregation type as the series data type,
            // not using pageReader.getAllSatisfiedPageData is ok
            dataType,
            context,
            queryDataSource,
            timeFilter,
            null,
            fileFilter,
            ascending);

    try {
      // : this might be bad to load all chunk metadata at first
      List<ChunkSuit4Tri> futureChunkList =
          new ArrayList<>(seriesReader.getAllChunkMetadatas4Tri()); // no need sort here
      // arrange futureChunkList into each bucket
      GroupByFilter groupByFilter = (GroupByFilter) timeFilter;
      long startTime = groupByFilter.getStartTime();
      long endTime = groupByFilter.getEndTime();
      long interval = groupByFilter.getInterval();
      N1 = (int) Math.floor((endTime * 1.0 - startTime) / interval); // 分桶数
      for (ChunkSuit4Tri chunkSuit4Tri : futureChunkList) {
        ChunkMetadata chunkMetadata = chunkSuit4Tri.chunkMetadata;
        long chunkMinTime = chunkMetadata.getStartTime();
        long chunkMaxTime = chunkMetadata.getEndTime();
        if (chunkMinTime >= endTime || chunkMaxTime < startTime) {
          continue; // note futureChunkList is not sorted in advance, so not break, just skip
        }
        int idx1 = (int) Math.floor((chunkMinTime - startTime) * 1.0 / interval);
        idx1 = Math.max(idx1, 0);
        int idx2 = (int) Math.floor((chunkMaxTime - startTime) * 1.0 / interval);
        idx2 = Math.min(idx2, N1 - 1);
        for (int i = idx1; i <= idx2; i++) {
          splitChunkList.computeIfAbsent(i, k -> new ArrayList<>());
          splitChunkList.get(i).add(chunkSuit4Tri);
        }
      }

      if (CONFIG.getAutoP1n()) {
        // get real p1
        List<ChunkSuit4Tri> firstBucket = splitChunkList.get(0);
        sortByStartTime(firstBucket);
        ChunkSuit4Tri firstChunk = firstBucket.get(0);
        if (firstChunk.pageReader == null) {
          firstChunk.pageReader =
              FileLoaderUtils.loadPageReaderList4CPV(firstChunk.chunkMetadata, this.timeFilter);
        }
        PageReader pageReader = firstChunk.pageReader;
        for (int j = 0; j < firstChunk.chunkMetadata.getStatistics().getCount(); j++) {
          long timestamp = pageReader.timeBuffer.getLong(j * 8);
          if (timestamp < startTime) {
            continue;
          } else if (timestamp >= startTime) {
            ByteBuffer valueBuffer = pageReader.valueBuffer;
            double v = valueBuffer.getDouble(pageReader.timeBufferLength + j * 8);
            p1t = timestamp;
            p1v = v;
            break;
          }
        }

        // get real pn
        List<ChunkSuit4Tri> lastBucket = splitChunkList.get(N1 - 1);
        sortByStartTime(lastBucket);
        ChunkSuit4Tri lastChunk = lastBucket.get(lastBucket.size() - 1);
        if (lastChunk.pageReader == null) {
          lastChunk.pageReader =
              FileLoaderUtils.loadPageReaderList4CPV(lastChunk.chunkMetadata, this.timeFilter);
        }
        pageReader = lastChunk.pageReader;
        for (int j = 0; j < lastChunk.chunkMetadata.getStatistics().getCount(); j++) {
          long timestamp = pageReader.timeBuffer.getLong(j * 8);
          if (timestamp > endTime) { // pn can be at endTime
            break;
          } else {
            ByteBuffer valueBuffer = pageReader.valueBuffer;
            double v = valueBuffer.getDouble(pageReader.timeBufferLength + j * 8);
            pnt = timestamp;
            pnv = v;
          }
        }
      } else {
        p1t = CONFIG.getP1t();
        p1v = CONFIG.getP1v();
        pnt = CONFIG.getPnt();
        pnv = CONFIG.getPnv();
      }

    } catch (IOException e) {
      throw new QueryProcessException(e.getMessage());
    }

    //    IOMonitor2.addMeasure(Operation.M4_LSM_INIT_LOAD_ALL_CHUNKMETADATAS, System.nanoTime() -
    // start);
  }

  private static void sortByStartTime(List<ChunkSuit4Tri> list) {
    Collections.sort(
        list,
        (a, b) -> Long.compare(a.chunkMetadata.getStartTime(), b.chunkMetadata.getStartTime()));
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    results.add(aggrResult);
  }

  @Override
  public List<AggregateResult> calcResult(
      long curStartTime, long curEndTime, long startTime, long endTime, long interval)
      throws IOException {
    StringBuilder series_final = new StringBuilder();

    // clear result cache
    for (AggregateResult result : results) {
      result.reset();
    }

    long[] lastIter_t = new long[N1];
    double[] lastIter_v = new double[N1];

    boolean[] lastSame = new boolean[N1 + 1];
    lastSame[N1] = true;

    int num = 0;
    for (; num < numIterations; num++) {
      // NOTE: init lt&lv at the start of each iteration is a must, because they are modified in
      // each iteration
      lt = p1t;
      lv = p1v;

      boolean allSameFlag = true;
      boolean currentLeftSame = true;

      for (int b = 0; b < N1; b++) {
        if (CONFIG.isAcc_iterRepeat() && num > 0 && lastSame[b + 1] && currentLeftSame) {
          lt = lastIter_t[b];
          lv = lastIter_v[b];
          lastSame[b] = true;
          continue;
        }
        double rt = 0; // must initialize as zero, because may be used as sum for average
        double rv = 0; // must initialize as zero, because may be used as sum for average
        if (b == N1 - 1) {
          rt = pnt;
          rv = pnv;
        } else {
          if (num == 0) {
            List<ChunkSuit4Tri> chunkSuit4TriList = splitChunkList.get(b + 1);
            if (chunkSuit4TriList == null) {
              throw new IOException("Empty bucket!");
            }
            long rightStartTime = startTime + (b + 1) * interval;
            long rightEndTime = startTime + (b + 2) * interval;
            int cnt = 0;
            for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
              TSDataType dataType = chunkSuit4Tri.chunkMetadata.getDataType();
              if (dataType != TSDataType.DOUBLE) {
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
              }
              if (CONFIG.isAcc_avg()) {
                if (chunkSuit4Tri.chunkMetadata.getStartTime() >= rightStartTime
                    && chunkSuit4Tri.chunkMetadata.getEndTime() < rightEndTime) {
                  rt +=
                      (chunkSuit4Tri.chunkMetadata.getStartTime()
                              + chunkSuit4Tri.chunkMetadata.getEndTime())
                          * chunkSuit4Tri.chunkMetadata.getStatistics().getCount()
                          / 2.0;
                  rv += chunkSuit4Tri.chunkMetadata.getStatistics().getSumDoubleValue();
                  cnt += chunkSuit4Tri.chunkMetadata.getStatistics().getCount();
                  continue;
                }
              }

              // 1. load page data if it hasn't been loaded
              if (chunkSuit4Tri.pageReader == null) {
                chunkSuit4Tri.pageReader =
                    FileLoaderUtils.loadPageReaderList4CPV(
                        chunkSuit4Tri.chunkMetadata, this.timeFilter);
                //  ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
                //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
                //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
                //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS
                //  ASSIGN DIRECTLY), WHICH WILL INTRODUCE BUGS!
              }
              // 2. calculate avg
              PageReader pageReader = chunkSuit4Tri.pageReader;
              for (int j = 0; j < chunkSuit4Tri.chunkMetadata.getStatistics().getCount(); j++) {
                IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
                long timestamp = pageReader.timeBuffer.getLong(j * 8);
                if (timestamp < rightStartTime) {
                  continue;
                } else if (timestamp >= rightEndTime) {
                  break;
                } else { // rightStartTime<=t<rightEndTime
                  ByteBuffer valueBuffer = pageReader.valueBuffer;
                  double v = valueBuffer.getDouble(pageReader.timeBufferLength + j * 8);
                  rt += timestamp;
                  rv += v;
                  cnt++;
                }
              }
            }
            if (cnt == 0) {
              throw new IOException("Empty bucket!");
            }
            rt = rt / cnt;
            rv = rv / cnt;
          } else {
            rt = lastIter_t[b + 1];
            rv = lastIter_v[b + 1];
          }
        }
        double maxDistance = -1;
        long select_t = -1;
        double select_v = -1;
        List<ChunkSuit4Tri> chunkSuit4TriList = splitChunkList.get(b);
        long localCurStartTime = startTime + (b) * interval;
        long localCurEndTime = startTime + (b + 1) * interval;
        if (CONFIG.isAcc_rectangle()) {
          for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
            long[] rect_t =
                new long[] {
                  chunkSuit4Tri.chunkMetadata.getStartTime(), // FPt
                  chunkSuit4Tri.chunkMetadata.getEndTime(), // LPt
                  chunkSuit4Tri.chunkMetadata.getStatistics().getBottomTimestamp(), // BPt
                  chunkSuit4Tri.chunkMetadata.getStatistics().getTopTimestamp() // TPt
                };
            double[] rect_v =
                new double[] {
                  (double) chunkSuit4Tri.chunkMetadata.getStatistics().getFirstValue(), // FPv
                  (double) chunkSuit4Tri.chunkMetadata.getStatistics().getLastValue(), // LPv
                  (double) chunkSuit4Tri.chunkMetadata.getStatistics().getMinValue(), // BPv
                  (double) chunkSuit4Tri.chunkMetadata.getStatistics().getMaxValue() // TPv
                };
            for (int i = 0; i < 4; i++) {
              if (rect_t[i] >= localCurStartTime && rect_t[i] < localCurEndTime) {
                double distance =
                    IOMonitor2.calculateDistance(lt, lv, rect_t[i], rect_v[i], rt, rv);
                if (distance > maxDistance) {
                  maxDistance = distance;
                  select_t = rect_t[i];
                  select_v = rect_v[i];
                }
              }
            }
            chunkSuit4Tri.distance_loose_upper_bound =
                IOMonitor2.calculateDistance(lt, lv, rect_t[0], rect_v[2], rt, rv); // FPt,BPv
            chunkSuit4Tri.distance_loose_upper_bound =
                Math.max(
                    chunkSuit4Tri.distance_loose_upper_bound,
                    IOMonitor2.calculateDistance(lt, lv, rect_t[0], rect_v[3], rt, rv)); // FPt,TPv
            chunkSuit4Tri.distance_loose_upper_bound =
                Math.max(
                    chunkSuit4Tri.distance_loose_upper_bound,
                    IOMonitor2.calculateDistance(lt, lv, rect_t[1], rect_v[2], rt, rv)); // LPt,BPv
            chunkSuit4Tri.distance_loose_upper_bound =
                Math.max(
                    chunkSuit4Tri.distance_loose_upper_bound,
                    IOMonitor2.calculateDistance(lt, lv, rect_t[1], rect_v[3], rt, rv)); // LPt,TPv
          }
        }
        for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
          TSDataType dataType = chunkSuit4Tri.chunkMetadata.getDataType();
          if (dataType != TSDataType.DOUBLE) {
            throw new UnSupportedDataTypeException(String.valueOf(dataType));
          }
          if (CONFIG.isAcc_rectangle()) {
            if (chunkSuit4Tri.distance_loose_upper_bound <= maxDistance) {
              continue;
            }
          }
          // load page data if it hasn't been loaded
          if (chunkSuit4Tri.pageReader == null) {
            chunkSuit4Tri.pageReader =
                FileLoaderUtils.loadPageReaderList4CPV(
                    chunkSuit4Tri.chunkMetadata, this.timeFilter);
            //  ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
            //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
            //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
            //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS
            //  ASSIGN DIRECTLY), WHICH WILL INTRODUCE BUGS!
          }
          PageReader pageReader = chunkSuit4Tri.pageReader;
          if (CONFIG.isAcc_convex()
              && chunkSuit4Tri.chunkMetadata.getStatistics().getCount() >= 3) {
            BitSet bitSet = chunkSuit4Tri.chunkMetadata.getStatistics().getQuickHullBitSet();
            List<QuickHullPoint> foundPoints =
                convexHullAcc(
                    lt,
                    lv,
                    rt,
                    rv,
                    pageReader,
                    bitSet,
                    chunkSuit4Tri.chunkMetadata.getStatistics().getCount());
            double ch_maxDistance = -1;
            long ch_select_t = -1;
            double ch_select_v = -1;
            for (QuickHullPoint point : foundPoints) {
              IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
              double distance = IOMonitor2.calculateDistance(lt, lv, point.t, point.v, rt, rv);
              if (distance > ch_maxDistance) {
                ch_maxDistance = distance;
                ch_select_t = point.t;
                ch_select_v = point.v;
              }
            }
            if (ch_maxDistance <= maxDistance) {
              continue;
            }
            if (ch_select_t >= localCurStartTime && ch_select_t < localCurEndTime) {
              maxDistance = ch_maxDistance;
              select_t = ch_select_t;
              select_v = ch_select_v;
              continue; // note this
            }
          }
          int count = chunkSuit4Tri.chunkMetadata.getStatistics().getCount();
          int j;
          for (j = 0; j < count; j++) {
            IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
            long timestamp = pageReader.timeBuffer.getLong(j * 8);
            if (timestamp < localCurStartTime) {
              continue;
            } else if (timestamp >= localCurEndTime) {
              break;
            } else { // localCurStartTime<=t<localCurEndTime
              ByteBuffer valueBuffer = pageReader.valueBuffer;
              double v = valueBuffer.getDouble(pageReader.timeBufferLength + j * 8);
              double distance = IOMonitor2.calculateDistance(lt, lv, timestamp, v, rt, rv);
              if (distance > maxDistance) {
                maxDistance = distance;
                select_t = timestamp;
                select_v = v;
              }
            }
          }
        }

        if (CONFIG.isAcc_iterRepeat()) {
          if (select_t != lastIter_t[b]) {
            allSameFlag = false;
            lastSame[b] = false;
            currentLeftSame = false;
          } else {
            lastSame[b] = true;
            currentLeftSame = true;
          }
        }

        lt = select_t;
        lv = select_v;
        lastIter_t[b] = select_t;
        lastIter_v[b] = select_v;
      }

      if (CONFIG.isAcc_iterRepeat() && allSameFlag) {
        num++;
        break;
      }
    } // end Iterations

    series_final.append(p1v).append("[").append(p1t).append("]").append(",");
    for (int i = 0; i < lastIter_t.length; i++) {
      series_final.append(lastIter_v[i]).append("[").append(lastIter_t[i]).append("]").append(",");
    }

    series_final.append(pnv).append("[").append(pnt).append("]").append(",");
    MinValueAggrResult minValueAggrResult = (MinValueAggrResult) results.get(0);
    minValueAggrResult.updateResult(new MinMaxInfo<>(series_final.toString(), 0));

    splitChunkList.clear();
    splitChunkList = null;

    return results;
  }

  public List<QuickHullPoint> convexHullAcc(
      double lt, double lv, double rt, double rv, PageReader pageReader, BitSet bitSet, int count) {
    double A = lv - rv;
    double B = rt - lt;

    BitSet reverseBitSet = IOMonitor2.reverse(bitSet);

    long fpt = pageReader.timeBuffer.getLong(0);
    double fpv = pageReader.valueBuffer.getDouble(pageReader.timeBufferLength);
    long lpt = pageReader.timeBuffer.getLong((count - 1) * 8);
    double lpv = pageReader.valueBuffer.getDouble(pageReader.timeBufferLength + (count - 1) * 8);

    List<QuickHullPoint> LU = new ArrayList<>();
    List<QuickHullPoint> LL = new ArrayList<>();
    IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
    int bitSetIdx = bitSet.nextSetBit(1); // 0 must true
    long t = pageReader.timeBuffer.getLong(bitSetIdx * 8);
    double v = pageReader.valueBuffer.getDouble(pageReader.timeBufferLength + bitSetIdx * 8);
    int check = IOMonitor2.pointLocation(fpt, fpv, t, v, lpt, lpv); // note this, not l & r!
    if (check > 0) { // p above the line connecting FP&LP
      // init LU clockwise
      LU.add(new QuickHullPoint(fpt, fpv));
      LU.add(new QuickHullPoint(t, v));
      // init LL counterclockwise
      LL.add(new QuickHullPoint(t, v));
      LL.add(new QuickHullPoint(fpt, fpv));
    } else { // p below the line connecting FP&LP
      // init LU clockwise
      LU.add(new QuickHullPoint(t, v));
      LU.add(new QuickHullPoint(fpt, fpv));
      // init LL counterclockwise
      LL.add(new QuickHullPoint(fpt, fpv));
      LL.add(new QuickHullPoint(t, v));
    }

    List<QuickHullPoint> RU = new ArrayList<>();
    List<QuickHullPoint> RL = new ArrayList<>();
    IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
    int reverseBitSetIdx = reverseBitSet.nextSetBit(1); // 0 must true
    t = pageReader.timeBuffer.getLong((count - reverseBitSetIdx - 1) * 8); // note this reverse
    v =
        pageReader.valueBuffer.getDouble(
            pageReader.timeBufferLength + (count - reverseBitSetIdx - 1) * 8);
    check = IOMonitor2.pointLocation(fpt, fpv, t, v, lpt, lpv); // note this, not l & r!
    if (check > 0) { // p above the line connecting FP&LP
      // init RU counterclockwise
      RU.add(new QuickHullPoint(lpt, lpv));
      RU.add(new QuickHullPoint(t, v));
      // init RL clockwise
      RL.add(new QuickHullPoint(t, v));
      RL.add(new QuickHullPoint(lpt, lpv));
    } else { // p below the line connecting FP&LP
      // init RU counterclockwise
      RU.add(new QuickHullPoint(t, v));
      RU.add(new QuickHullPoint(lpt, lpv));
      // init RL clockwise
      RL.add(new QuickHullPoint(lpt, lpv));
      RL.add(new QuickHullPoint(t, v));
    }

    boolean findHighest = false;
    boolean findLowest = false;
    List<QuickHullPoint> foundPoints = new ArrayList<>();

    while (bitSetIdx != -1 || reverseBitSetIdx != -1) {
      // from left to right
      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
      bitSetIdx = bitSet.nextSetBit(bitSetIdx + 1);
      if (bitSetIdx != -1) {
        t = pageReader.timeBuffer.getLong(bitSetIdx * 8);
        v = pageReader.valueBuffer.getDouble(pageReader.timeBufferLength + bitSetIdx * 8);
        check = IOMonitor2.pointLocation(fpt, fpv, t, v, lpt, lpv); // note this, not l & r!
        // note that below if check equals 0, that point is added to both upper and lower, which is
        // necessary
        if (check >= 0) { // p above or on the line connecting FP&LP
          LU.add(new QuickHullPoint(t, v));
          int sign = IOMonitor2.checkSumSigns(A, B, LU);
          if (sign > 0) {
            findLowest = true;
            foundPoints.add(LU.get(LU.size() - 2));
          } else if (sign < 0) {
            findHighest = true;
            foundPoints.add(LU.get(LU.size() - 2));
          }
        }
        if (check <= 0) { // p below or on the line connecting FP&LP
          LL.add(new QuickHullPoint(t, v));
          int sign = IOMonitor2.checkSumSigns(A, B, LL);
          if (sign > 0) {
            findLowest = true;
            foundPoints.add(LL.get(LL.size() - 2));
          } else if (sign < 0) {
            findHighest = true;
            foundPoints.add(LL.get(LL.size() - 2));
          }
        }
        if (findLowest && findHighest) {
          break;
        }
      }
      // from right to left
      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
      reverseBitSetIdx =
          reverseBitSet.nextSetBit(reverseBitSetIdx + 1); // note this! use reverseBitSet!!
      if (reverseBitSetIdx != -1) {
        t = pageReader.timeBuffer.getLong((count - reverseBitSetIdx - 1) * 8);
        v =
            pageReader.valueBuffer.getDouble(
                pageReader.timeBufferLength + (count - reverseBitSetIdx - 1) * 8);
        // note that below if check equals 0, that point is added to both upper and lower, which is
        // necessary
        check = IOMonitor2.pointLocation(fpt, fpv, t, v, lpt, lpv); // note this, not l & r!
        if (check >= 0) { // p is above or on the line connecting FP&LP
          RU.add(new QuickHullPoint(t, v));
          int sign = IOMonitor2.checkSumSigns(A, B, RU);
          if (sign > 0) {
            findLowest = true;
            foundPoints.add(RU.get(RU.size() - 2));
          } else if (sign < 0) {
            findHighest = true;
            foundPoints.add(RU.get(RU.size() - 2));
          }
        }
        if (check <= 0) { // p below or on the line connecting FP&LP
          RL.add(new QuickHullPoint(t, v));
          int sign = IOMonitor2.checkSumSigns(A, B, RL);
          if (sign > 0) {
            findLowest = true;
            foundPoints.add(RL.get(RL.size() - 2));
          } else if (sign < 0) {
            findHighest = true;
            foundPoints.add(RL.get(RL.size() - 2));
          }
        }
        if (findLowest && findHighest) {
          break;
        }
      }
    }
    return foundPoints;
  }

  @Override
  public Pair<Long, Object> peekNextNotNullValue(long nextStartTime, long nextEndTime)
      throws IOException {
    throw new IOException("no implemented");
  }

  @Override
  public List<AggregateResult> calcResult(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {
    throw new IOException("no implemented");
  }
}
