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

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader.MergeReaderPriority;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.FloatStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.ChunkSuit4CPV;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/**
 * Sql format: SELECT min_time(s0), max_time(s0), first_value(s0), last_value(s0), min_value(s0),
 * max_value(s0) ROM root.xx group by ([tqs,tqe),IntervalLength). Requirements: (1) Don't change the
 * sequence of the above six aggregates (2) Make sure (tqe-tqs) is divisible by IntervalLength. (3)
 * Assume each chunk has only one page.
 */
// This is the CPVGroupByExecutor in paper.
public class LocalGroupByExecutor4CPV implements GroupByExecutor {

  // Aggregate result buffer of this path
  private final List<AggregateResult> results = new ArrayList<>();
  //  private final TimeRange timeRange;

  private List<ChunkSuit4CPV> currentChunkList;
  private final List<ChunkSuit4CPV> futureChunkList = new ArrayList<>();

  private Filter timeFilter;

  private TSDataType tsDataType;

  private PriorityMergeReader mergeReader;

  public LocalGroupByExecutor4CPV(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      Filter timeFilter,
      TsFileFilter fileFilter,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {

    this.tsDataType = dataType;
    this.mergeReader = new PriorityMergeReader();

    // TODO: load all chunk metadatas into futureChunkList
    // get all data sources
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(path, context, this.timeFilter);

    // update filter by TTL
    this.timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    SeriesReader seriesReader =
        new SeriesReader(
            path,
            allSensors,
            dataType,
            context,
            queryDataSource,
            timeFilter,
            null,
            fileFilter,
            ascending);

    // unpackAllOverlappedFilesToTimeSeriesMetadata
    try {
      futureChunkList.addAll(seriesReader.getAllChunkMetadatas4CPV());
    } catch (IOException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    results.add(aggrResult);
  }

  /**
   * @param curStartTime closed
   * @param curEndTime open
   * @param startTime closed
   * @param endTime open
   */
  @Override
  public List<AggregateResult> calcResult(
      long curStartTime, long curEndTime, long startTime, long endTime, long interval)
      throws IOException, QueryProcessException {
    //    System.out.println("====DEBUG====: calcResult for [" + curStartTime + "," + curEndTime +
    // ")");

    // clear result cache
    for (AggregateResult result : results) {
      result.reset();
    }
    // empty currentChunkList
    currentChunkList = new ArrayList<>();

    //    System.out.println("====DEBUG====: deal with futureChunkList");

    ListIterator itr = futureChunkList.listIterator();
    List<ChunkSuit4CPV> tmpFutureChunkList = new ArrayList<>();
    while (itr.hasNext()) {
      ChunkSuit4CPV chunkSuit4CPV = (ChunkSuit4CPV) (itr.next());
      ChunkMetadata chunkMetadata = chunkSuit4CPV.getChunkMetadata();
      long chunkMinTime = chunkMetadata.getStartTime();
      long chunkMaxTime = chunkMetadata.getEndTime();
      if (chunkMinTime >= curEndTime && chunkMinTime < endTime) {
        // the chunk falls on the right side of the current M4 interval Ii
        continue;
      } else if (chunkMaxTime < curStartTime || chunkMinTime >= endTime) {
        // the chunk falls on the left side of the current M4 interval Ii
        // or the chunk falls on the right side of the total query range
        itr.remove();
      } else if (chunkMinTime >= curStartTime && chunkMaxTime < curEndTime) {
        // the chunk falls completely within the current M4 interval Ii
        currentChunkList.add(chunkSuit4CPV);
        itr.remove();
      } else {
        // the chunk partially overlaps in time with the current M4 interval Ii.
        // load this chunk, split it on deletes and all w intervals.
        // add to currentChunkList and futureChunkList.
        itr.remove();
        List<IPageReader> pageReaderList =
            FileLoaderUtils.loadPageReaderList(chunkSuit4CPV.getChunkMetadata(), this.timeFilter);
        for (IPageReader pageReader : pageReaderList) {
          // assume only one page in a chunk
          // assume all data on disk, no data in memory
          ((PageReader) pageReader)
              .split4CPV(
                  startTime,
                  endTime,
                  interval,
                  curStartTime,
                  currentChunkList,
                  tmpFutureChunkList,
                  chunkMetadata);
        }

        //        System.out.println(
        //            "====DEBUG====: load the chunk because overlaps the M4 interval. Version="
        //                + chunkMetadata.getVersion()
        //                + " "
        //                + chunkMetadata.getOffsetOfChunkHeader());
      }
    }
    futureChunkList.addAll(tmpFutureChunkList);
    tmpFutureChunkList = null;
    itr = null;

    //    System.out.println("====DEBUG====: deal with currentChunkList");

    if (currentChunkList.size() == 0) {
      return results;
    }

    boolean[] isFinal = new boolean[4]; // default false
    do {
      long[] timestamps = new long[4]; // firstTime, lastTime, bottomTime, topTime
      Object[] values = new Object[4]; // firstValue, lastValue, bottomValue, topValue
      PriorityMergeReader.MergeReaderPriority[] versions =
          new PriorityMergeReader.MergeReaderPriority[4];
      int[] listIdx = new int[4];
      timestamps[0] = -1;
      timestamps[1] = -1;
      values[2] = null;
      values[3] = null;

      // find candidate points
      //      System.out.println("====DEBUG====: find candidate points");

      for (int j = 0; j < currentChunkList.size(); j++) {
        ChunkMetadata chunkMetadata = currentChunkList.get(j).getChunkMetadata();
        Statistics statistics = chunkMetadata.getStatistics();
        MergeReaderPriority version =
            new MergeReaderPriority(
                chunkMetadata.getVersion(), chunkMetadata.getOffsetOfChunkHeader());
        // update firstPoint
        if (!isFinal[0]) {
          if (timestamps[0] == -1
              || (statistics.getStartTime() < timestamps[0])
              || (statistics.getStartTime() == timestamps[0]
                  && version.compareTo(versions[0]) > 0)) {
            timestamps[0] = statistics.getStartTime();
            values[0] = statistics.getFirstValue();
            versions[0] = version;
            listIdx[0] = j;
          }
        }
        // update lastPoint
        if (!isFinal[1]) {
          if (timestamps[1] == -1
              || (statistics.getEndTime() > timestamps[1])
              || (statistics.getEndTime() == timestamps[1] && version.compareTo(versions[1]) > 0)) {
            timestamps[1] = statistics.getEndTime();
            values[1] = statistics.getLastValue();
            versions[1] = version;
            listIdx[1] = j;
          }
        }
        // update bottomPoint
        if (!isFinal[2]) {
          if (values[2] == null
              || (((Comparable) (values[2])).compareTo(statistics.getMinValue()) > 0)) {
            timestamps[2] = statistics.getBottomTimestamp();
            values[2] = statistics.getMinValue();
            versions[2] = version;
            listIdx[2] = j;
          }
        }
        // update topPoint
        if (!isFinal[3]) {
          if (values[3] == null
              || (((Comparable) (values[3])).compareTo(statistics.getMaxValue()) < 0)) {
            timestamps[3] = statistics.getTopTimestamp();
            values[3] = statistics.getMaxValue();
            versions[3] = version;
            listIdx[3] = j;
          }
        }
      }

      //      System.out.println("====DEBUG====: verify candidate points");

      // verify candidate points.
      // firstPoint and lastPoint are valid for sure.
      // default results sequence: min_time(%s), max_time(%s), first_value(%s), last_value(%s),
      // min_value(%s), max_value(%s)
      if (!isFinal[0]) { // firstPoint
        long firstTimestamp = timestamps[0];
        ChunkMetadata firstChunkMetadata = currentChunkList.get(listIdx[0]).getChunkMetadata();
        // check if the point is deleted:
        List<TimeRange> firstDeleteIntervalList = firstChunkMetadata.getDeleteIntervalList();
        boolean isDeletedItself = false;
        if (firstDeleteIntervalList != null) {
          for (TimeRange timeRange : firstDeleteIntervalList) {
            if (timeRange.contains(firstTimestamp)) {
              isDeletedItself = true;
              break;
            }
          }
        }
        if (isDeletedItself) {
          //          System.out.println(
          //              "====DEBUG====: load the chunk because candidate firstPoint is actually
          // deleted. Version="
          //                  + firstChunkMetadata.getVersion()
          //                  + " "
          //                  + firstChunkMetadata.getOffsetOfChunkHeader());

          currentChunkList.remove(listIdx[0]);
          List<IPageReader> pageReaderList =
              FileLoaderUtils.loadPageReaderList(firstChunkMetadata, this.timeFilter);
          for (IPageReader pageReader : pageReaderList) { // assume only one page in a chunk
            ((PageReader) pageReader)
                .split4CPV(
                    startTime,
                    endTime,
                    interval,
                    curStartTime,
                    currentChunkList,
                    null,
                    firstChunkMetadata);
          }
          continue; // next iteration to check currentChunkList
        } else {
          results
              .get(0)
              .updateResultUsingValues(
                  Arrays.copyOfRange(timestamps, 0, 1),
                  1,
                  Arrays.copyOfRange(values, 0, 1)); // min_time
          results
              .get(2)
              .updateResultUsingValues(
                  Arrays.copyOfRange(timestamps, 0, 1),
                  1,
                  Arrays.copyOfRange(values, 0, 1)); // first_value
          isFinal[0] = true;
          //          System.out.println("====DEBUG====: find firstPoint");
        }
      }
      if (!isFinal[1]) { // lastPoint
        long lastTimestamp = timestamps[1];
        ChunkMetadata lastChunkMetadata = currentChunkList.get(listIdx[1]).getChunkMetadata();
        // check if the point is deleted:
        List<TimeRange> lastDeleteIntervalList = lastChunkMetadata.getDeleteIntervalList();
        boolean isDeletedItself = false;
        if (lastDeleteIntervalList != null) {
          for (TimeRange timeRange : lastDeleteIntervalList) {
            if (timeRange.contains(lastTimestamp)) {
              isDeletedItself = true;
              break;
            }
          }
        }
        if (isDeletedItself) {
          //          System.out.println(
          //              "====DEBUG====: load the chunk because candidate lastPoint is actually
          // deleted. Version="
          //                  + lastChunkMetadata.getVersion()
          //                  + " "
          //                  + lastChunkMetadata.getOffsetOfChunkHeader());

          currentChunkList.remove(listIdx[1]);
          List<IPageReader> pageReaderList =
              FileLoaderUtils.loadPageReaderList(lastChunkMetadata, this.timeFilter);
          for (IPageReader pageReader : pageReaderList) { // assume only one page in a chunk
            ((PageReader) pageReader)
                .split4CPV(
                    startTime,
                    endTime,
                    interval,
                    curStartTime,
                    currentChunkList,
                    null,
                    lastChunkMetadata);
          }
          continue; // next iteration to check currentChunkList
        } else {
          results
              .get(1)
              .updateResultUsingValues(
                  Arrays.copyOfRange(timestamps, 1, 2),
                  1,
                  Arrays.copyOfRange(values, 1, 2)); // min_time
          results
              .get(3)
              .updateResultUsingValues(
                  Arrays.copyOfRange(timestamps, 1, 2),
                  1,
                  Arrays.copyOfRange(values, 1, 2)); // first_value
          isFinal[1] = true;

          //          System.out.println("====DEBUG====: find lastPoint");
        }
      }
      if (!isFinal[2]) { // bottomPoint
        long bottomTimestamp = timestamps[2];
        ChunkMetadata bottomChunkMetadata = currentChunkList.get(listIdx[2]).getChunkMetadata();
        List<Long> mergedVersionList = currentChunkList.get(listIdx[2]).getMergeVersionList();
        List<Long> mergedOffsetList = currentChunkList.get(listIdx[2]).getMergeOffsetList();
        // check if the point is deleted:
        List<TimeRange> bottomDeleteIntervalList = bottomChunkMetadata.getDeleteIntervalList();
        boolean isDeletedItself = false;
        if (bottomDeleteIntervalList != null) {
          for (TimeRange timeRange : bottomDeleteIntervalList) {
            if (timeRange.contains(bottomTimestamp)) {
              isDeletedItself = true;
              break;
            }
          }
        }
        if (isDeletedItself) {
          //          System.out.println(
          //              "====DEBUG====: load the chunk because candidate bottomPoint is actually
          // deleted. Version="
          //                  + bottomChunkMetadata.getVersion()
          //                  + " "
          //                  + bottomChunkMetadata.getOffsetOfChunkHeader());

          currentChunkList.remove(listIdx[2]);
          List<IPageReader> pageReaderList =
              FileLoaderUtils.loadPageReaderList(bottomChunkMetadata, this.timeFilter);
          for (IPageReader pageReader : pageReaderList) { // assume only one page in a chunk
            ((PageReader) pageReader)
                .split4CPV(
                    startTime,
                    endTime,
                    interval,
                    curStartTime,
                    currentChunkList,
                    null,
                    bottomChunkMetadata);
          }
          continue; // next iteration to check currentChunkList
        } else { // verify if it is overlapped by other chunks with larger version number and not in
          // the deleted time interval
          List<Integer> toMerge = new ArrayList<>();
          for (int i = 0; i < currentChunkList.size(); i++) {
            ChunkMetadata chunkMetadata = currentChunkList.get(i).getChunkMetadata();
            MergeReaderPriority version =
                new MergeReaderPriority(
                    chunkMetadata.getVersion(), chunkMetadata.getOffsetOfChunkHeader());
            if (version.compareTo(versions[2]) <= 0) { // including bottomChunkMetadata
              continue;
            }
            if (bottomTimestamp < chunkMetadata.getStartTime()
                || bottomTimestamp > chunkMetadata.getEndTime()) {
              continue;
            }
            boolean isMerged = false;
            for (int k = 0; k < mergedVersionList.size(); k++) {
              // these chunks are MARKED "merged" - not overlapped any more
              if (mergedVersionList.get(k) == chunkMetadata.getVersion()
                  && mergedOffsetList.get(k) == chunkMetadata.getOffsetOfChunkHeader()) {
                isMerged = true;
                break;
              }
            }
            if (isMerged) {
              continue;
            }
            toMerge.add(i);
          }
          if (toMerge.isEmpty()) {
            //            System.out.println("====DEBUG====: find bottomPoint");

            results
                .get(4)
                .updateResultUsingValues(
                    Arrays.copyOfRange(timestamps, 2, 3),
                    1,
                    Arrays.copyOfRange(values, 2, 3)); // min_value
            isFinal[2] = true;
          } else {
            // deal with toMerge chunks: delete updated points
            toMerge.add(listIdx[2]);
            List<Long> newMergedVersionList = new ArrayList<>();
            List<Long> newMergedOffsetList = new ArrayList<>();
            for (int m : toMerge) { // to MARK these chunks are "merged" - not overlapped any more
              ChunkMetadata tmpChunkMetadata = currentChunkList.get(m).getChunkMetadata();
              newMergedVersionList.add(tmpChunkMetadata.getVersion());
              newMergedOffsetList.add(tmpChunkMetadata.getOffsetOfChunkHeader());
            }
            Map<MergeReaderPriority, BatchData> updateBatchDataMap = new HashMap<>();
            Map<MergeReaderPriority, Statistics> statisticsMap = new HashMap<>();
            for (int o = 0; o < toMerge.size(); o++) {
              // create empty batchData
              ChunkSuit4CPV chunkSuit4CPV = currentChunkList.get(toMerge.get(o));
              ChunkMetadata chunkMetadata = chunkSuit4CPV.getChunkMetadata();
              MergeReaderPriority mergeReaderPriority =
                  new MergeReaderPriority(
                      chunkMetadata.getVersion(), chunkMetadata.getOffsetOfChunkHeader());
              BatchData batch1 = BatchDataFactory.createBatchData(tsDataType, true, false);
              updateBatchDataMap.put(mergeReaderPriority, batch1);
              // create empty statistics
              Statistics statistics = null;
              switch (tsDataType) {
                case INT32:
                  statistics = new IntegerStatistics();
                  break;
                case INT64:
                  statistics = new LongStatistics();
                  break;
                case FLOAT:
                  statistics = new FloatStatistics();
                  break;
                case DOUBLE:
                  statistics = new DoubleStatistics();
                  break;
                default:
                  break;
              }
              statisticsMap.put(mergeReaderPriority, statistics);
              // prepare mergeReader
              if (chunkSuit4CPV.getBatchData() == null) {
                List<IPageReader> pageReaderList =
                    FileLoaderUtils.loadPageReaderList(chunkMetadata, this.timeFilter);
                List<ChunkSuit4CPV> tmpCurrentChunkList = new ArrayList<>();
                for (IPageReader pageReader : pageReaderList) { // assume only one page in a chunk
                  ((PageReader) pageReader)
                      .split4CPV(
                          startTime,
                          endTime,
                          interval,
                          curStartTime,
                          tmpCurrentChunkList,
                          null,
                          chunkMetadata);
                }
                currentChunkList.set(toMerge.get(o), tmpCurrentChunkList.get(0));
                chunkSuit4CPV = currentChunkList.get(toMerge.get(o));

                //                System.out.println(
                //                    "====DEBUG====: load chunk for update merge. Version="
                //                        + chunkMetadata.getVersion()
                //                        + " "
                //                        + chunkMetadata.getOffsetOfChunkHeader());
              }
              mergeReader.addReader(
                  chunkSuit4CPV.getBatchData().getBatchDataIterator(),
                  new MergeReaderPriority(chunkSuit4CPV.getVersion(), chunkSuit4CPV.getOffset()));
            }
            while (mergeReader.hasNextTimeValuePair()) {
              Pair<TimeValuePair, MergeReaderPriority> res = mergeReader.nextElement();
              TimeValuePair ret = res.left;
              //                            System.out.println(
              //                                "====DEBUG====: merge for bottomPoint. (t,v)="
              //                                    + ret.getTimestamp()
              //                                    + ","
              //                                    + ret.getValue().getValue());
              updateBatchDataMap
                  .get(res.right)
                  .putAnObject(ret.getTimestamp(), ret.getValue().getValue());
              switch (tsDataType) {
                case INT32:
                  statisticsMap
                      .get(res.right)
                      .update(ret.getTimestamp(), (int) ret.getValue().getValue());
                  break;
                case INT64:
                  statisticsMap
                      .get(res.right)
                      .update(ret.getTimestamp(), (long) ret.getValue().getValue());
                  break;
                case FLOAT:
                  statisticsMap
                      .get(res.right)
                      .update(ret.getTimestamp(), (float) ret.getValue().getValue());
                  break;
                case DOUBLE:
                  statisticsMap
                      .get(res.right)
                      .update(ret.getTimestamp(), (double) ret.getValue().getValue());
                  break;
                default:
                  throw new UnSupportedDataTypeException(String.valueOf(tsDataType));
              }
            }
            mergeReader.close();

            for (int o = 0; o < toMerge.size(); o++) {
              ChunkSuit4CPV chunkSuit4CPV = currentChunkList.get(toMerge.get(o));
              // to MARK these chunks are "merged" - not overlapped any more
              chunkSuit4CPV.getMergeVersionList().addAll(newMergedVersionList);
              chunkSuit4CPV.getMergeOffsetList().addAll(newMergedOffsetList);
              // update BatchData
              MergeReaderPriority mergeReaderPriority =
                  new MergeReaderPriority(chunkSuit4CPV.getVersion(), chunkSuit4CPV.getOffset());
              chunkSuit4CPV.setBatchData(updateBatchDataMap.get(mergeReaderPriority));
              chunkSuit4CPV
                  .getChunkMetadata()
                  .setStatistics(statisticsMap.get(mergeReaderPriority));
            }
            //            System.out.println(
            //                "====DEBUG====: merged chunks are : version="
            //                    + newMergedVersionList
            //                    + " offsets="
            //                    + newMergedOffsetList);
            continue;
          }
        }
      }

      if (!isFinal[3]) { // topPoint
        long topTimestamp = timestamps[3];
        ChunkMetadata topChunkMetadata = currentChunkList.get(listIdx[3]).getChunkMetadata();
        List<Long> mergedVersionList = currentChunkList.get(listIdx[3]).getMergeVersionList();
        List<Long> mergedOffsetList = currentChunkList.get(listIdx[3]).getMergeOffsetList();
        // check if the point is deleted:
        List<TimeRange> topDeleteIntervalList = topChunkMetadata.getDeleteIntervalList();
        boolean isDeletedItself = false;
        if (topDeleteIntervalList != null) {
          for (TimeRange timeRange : topDeleteIntervalList) {
            if (timeRange.contains(topTimestamp)) {
              isDeletedItself = true;
              break;
            }
          }
        }
        if (isDeletedItself) {
          //          System.out.println(
          //              "====DEBUG====: load the chunk because candidate topPoint is actually
          // deleted. Version="
          //                  + topChunkMetadata.getVersion()
          //                  + " "
          //                  + topChunkMetadata.getOffsetOfChunkHeader());

          currentChunkList.remove(listIdx[3]);
          List<IPageReader> pageReaderList =
              FileLoaderUtils.loadPageReaderList(topChunkMetadata, this.timeFilter);
          for (IPageReader pageReader : pageReaderList) { // assume only one page in a chunk
            ((PageReader) pageReader)
                .split4CPV(
                    startTime,
                    endTime,
                    interval,
                    curStartTime,
                    currentChunkList,
                    null,
                    topChunkMetadata);
          }
          continue; // next iteration to check currentChunkList
        } else { // verify if it is overlapped by other chunks with larger version number and not in
          // the deleted time interval
          List<Integer> toMerge = new ArrayList<>();
          for (int i = 0; i < currentChunkList.size(); i++) {
            ChunkMetadata chunkMetadata = currentChunkList.get(i).getChunkMetadata();
            MergeReaderPriority version =
                new MergeReaderPriority(
                    chunkMetadata.getVersion(), chunkMetadata.getOffsetOfChunkHeader());
            if (version.compareTo(versions[3]) <= 0) { // including topChunkMetadata
              continue;
            }
            if (topTimestamp < chunkMetadata.getStartTime()
                || topTimestamp > chunkMetadata.getEndTime()) {
              continue;
            }
            boolean isMerged = false;
            for (int k = 0; k < mergedVersionList.size(); k++) {
              if (mergedVersionList.get(k) == chunkMetadata.getVersion()
                  && mergedOffsetList.get(k) == chunkMetadata.getOffsetOfChunkHeader()) {
                isMerged = true;
                break;
              }
            }
            if (isMerged) {
              continue;
            }
            toMerge.add(i);
          }
          if (toMerge.isEmpty()) {
            results
                .get(5)
                .updateResultUsingValues(
                    Arrays.copyOfRange(timestamps, 3, 4),
                    1,
                    Arrays.copyOfRange(values, 3, 4)); // max_value
            isFinal[3] = true;
            //            System.out.println("====DEBUG====: find topPoint");
            return results;
          } else {
            // deal with toMerge chunks: delete updated points
            toMerge.add(listIdx[3]);
            List<Long> newMergedVersionList = new ArrayList<>();
            List<Long> newMergedOffsetList = new ArrayList<>();
            for (int m : toMerge) { // to MARK these chunks are "merged" - not overlapped any more
              ChunkMetadata tmpChunkMetadata = currentChunkList.get(m).getChunkMetadata();
              newMergedVersionList.add(tmpChunkMetadata.getVersion());
              newMergedOffsetList.add(tmpChunkMetadata.getOffsetOfChunkHeader());
            }
            Map<MergeReaderPriority, BatchData> updateBatchDataMap = new HashMap<>();
            Map<MergeReaderPriority, Statistics> statisticsMap = new HashMap<>();
            for (int o = 0; o < toMerge.size(); o++) {
              // create empty batchData
              ChunkSuit4CPV chunkSuit4CPV = currentChunkList.get(toMerge.get(o));
              ChunkMetadata chunkMetadata = chunkSuit4CPV.getChunkMetadata();
              MergeReaderPriority mergeReaderPriority =
                  new MergeReaderPriority(
                      chunkMetadata.getVersion(), chunkMetadata.getOffsetOfChunkHeader());
              BatchData batch1 = BatchDataFactory.createBatchData(tsDataType, true, false);
              updateBatchDataMap.put(mergeReaderPriority, batch1);
              // create empty statistics
              Statistics statistics = null;
              switch (tsDataType) {
                case INT32:
                  statistics = new IntegerStatistics();
                  break;
                case INT64:
                  statistics = new LongStatistics();
                  break;
                case FLOAT:
                  statistics = new FloatStatistics();
                  break;
                case DOUBLE:
                  statistics = new DoubleStatistics();
                  break;
                default:
                  break;
              }
              statisticsMap.put(mergeReaderPriority, statistics);
              // prepare mergeReader
              if (chunkSuit4CPV.getBatchData() == null) {
                List<IPageReader> pageReaderList =
                    FileLoaderUtils.loadPageReaderList(chunkMetadata, this.timeFilter);
                List<ChunkSuit4CPV> tmpCurrentChunkList = new ArrayList<>();
                for (IPageReader pageReader : pageReaderList) { // assume only one page in a chunk
                  ((PageReader) pageReader)
                      .split4CPV(
                          startTime,
                          endTime,
                          interval,
                          curStartTime,
                          tmpCurrentChunkList,
                          null,
                          chunkMetadata);
                }
                currentChunkList.set(toMerge.get(o), tmpCurrentChunkList.get(0));
                chunkSuit4CPV = currentChunkList.get(toMerge.get(o));

                //                System.out.println(
                //                    "====DEBUG====: load chunk for update merge. Version="
                //                        + chunkMetadata.getVersion()
                //                        + " "
                //                        + chunkMetadata.getOffsetOfChunkHeader());
              }
              mergeReader.addReader(
                  chunkSuit4CPV.getBatchData().getBatchDataIterator(),
                  new MergeReaderPriority(chunkSuit4CPV.getVersion(), chunkSuit4CPV.getOffset()));
            }
            while (mergeReader.hasNextTimeValuePair()) {
              Pair<TimeValuePair, MergeReaderPriority> res = mergeReader.nextElement();
              TimeValuePair ret = res.left;
              //              System.out.println(
              //                  "====DEBUG====: merge for topPoint. (t,v)="
              //                      + ret.getTimestamp()
              //                      + ","
              //                      + ret.getValue().getValue());
              updateBatchDataMap
                  .get(res.right)
                  .putAnObject(ret.getTimestamp(), ret.getValue().getValue());
              switch (tsDataType) {
                case INT32:
                  statisticsMap
                      .get(res.right)
                      .update(ret.getTimestamp(), (int) ret.getValue().getValue());
                  break;
                case INT64:
                  statisticsMap
                      .get(res.right)
                      .update(ret.getTimestamp(), (long) ret.getValue().getValue());
                  break;
                case FLOAT:
                  statisticsMap
                      .get(res.right)
                      .update(ret.getTimestamp(), (float) ret.getValue().getValue());
                  break;
                case DOUBLE:
                  statisticsMap
                      .get(res.right)
                      .update(ret.getTimestamp(), (double) ret.getValue().getValue());
                  break;
                default:
                  throw new UnSupportedDataTypeException(String.valueOf(tsDataType));
              }
            }
            mergeReader.close();

            for (int o = 0; o < toMerge.size(); o++) {
              ChunkSuit4CPV chunkSuit4CPV = currentChunkList.get(toMerge.get(o));
              // to MARK these chunks are "merged" - not overlapped any more
              chunkSuit4CPV.getMergeVersionList().addAll(newMergedVersionList);
              chunkSuit4CPV.getMergeOffsetList().addAll(newMergedOffsetList);
              // update BatchData
              MergeReaderPriority mergeReaderPriority =
                  new MergeReaderPriority(chunkSuit4CPV.getVersion(), chunkSuit4CPV.getOffset());
              chunkSuit4CPV.setBatchData(updateBatchDataMap.get(mergeReaderPriority));
              chunkSuit4CPV
                  .getChunkMetadata()
                  .setStatistics(statisticsMap.get(mergeReaderPriority));
            }
            continue;
          }
        }
      }
    } while (true);
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

  public List<ChunkSuit4CPV> getCurrentChunkList() {
    return currentChunkList;
  }

  public List<ChunkSuit4CPV> getFutureChunkList() {
    return futureChunkList;
  }
}
