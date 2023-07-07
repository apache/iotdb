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
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader.MergeReaderPriority;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.FloatStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.ChunkSuit4CPV;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;
import org.apache.iotdb.tsfile.read.common.IOMonitor2.Operation;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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
// This is the CPVGroupByExecutor in M4-LSM paper.
public class LocalGroupByExecutor4CPV implements GroupByExecutor {

  private static final Logger M4_CHUNK_METADATA = LoggerFactory.getLogger("M4_CHUNK_METADATA");

  // Aggregate result buffer of this path
  private final List<AggregateResult> results = new ArrayList<>();
  //  private final TimeRange timeRange;

  private List<ChunkSuit4CPV> currentChunkList;
  private final List<ChunkSuit4CPV> futureChunkList = new ArrayList<>();

  // this is designed to keep the split chunk from futureChunkList, not destroying the sorted order
  // of futureChunkList
  private Map<Integer, List<ChunkSuit4CPV>> splitChunkList = new HashMap<>();

  private Filter timeFilter;

  private TSDataType tsDataType;

  //  private PriorityMergeReader mergeReader;

  public LocalGroupByExecutor4CPV(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      Filter timeFilter,
      TsFileFilter fileFilter,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    long start = System.nanoTime();

    this.tsDataType = dataType;
    //    this.mergeReader = new PriorityMergeReader();

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
      // TODO: this might be bad to load all chunk metadata at first
      futureChunkList.addAll(seriesReader.getAllChunkMetadatas4CPV());
      // order futureChunkList by chunk startTime
      futureChunkList.sort(
          new Comparator<ChunkSuit4CPV>() {
            public int compare(ChunkSuit4CPV o1, ChunkSuit4CPV o2) {
              return ((Comparable) (o1.getChunkMetadata().getStartTime()))
                  .compareTo(o2.getChunkMetadata().getStartTime());
            }
          });

      if (M4_CHUNK_METADATA.isDebugEnabled()) {
        if (timeFilter instanceof GroupByFilter) {
          M4_CHUNK_METADATA.debug(
              "M4_QUERY_PARAM,{},{},{}",
              ((GroupByFilter) timeFilter).getStartTime(),
              ((GroupByFilter) timeFilter).getEndTime(),
              ((GroupByFilter) timeFilter).getInterval());
        }
        for (ChunkSuit4CPV chunkSuit4CPV : futureChunkList) {
          Statistics statistics = chunkSuit4CPV.getChunkMetadata().getStatistics();
          long FP_t = statistics.getStartTime();
          long LP_t = statistics.getEndTime();
          long BP_t = statistics.getBottomTimestamp();
          long TP_t = statistics.getTopTimestamp();
          switch (statistics.getType()) {
            case INT32:
              int FP_v_int = ((IntegerStatistics) statistics).getFirstValue();
              int LP_v_int = ((IntegerStatistics) statistics).getLastValue();
              int BP_v_int = ((IntegerStatistics) statistics).getMinValue();
              int TP_v_int = ((IntegerStatistics) statistics).getMaxValue();
              M4_CHUNK_METADATA.debug(
                  "M4_CHUNK_METADATA,{},{},{},{},{},{},{},{},{},{},{}",
                  FP_t,
                  LP_t,
                  BP_t,
                  TP_t,
                  FP_v_int,
                  LP_v_int,
                  BP_v_int,
                  TP_v_int,
                  chunkSuit4CPV.getChunkMetadata().getVersion(),
                  chunkSuit4CPV.getChunkMetadata().getOffsetOfChunkHeader(),
                  statistics.getCount());
              break;
            case INT64:
              long FP_v_long = ((LongStatistics) statistics).getFirstValue();
              long LP_v_long = ((LongStatistics) statistics).getLastValue();
              long BP_v_long = ((LongStatistics) statistics).getMinValue();
              long TP_v_long = ((LongStatistics) statistics).getMaxValue();
              M4_CHUNK_METADATA.debug(
                  "M4_CHUNK_METADATA,{},{},{},{},{},{},{},{},{},{},{}",
                  FP_t,
                  LP_t,
                  BP_t,
                  TP_t,
                  FP_v_long,
                  LP_v_long,
                  BP_v_long,
                  TP_v_long,
                  chunkSuit4CPV.getChunkMetadata().getVersion(),
                  chunkSuit4CPV.getChunkMetadata().getOffsetOfChunkHeader(),
                  statistics.getCount());
              break;
            case FLOAT:
              float FP_v_float = ((FloatStatistics) statistics).getFirstValue();
              float LP_v_float = ((FloatStatistics) statistics).getLastValue();
              float BP_v_float = ((FloatStatistics) statistics).getMinValue();
              float TP_v_float = ((FloatStatistics) statistics).getMaxValue();
              M4_CHUNK_METADATA.debug(
                  "M4_CHUNK_METADATA,{},{},{},{},{},{},{},{},{},{},{}",
                  FP_t,
                  LP_t,
                  BP_t,
                  TP_t,
                  FP_v_float,
                  LP_v_float,
                  BP_v_float,
                  TP_v_float,
                  chunkSuit4CPV.getChunkMetadata().getVersion(),
                  chunkSuit4CPV.getChunkMetadata().getOffsetOfChunkHeader(),
                  statistics.getCount());
              break;
            case DOUBLE:
              double FP_v_double = ((DoubleStatistics) statistics).getFirstValue();
              double LP_v_double = ((DoubleStatistics) statistics).getLastValue();
              double BP_v_double = ((DoubleStatistics) statistics).getMinValue();
              double TP_v_double = ((DoubleStatistics) statistics).getMaxValue();
              M4_CHUNK_METADATA.debug(
                  "M4_CHUNK_METADATA,{},{},{},{},{},{},{},{},{},{},{}",
                  FP_t,
                  LP_t,
                  BP_t,
                  TP_t,
                  FP_v_double,
                  LP_v_double,
                  BP_v_double,
                  TP_v_double,
                  chunkSuit4CPV.getChunkMetadata().getVersion(),
                  chunkSuit4CPV.getChunkMetadata().getOffsetOfChunkHeader(),
                  statistics.getCount());
              break;
            default:
              throw new QueryProcessException("unsupported data type!");
          }
        }
      }

    } catch (IOException e) {
      throw new QueryProcessException(e.getMessage());
    }

    IOMonitor2.addMeasure(Operation.M4_LSM_INIT_LOAD_ALL_CHUNKMETADATAS, System.nanoTime() - start);
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    results.add(aggrResult);
  }

  private void getCurrentChunkListFromFutureChunkList(
      long curStartTime, long curEndTime, long startTime, long endTime, long interval)
      throws IOException {
    IOMonitor2.M4_LSM_status = Operation.M4_LSM_MERGE_M4_TIME_SPAN;

    // empty currentChunkList
    currentChunkList = new ArrayList<>();

    // get related chunks from splitChunkList
    int curIdx = (int) Math.floor((curStartTime - startTime) * 1.0 / interval);
    if (splitChunkList.get(curIdx) != null) {
      currentChunkList.addAll(splitChunkList.get(curIdx));
      // TODO when to free splitChunkList memory
    }

    // iterate futureChunkList
    ListIterator<ChunkSuit4CPV> itr = futureChunkList.listIterator();
    while (itr.hasNext()) {
      ChunkSuit4CPV chunkSuit4CPV = (ChunkSuit4CPV) (itr.next());
      ChunkMetadata chunkMetadata = chunkSuit4CPV.getChunkMetadata();
      long chunkMinTime = chunkMetadata.getStartTime();
      long chunkMaxTime = chunkMetadata.getEndTime();
      if (chunkMinTime >= curEndTime && chunkMinTime < endTime) {
        // the chunk falls on the right side of the current M4 interval Ii,
        // and since futureChunkList is ordered by the startTime of chunkMetadata,
        // the loop can be terminated early.
        break;
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
        // B: loads chunk data from disk to memory
        // C: decompress page data, split time&value buffers
        PageReader pageReader =
            FileLoaderUtils.loadPageReaderList4CPV(
                chunkSuit4CPV.getChunkMetadata(), this.timeFilter);
        // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
        //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
        //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
        //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN
        //  DIRECTLY), WHICH WILL INTRODUCE BUGS!

        // chunk data read operation (b) get the closest data point after or before a timestamp
        pageReader.split4CPV(
            startTime,
            endTime,
            interval,
            curStartTime,
            currentChunkList,
            splitChunkList,
            chunkMetadata);
      }
    }
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
      throws IOException {
    // clear result cache
    for (AggregateResult result : results) {
      result.reset();
    }

    long start = System.nanoTime();
    getCurrentChunkListFromFutureChunkList(curStartTime, curEndTime, startTime, endTime, interval);
    IOMonitor2.addMeasure(Operation.M4_LSM_MERGE_M4_TIME_SPAN, System.nanoTime() - start);

    if (currentChunkList.size() == 0) {
      return results;
    }

    start = System.nanoTime();
    calculateFirstPoint(currentChunkList, startTime, endTime, interval, curStartTime);
    IOMonitor2.addMeasure(Operation.M4_LSM_FP, System.nanoTime() - start);

    start = System.nanoTime();
    calculateLastPoint(currentChunkList, startTime, endTime, interval, curStartTime);
    IOMonitor2.addMeasure(Operation.M4_LSM_LP, System.nanoTime() - start);

    start = System.nanoTime();
    calculateBottomPoint(currentChunkList, startTime, endTime, interval, curStartTime);
    IOMonitor2.addMeasure(Operation.M4_LSM_BP, System.nanoTime() - start);

    start = System.nanoTime();
    calculateTopPoint(currentChunkList, startTime, endTime, interval, curStartTime);
    IOMonitor2.addMeasure(Operation.M4_LSM_TP, System.nanoTime() - start);

    return results;
  }

  private void calculateBottomPoint(
      List<ChunkSuit4CPV> currentChunkList,
      long startTime,
      long endTime,
      long interval,
      long curStartTime)
      throws IOException {
    IOMonitor2.M4_LSM_status = Operation.M4_LSM_BP;
    // check size>0 because after updateBPTP because empty ChunkSuit4CPV will be removed from
    // currentChunkList
    while (currentChunkList.size() > 0) { // loop 1
      // sorted by bottomValue, find BP candidate set
      // TODO double check the sort order logic for different aggregations
      currentChunkList.sort(
          (o1, o2) -> {
            return ((Comparable) (o1.getStatistics().getMinValue()))
                .compareTo(o2.getStatistics().getMinValue());
            // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
          });
      // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
      Object value = currentChunkList.get(0).getStatistics().getMinValue();
      List<ChunkSuit4CPV> candidateSet = new ArrayList<>();
      for (ChunkSuit4CPV chunkSuit4CPV : currentChunkList) {
        // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
        if (chunkSuit4CPV.getStatistics().getMinValue().equals(value)) {
          candidateSet.add(chunkSuit4CPV);
        } else {
          break; // note that this is an early break since currentChunkList is sorted
        }
      }

      // TODO check, whether nonLazyLoad remove affects candidateSet
      List<ChunkSuit4CPV> nonLazyLoad = new ArrayList<>(candidateSet);
      // TODO double check the sort order logic for version
      nonLazyLoad.sort(
          (o1, o2) ->
              new MergeReaderPriority(
                      o2.getChunkMetadata().getVersion(),
                      o2.getChunkMetadata().getOffsetOfChunkHeader())
                  .compareTo(
                      new MergeReaderPriority(
                          o1.getChunkMetadata().getVersion(),
                          o1.getChunkMetadata().getOffsetOfChunkHeader())));
      while (true) { // loop 2
        // if there is no chunk for lazy loading, then load all chunks in candidateSet,
        // and apply deleteIntervals, deleting BP no matter out of deletion or update
        if (nonLazyLoad.size() == 0) {
          for (ChunkSuit4CPV chunkSuit4CPV : candidateSet) {
            // Note the pass of delete intervals
            if (chunkSuit4CPV.getPageReader() == null) {
              PageReader pageReader =
                  FileLoaderUtils.loadPageReaderList4CPV(
                      chunkSuit4CPV.getChunkMetadata(), this.timeFilter);
              // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
              //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
              //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
              //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN
              //  DIRECTLY), WHICH WILL INTRODUCE BUGS!
              chunkSuit4CPV.setPageReader(pageReader);
            } else {
              // Note the pass of delete intervals, especially deleting the non-latest candidate
              // point.
              // pageReader does not refer to the same deleteInterval as those in chunkMetadata
              // after chunkMetadata executes insertIntoSortedDeletions
              chunkSuit4CPV
                  .getPageReader()
                  .setDeleteIntervalList(chunkSuit4CPV.getChunkMetadata().getDeleteIntervalList());
            }
            // chunk data read operation (c)
            //            chunkSuit4CPV.getPageReader().updateBPTP(chunkSuit4CPV);
            chunkSuit4CPV.getPageReader().updateBP_withValueIndex(chunkSuit4CPV);
            // check if empty
            if (chunkSuit4CPV.statistics.getCount() == 0) {
              currentChunkList.remove(chunkSuit4CPV);
            }
          }
          break; // exit loop 2, enter loop 1
        }
        // otherwise, extract the next new candidate point from the candidate set with the lazy load
        // strategy
        ChunkSuit4CPV candidate = nonLazyLoad.get(0); // sorted by version
        MergeReaderPriority candidateVersion =
            new MergeReaderPriority(
                candidate.getChunkMetadata().getVersion(),
                candidate.getChunkMetadata().getOffsetOfChunkHeader());
        long candidateTimestamp = candidate.getStatistics().getBottomTimestamp(); // TODO check
        Object candidateValue = candidate.getStatistics().getMinValue(); // TODO check

        // verify if this candidate point is deleted
        boolean isDeletedItself = false;
        if (candidateTimestamp < curStartTime || candidateTimestamp >= curStartTime + interval) {
          isDeletedItself = true;
        } else {
          isDeletedItself =
              PageReader.isDeleted(
                  candidateTimestamp, candidate.getChunkMetadata().getDeleteIntervalList());
        }
        if (isDeletedItself) {
          // the candidate point is deleted, then label the chunk as already lazy loaded, and back
          // to loop 2
          nonLazyLoad.remove(candidate);
          // TODO check this can really remove the element
          // TODO check whether nonLazyLoad remove affects candidateSet
          // TODO check nonLazyLoad sorted by version number from high to low
          continue; // back to loop 2

        } else { // not deleted
          boolean isUpdate = false;
          // find overlapping chunks with higher versions
          List<ChunkSuit4CPV> overlaps = new ArrayList<>();
          for (ChunkSuit4CPV chunkSuit4CPV : currentChunkList) {
            ChunkMetadata chunkMetadata = chunkSuit4CPV.getChunkMetadata();
            MergeReaderPriority version =
                new MergeReaderPriority(
                    chunkMetadata.getVersion(), chunkMetadata.getOffsetOfChunkHeader());
            if (version.compareTo(candidateVersion) <= 0) { // including bottomChunkMetadata
              continue;
            }
            if (candidateTimestamp < chunkSuit4CPV.getStatistics().getStartTime()
                || candidateTimestamp > chunkSuit4CPV.getStatistics().getEndTime()) {
              continue;
            }
            if (candidateTimestamp == chunkSuit4CPV.getStatistics().getStartTime()
                || candidateTimestamp == chunkSuit4CPV.getStatistics().getEndTime()) {
              isUpdate = true;
              // this case does not need to execute chunk data read operation (a),
              // because definitely overwrite
              break;
            }
            overlaps.add(chunkSuit4CPV);
          }

          if (!isUpdate && overlaps.size() == 0) {
            // no overlaps, then the candidate point is not updated, then it is the final result
            results
                .get(4)
                .updateResultUsingValues(
                    new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
            return; // finished
          } else if (!isUpdate) {
            // verify whether the candidate point is updated
            for (ChunkSuit4CPV chunkSuit4CPV : overlaps) {
              if (chunkSuit4CPV.getPageReader() == null) {
                PageReader pageReader =
                    FileLoaderUtils.loadPageReaderList4CPV(
                        chunkSuit4CPV.getChunkMetadata(), this.timeFilter);
                // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
                //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
                //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
                //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS
                //  ASSIGN DIRECTLY), WHICH WILL INTRODUCE BUGS!
                chunkSuit4CPV.setPageReader(pageReader);
              }
              // chunk data read operation (a): check existence of data point at a timestamp
              isUpdate = chunkSuit4CPV.checkIfExist(candidateTimestamp);
              if (isUpdate) {
                // since the candidate point is updated, early break
                break;
              }
            }
          }
          if (!isUpdate) {
            // the candidate point is not updated, then it is the final result
            results
                .get(4)
                .updateResultUsingValues(
                    new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
            return; // finished
          } else {
            // the candidate point is updated, then label the chunk as already lazy loaded,
            // add the deletion of the candidate point in deleteInterval, and back to loop 2
            if (candidate.getChunkMetadata().getDeleteIntervalList() == null) {
              List<TimeRange> tmp = new ArrayList<>();
              tmp.add(new TimeRange(candidateTimestamp, candidateTimestamp));
              candidate.getChunkMetadata().setDeleteIntervalList(tmp);
            } else {
              candidate
                  .getChunkMetadata()
                  .insertIntoSortedDeletions(candidateTimestamp, candidateTimestamp); // TODO check
            }
            nonLazyLoad.remove(candidate);
            // TODO check this can really remove the element
            // TODO check whether nonLazyLoad remove affects candidateSet
            // TODO check nonLazyLoad sorted by version number from high to low
            continue; // back to loop 2
          }
        }
      }
    }
  }

  private void calculateTopPoint(
      List<ChunkSuit4CPV> currentChunkList,
      long startTime,
      long endTime,
      long interval,
      long curStartTime)
      throws IOException {
    IOMonitor2.M4_LSM_status = Operation.M4_LSM_TP;
    // check size>0 because after updateBPTP empty ChunkSuit4CPV will be removed from
    // currentChunkList
    while (currentChunkList.size() > 0) { // loop 1
      // sorted by topValue, find TP candidate set
      currentChunkList.sort(
          new Comparator<ChunkSuit4CPV>() { // TODO double check the sort order logic for different
            // aggregations
            public int compare(ChunkSuit4CPV o1, ChunkSuit4CPV o2) {
              return ((Comparable) (o2.getStatistics().getMaxValue()))
                  .compareTo(o1.getStatistics().getMaxValue());
              // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata,
              // because statistics of ChunkSuit4CPV is updated, while statistics of
              // ChunkSuit4CPV.ChunkMetadata
              // is fixed.
            }
          });
      // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
      Object value = currentChunkList.get(0).getStatistics().getMaxValue();
      List<ChunkSuit4CPV> candidateSet = new ArrayList<>();
      for (ChunkSuit4CPV chunkSuit4CPV : currentChunkList) {
        // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
        if (chunkSuit4CPV.getStatistics().getMaxValue().equals(value)) {
          candidateSet.add(chunkSuit4CPV);
        } else {
          break; // note that this is an early break since currentChunkList is sorted
        }
      }

      List<ChunkSuit4CPV> nonLazyLoad = new ArrayList<>(candidateSet);
      // TODO check, whether nonLazyLoad remove affects candidateSet
      nonLazyLoad.sort(
          new Comparator<ChunkSuit4CPV>() { // TODO double check the sort order logic for version
            public int compare(ChunkSuit4CPV o1, ChunkSuit4CPV o2) {
              return new MergeReaderPriority(
                      o2.getChunkMetadata().getVersion(),
                      o2.getChunkMetadata().getOffsetOfChunkHeader())
                  .compareTo(
                      new MergeReaderPriority(
                          o1.getChunkMetadata().getVersion(),
                          o1.getChunkMetadata().getOffsetOfChunkHeader()));
            }
          });
      while (true) { // loop 2
        // if there is no chunk for lazy loading, then load all chunks in candidateSet,
        // and apply deleteIntervals, deleting TP no matter out of deletion or update
        if (nonLazyLoad.size() == 0) {
          for (ChunkSuit4CPV chunkSuit4CPV : candidateSet) {
            // Note the pass of delete intervals
            if (chunkSuit4CPV.getPageReader() == null) {
              PageReader pageReader =
                  FileLoaderUtils.loadPageReaderList4CPV(
                      chunkSuit4CPV.getChunkMetadata(), this.timeFilter);
              // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
              //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
              //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
              //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN
              //  DIRECTLY), WHICH WILL INTRODUCE BUGS!
              chunkSuit4CPV.setPageReader(pageReader);
            } else {
              // Note the pass of delete intervals, especially deleting the non-latest candidate
              // point.
              // pageReader does not refer to the same deleteInterval as those in chunkMetadata
              // after chunkMetadata executes insertIntoSortedDeletions
              chunkSuit4CPV
                  .getPageReader()
                  .setDeleteIntervalList(chunkSuit4CPV.getChunkMetadata().getDeleteIntervalList());
            }
            // chunk data read operation (c)
            //            chunkSuit4CPV.getPageReader().updateBPTP(chunkSuit4CPV);
            chunkSuit4CPV.getPageReader().updateTP_withValueIndex(chunkSuit4CPV); // TODO
            // check if empty
            if (chunkSuit4CPV.statistics.getCount() == 0) {
              currentChunkList.remove(chunkSuit4CPV);
            }
          }
          break; // exit loop 2, enter loop 1
        }
        // otherwise, extract the next new candidate point from the candidate set with the lazy load
        // strategy
        ChunkSuit4CPV candidate = nonLazyLoad.get(0); // sorted by version
        MergeReaderPriority candidateVersion =
            new MergeReaderPriority(
                candidate.getChunkMetadata().getVersion(),
                candidate.getChunkMetadata().getOffsetOfChunkHeader());
        // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata,
        // because statistics of ChunkSuit4CPV is updated, while statistics of
        // ChunkSuit4CPV.ChunkMetadata
        // is fixed.
        long candidateTimestamp = candidate.getStatistics().getTopTimestamp(); // TODO check
        Object candidateValue = candidate.getStatistics().getMaxValue(); // TODO check

        // verify if this candidate point is deleted
        boolean isDeletedItself = false;
        if (candidateTimestamp < curStartTime || candidateTimestamp >= curStartTime + interval) {
          isDeletedItself = true;
        } else {
          isDeletedItself =
              PageReader.isDeleted(
                  candidateTimestamp, candidate.getChunkMetadata().getDeleteIntervalList());
        }
        if (isDeletedItself) {
          // the candidate point is deleted, then label the chunk as already lazy loaded, and back
          // to loop 2
          nonLazyLoad.remove(candidate);
          // TODO check this can really remove the element
          // TODO check whether nonLazyLoad remove affects candidateSet
          // TODO check nonLazyLoad sorted by version number from high to low
          continue; // back to loop 2

        } else { // not deleted
          boolean isUpdate = false;
          // find overlapping chunks with higher versions
          List<ChunkSuit4CPV> overlaps = new ArrayList<>();
          for (ChunkSuit4CPV chunkSuit4CPV : currentChunkList) {
            ChunkMetadata chunkMetadata = chunkSuit4CPV.getChunkMetadata();
            MergeReaderPriority version =
                new MergeReaderPriority(
                    chunkMetadata.getVersion(), chunkMetadata.getOffsetOfChunkHeader());
            if (version.compareTo(candidateVersion) <= 0) { // including topChunkMetadata
              continue;
            }
            if (candidateTimestamp < chunkMetadata.getStartTime()
                || candidateTimestamp > chunkMetadata.getEndTime()) {
              continue;
            }
            if (candidateTimestamp == chunkSuit4CPV.getStatistics().getStartTime()
                || candidateTimestamp == chunkSuit4CPV.getStatistics().getEndTime()) {
              isUpdate = true; // note that here overlaps does not add.
              // this case does not need to execute chunk data read operation (a),
              // because definitely overwrite
              break;
            }
            overlaps.add(chunkSuit4CPV);
          }

          if (!isUpdate && overlaps.size() == 0) {
            // no overlaps, then the candidate point is not updated, then it is the final result
            results
                .get(5)
                .updateResultUsingValues(
                    new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
            return; // finished
          } else if (!isUpdate) {
            // verify whether the candidate point is updated
            for (ChunkSuit4CPV chunkSuit4CPV : overlaps) {
              if (chunkSuit4CPV.getPageReader() == null) {
                PageReader pageReader =
                    FileLoaderUtils.loadPageReaderList4CPV(
                        chunkSuit4CPV.getChunkMetadata(), this.timeFilter);
                // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
                //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
                //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
                //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS
                //  ASSIGN DIRECTLY), WHICH WILL INTRODUCE BUGS!
                chunkSuit4CPV.setPageReader(pageReader);
              }
              // chunk data read operation (a): check existence of data point at a timestamp
              isUpdate = chunkSuit4CPV.checkIfExist(candidateTimestamp);
              if (isUpdate) {
                // since the candidate point is updated, early break
                break;
              }
            }
          }
          if (!isUpdate) {
            // the candidate point is not updated, then it is the final result
            results
                .get(5)
                .updateResultUsingValues(
                    new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
            return; // finished
          } else {
            // the candidate point is updated, then label the chunk as already lazy loaded,
            // add the deletion of the candidate point in deleteInterval, and back to loop 2
            if (candidate.getChunkMetadata().getDeleteIntervalList() == null) {
              List<TimeRange> tmp = new ArrayList<>();
              tmp.add(new TimeRange(candidateTimestamp, candidateTimestamp));
              candidate.getChunkMetadata().setDeleteIntervalList(tmp);
            } else {
              candidate
                  .getChunkMetadata()
                  .insertIntoSortedDeletions(candidateTimestamp, candidateTimestamp); // TODO check
            }
            nonLazyLoad.remove(candidate);
            // TODO check this can really remove the element
            // TODO check whether nonLazyLoad remove affects candidateSet
            // TODO check nonLazyLoad sorted by version number from high to low
            continue; // back to loop 2
          }
        }
      }
    }
  }

  private void calculateFirstPoint(
      List<ChunkSuit4CPV> currentChunkList,
      long startTime,
      long endTime,
      long interval,
      long curStartTime)
      throws IOException {
    IOMonitor2.M4_LSM_status = Operation.M4_LSM_FP;
    while (currentChunkList.size() > 0) { // loop 1
      // sorted by startTime and version, find FP candidate
      currentChunkList.sort(
          new Comparator<ChunkSuit4CPV>() { // TODO double check the sort order logic for different
            // aggregations
            public int compare(ChunkSuit4CPV o1, ChunkSuit4CPV o2) {
              // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
              int res =
                  ((Comparable) (o1.getStatistics().getStartTime()))
                      .compareTo(o2.getStatistics().getStartTime());
              if (res != 0) {
                return res;
              } else {
                return new MergeReaderPriority(
                        o2.getChunkMetadata().getVersion(),
                        o2.getChunkMetadata().getOffsetOfChunkHeader())
                    .compareTo(
                        new MergeReaderPriority(
                            o1.getChunkMetadata().getVersion(),
                            o1.getChunkMetadata().getOffsetOfChunkHeader()));
              }
            }
          });

      ChunkSuit4CPV susp_candidate = currentChunkList.get(0);
      if (susp_candidate.isLazyLoad()) {
        // means the chunk is already lazy loaded, then load the chunk, apply deletes, update
        // statistics,
        // cancel the lazy loaded mark, and back to loop 1
        if (susp_candidate.getPageReader() == null) {
          PageReader pageReader =
              FileLoaderUtils.loadPageReaderList4CPV(
                  susp_candidate.getChunkMetadata(), this.timeFilter);
          // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
          //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
          //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
          //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN
          //  DIRECTLY), WHICH WILL INTRODUCE BUGS!
          susp_candidate.setPageReader(pageReader);
        }
        // chunk data read operation (b): get the closest data point after or before a timestamp
        susp_candidate.updateFPwithTheClosetPointEqualOrAfter(
            susp_candidate.getStatistics().getStartTime());
        susp_candidate.setLazyLoad(false); // DO NOT FORGET THIS!!!
        continue; // back to loop 1
      } else {
        // the chunk has not been lazy loaded, then verify whether the candidate point is deleted
        // Note the higher versions of deletes are guaranteed by
        // QueryUtils.modifyChunkMetaData(chunkMetadataList,pathModifications)
        // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
        long candidateTimestamp = susp_candidate.getStatistics().getStartTime(); // TODO check
        Object candidateValue = susp_candidate.getStatistics().getFirstValue(); // TODO check

        boolean isDeletedItself = false;
        long deleteEndTime = -1;
        List<TimeRange> deleteIntervalList =
            susp_candidate.getChunkMetadata().getDeleteIntervalList();
        if (deleteIntervalList != null) {
          int deleteCursor = 0;
          while (deleteCursor < deleteIntervalList.size()) {
            if (deleteIntervalList.get(deleteCursor).getMax() < candidateTimestamp) {
              deleteCursor++;
            } else if (deleteIntervalList.get(deleteCursor).contains(candidateTimestamp)) {
              isDeletedItself = true;
              deleteEndTime = deleteIntervalList.get(deleteCursor).getMax();
              break; // since delete intervals are already sorted and merged
            } else {
              break; // since delete intervals are already sorted and merged
            }
          }
        }
        if (isDeletedItself) {
          // deleteEndTime may be after the current endTime,
          // because deleteStartTime can be after the startTime of the whole chunk
          if (deleteEndTime
              >= susp_candidate.getStatistics().getEndTime()) { // NOTE here calculate FP
            // deleted as a whole
            currentChunkList.remove(susp_candidate);
          } else {
            // the candidate point is deleted, then label the chunk as already lazy loaded,
            // update chunkStartTime without loading data, and back to loop 1
            susp_candidate.setLazyLoad(true);
            // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
            susp_candidate.getStatistics().setStartTime(deleteEndTime + 1); // TODO check
            // +1 is because delete is closed interval
          }
          continue; // back to loop 1
        } else {
          // the candidate point is not deleted, then it is the final result
          results
              .get(0)
              .updateResultUsingValues(
                  new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
          results
              .get(2)
              .updateResultUsingValues(
                  new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
          return;
        }
      }
    }
  }

  private void calculateLastPoint(
      List<ChunkSuit4CPV> currentChunkList,
      long startTime,
      long endTime,
      long interval,
      long curStartTime)
      throws IOException {
    IOMonitor2.M4_LSM_status = Operation.M4_LSM_LP;
    while (currentChunkList.size() > 0) { // loop 1
      // sorted by endTime and version, find LP candidate
      currentChunkList.sort(
          new Comparator<ChunkSuit4CPV>() { // TODO double check the sort order logic for different
            // aggregations
            public int compare(ChunkSuit4CPV o1, ChunkSuit4CPV o2) {
              int res =
                  ((Comparable) (o2.getStatistics().getEndTime()))
                      .compareTo(o1.getStatistics().getEndTime());
              // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
              if (res != 0) {
                return res;
              } else {
                return new MergeReaderPriority(
                        o2.getChunkMetadata().getVersion(),
                        o2.getChunkMetadata().getOffsetOfChunkHeader())
                    .compareTo(
                        new MergeReaderPriority(
                            o1.getChunkMetadata().getVersion(),
                            o1.getChunkMetadata().getOffsetOfChunkHeader()));
              }
            }
          });

      ChunkSuit4CPV susp_candidate = currentChunkList.get(0);
      if (susp_candidate.isLazyLoad()) {
        // means the chunk is already lazy loaded, then load the chunk, apply deletes, update
        // statistics,
        // cancel the lazy loaded mark, and back to loop 1
        if (susp_candidate.getPageReader() == null) {
          PageReader pageReader =
              FileLoaderUtils.loadPageReaderList4CPV(
                  susp_candidate.getChunkMetadata(), this.timeFilter);
          // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
          //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
          //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
          //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN
          //  DIRECTLY), WHICH WILL INTRODUCE BUGS!
          susp_candidate.setPageReader(pageReader);
        }
        // TODO update LP equal to or before statistics.getEndTime
        // (b) get the closest data point after or before a timestamp
        susp_candidate.updateLPwithTheClosetPointEqualOrBefore(
            susp_candidate.getStatistics().getEndTime()); // TODO DEBUG
        susp_candidate.setLazyLoad(false); // TODO DO NOT FORGET THIS!!!
        continue; // back to loop 1
      } else {
        // the chunk has not been lazy loaded, then verify whether the candidate point is deleted
        // Note the higher versions of deletes are guaranteed by
        // QueryUtils.modifyChunkMetaData(chunkMetadataList,pathModifications)
        // TODO NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
        long candidateTimestamp = susp_candidate.getStatistics().getEndTime(); // TODO check
        Object candidateValue = susp_candidate.getStatistics().getLastValue(); // TODO check

        boolean isDeletedItself = false;
        long deleteStartTime = Long.MAX_VALUE; // TODO check
        List<TimeRange> deleteIntervalList =
            susp_candidate.getChunkMetadata().getDeleteIntervalList();
        if (deleteIntervalList != null) {
          int deleteCursor = 0;
          while (deleteCursor < deleteIntervalList.size()) {
            if (deleteIntervalList.get(deleteCursor).getMax() < candidateTimestamp) {
              deleteCursor++;
            } else if (deleteIntervalList.get(deleteCursor).contains(candidateTimestamp)) {
              isDeletedItself = true;
              deleteStartTime = deleteIntervalList.get(deleteCursor).getMin();
              break; // since delete intervals are already sorted and merged
            } else {
              break; // since delete intervals are already sorted and merged
            }
          }
        }
        if (isDeletedItself) {
          // deleteStartTime may be before the current startTime,
          // because deleteEndTime can be before the endTime of the whole chunk
          if (deleteStartTime <= susp_candidate.getStatistics().getStartTime()) {
            // NOTE here calculate LP.
            // deleted as a whole
            currentChunkList.remove(susp_candidate);
          } else {
            susp_candidate.setLazyLoad(true);
            // TODO NOTE here get statistics from ChunkSuit4CPV, not from
            // ChunkSuit4CPV.ChunkMetadata
            susp_candidate.getStatistics().setEndTime(deleteStartTime - 1);
            // -1 is because delete is closed interval
            // TODO check
          }
          continue; // back to loop 1
        } else {
          // the candidate point is not deleted, then it is the final result
          results
              .get(1)
              .updateResultUsingValues(
                  new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
          results
              .get(3)
              .updateResultUsingValues(
                  new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
          return;
        }
      }
    }
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
