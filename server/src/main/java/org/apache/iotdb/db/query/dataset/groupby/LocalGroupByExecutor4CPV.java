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
import org.apache.iotdb.tsfile.read.common.ChunkSuit4CPV;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Pair;

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
    } catch (IOException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    results.add(aggrResult);
  }

  private void getCurrentChunkListFromFutureChunkList(
      long curStartTime, long curEndTime, long startTime, long endTime, long interval)
      throws IOException {
    // empty currentChunkList
    currentChunkList = new ArrayList<>();

    // get related chunks from splitChunkList
    int curIdx = (int) Math.floor((curStartTime - startTime) * 1.0 / interval);
    if (splitChunkList.get(curIdx) != null) {
      currentChunkList.addAll(splitChunkList.get(curIdx));
      // TODO when to free splitChunkList memory
    }

    // iterate futureChunkList
    ListIterator itr = futureChunkList.listIterator();
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
        // TODO chunk data read operation (b) get the closest data point after or before a timestamp
        itr.remove();
        // B: loads chunk data from disk to memory
        // C: decompress page data, split time&value buffers
        List<IPageReader> pageReaderList =
            FileLoaderUtils.loadPageReaderList(chunkSuit4CPV.getChunkMetadata(), this.timeFilter);
        //        if (pageReaderList.size() > 1) {
        //          throw new IOException("Against the assumption that there is only one page in a
        // chunk!");
        //        }
        //        for (IPageReader pageReader : pageReaderList) {
        // assume only one page in a chunk
        // assume all data on disk, no data in memory
        // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
        //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
        //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
        //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN DIRECTLY),
        //  WHICH WILL INTRODUCE BUGS!
        ((PageReader) pageReaderList.get(0))
            .split4CPV(
                startTime,
                endTime,
                interval,
                curStartTime,
                currentChunkList,
                splitChunkList,
                chunkMetadata);
        //        }
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

    getCurrentChunkListFromFutureChunkList(curStartTime, curEndTime, startTime, endTime, interval);

    if (currentChunkList.size() == 0) {
      return results;
    }

    calculateFirstPoint(currentChunkList, startTime, endTime, interval, curStartTime);
    calculateLastPoint(currentChunkList, startTime, endTime, interval, curStartTime);
    calculateBottomPoint(currentChunkList, startTime, endTime, interval, curStartTime);
    calculateTopPoint(currentChunkList, startTime, endTime, interval, curStartTime);

    return results;
  }

  private void calculateBottomPoint(
      List<ChunkSuit4CPV> currentChunkList,
      long startTime,
      long endTime,
      long interval,
      long curStartTime)
      throws IOException {
    while (currentChunkList.size() > 0) { // 循环1 TODO debug
      // check size>0 because after updateBPTP empty ChunkSuit4CPV will be removed from
      // currentChunkList
      // 按照bottomValue排序，找出BP candidate set
      currentChunkList.sort(
          new Comparator<ChunkSuit4CPV>() { // TODO double check the sort order logic for different
            // aggregations
            public int compare(ChunkSuit4CPV o1, ChunkSuit4CPV o2) {
              return ((Comparable) (o1.getStatistics().getMinValue()))
                  .compareTo(o2.getStatistics().getMinValue());
              // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
            }
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

      List<ChunkSuit4CPV> nonLazyLoad =
          new ArrayList<>(
              candidateSet); // TODO check, whether nonLazyLoad remove affects candidateSet
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
      while (true) { // 循环2
        // 如果set里所有点所在的chunk都是lazy
        // load，则对所有块进行load，应用deleteIntervals，并把BP删掉（因为不管是被删除删掉还是被更新删掉都是删掉这个点）
        if (nonLazyLoad.size() == 0) {
          for (ChunkSuit4CPV chunkSuit4CPV : candidateSet) {
            // TODO 注意delete intervals的传递
            if (chunkSuit4CPV.getPageReader() == null) {
              List<IPageReader> pageReaderList =
                  FileLoaderUtils.loadPageReaderList(
                      chunkSuit4CPV.getChunkMetadata(), this.timeFilter);
              // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
              //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
              //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
              //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN DIRECTLY),
              //  WHICH WILL INTRODUCE BUGS!
              chunkSuit4CPV.setPageReader((PageReader) pageReaderList.get(0));
            } else {
              // TODO 注意delete intervals的传递：主要是被重写点作为点删除传递
              // pageReader does not refer to the same deleteInterval as those in chunkMetadata
              // after chunkMetadata executes insertIntoSortedDeletions
              chunkSuit4CPV
                  .getPageReader()
                  .setDeleteIntervalList(chunkSuit4CPV.getChunkMetadata().getDeleteIntervalList());
            }
            // TODO chunk data read operation (c): get all data points
            chunkSuit4CPV.getPageReader().updateBPTP(chunkSuit4CPV);
            // TODO check if empty
            if (chunkSuit4CPV.statistics.getCount() == 0) {
              currentChunkList.remove(chunkSuit4CPV);
            }
          }
          break; // 退出循环2，进入循环1
        }
        // 否则，找出candidate set里非lazy load里version最高的那个块的BP点作为candidate point
        ChunkSuit4CPV candidate = nonLazyLoad.get(0); // TODO check sort right
        MergeReaderPriority candidateVersion =
            new MergeReaderPriority(
                candidate.getChunkMetadata().getVersion(),
                candidate.getChunkMetadata().getOffsetOfChunkHeader());
        long candidateTimestamp = candidate.getStatistics().getBottomTimestamp(); // TODO check
        Object candidateValue = candidate.getStatistics().getMinValue(); // TODO check

        // verify if this candidate point is deleted
        boolean isDeletedItself = false;
        // TODO add M4 interval virtual delete since BP/TP is not updated in
        // getCurrentChunkListFromFutureChunkList
        if (candidateTimestamp < curStartTime || candidateTimestamp >= curStartTime + interval) {
          isDeletedItself = true;
        }
        else {
          isDeletedItself =
              PageReader.isDeleted(
                  candidateTimestamp, candidate.getChunkMetadata().getDeleteIntervalList());
        }
        if (isDeletedItself) { // 是被删除，则标记candidate point所在块为lazy load，然后回到循环2
          nonLazyLoad.remove(candidate);
          // TODO check this can really remove the element
          // TODO check whether nonLazyLoad remove affects candidateSet
          // TODO check nonLazyLoad sorted by version number from high to low
          continue; // 回到循环2

        } else { // 否被删除
          boolean isUpdate = false;
          // 找出所有更高版本的overlap它的块
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

          if (!isUpdate && overlaps.size() == 0) { // 否被overlap，则当前candidate point就是计算结果，结束
            results
                .get(4) // TODO check: minTimestamp, maxTimestamp, firstValue, lastValue,
                // minValue[bottomTimestamp], maxValue[topTimestamp]
                .updateResultUsingValues(
                    new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
            // TODO check updateResult
            return; // 计算结束
          } else if (!isUpdate) { // 是被overlap，则partial scan所有这些overlap的块
            for (ChunkSuit4CPV chunkSuit4CPV : overlaps) {
              // scan这个chunk的数据
              // TODO chunk data read operation (a): check existence of data point at a timestamp
              if (chunkSuit4CPV.getPageReader() == null) {
                List<IPageReader> pageReaderList =
                    FileLoaderUtils.loadPageReaderList(
                        chunkSuit4CPV.getChunkMetadata(), this.timeFilter);
                // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
                //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
                //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
                //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN DIRECTLY),
                //  WHICH WILL INTRODUCE BUGS!
                chunkSuit4CPV.setPageReader((PageReader) pageReaderList.get(0));
              }
              isUpdate = chunkSuit4CPV.checkIfExist(candidateTimestamp);
              if (isUpdate) { // 提前结束对overlaps块的scan，因为已经找到一个update点证明candidate失效
                break;
              }
            }
          }
          if (!isUpdate) { // partial scan了所有overlap的块都没有找到这样的点，则当前candidate point就是计算结果，结束
            results
                .get(4) // TODO check: minTimestamp, maxTimestamp, firstValue, lastValue,
                // minValue[bottomTimestamp], maxValue[topTimestamp]
                .updateResultUsingValues(
                    new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
            // TODO check updateResult
            return; // 计算结束
          } else { // 找到这样的点，于是标记candidate point所在块为lazy
            // load，并对其chunkMetadata的deleteInterval里加上对该点时间的删除，然后进入循环2
            if (candidate.getChunkMetadata().getDeleteIntervalList() == null) {
              List<TimeRange> tmp = new ArrayList<>();
              tmp.add(new TimeRange(candidateTimestamp, candidateTimestamp));
              candidate.getChunkMetadata().setDeleteIntervalList(tmp);
            } else {
              //              candidate.getChunkMetadata().getDeleteIntervalList()
              //                  .add(new TimeRange(candidateTimestamp, candidateTimestamp)); //
              // TODO check
              candidate
                  .getChunkMetadata()
                  .insertIntoSortedDeletions(candidateTimestamp, candidateTimestamp); // TODO check
              // TODO debug chunk and page deleteInterval not the same
            }
            // 删除那里不需要再加了，而这里更新就需要手动加一下删除操作
            nonLazyLoad.remove(candidate);
            // TODO check this can really remove the element
            // TODO check whether nonLazyLoad remove affects candidateSet
            // TODO check nonLazyLoad sorted by version number from high to low
            continue; // 回到循环2
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
    while (currentChunkList.size() > 0) { // 循环1
      // check size>0 because after updateBPTP empty ChunkSuit4CPV will be removed from
      // currentChunkList
      // 按照topValue排序，找出TP candidate set
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
      // TODO check, whether nonLazyLoad remov  e affects candidateSet
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
      while (true) { // 循环2
        // 如果set里所有点所在的chunk都是lazy
        // load，则对所有块进行load，应用deleteIntervals，并把TP删掉（因为不管是被删除删掉还是被更新删掉都是删掉这个点）
        if (nonLazyLoad.size() == 0) {
          for (ChunkSuit4CPV chunkSuit4CPV : candidateSet) {
            // TODO 注意delete intervals的传递
            if (chunkSuit4CPV.getPageReader() == null) {
              List<IPageReader> pageReaderList =
                  FileLoaderUtils.loadPageReaderList(
                      chunkSuit4CPV.getChunkMetadata(), this.timeFilter);
              // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
              //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
              //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
              //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN DIRECTLY),
              //  WHICH WILL INTRODUCE BUGS!
              chunkSuit4CPV.setPageReader((PageReader) pageReaderList.get(0));
            } else {
              // TODO 注意delete intervals的传递：主要是被重写点作为点删除传递
              // pageReader does not refer to the same deleteInterval as those in chunkMetadata
              // after chunkMetadata executes insertIntoSortedDeletions
              chunkSuit4CPV
                  .getPageReader()
                  .setDeleteIntervalList(chunkSuit4CPV.getChunkMetadata().getDeleteIntervalList());
            }
            // TODO chunk data read operation (c): get all data points
            chunkSuit4CPV.getPageReader().updateBPTP(chunkSuit4CPV);
            // TODO check if empty
            if (chunkSuit4CPV.statistics.getCount() == 0) {
              currentChunkList.remove(chunkSuit4CPV);
            }
          }
          break; // 退出循环2，进入循环1
        }
        // 否则，找出candidate set里非lazy load里version最高的那个块的TP点作为candidate point
        ChunkSuit4CPV candidate = nonLazyLoad.get(0); // TODO check sort right
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

        // verify这个candidate point
        // 是否被删除
        boolean isDeletedItself = false;
        // TODO add M4 interval virtual delete since BP/TP is not updated in
        // getCurrentChunkListFromFutureChunkList
        if (candidateTimestamp < curStartTime || candidateTimestamp >= curStartTime + interval) {
          isDeletedItself = true;
        } else {
          isDeletedItself =
              PageReader.isDeleted(
                  candidateTimestamp, candidate.getChunkMetadata().getDeleteIntervalList());
        }
        if (isDeletedItself) { // 是被删除，则标记candidate point所在块为lazy load，然后回到循环2
          nonLazyLoad.remove(candidate);
          // TODO check this can really remove the element
          // TODO check whether nonLazyLoad remove affects candidateSet
          // TODO check nonLazyLoad sorted by version number from high to low
          continue; // 回到循环2

        } else { // 否被删除
          boolean isUpdate = false;
          // 找出所有更高版本的overlap它的块
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

          if (!isUpdate && overlaps.size() == 0) { // 否被overlap，则当前candidate point就是计算结果，结束
            results
                .get(5) // TODO check: minTimestamp, maxTimestamp, firstValue, lastValue,
                // minValue[bottomTimestamp], maxValue[topTimestamp]
                .updateResultUsingValues(
                    new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
            // TODO check updateResult
            return; // 计算结束
          } else if (!isUpdate) { // 是被overlap，则partial scan所有这些overlap的块
            for (ChunkSuit4CPV chunkSuit4CPV : overlaps) {
              // scan这个chunk的数据
              // TODO chunk data read operation (a): check existence of data point at a timestamp
              if (chunkSuit4CPV.getPageReader() == null) {
                List<IPageReader> pageReaderList =
                    FileLoaderUtils.loadPageReaderList(
                        chunkSuit4CPV.getChunkMetadata(), this.timeFilter);
                // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
                //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
                //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
                //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN DIRECTLY),
                //  WHICH WILL INTRODUCE BUGS!
                chunkSuit4CPV.setPageReader((PageReader) pageReaderList.get(0));
              }
              isUpdate = chunkSuit4CPV.checkIfExist(candidateTimestamp);
              if (isUpdate) { // 提前结束对overlaps块的scan，因为已经找到一个update点证明candidate失效
                break;
              }
            }
          }
          if (!isUpdate) { // partial scan了所有overlap的块都没有找到这样的点，则当前candidate point就是计算结果，结束
            results
                .get(5) // TODO check: minTimestamp, maxTimestamp, firstValue, lastValue,
                // minValue[bottomTimestamp], maxValue[topTimestamp]
                .updateResultUsingValues(
                    new long[] {candidateTimestamp}, 1, new Object[] {candidateValue});
            // TODO check updateResult
            return; // 计算结束
          } else { // 找到这样的点，于是标记candidate point所在块为lazy
            // load，并对其chunkMetadata的deleteInterval里加上对该点时间的删除，然后进入循环2
            if (candidate.getChunkMetadata().getDeleteIntervalList() == null) {
              List<TimeRange> tmp = new ArrayList<>();
              tmp.add(new TimeRange(candidateTimestamp, candidateTimestamp));
              candidate.getChunkMetadata().setDeleteIntervalList(tmp);
            } else {
              //              candidate.getChunkMetadata().getDeleteIntervalList()
              //                  .add(new TimeRange(candidateTimestamp, candidateTimestamp)); //
              // TODO check
              candidate
                  .getChunkMetadata()
                  .insertIntoSortedDeletions(candidateTimestamp, candidateTimestamp); // TODO check
            }
            // 删除那里不需要再加了，而这里更新就需要手动加一下删除操作
            nonLazyLoad.remove(candidate);
            // TODO check this can really remove the element
            // TODO check whether nonLazyLoad remove affects candidateSet
            // TODO check nonLazyLoad sorted by version number from high to low
            continue; // 回到循环2
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
    while (currentChunkList.size() > 0) { // 循环1 TODO debug when currentChunkList size=0
      // 按照startTime和version排序，找出疑似FP candidate
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

      // 判断该疑似candidate所在chunk是否lazy load
      ChunkSuit4CPV susp_candidate = currentChunkList.get(0);
      if (susp_candidate.isLazyLoad()) { // 如果是lazy
        // load，则此时load、应用deletes、更新batchData和statistics，取消lazyLoad标记，然后回到循环1
        if (susp_candidate.getPageReader() == null) {
          List<IPageReader> pageReaderList =
              FileLoaderUtils.loadPageReaderList(
                  susp_candidate.getChunkMetadata(), this.timeFilter);
          // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
          //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
          //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
          //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN DIRECTLY),
          //  WHICH WILL INTRODUCE BUGS!
          susp_candidate.setPageReader((PageReader) pageReaderList.get(0));
        }
        // TODO update FP equal to or after statistics.getEndTime
        susp_candidate.updateFPwithTheClosetPointEqualOrAfter(
            susp_candidate.getStatistics().getStartTime()); // TODO DEBUG
        susp_candidate.setLazyLoad(false); // DO NOT FORGET THIS!!!
        continue; // 回到循环1
      } else { // 如果不是lazy load，则该疑似candidate就是真正的candidate。
        // 于是verification判断该点是否被更高优先级（更高优先级这一点在QueryUtils.modifyChunkMetaData(chunkMetadataList,
        // pathModifications)已做好）的deletes覆盖
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
              // deleteEndTime可能会大于当前的endTime，因为delete起点可以在整个chunk startTime之后
              break; // since delete intervals are already sorted and merged
            } else {
              break; // since delete intervals are already sorted and merged
            }
          }
        }
        // 如果被删除，标记该点所在chunk为lazy load，并且在不load数据的情况下更新chunkStartTime，然后回到循环1
        if (isDeletedItself) {
          if (deleteEndTime >= susp_candidate.getStatistics().getEndTime()) { // NOTE 这里计算的是FP
            // TODO debug 整个区间内点删掉
            currentChunkList.remove(susp_candidate);
          } else {
            susp_candidate.setLazyLoad(true);
            // NOTE here get statistics from ChunkSuit4CPV, not from ChunkSuit4CPV.ChunkMetadata
            susp_candidate.getStatistics().setStartTime(deleteEndTime + 1); // TODO check
            // +1 is because delete is closed interval
          }
          continue; // 回到循环1
        } else {
          // 否则，则就是计算结果，结束
          // TODO check: minTimestamp, maxTimestamp, firstValue, lastValue,
          // minValue[bottomTimestamp], maxValue[topTimestamp]
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
    while (currentChunkList.size() > 0) { // 循环1
      // 按照startTime和version排序，找出疑似LP candidate
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

      // 判断该疑似candidate所在chunk是否lazy load
      ChunkSuit4CPV susp_candidate = currentChunkList.get(0);
      if (susp_candidate.isLazyLoad()) { // 如果是lazy
        // load，则此时load、应用deletes、更新batchData和statistics，取消lazyLoad标记，然后回到循环1
        if (susp_candidate.getPageReader() == null) {
          List<IPageReader> pageReaderList =
              FileLoaderUtils.loadPageReaderList(
                  susp_candidate.getChunkMetadata(), this.timeFilter);
          // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
          //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
          //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
          //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN DIRECTLY),
          //  WHICH WILL INTRODUCE BUGS!
          susp_candidate.setPageReader((PageReader) pageReaderList.get(0));
        }
        // TODO update FP equal to or after statistics.getEndTime
        susp_candidate.updateLPwithTheClosetPointEqualOrBefore(
            susp_candidate.getStatistics().getEndTime()); // TODO DEBUG
        susp_candidate.setLazyLoad(false); // TODO DO NOT FORGET THIS!!!
        continue; // 回到循环1
      } else { // 如果不是lazy load，则该疑似candidate就是真正的candidate。
        // 于是verification判断该点是否被更高优先级（更高优先级这一点在QueryUtils.modifyChunkMetaData(chunkMetadataList,
        // pathModifications)已做好）的deletes覆盖
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
              // deleteEndTime可能会大于当前的endTime，因为delete起点可以在整个chunk startTime之后
              break; // since delete intervals are already sorted and merged
            } else {
              break; // since delete intervals are already sorted and merged
            }
          }
        }
        // 如果被删除，标记该点所在chunk为lazy load，并且在不load数据的情况下更新chunkEndTime，然后回到循环1
        if (isDeletedItself) {
          if (deleteStartTime <= susp_candidate.getStatistics().getStartTime()) { // NOTE 这里计算的是LP
            // TODO debug 整个区间内点删掉
            currentChunkList.remove(susp_candidate);
          } else {
            susp_candidate.setLazyLoad(true);
            susp_candidate.getChunkMetadata().getStatistics().setEndTime(deleteStartTime - 1);
            // -1 is because delete is closed interval
            // TODO check
          }
          continue; // 回到循环1
        } else {
          // 否则，则就是计算结果，结束
          // TODO check: minTimestamp, maxTimestamp, firstValue, lastValue,
          // minValue[bottomTimestamp], maxValue[topTimestamp]
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
