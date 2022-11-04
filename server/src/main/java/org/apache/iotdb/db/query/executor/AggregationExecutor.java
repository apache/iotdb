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

package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.*;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.ValueIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

@SuppressWarnings("java:S1135") // ignore todos
public class AggregationExecutor {

  protected static final Logger logger = LoggerFactory.getLogger(AggregationExecutor.class);

  protected List<PartialPath> selectedSeries;
  protected List<TSDataType> dataTypes;
  protected List<String> aggregations;
  protected IExpression expression;
  protected boolean ascending;
  protected QueryContext context;
  protected AggregateResult[] aggregateResultList;

  /**
   * e.g. when there are 2 time series root.test.d0.s0, root.vehicle.d0.s0 the query is "select
   * exact_median(s0) from root.** group by level=0" let exact_median(root.test.d0.s0) ->
   * exact_median(root.*.*.s0) exact_median(root.vehicle.d0.s0) -> exact_median(root.*.*.s0) only
   * for aggregations like exact_median.
   */
  protected Map<AggregateResult, AggregateResult> resultToGroupedAhead;

  /** aggregation batch calculation size. */
  private int aggregateFetchSize;

  protected AggregationExecutor(QueryContext context, AggregationPlan aggregationPlan) {
    this.selectedSeries = new ArrayList<>();
    aggregationPlan
        .getDeduplicatedPaths()
        .forEach(k -> selectedSeries.add(((MeasurementPath) k).transformToExactPath()));
    this.dataTypes = aggregationPlan.getDeduplicatedDataTypes();
    this.aggregations = aggregationPlan.getDeduplicatedAggregations();
    this.expression = aggregationPlan.getExpression();
    this.aggregateFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
    this.ascending = aggregationPlan.isAscending();
    this.context = context;
    this.aggregateResultList = new AggregateResult[selectedSeries.size()];
  }

  /** execute aggregate function with only time filter or no filter. */
  public QueryDataSet executeWithoutValueFilter(AggregationPlan aggregationPlan)
      throws StorageEngineException, IOException, QueryProcessException {

    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }

    // TODO use multi-thread
    Map<PartialPath, List<Integer>> pathToAggrIndexesMap =
        MetaUtils.groupAggregationsBySeries(selectedSeries);
    // Attention: this method will REMOVE aligned path from pathToAggrIndexesMap
    Map<AlignedPath, List<List<Integer>>> alignedPathToAggrIndexesMap =
        MetaUtils.groupAlignedSeriesWithAggregations(pathToAggrIndexesMap);

    List<PartialPath> groupedPathList =
        new ArrayList<>(pathToAggrIndexesMap.size() + alignedPathToAggrIndexesMap.size());
    groupedPathList.addAll(pathToAggrIndexesMap.keySet());
    groupedPathList.addAll(alignedPathToAggrIndexesMap.keySet());

    // TODO-Cluster: group the paths by storage group to reduce communications
    Pair<List<VirtualStorageGroupProcessor>, Map<VirtualStorageGroupProcessor, List<PartialPath>>>
        lockListAndProcessorToSeriesMapPair =
            StorageEngine.getInstance().mergeLock(groupedPathList);
    List<VirtualStorageGroupProcessor> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<VirtualStorageGroupProcessor, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    try {
      // init QueryDataSource Cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(processorToSeriesMap, context, timeFilter);
    } catch (Exception e) {
      logger.error("Meet error when init QueryDataSource ", e);
      throw new QueryProcessException("Meet error when init QueryDataSource.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }

    // construct AggregateResult
    int maxIteration = 1;
    for (int i = 0; i < selectedSeries.size(); i++) {
      aggregateResultList[i] =
          AggregateResultFactory.getAggrResultByName(aggregations.get(i), dataTypes.get(i));
      maxIteration = Math.max(maxIteration, aggregateResultList[i].maxIteration());
    }
    //    for (int i = 0; i < selectedSeries.size(); i++)
    // System.out.println("[DEBUG]\t[aggrHashCode]"+aggregateResultList[i].hashCode());

    // for aggregations like EXACT_MEDIAN, need to group by level ahead of aggregation executed
    aggregationPlan.groupAggResultByLevelBeforeAggregation(Arrays.asList(aggregateResultList));
    resultToGroupedAhead = aggregationPlan.getresultToGroupedAhead();

    for (int iteration = 0; iteration < maxIteration; iteration++) {

      List<AggregateResult> remainingGroupedAheadAggr =
          findUnfinishedGroupedAheadAggr(iteration, resultToGroupedAhead);
      for (AggregateResult groupedAheadAggr : remainingGroupedAheadAggr)
        groupedAheadAggr.startIteration();

      // find unfinished AggrIndexes
      Set<Integer> remainingAggrIndexes =
          findUnfinishedAggrIndexes(iteration, aggregateResultList, resultToGroupedAhead);

      for (Map.Entry<PartialPath, List<Integer>> entry : pathToAggrIndexesMap.entrySet()) {
        PartialPath seriesPath = entry.getKey();
        List<Integer> indexes =
            new ArrayList<>(CollectionUtils.intersection(entry.getValue(), remainingAggrIndexes));
        if (!indexes.isEmpty())
          aggregateOneSeries(
              seriesPath,
              indexes,
              aggregationPlan.getAllMeasurementsInDevice(seriesPath.getDevice()),
              timeFilter);
      }
      for (Map.Entry<AlignedPath, List<List<Integer>>> entry :
          alignedPathToAggrIndexesMap.entrySet()) {
        AlignedPath alignedPath = entry.getKey();
        List<List<Integer>> subIndexes = new ArrayList<>();
        for (List<Integer> indexes : entry.getValue())
          subIndexes.add(
              new ArrayList<>(CollectionUtils.intersection(indexes, remainingAggrIndexes)));
        if (!isAggrIndexEmpty(subIndexes))
          aggregateOneAlignedSeries(
              alignedPath,
              subIndexes,
              aggregationPlan.getAllMeasurementsInDevice(alignedPath.getDevice()),
              timeFilter);
      }

      for (AggregateResult groupedAheadAggr : remainingGroupedAheadAggr)
        groupedAheadAggr.finishIteration();
    }
    //    for (int i = 0; i < selectedSeries.size(); i++)
    // System.out.println("[DEBUG]\t\t[aggrHashCode]"+aggregateResultList[i].hashCode());
    //    System.out.println("\n\n");

    return constructDataSetAfterAggregation(Arrays.asList(aggregateResultList), aggregationPlan);
  }

  /**
   * get aggregation result for one series
   *
   * @param timeFilter time filter
   */
  protected void aggregateOneSeries(
      PartialPath seriesPath,
      List<Integer> indexes,
      Set<String> allMeasurementsInDevice,
      Filter timeFilter)
      throws IOException, QueryProcessException, StorageEngineException {
    List<AggregateResult> ascAggregateResultList = new ArrayList<>();
    List<AggregateResult> descAggregateResultList = new ArrayList<>();

    TSDataType tsDataType = dataTypes.get(indexes.get(0));
    for (int i : indexes) {
      AggregateResult aggregateResult = aggregateResultList[i];
      if (aggregateResult.isAscending()) {
        ascAggregateResultList.add(aggregateResult);
      } else {
        descAggregateResultList.add(aggregateResult);
      }
    }
    aggregateOneSeries(
        seriesPath,
        allMeasurementsInDevice,
        context,
        timeFilter,
        tsDataType,
        ascAggregateResultList,
        descAggregateResultList,
        null,
        ascending,
        resultToGroupedAhead);
  }

  protected void aggregateOneAlignedSeries(
      AlignedPath alignedPath,
      List<List<Integer>> subIndexes,
      Set<String> allMeasurementsInDevice,
      Filter timeFilter)
      throws IOException, QueryProcessException, StorageEngineException {
    List<List<AggregateResult>> ascAggregateResultList = new ArrayList<>();
    List<List<AggregateResult>> descAggregateResultList = new ArrayList<>();

    for (List<Integer> subIndex : subIndexes) {
      List<AggregateResult> subAscResultList = new ArrayList<>();
      List<AggregateResult> subDescResultList = new ArrayList<>();
      for (int i : subIndex) {
        AggregateResult aggregateResult = aggregateResultList[i];
        if (aggregateResult.isAscending()) {
          subAscResultList.add(aggregateResult);
        } else {
          subDescResultList.add(aggregateResult);
        }
      }
      ascAggregateResultList.add(subAscResultList);
      descAggregateResultList.add(subDescResultList);
    }

    aggregateOneAlignedSeries(
        alignedPath,
        allMeasurementsInDevice,
        context,
        timeFilter,
        TSDataType.VECTOR,
        ascAggregateResultList,
        descAggregateResultList,
        null,
        ascending);
  }

  // single iteration
  @SuppressWarnings("squid:S107")
  public static void aggregateOneSeries(
      PartialPath seriesPath,
      Set<String> measurements,
      QueryContext context,
      Filter timeFilter,
      TSDataType tsDataType,
      List<AggregateResult> ascAggregateResultList,
      List<AggregateResult> descAggregateResultList,
      TsFileFilter fileFilter,
      boolean ascending,
      Map<AggregateResult, AggregateResult> resultToGroupedAhead)
      throws StorageEngineException, IOException, QueryProcessException {

    if (resultToGroupedAhead == null) {
      resultToGroupedAhead = new HashMap<>();
      for (AggregateResult aggregateResult : ascAggregateResultList)
        resultToGroupedAhead.put(aggregateResult, aggregateResult);
      for (AggregateResult aggregateResult : descAggregateResultList)
        resultToGroupedAhead.put(aggregateResult, aggregateResult);
    }

    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance()
            .getQueryDataSource(seriesPath, context, timeFilter, ascending);
    if (fileFilter != null) {
      QueryUtils.filterQueryDataSource(queryDataSource, fileFilter);
    }
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    if (ascAggregateResultList != null && !ascAggregateResultList.isEmpty()) {
      boolean allUseStatistics = true;
      for (AggregateResult aggregateResult : ascAggregateResultList)
        allUseStatistics &= aggregateResult.useStatisticsIfPossible();
      IAggregateReader seriesReader;
      int strategy = IoTDBDescriptor.getInstance().getConfig().getAggregationStrategy();
      if (!allUseStatistics || strategy == 0)
        seriesReader =
            new SeriesAggregateReader(
                seriesPath,
                measurements,
                tsDataType,
                context,
                queryDataSource,
                timeFilter,
                null,
                null,
                true);
      else {
        if (strategy == 1)
          seriesReader =
              new SeriesAggregateReaderForStatChain(
                  seriesPath,
                  measurements,
                  tsDataType,
                  context,
                  queryDataSource,
                  timeFilter,
                  null,
                  null);
        else
          seriesReader =
              new SeriesAggregateReaderForStat(
                  seriesPath,
                  measurements,
                  tsDataType,
                  context,
                  queryDataSource,
                  timeFilter,
                  null,
                  null);
      }
      aggregateFilesFromReader(seriesReader, ascAggregateResultList, resultToGroupedAhead);
    }
    // Quantile Aggregation is ascending.
    if (descAggregateResultList != null && !descAggregateResultList.isEmpty()) {
      IAggregateReader seriesReader =
          new SeriesAggregateReader(
              seriesPath,
              measurements,
              tsDataType,
              context,
              queryDataSource,
              timeFilter,
              null,
              null,
              false);
      aggregateFilesFromReader(seriesReader, descAggregateResultList, resultToGroupedAhead);
    }
  }

  public static void aggregateOneAlignedSeries(
      AlignedPath alignedPath,
      Set<String> measurements,
      QueryContext context,
      Filter timeFilter,
      TSDataType tsDataType,
      List<List<AggregateResult>> ascAggregateResultList,
      List<List<AggregateResult>> descAggregateResultList,
      TsFileFilter fileFilter,
      boolean ascending)
      throws StorageEngineException, IOException, QueryProcessException {

    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance()
            .getQueryDataSource(alignedPath, context, timeFilter, ascending);
    if (fileFilter != null) {
      QueryUtils.filterQueryDataSource(queryDataSource, fileFilter);
    }
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    if (!isAggregateResultEmpty(ascAggregateResultList)) {
      AlignedSeriesAggregateReader seriesReader =
          new AlignedSeriesAggregateReader(
              alignedPath,
              measurements,
              tsDataType,
              context,
              queryDataSource,
              timeFilter,
              null,
              null,
              true);
      aggregateFromAlignedReader(seriesReader, ascAggregateResultList);
    }
    if (!isAggregateResultEmpty(descAggregateResultList)) {
      AlignedSeriesAggregateReader seriesReader =
          new AlignedSeriesAggregateReader(
              alignedPath,
              measurements,
              tsDataType,
              context,
              queryDataSource,
              timeFilter,
              null,
              null,
              false);
      aggregateFromAlignedReader(seriesReader, descAggregateResultList);
    }
  }

  private static boolean isAggregateResultEmpty(List<List<AggregateResult>> resultList) {
    for (List<AggregateResult> result : resultList) {
      if (!result.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private static boolean isAggrIndexEmpty(List<List<Integer>> resultList) {
    for (List<Integer> result : resultList) {
      if (!result.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  /**
   * find results which doesn't hasFinalResult. Returns a new List iteration is checked before this
   */
  private static List<AggregateResult> findUnfinishedAggregateResults(
      List<AggregateResult> aggregateResultList,
      Map<AggregateResult, AggregateResult> resultToGroupedAhead) {
    List<AggregateResult> remainingAggregateResultList = new ArrayList<>();
    for (AggregateResult aggregateResult : aggregateResultList)
      if (!resultToGroupedAhead.get(aggregateResult).hasFinalResult())
        remainingAggregateResultList.add(aggregateResult);
    return remainingAggregateResultList;
  }

  /** find unfinished AggrIndexes */
  private static Set<Integer> findUnfinishedAggrIndexes(
      int iteration,
      AggregateResult[] aggregateResultList,
      Map<AggregateResult, AggregateResult> resultToGroupedAhead) {
    Set<Integer> remainingAggrIndexes = new HashSet<>();
    for (int aggrIndex = 0; aggrIndex < aggregateResultList.length; aggrIndex++) {
      AggregateResult aggregateResult = resultToGroupedAhead.get(aggregateResultList[aggrIndex]);
      if (!aggregateResult.hasFinalResult() && aggregateResult.maxIteration() > iteration)
        remainingAggrIndexes.add(aggrIndex);
    }
    return remainingAggrIndexes;
  }

  /** find unfinished groupedAheadAggr */
  private static List<AggregateResult> findUnfinishedGroupedAheadAggr(
      int iteration, Map<AggregateResult, AggregateResult> resultToGroupedAhead) {
    List<AggregateResult> remainingAggrIndexes = new ArrayList<>();
    for (AggregateResult aggregateResult : new HashSet<>(resultToGroupedAhead.values()))
      if (!aggregateResult.hasFinalResult() && aggregateResult.maxIteration() > iteration)
        remainingAggrIndexes.add(aggregateResult);
    return remainingAggrIndexes;
  }

  private static void aggregatePagesFromReader(
      IAggregateReader seriesReader,
      List<AggregateResult> aggregateResultList,
      Map<AggregateResult, AggregateResult> resultToGroupedAhead)
      throws QueryProcessException, IOException {
    while (seriesReader.hasNextPage()) {
      aggregateResultList =
          findUnfinishedAggregateResults(aggregateResultList, resultToGroupedAhead);

      if (seriesReader instanceof SeriesAggregateReaderForStatChain) {
        SeriesAggregateReaderForStatChain reader = (SeriesAggregateReaderForStatChain) seriesReader;
        if (reader.seriesReader.tryToUseBF && !reader.seriesReader.readingOldPages) {
          reader.seriesReader.readyToUseChain();
          if (reader.seriesReader.readingOldPages) continue;
          if (reader.seriesReader.hasNoUpdateChunkMd()) {
            if (reader.seriesReader.canUseNoUpdateChunkStat()) {
              //              System.out.println("\t\t\tAggregate with noUpdateChunkStat!!!");
              List<AggregateResult> remainingAggregateResultList =
                  tryToAggregateFromStatistics(
                      aggregateResultList,
                      reader.seriesReader.popNoUpdateChunkMd().getStatistics(),
                      resultToGroupedAhead);
              if (!remainingAggregateResultList.isEmpty())
                System.out.println("\t\tERROR!\tcan't aggr from (unoverlapped) stat");
              //              System.out.println("\t\t\tAggregate with noUpdateChunkStat over.");
            } else {
              reader.seriesReader.giveUpNoUpdateChunkMd();
            }
          }
          continue;
        }
      }
      if (seriesReader instanceof SeriesAggregateReaderForStat) {
        SeriesAggregateReaderForStat reader = (SeriesAggregateReaderForStat) seriesReader;
        if (reader.seriesReader.tryToUseBF && !reader.seriesReader.readingOldPages) {
          reader.seriesReader.readyToUseNoUpdateChunkMd();
          // after oldest updated block consumed.
          if (reader.seriesReader.hasNoUpdateChunkMd()) {
            if (reader.seriesReader.canUseNoUpdateChunkStat()) {
              //              System.out.println("\t\t\tAggregate with noUpdateChunkStat!!!");
              List<AggregateResult> remainingAggregateResultList =
                  tryToAggregateFromStatistics(
                      aggregateResultList,
                      reader.seriesReader.popNoUpdateChunkMd().getStatistics(),
                      resultToGroupedAhead);
              if (!remainingAggregateResultList.isEmpty())
                System.out.println("\t\tERROR!\tcan't aggr from (unoverlapped) stat");
              //              System.out.println("\t\t\tAggregate with noUpdateChunkStat over.");
            } else {
              reader.seriesReader.giveUpNoUpdateChunkMd();
            }
          }
          continue;
        }
      }

      // try to calc by page statistics
      if (seriesReader.canUseCurrentPageStatistics()) {
        Statistics pageStatistics = seriesReader.currentPageStatistics();
        List<AggregateResult> remainingAggregateResultList =
            tryToAggregateFromStatistics(aggregateResultList, pageStatistics, resultToGroupedAhead);
        if (remainingAggregateResultList.isEmpty()) {
          seriesReader.skipCurrentPage();
          continue;
        }
        IBatchDataIterator batchDataIterator = seriesReader.nextPage().getBatchDataIterator();
        for (AggregateResult aggregateResult : remainingAggregateResultList) {
          AggregateResult groupedAheadAggr = resultToGroupedAhead.get(aggregateResult);
          groupedAheadAggr.updateResultFromPageData(batchDataIterator);
          batchDataIterator.reset();
        }
      } else {
        BatchData batchData = seriesReader.nextPage();
        if (batchData == null) {
          System.out.println("[DEBUG] pageData NULL");
          continue;
        }
        IBatchDataIterator batchDataIterator = batchData.getBatchDataIterator();
        //        System.out.print(
        //            "[DEBUG] got pageData. minT:"
        //                + batchDataIterator.currentTime()
        //                + "\tlen:"
        //                + batchDataIterator.totalLength());
        for (AggregateResult aggregateResult : aggregateResultList) {
          AggregateResult groupedAheadAggr = resultToGroupedAhead.get(aggregateResult);
          //          System.out.println("[DEBUG] "+aggregateResult.hashCode()+"
          // "+groupedAheadAggr.hashCode());
          if (groupedAheadAggr.useOverlapStat()) {
            groupedAheadAggr.updateResultFromOverlap(seriesReader);
          } else {
            groupedAheadAggr.updateResultFromPageData(batchDataIterator);
            batchDataIterator.reset();
          }
        }
        long mxT = Long.MIN_VALUE;
        while (batchDataIterator.hasNext()) {
          mxT = Math.max(mxT, batchDataIterator.currentTime());
          batchDataIterator.next();
        }
        //        System.out.println("\tmxT:" + mxT);
      }
    }
  }

  private static void aggregateChunksFromReader(
      IAggregateReader seriesReader,
      List<AggregateResult> aggregateResultList,
      Map<AggregateResult, AggregateResult> resultToGroupedAhead)
      throws QueryProcessException, IOException {
    while (seriesReader.hasNextChunk()) {
      aggregateResultList =
          findUnfinishedAggregateResults(aggregateResultList, resultToGroupedAhead);
      // try to calc by chunk statistics
      if (seriesReader.canUseCurrentChunkStatistics()) {
        Statistics chunkStatistics = seriesReader.currentChunkStatistics();
        List<AggregateResult> remainingAggregateResultList =
            tryToAggregateFromStatistics(
                aggregateResultList, chunkStatistics, resultToGroupedAhead);
        if (remainingAggregateResultList.isEmpty()) {
          seriesReader.skipCurrentChunk();
          continue;
        }
        aggregatePagesFromReader(seriesReader, remainingAggregateResultList, resultToGroupedAhead);
      } else {
        aggregatePagesFromReader(seriesReader, aggregateResultList, resultToGroupedAhead);
      }
    }
  }

  private static void aggregateFilesFromReader(
      IAggregateReader seriesReader,
      List<AggregateResult> aggregateResultList,
      Map<AggregateResult, AggregateResult> resultToGroupedAhead)
      throws QueryProcessException, IOException {
    while (seriesReader.hasNextFile()) {
      aggregateResultList =
          findUnfinishedAggregateResults(aggregateResultList, resultToGroupedAhead);
      // try to calc by file statistics
      if (seriesReader.canUseCurrentFileStatistics()) {
        Statistics fileStatistics = seriesReader.currentFileStatistics();
        List<AggregateResult> remainingAggregateResultList =
            tryToAggregateFromStatistics(aggregateResultList, fileStatistics, resultToGroupedAhead);
        if (remainingAggregateResultList.isEmpty()) {
          seriesReader.skipCurrentFile();
          continue;
        }
        aggregateChunksFromReader(seriesReader, remainingAggregateResultList, resultToGroupedAhead);
      } else {
        aggregateChunksFromReader(seriesReader, aggregateResultList, resultToGroupedAhead);
      }
    }
  }

  private static void aggregateFromAlignedReader(
      AlignedSeriesAggregateReader seriesReader, List<List<AggregateResult>> aggregateResultList)
      throws QueryProcessException, IOException {
    int remainingToCalculate = 0;
    List<boolean[]> isCalculatedArray = new ArrayList<>();
    for (List<AggregateResult> subAggregateResults : aggregateResultList) {
      remainingToCalculate += subAggregateResults.size();
      boolean[] subCalculatedArray = new boolean[subAggregateResults.size()];
      isCalculatedArray.add(subCalculatedArray);
    }

    while (seriesReader.hasNextFile()) {
      // cal by file statistics
      // TODO
      //      if (seriesReader.canUseCurrentFileStatistics()) {
      //        while (seriesReader.hasNextSubSeries()) {
      //          Statistics fileStatistics = seriesReader.currentFileStatistics();
      //          remainingToCalculate =
      //              aggregateStatistics(
      //                  aggregateResultList.get(seriesReader.getCurIndex()),
      //                  isCalculatedArray.get(seriesReader.getCurIndex()),
      //                  remainingToCalculate,
      //                  fileStatistics);
      //          if (remainingToCalculate == 0) {
      //            seriesReader.resetIndex();
      //            return;
      //          }
      //          seriesReader.nextSeries();
      //        }
      //        seriesReader.skipCurrentFile();
      //        continue;
      //      }

      while (seriesReader.hasNextChunk()) {
        // cal by chunk statistics
        if (seriesReader.canUseCurrentChunkStatistics()) {
          while (seriesReader.hasNextSubSeries()) {
            Statistics chunkStatistics = seriesReader.currentChunkStatistics();
            remainingToCalculate =
                aggregateStatistics(
                    aggregateResultList.get(seriesReader.getCurIndex()),
                    isCalculatedArray.get(seriesReader.getCurIndex()),
                    remainingToCalculate,
                    chunkStatistics);
            if (remainingToCalculate == 0) {
              seriesReader.resetIndex();
              return;
            }
            seriesReader.nextSeries();
          }
          seriesReader.skipCurrentChunk();
          continue;
        }

        remainingToCalculate =
            aggregateAlignedPages(
                seriesReader, aggregateResultList, isCalculatedArray, remainingToCalculate);
        if (remainingToCalculate == 0) {
          return;
        }
      }
    }
  }

  /**
   * Try To Aggregate each result in the list with the statistics Returns results which can't be
   * updated by the statistics
   */
  private static List<AggregateResult> tryToAggregateFromStatistics(
      List<AggregateResult> aggregateResultList,
      Statistics statistics,
      Map<AggregateResult, AggregateResult> resultToGroupedAhead)
      throws QueryProcessException {
    // some aligned paths' statistics may be null
    if (statistics == null) {
      return aggregateResultList;
    }
    List<AggregateResult> remainingAggregateResultList = new ArrayList<>();
    for (AggregateResult aggregateResult : aggregateResultList) {
      AggregateResult groupedAheadAggr = resultToGroupedAhead.get(aggregateResult);
      if (groupedAheadAggr.canUpdateFromStatistics(statistics))
        groupedAheadAggr.updateResultFromStatistics(statistics);
      else remainingAggregateResultList.add(aggregateResult);
    }
    return remainingAggregateResultList;
  }

  /** Aggregate each result in the list with the statistics */
  private static int aggregateStatistics(
      List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedArray,
      int remainingToCalculate,
      Statistics statistics)
      throws QueryProcessException {
    // some aligned paths' statistics may be null
    if (statistics == null) {
      return remainingToCalculate;
    }
    int newRemainingToCalculate = remainingToCalculate;
    for (int i = 0; i < aggregateResultList.size(); i++) {
      if (!isCalculatedArray[i]) {
        AggregateResult aggregateResult = aggregateResultList.get(i);
        aggregateResult.updateResultFromStatistics(statistics);
        if (aggregateResult.hasFinalResult()) {
          isCalculatedArray[i] = true;
          newRemainingToCalculate--;
          if (newRemainingToCalculate == 0) {
            return newRemainingToCalculate;
          }
        }
      }
    }
    return newRemainingToCalculate;
  }

  private static int aggregateAlignedPages(
      AlignedSeriesAggregateReader seriesReader,
      List<List<AggregateResult>> aggregateResultList,
      List<boolean[]> isCalculatedArray,
      int remainingToCalculate)
      throws IOException, QueryProcessException {
    while (seriesReader.hasNextPage()) {
      // cal by page statistics
      if (seriesReader.canUseCurrentPageStatistics()) {
        while (seriesReader.hasNextSubSeries()) {
          Statistics pageStatistic = seriesReader.currentPageStatistics();
          remainingToCalculate =
              aggregateStatistics(
                  aggregateResultList.get(seriesReader.getCurIndex()),
                  isCalculatedArray.get(seriesReader.getCurIndex()),
                  remainingToCalculate,
                  pageStatistic);
          if (remainingToCalculate == 0) {
            seriesReader.resetIndex();
            return 0;
          }
          seriesReader.nextSeries();
        }
        seriesReader.skipCurrentPage();
        continue;
      }

      BatchData nextOverlappedPageData = seriesReader.nextPage();
      while (seriesReader.hasNextSubSeries()) {
        int subIndex = seriesReader.getCurIndex();
        IBatchDataIterator batchIterator = nextOverlappedPageData.getBatchDataIterator(subIndex);
        remainingToCalculate =
            aggregateBatchData(
                aggregateResultList.get(subIndex),
                isCalculatedArray.get(subIndex),
                remainingToCalculate,
                batchIterator);
        if (remainingToCalculate == 0) {
          seriesReader.resetIndex();
          return 0;
        }
        seriesReader.nextSeries();
      }
    }
    return remainingToCalculate;
  }

  private static int aggregateBatchData(
      List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedArray,
      int remainingToCalculate,
      IBatchDataIterator batchIterator)
      throws QueryProcessException, IOException {
    int newRemainingToCalculate = remainingToCalculate;
    for (int i = 0; i < aggregateResultList.size(); i++) {
      if (!isCalculatedArray[i]) {
        AggregateResult aggregateResult = aggregateResultList.get(i);
        aggregateResult.updateResultFromPageData(batchIterator);
        batchIterator.reset();
        if (aggregateResult.hasFinalResult()) {
          isCalculatedArray[i] = true;
          remainingToCalculate--;
          if (remainingToCalculate == 0) {
            return newRemainingToCalculate;
          }
        }
      }
    }
    return newRemainingToCalculate;
  }

  /** execute aggregate function with value filter. */
  public QueryDataSet executeWithValueFilter(AggregationPlan queryPlan)
      throws StorageEngineException, IOException, QueryProcessException {
    optimizeLastElementFunc(queryPlan);

    Map<List<IReaderByTimestamp>, List<List<Integer>>> readersToAggrIndexesMap = new HashMap<>();

    // group by path name
    Map<PartialPath, List<Integer>> pathToAggrIndexesMap =
        MetaUtils.groupAggregationsBySeries(selectedSeries);
    Map<AlignedPath, List<List<Integer>>> alignedPathToAggrIndexesMap =
        MetaUtils.groupAlignedSeriesWithAggregations(pathToAggrIndexesMap);

    List<PartialPath> groupedPathList =
        new ArrayList<>(pathToAggrIndexesMap.size() + alignedPathToAggrIndexesMap.size());
    groupedPathList.addAll(pathToAggrIndexesMap.keySet());
    groupedPathList.addAll(alignedPathToAggrIndexesMap.keySet());

    Pair<List<VirtualStorageGroupProcessor>, Map<VirtualStorageGroupProcessor, List<PartialPath>>>
        lockListAndProcessorToSeriesMapPair =
            StorageEngine.getInstance().mergeLock(groupedPathList);
    List<VirtualStorageGroupProcessor> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<VirtualStorageGroupProcessor, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    int maxIteration = 1;
    for (int i = 0; i < selectedSeries.size(); i++) {
      aggregateResultList[i] =
          AggregateResultFactory.getAggrResultByName(
              aggregations.get(i), dataTypes.get(i), ascending);
      maxIteration = Math.max(maxIteration, aggregateResultList[i].maxIteration());
    }
    List<TimeGenerator> timestampGenerators = new ArrayList<>(maxIteration);
    for (int iteration = 0; iteration < maxIteration; iteration++)
      timestampGenerators.add(getTimeGenerator(context, queryPlan));

    // for aggregations like EXACT_MEDIAN, need to group by level ahead of aggregation executed
    queryPlan.groupAggResultByLevelBeforeAggregation(Arrays.asList(aggregateResultList));
    resultToGroupedAhead = queryPlan.getresultToGroupedAhead();

    try {
      // init QueryDataSource Cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(
              processorToSeriesMap, context, timestampGenerators.get(0).getTimeFilter());
    } catch (Exception e) {
      logger.error("Meet error when init QueryDataSource ", e);
      throw new QueryProcessException("Meet error when init QueryDataSource.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }

    for (PartialPath path : pathToAggrIndexesMap.keySet()) {
      List<Integer> aggrIndexes = pathToAggrIndexesMap.get(path);
      int maxIterationInPath = getMaxIterationFromAggrIndexes(aggrIndexes);
      List<IReaderByTimestamp> seriesReadersByTimestamp =
          getReaderListByTime(maxIterationInPath, path, queryPlan, path.getSeriesType(), context);
      readersToAggrIndexesMap.put(seriesReadersByTimestamp, Collections.singletonList(aggrIndexes));
    }
    for (AlignedPath vectorPath : alignedPathToAggrIndexesMap.keySet()) {
      List<List<Integer>> aggrIndexes = alignedPathToAggrIndexesMap.get(vectorPath);
      int maxIterationInDevice = getMaxIterationInDevice(aggrIndexes);
      List<IReaderByTimestamp> seriesReadersByTimestamp =
          getReaderListByTime(
              maxIterationInDevice, vectorPath, queryPlan, vectorPath.getSeriesType(), context);
      readersToAggrIndexesMap.put(seriesReadersByTimestamp, aggrIndexes);
    }
    // assign null to be friendly for GC
    pathToAggrIndexesMap = null;
    alignedPathToAggrIndexesMap = null;

    for (int iteration = 0; iteration < maxIteration; iteration++) {

      List<AggregateResult> remainingGroupedAheadAggr =
          findUnfinishedGroupedAheadAggr(iteration, resultToGroupedAhead);

      for (AggregateResult groupedAheadAggr : remainingGroupedAheadAggr)
        groupedAheadAggr.startIteration();

      aggregateWithValueFilter(
          iteration, timestampGenerators.get(iteration), readersToAggrIndexesMap);

      for (AggregateResult groupedAheadAggr : remainingGroupedAheadAggr)
        groupedAheadAggr.finishIteration();
    }
    return constructDataSetAfterAggregation(Arrays.asList(aggregateResultList), queryPlan);
  }

  private void optimizeLastElementFunc(QueryPlan queryPlan) {
    int index = 0;
    for (; index < aggregations.size(); index++) {
      String aggregationFunc = aggregations.get(index);
      if (!aggregationFunc.equals(IoTDBConstant.MAX_TIME)
          && !aggregationFunc.equals(IoTDBConstant.LAST_VALUE)) {
        break;
      }
    }
    if (index >= aggregations.size()) {
      queryPlan.setAscending(false);
      this.ascending = false;
    }
  }

  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan queryPlan)
      throws StorageEngineException {
    return new ServerTimeGenerator(context, queryPlan);
  }

  protected IReaderByTimestamp getReaderByTime(
      PartialPath path, RawDataQueryPlan queryPlan, TSDataType dataType, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return new SeriesReaderByTimestamp(
        path,
        queryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        QueryResourceManager.getInstance().getQueryDataSource(path, context, null, ascending),
        null,
        ascending);
  }

  private List<IReaderByTimestamp> getReaderListByTime(
      int maxIteration,
      PartialPath path,
      RawDataQueryPlan queryPlan,
      TSDataType dataType,
      QueryContext context)
      throws StorageEngineException, QueryProcessException {
    List<IReaderByTimestamp> readerList = new ArrayList<>();
    for (int i = 0; i < maxIteration; i++)
      readerList.add(getReaderByTime(path, queryPlan, dataType, context));
    return readerList;
  }

  /** calculate aggregation result with value filter. */
  private void aggregateWithValueFilter(
      int iteration,
      TimeGenerator timestampGenerator,
      Map<List<IReaderByTimestamp>, List<List<Integer>>> readersToAggrIndexesMap)
      throws IOException {
    //    System.out.println("[aggregateWithValueFilter] "+iteration);
    List<Boolean> cached =
        markFilterdPaths(
            expression, new ArrayList<>(selectedSeries), timestampGenerator.hasOrNode());
    //    System.out.println("[AggrWithVF]" + selectedSeries.toString());
    while (timestampGenerator.hasNext()) {

      // find unfinished AggregateResults
      Set<Integer> remainingAggrIndexes =
          findUnfinishedAggrIndexes(iteration, aggregateResultList, resultToGroupedAhead);
      if (remainingAggrIndexes.isEmpty()) break;

      // generate timestamps for aggregate
      long[] timeArray = new long[aggregateFetchSize];
      int timeArrayLength = 0;
      for (int cnt = 0; cnt < aggregateFetchSize; cnt++) {
        if (!timestampGenerator.hasNext()) {
          break;
        }
        timeArray[timeArrayLength++] = timestampGenerator.next();
        //        System.out.println("[TimeStamp]"+ timeArray[timeArrayLength-1]);
      }

      // cal part of aggregate result
      for (Entry<List<IReaderByTimestamp>, List<List<Integer>>> entry :
          readersToAggrIndexesMap.entrySet()) {
        if (entry.getKey().size() <= iteration) continue;
        // use cache data as much as possible
        boolean[] cachedOrNot = new boolean[entry.getValue().size()];

        // the i th series in the device
        for (int i = 0; i < entry.getValue().size(); i++) {
          List<Integer> subIndexes = entry.getValue().get(i);

          // skip if all finished
          Collection<Integer> remainingSubIndexes =
              CollectionUtils.intersection(subIndexes, remainingAggrIndexes);
          if (remainingSubIndexes.isEmpty()) continue;

          int pathId = subIndexes.get(0);
          // if the series is cached in timeGenerator
          if (cached.get(pathId)) {
            Object[] values = timestampGenerator.getValues(selectedSeries.get(pathId));
            ValueIterator valueIterator = QueryUtils.generateValueIterator(values);
            //            System.out.println("[path cached]"+Arrays.toString(values));
            if (valueIterator != null) {
              for (Integer index : remainingSubIndexes) {
                resultToGroupedAhead
                    .get(aggregateResultList[index])
                    .updateResultUsingValues(timeArray, timeArrayLength, valueIterator);
                valueIterator.reset();
              }
              cachedOrNot[i] = true;
            }
          }
        }

        if (hasRemaining(cachedOrNot)) {

          Object[] values =
              entry.getKey().get(iteration).getValuesInTimestamps(timeArray, timeArrayLength);
          //          System.out.println("[HasRemaining]"+ Arrays.toString(values));
          ValueIterator valueIterator = QueryUtils.generateValueIterator(values);
          if (valueIterator != null) {
            for (int i = 0; i < entry.getValue().size(); i++) {
              if (!cachedOrNot[i]) {
                List<Integer> subIndexes = entry.getValue().get(i);
                // skip if all finished
                Collection<Integer> remainingSubIndexes =
                    CollectionUtils.intersection(subIndexes, remainingAggrIndexes);
                if (remainingSubIndexes.isEmpty()) continue;
                valueIterator.setSubMeasurementIndex(i);
                for (Integer index : remainingSubIndexes) {
                  resultToGroupedAhead
                      .get(aggregateResultList[index])
                      .updateResultUsingValues(timeArray, timeArrayLength, valueIterator);
                  valueIterator.reset();
                }
              }
            }
          }
        }
      }
    }
  }

  /** Return whether there is result that has not been cached */
  private boolean hasRemaining(boolean[] cachedOrNot) {
    for (int i = 0; i < cachedOrNot.length; i++) {
      if (!cachedOrNot[i]) {
        return true;
      }
    }
    return false;
  }

  private int getMaxIterationFromAggrIndexes(List<Integer> aggrIndexes) {
    int maxIteration = 0;
    for (int aggrIndex : aggrIndexes)
      maxIteration =
          Math.max(
              maxIteration,
              resultToGroupedAhead.get(aggregateResultList[aggrIndex]).maxIteration());
    return maxIteration;
  }

  private int getMaxIterationInDevice(List<List<Integer>> aggrIndexes) {
    int maxIteration = 0;
    for (List<Integer> aggrIndexList : aggrIndexes)
      maxIteration = Math.max(maxIteration, getMaxIterationFromAggrIndexes(aggrIndexList));
    return maxIteration;
  }

  private QueryDataSet constructDataSetAfterAggregation(
      List<AggregateResult> aggregateResultList, AggregationPlan plan) {
    SingleDataSet dataSet;
    RowRecord record = new RowRecord(0);

    if (plan.isGroupByLevel()) {
      Map<String, AggregateResult> groupPathsResultMap =
          plan.groupAggResultByLevelAfterAggregation(aggregateResultList);
      List<PartialPath> paths = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      for (AggregateResult resultData : groupPathsResultMap.values()) {
        //
        // System.out.println("[DEBUG]\t[constructDataSetAfterAggregation]"+resultData.hashCode());
        dataTypes.add(resultData.getResultDataType());
        record.addField(resultData.getResult(), resultData.getResultDataType());
      }
      dataSet = new SingleDataSet(paths, dataTypes);
    } else {
      for (AggregateResult resultData : aggregateResultList) {
        TSDataType dataType = resultData.getResultDataType();
        record.addField(resultData.getResult(), dataType);
      }
      dataSet = new SingleDataSet(selectedSeries, dataTypes);
    }
    dataSet.setRecord(record);

    return dataSet;
  }
}
