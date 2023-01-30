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
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.aggregation.impl.ArAggrResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.series.AlignedSeriesAggregateReader;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.ValueIterator;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

@SuppressWarnings("java:S1135") // ignore todos
public class AggregationExecutor {

  private static final Logger logger = LoggerFactory.getLogger(AggregationExecutor.class);

  private List<PartialPath> selectedSeries;
  protected List<TSDataType> dataTypes;
  protected List<String> aggregations;
  protected IExpression expression;
  protected boolean ascending;
  protected QueryContext context;
  protected AggregateResult[] aggregateResultList;
  protected List<Map<String, String>> parameters;

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
    this.parameters = aggregationPlan.getParameters();
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

    for (Map.Entry<PartialPath, List<Integer>> entry : pathToAggrIndexesMap.entrySet()) {
      PartialPath seriesPath = entry.getKey();
      aggregateOneSeries(
          seriesPath,
          entry.getValue(),
          aggregationPlan.getAllMeasurementsInDevice(seriesPath.getDevice()),
          timeFilter);
    }
    for (Map.Entry<AlignedPath, List<List<Integer>>> entry :
        alignedPathToAggrIndexesMap.entrySet()) {
      AlignedPath alignedPath = entry.getKey();
      aggregateOneAlignedSeries(
          alignedPath,
          entry.getValue(),
          aggregationPlan.getAllMeasurementsInDevice(alignedPath.getDevice()),
          timeFilter);
    }

    return constructDataSet(Arrays.asList(aggregateResultList), aggregationPlan);
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
    ArAggrResult arAggrResult = null;
    boolean[] isAsc = new boolean[aggregateResultList.length];
    boolean[] isAr = new boolean[aggregateResultList.length];

    TSDataType tsDataType = dataTypes.get(indexes.get(0));
    for (int i : indexes) {
      // construct AggregateResult
      AggregateResult aggregateResult =
          AggregateResultFactory.getAggrResultByName(aggregations.get(i), tsDataType);
      if (aggregateResult.getAggregationType() == AggregationType.AR) {
        arAggrResult = (ArAggrResult) aggregateResult;
        arAggrResult.setParameters(parameters.get(i));
        isAr[i] = true;
        continue;
      }
      if (aggregateResult.isAscending()) {
        ascAggregateResultList.add(aggregateResult);
        isAsc[i] = true;
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
        ascending);
    if (arAggrResult != null) {
      aggregateAr(
          seriesPath, allMeasurementsInDevice, context, timeFilter, tsDataType, arAggrResult, null);
    }
    int ascIndex = 0;
    int descIndex = 0;
    for (int i : indexes) {
      if (isAr[i]) {
        aggregateResultList[i] = arAggrResult;
      } else {
        aggregateResultList[i] =
            isAsc[i]
                ? ascAggregateResultList.get(ascIndex++)
                : descAggregateResultList.get(descIndex++);
      }
    }
  }

  protected void aggregateOneAlignedSeries(
      AlignedPath alignedPath,
      List<List<Integer>> subIndexes,
      Set<String> allMeasurementsInDevice,
      Filter timeFilter)
      throws IOException, QueryProcessException, StorageEngineException {
    List<List<AggregateResult>> ascAggregateResultList = new ArrayList<>();
    List<List<AggregateResult>> descAggregateResultList = new ArrayList<>();
    boolean[] isAsc = new boolean[aggregateResultList.length];

    for (List<Integer> subIndex : subIndexes) {
      TSDataType tsDataType = dataTypes.get(subIndex.get(0));
      List<AggregateResult> subAscResultList = new ArrayList<>();
      List<AggregateResult> subDescResultList = new ArrayList<>();
      for (int i : subIndex) {
        // construct AggregateResult
        AggregateResult aggregateResult =
            AggregateResultFactory.getAggrResultByName(aggregations.get(i), tsDataType);
        if (aggregateResult.isAscending()) {
          subAscResultList.add(aggregateResult);
          isAsc[i] = true;
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

    for (int i = 0; i < subIndexes.size(); i++) {
      List<Integer> subIndex = subIndexes.get(i);
      List<AggregateResult> subAscResultList = ascAggregateResultList.get(i);
      List<AggregateResult> subDescResultList = descAggregateResultList.get(i);
      int ascIndex = 0;
      int descIndex = 0;
      for (int index : subIndex) {
        aggregateResultList[index] =
            isAsc[index] ? subAscResultList.get(ascIndex++) : subDescResultList.get(descIndex++);
      }
    }
  }

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
      boolean ascending)
      throws StorageEngineException, IOException, QueryProcessException {

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
      QueryUtils.fillOrderIndexes(queryDataSource, seriesPath.getDevice(), true);
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
              true);
      aggregateFromReader(seriesReader, ascAggregateResultList);
    }
    if (descAggregateResultList != null && !descAggregateResultList.isEmpty()) {
      QueryUtils.fillOrderIndexes(queryDataSource, seriesPath.getDevice(), false);
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
      aggregateFromReader(seriesReader, descAggregateResultList);
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

    if (ascAggregateResultList != null && !isAggregateResultEmpty(ascAggregateResultList)) {
      QueryUtils.fillOrderIndexes(queryDataSource, alignedPath.getDevice(), true);
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
    if (descAggregateResultList != null && !isAggregateResultEmpty(descAggregateResultList)) {
      QueryUtils.fillOrderIndexes(queryDataSource, alignedPath.getDevice(), false);
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

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private static void aggregateFromReader(
      IAggregateReader seriesReader, List<AggregateResult> aggregateResultList)
      throws QueryProcessException, IOException {
    int remainingToCalculate = aggregateResultList.size();
    boolean[] isCalculatedArray = new boolean[aggregateResultList.size()];

    while (seriesReader.hasNextFile()) {
      // cal by file statistics
      if (seriesReader.canUseCurrentFileStatistics()) {
        Statistics fileStatistics = seriesReader.currentFileStatistics();
        remainingToCalculate =
            aggregateStatistics(
                aggregateResultList, isCalculatedArray, remainingToCalculate, fileStatistics);
        if (remainingToCalculate == 0) {
          return;
        }
        seriesReader.skipCurrentFile();
        continue;
      }

      while (seriesReader.hasNextChunk()) {
        // cal by chunk statistics
        if (seriesReader.canUseCurrentChunkStatistics()) {
          Statistics chunkStatistics = seriesReader.currentChunkStatistics();
          remainingToCalculate =
              aggregateStatistics(
                  aggregateResultList, isCalculatedArray, remainingToCalculate, chunkStatistics);
          if (remainingToCalculate == 0) {
            return;
          }
          seriesReader.skipCurrentChunk();
          continue;
        }

        remainingToCalculate =
            aggregatePages(
                seriesReader, aggregateResultList, isCalculatedArray, remainingToCalculate);
        if (remainingToCalculate == 0) {
          return;
        }
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
      if (seriesReader.canUseCurrentFileStatistics()) {
        while (seriesReader.hasNextSubSeries()) {
          Statistics fileStatistics = seriesReader.currentFileStatistics();
          remainingToCalculate =
              aggregateStatistics(
                  aggregateResultList.get(seriesReader.getCurIndex()),
                  isCalculatedArray.get(seriesReader.getCurIndex()),
                  remainingToCalculate,
                  fileStatistics);
          if (remainingToCalculate == 0) {
            seriesReader.resetIndex();
            return;
          }
          seriesReader.nextSeries();
        }
        seriesReader.skipCurrentFile();
        continue;
      }

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

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private static int aggregatePages(
      IAggregateReader seriesReader,
      List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedArray,
      int remainingToCalculate)
      throws IOException, QueryProcessException {
    while (seriesReader.hasNextPage()) {
      // cal by page statistics
      if (seriesReader.canUseCurrentPageStatistics()) {
        Statistics pageStatistic = seriesReader.currentPageStatistics();
        remainingToCalculate =
            aggregateStatistics(
                aggregateResultList, isCalculatedArray, remainingToCalculate, pageStatistic);
        if (remainingToCalculate == 0) {
          return 0;
        }
        seriesReader.skipCurrentPage();
        continue;
      }
      IBatchDataIterator batchDataIterator = seriesReader.nextPage().getBatchDataIterator();
      remainingToCalculate =
          aggregateBatchData(
              aggregateResultList, isCalculatedArray, remainingToCalculate, batchDataIterator);
    }
    return remainingToCalculate;
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

    TimeGenerator timestampGenerator = getTimeGenerator(context, queryPlan);

    Map<IReaderByTimestamp, List<List<Integer>>> readerToAggrIndexesMap = new HashMap<>();

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

    try {
      // init QueryDataSource Cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(
              processorToSeriesMap, context, timestampGenerator.getTimeFilter());
    } catch (Exception e) {
      logger.error("Meet error when init QueryDataSource ", e);
      throw new QueryProcessException("Meet error when init QueryDataSource.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }

    for (PartialPath path : pathToAggrIndexesMap.keySet()) {
      IReaderByTimestamp seriesReaderByTimestamp =
          getReaderByTime(path, queryPlan, path.getSeriesType(), context);
      readerToAggrIndexesMap.put(
          seriesReaderByTimestamp, Collections.singletonList(pathToAggrIndexesMap.get(path)));
    }
    // assign null to be friendly for GC
    pathToAggrIndexesMap = null;
    for (AlignedPath vectorPath : alignedPathToAggrIndexesMap.keySet()) {
      IReaderByTimestamp seriesReaderByTimestamp =
          getReaderByTime(vectorPath, queryPlan, vectorPath.getSeriesType(), context);
      readerToAggrIndexesMap.put(
          seriesReaderByTimestamp, alignedPathToAggrIndexesMap.get(vectorPath));
    }
    // assign null to be friendly for GC
    alignedPathToAggrIndexesMap = null;

    for (int i = 0; i < selectedSeries.size(); i++) {
      aggregateResultList[i] =
          AggregateResultFactory.getAggrResultByName(
              aggregations.get(i), dataTypes.get(i), ascending);
    }
    aggregateWithValueFilter(timestampGenerator, readerToAggrIndexesMap);
    return constructDataSet(Arrays.asList(aggregateResultList), queryPlan);
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

  /** calculate aggregation result with value filter. */
  private void aggregateWithValueFilter(
      TimeGenerator timestampGenerator,
      Map<IReaderByTimestamp, List<List<Integer>>> readerToAggrIndexesMap)
      throws IOException {
    List<Boolean> cached =
        markFilterdPaths(
            expression, new ArrayList<>(selectedSeries), timestampGenerator.hasOrNode());

    while (timestampGenerator.hasNext()) {

      // generate timestamps for aggregate
      long[] timeArray = new long[aggregateFetchSize];
      int timeArrayLength = 0;
      for (int cnt = 0; cnt < aggregateFetchSize; cnt++) {
        if (!timestampGenerator.hasNext()) {
          break;
        }
        timeArray[timeArrayLength++] = timestampGenerator.next();
      }

      // cal part of aggregate result
      for (Entry<IReaderByTimestamp, List<List<Integer>>> entry :
          readerToAggrIndexesMap.entrySet()) {
        // use cache data as much as possible
        boolean[] cachedOrNot = new boolean[entry.getValue().size()];
        for (int i = 0; i < entry.getValue().size(); i++) {
          List<Integer> subIndexes = entry.getValue().get(i);
          int pathId = subIndexes.get(0);
          // if cached in timeGenerator
          if (cached.get(pathId)) {
            Object[] values = timestampGenerator.getValues(selectedSeries.get(pathId));
            ValueIterator valueIterator = QueryUtils.generateValueIterator(values);
            if (valueIterator != null) {
              for (Integer index : subIndexes) {
                aggregateResultList[index].updateResultUsingValues(
                    timeArray, timeArrayLength, valueIterator);
                valueIterator.reset();
              }
              cachedOrNot[i] = true;
            }
          }
        }

        if (hasRemaining(cachedOrNot)) {
          // TODO: if we only need to get firstValue/minTime that's not need to traverse all values,
          //  it's enough to get the exact number of values for these specific aggregate func
          Object[] values = entry.getKey().getValuesInTimestamps(timeArray, timeArrayLength);
          ValueIterator valueIterator = QueryUtils.generateValueIterator(values);
          if (valueIterator != null) {
            for (int i = 0; i < entry.getValue().size(); i++) {
              if (!cachedOrNot[i]) {
                valueIterator.setSubMeasurementIndex(i);
                for (Integer index : entry.getValue().get(i)) {
                  aggregateResultList[index].updateResultUsingValues(
                      timeArray, timeArrayLength, valueIterator);
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

  /**
   * using aggregate result data list construct QueryDataSet.
   *
   * @param aggregateResultList aggregate result list
   */
  private QueryDataSet constructDataSet(
      List<AggregateResult> aggregateResultList, AggregationPlan plan) {
    ListDataSet dataSet;
    RowRecord record = new RowRecord(0);

    if (plan.isGroupByLevel()) {
      Map<String, AggregateResult> groupPathsResultMap =
          plan.groupAggResultByLevel(aggregateResultList);

      List<PartialPath> paths = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      for (AggregateResult resultData : groupPathsResultMap.values()) {
        dataTypes.add(resultData.getResultDataType());
        record.addField(resultData.getResult(), resultData.getResultDataType());
      }
      dataSet = new ListDataSet(paths, dataTypes);
      dataSet.putRecord(record);
    } else {
      dataSet = new ListDataSet(selectedSeries, dataTypes);
      for (AggregateResult resultData : aggregateResultList) {
        TSDataType dataType = resultData.getResultDataType();
        if (resultData instanceof ArAggrResult){
          double[] coefficients =  ((ArAggrResult) resultData).getCoefficients();
          for (int i = 0; i<coefficients.length; i++) {
            record = new RowRecord(i);
            record.addField(coefficients[i], dataType);
            dataSet.putRecord(record);
          }
        }else record.addField(resultData.getResult(), dataType);
      }
    }

    return dataSet;
  }

  private void aggregateAr(
      PartialPath seriesPath,
      Set<String> measurements,
      QueryContext context,
      Filter timeFilter,
      TSDataType tsDataType,
      ArAggrResult arAggrResult,
      TsFileFilter fileFilter)
      throws QueryProcessException, StorageEngineException, IOException {
    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance()
            .getQueryDataSource(seriesPath, context, timeFilter, ascending);
    if (fileFilter != null) {
      QueryUtils.filterQueryDataSource(queryDataSource, fileFilter);
    }
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);
    long startTimeBound = Long.MIN_VALUE, endTimeBound = Long.MAX_VALUE;
    long timeInterval = 0;
    // TODO: manually update time filter
    if (timeFilter != null) {
      String strTimeFilter = timeFilter.toString().replace(" ", "");
      strTimeFilter = strTimeFilter.replace("(", "").replace(")", "").replaceAll("time", "");
      String[] strTimes = strTimeFilter.split("&&");
      for (String strTime : strTimes) {
        if (strTime.contains(">=")) startTimeBound = Long.parseLong(strTime.replaceAll(">=", ""));
        else if (strTime.contains(">"))
          startTimeBound = Long.parseLong(strTime.replaceAll(">", "")) + 1;
        if (strTime.contains("<=")) endTimeBound = Long.parseLong(strTime.replaceAll("<=", ""));
        else if (strTime.contains("<"))
          endTimeBound = Long.parseLong(strTime.replaceAll("<", "")) - 1;
      }
    }

    IAggregateReader seriesReader =
        new SeriesAggregateReader(
            seriesPath, measurements, tsDataType, context, queryDataSource, null, null, null, true);

    int curIndex = 0;
    int cnt = 0;
    int p = arAggrResult.getParameterP();
    List<Statistics> statisticsList = new ArrayList<>();

    List<Integer> unseqIndexList = new ArrayList<>();
    List<Long> startTimes = new ArrayList<>();
    List<Long> endTimes = new ArrayList<>();
    List<Boolean> ifUnseq = new ArrayList<>();
    // TODO: determine seq or unseq pages
    if (arAggrResult.getStatisticLevel().equals("page")) {
      while (seriesReader.hasNextFile()) {
        while (seriesReader.hasNextChunk()) {
          while (seriesReader.hasNextPage()) {
            cnt += 1;
            if (seriesReader.canUseCurrentPageStatistics()) {
              Statistics chunkStatistic = seriesReader.currentPageStatistics();
              timeInterval = chunkStatistic.getTimeInterval();
              statisticsList.add(chunkStatistic);
              if (chunkStatistic.getStartTime() < startTimeBound
                  && chunkStatistic.getEndTime() >= endTimeBound) ifUnseq.add(true);
              else if (chunkStatistic.getStartTime() <= endTimeBound
                  && chunkStatistic.getEndTime() > endTimeBound) ifUnseq.add(true);
              else ifUnseq.add(false);
              startTimes.add(chunkStatistic.getStartTime());
              endTimes.add(chunkStatistic.getEndTime());
              seriesReader.skipCurrentPage();
            } else {
              System.out.println("unSeq");
              Statistics chunkStatistic = seriesReader.currentPageStatistics();
              timeInterval = chunkStatistic.getTimeInterval();
              unseqIndexList.add(curIndex);
              statisticsList.add(chunkStatistic);
              ifUnseq.add(true);
              startTimes.add(chunkStatistic.getStartTime());
              endTimes.add(chunkStatistic.getEndTime());
              seriesReader.skipCurrentPage();
            }
            curIndex++;
          }
        }
      }
    }
    // TODO: determine seq or unseq chunks
    if (arAggrResult.getStatisticLevel().equals("chunk")) {
      while (seriesReader.hasNextFile()) {
        while (seriesReader.hasNextChunk()) {
          cnt += 1;

          if (seriesReader.canUseCurrentChunkStatistics()) {
            Statistics chunkStatistic = seriesReader.currentChunkStatistics();
            timeInterval = chunkStatistic.getTimeInterval();
            statisticsList.add(chunkStatistic);
            if (chunkStatistic.getStartTime() < startTimeBound
                && chunkStatistic.getEndTime() >= endTimeBound) ifUnseq.add(true);
            else if (chunkStatistic.getStartTime() <= endTimeBound
                && chunkStatistic.getEndTime() > endTimeBound) ifUnseq.add(true);
            else ifUnseq.add(false);
            startTimes.add(chunkStatistic.getStartTime());
            endTimes.add(chunkStatistic.getEndTime());
            seriesReader.skipCurrentChunk();
          } else {
            System.out.println("unSeq");
            Statistics chunkStatistic = seriesReader.currentChunkStatistics();
            timeInterval = chunkStatistic.getTimeInterval();
            unseqIndexList.add(curIndex);
            statisticsList.add(chunkStatistic);
            ifUnseq.add(true);
            startTimes.add(chunkStatistic.getStartTime());
            endTimes.add(chunkStatistic.getEndTime());
            seriesReader.skipCurrentChunk();
          }
          curIndex++;
        }
      }
    }
    System.out.println(arAggrResult.getStatisticLevel() + " num:" + cnt);
    System.out.println("true unseq num: " + unseqIndexList.size());
    System.out.println("time interval: " + timeInterval);

    SeriesReader tmpSeriesReader =
        new SeriesReader(
            seriesPath, measurements, tsDataType, context, queryDataSource, null, null, null, true);
    curIndex = 0;

    List<Integer> invalidChunkList = new ArrayList<>();
    // TODO: split overlapped pages
    if (arAggrResult.getStatisticLevel().equals("page")) {
      while (seriesReader.hasNextFile()) {
        while (seriesReader.hasNextChunk()) {
          while (seriesReader.hasNextPage()) {
            if (statisticsList.get(curIndex).getEndTime() < startTimeBound
                || statisticsList.get(curIndex).getStartTime() > endTimeBound) {
              invalidChunkList.add(curIndex);
              curIndex++;
              seriesReader.skipCurrentPage();
              continue;
            }
            if (!ifUnseq.get(curIndex)) {
              curIndex++;
              seriesReader.skipCurrentPage();
            } else {
              System.out.println("Split one unseq.");
              if (curIndex + 1 < startTimes.size()
                  && startTimes.get(curIndex) >= startTimes.get(curIndex + 1)) {
                invalidChunkList.add(curIndex);
                curIndex++;
                continue;
              }
              arAggrResult.setStatisticsInstance(statisticsList.get(curIndex));

              IBatchDataIterator batchDataIterator = seriesReader.nextPage().getBatchDataIterator();
              arAggrResult.updateTimeAndValueWindowFromPageData(batchDataIterator);
              List<Long> timeWindow = arAggrResult.getStatisticsInstance().getTimeWindow();
              List<Double> valueWindow = arAggrResult.getStatisticsInstance().getValueWindow();
              double[] oldCovariances =
                  arAggrResult.getStatisticsInstance().getCovariances().clone();
              double[] newCovariances = new double[p + 1];
              for (int i = 0; i <= p; i++) newCovariances[i] = oldCovariances[i];
              List<Double> newFirstPoints = new ArrayList<>();
              List<Double> newLastPoints = new ArrayList<>();

              int length = timeWindow.size();
              int newCount = 0;
              long newStartTimeBound = Math.max(timeWindow.get(0), startTimeBound);
              long newEndTimeBound = Math.min(timeWindow.get(timeWindow.size() - 1), endTimeBound);
              if (curIndex + 1 < startTimes.size())
                newEndTimeBound = Math.min(newEndTimeBound, startTimes.get(curIndex + 1) - 1);

              // TODO: filter
              for (int i = 0; i < length; i++) {
                if (timeWindow.get(i) < newStartTimeBound) {
                  for (int j = 0; j < p + 1; j++)
                    if (i + j < length)
                      newCovariances[j] -= valueWindow.get(i) * valueWindow.get(i + j);
                } else if (timeWindow.get(i) > newEndTimeBound) {
                  for (int j = 0; j < p + 1; j++)
                    if (i - j >= 0)
                      newCovariances[j] -= valueWindow.get(i) * valueWindow.get(i - j);
                } else {
                  if (newFirstPoints.size() < p + 1) newFirstPoints.add(valueWindow.get(i));
                  if (newLastPoints.size() == p + 1) newLastPoints.remove(0);
                  newLastPoints.add(valueWindow.get(i));
                  newCount++;
                }
              }
              Statistics statisticsInstance = new DoubleStatistics();
              statisticsInstance.setEndTime(newEndTimeBound);
              statisticsInstance.setCount(newCount);
              statisticsInstance.setCovariances(newCovariances);
              statisticsInstance.setLastPoints(newLastPoints);
              statisticsList.set(curIndex, statisticsInstance);
              arAggrResult.getStatisticsInstance().clearTimeAndValueWindow();
              arAggrResult.reset();
              curIndex++;
            }
          }
        }
      }
    }
    // TODO: split overlapped chunks
    if (arAggrResult.getStatisticLevel().equals("chunk")) {
      List<ChunkMetadata> chunkMetadataList = tmpSeriesReader.getAllChunkMetadatas();

      for (ChunkMetadata chunkMetaData : chunkMetadataList) {
        if (!ifUnseq.get(curIndex)) {
          curIndex++;
          continue;
        } else {
          System.out.println("Split one unseq Chunk.");
          if (curIndex + 1 < startTimes.size()
              && startTimes.get(curIndex) >= startTimes.get(curIndex + 1)) {
            invalidChunkList.add(curIndex);
            curIndex++;
            continue;
          }
          if (statisticsList.get(curIndex).getEndTime() < startTimeBound
              || statisticsList.get(curIndex).getStartTime() > endTimeBound) {
            invalidChunkList.add(curIndex);
            curIndex++;
            continue;
          }
          arAggrResult.setStatisticsInstance(statisticsList.get(curIndex));

          IChunkReader chunkReader;
          IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
          if (chunkLoader instanceof MemChunkLoader) {
            MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
            chunkReader = memChunkLoader.getChunkReader(chunkMetaData, null);
          } else {
            Chunk chunk =
                chunkLoader.loadChunk(chunkMetaData); // loads chunk data from disk to memory
            chunk.setFromOldFile(chunkMetaData.isFromOldTsFile());
            chunkReader =
                new ChunkReader(chunk, null); // decompress page data, split time&value buffers
          }
          BatchData batchData = chunkReader.nextPageData();
//            arAggrResult.updateTimeAndValueWindowFromBatchData(batchData);


          List<Long> timeWindow = new ArrayList<>();
          List<Double> valueWindow = new ArrayList<>();

          double[] oldCovariances = arAggrResult.getStatisticsInstance().getCovariances().clone();
          double[] newCovariances = new double[p + 1];
          for (int i = 0; i <= p; i++) newCovariances[i] = oldCovariances[i];
          List<Double> newLastPoints = new ArrayList<>();

          for (int i = (int) ((startTimes.get(curIndex+1) - startTimes.get(curIndex))/timeInterval - p);
               i <= (endTimes.get(curIndex) - startTimes.get(curIndex))/timeInterval; i++){
            timeWindow.add(batchData.getTimeByIndex(i));
            valueWindow.add(batchData.getDoubleByIndex(i));
          }

          int length = timeWindow.size();
          int newCount = 0;
          long newEndTime = startTimes.get(curIndex + 1) - timeInterval;
          boolean flagStart = true, flagEnd = true;
          for (int i = 0; i < length; i++) {
              for (int j = 0; j < p + 1; j++)
                if (i + j < valueWindow.size()) newCovariances[j] -= valueWindow.get(i) * valueWindow.get(i + j);
              if (newLastPoints.size() == p + 1) newLastPoints.remove(0);
              newLastPoints.add(valueWindow.get(i));
              newCount++;
          }
          Statistics statisticsInstance = new DoubleStatistics();
          statisticsInstance.setStartTime(startTimes.get(curIndex));
          statisticsInstance.setEndTime(newEndTime);
          statisticsInstance.setCount(statisticsList.get(curIndex).getCount() - newCount + p);
          statisticsInstance.setCovariances(newCovariances);
          for (int k = 0; k < statisticsInstance.getFirstPoints().length; k++)
            statisticsInstance.setFirstPoints(statisticsInstance.getFirstPoints()[k], k);
          statisticsInstance.setLastPoints(newLastPoints);
          statisticsList.set(curIndex, statisticsInstance);
          arAggrResult.getStatisticsInstance().clearTimeAndValueWindow();
          arAggrResult.reset();
          curIndex++;
        }
      }
    }

    // TODO: remove statistics filtered by time filter
    for (int j = invalidChunkList.size() - 1; j >= 0; j--) {
      statisticsList.remove(invalidChunkList.get(j).intValue());
    }

    // TODO: merge adjacent statistics
    double[] resultCovariances = new double[p + 1];
    int finalCount = statisticsList.get(0).getCount();
    for (int j = 0; j < p + 1; j++)
      resultCovariances[j] = statisticsList.get(0).getCovariances()[j];

    for (int i = 1; i < statisticsList.size(); i++) {
      if (Math.abs(
              statisticsList.get(i).getStartTime()
                  - statisticsList.get(i - 1).getEndTime()
                  - timeInterval)
          <= 1e-4) { // adjacent cases
        finalCount += statisticsList.get(i).getCount();
        for (int j = 0; j < p + 1; j++)
          resultCovariances[j] += statisticsList.get(i).getCovariances()[j];

        double[] firstPoints = statisticsList.get(i).getFirstPoints();
        double[] lastPoints = statisticsList.get(i - 1).getLastPoints();
        for (int j = 0; j < p + 1; j++)
          for (int k = 0; k < lastPoints.length; k++)
            if (k + j - lastPoints.length >= 0 && k + j - lastPoints.length < firstPoints.length)
              resultCovariances[j] += lastPoints[k] * firstPoints[k + j - lastPoints.length];
      } else { // disjoint cases
        int u =
            (int)
                ((statisticsList.get(i).getStartTime() - statisticsList.get(i - 1).getEndTime())
                    / timeInterval);
        finalCount += statisticsList.get(i).getCount() + u - 1;
        for (int j = 0; j < p + 1; j++) {
          if (u - j - 1 > 0) { // proposition 4.3
            resultCovariances[j] += statisticsList.get(i).getCovariances()[j];
            double[] lastPoints = statisticsList.get(i - 1).getLastPoints();
            double[] firstPoints = statisticsList.get(i).getFirstPoints();
            double deltaToU = (firstPoints[0] - lastPoints[lastPoints.length - 1]) / u;
            resultCovariances[j] +=
                (u - j - 1)
                    * (lastPoints[lastPoints.length - 1] * firstPoints[0]
                        + (u - j) * j * deltaToU * deltaToU / 2
                        + (u - j) * (2 * u - 2 * j - 1) * deltaToU * deltaToU / 6);
            for (int l = 0; l <= Math.min(lastPoints.length - 1, j - 1); l++)
              resultCovariances[j] +=
                  lastPoints[lastPoints.length - 1 - l]
                      * (lastPoints[lastPoints.length - 1] + (j - l) * deltaToU);
            for (int l = 0; l <= Math.min(firstPoints.length - 1, j - 1); l++)
              resultCovariances[j] +=
                  firstPoints[l] * (lastPoints[lastPoints.length - 1] + (u - j + l) * deltaToU);
          } else { // proposition 4.4
            resultCovariances[j] += statisticsList.get(i).getCovariances()[j];
            double[] lastPoints = statisticsList.get(i - 1).getLastPoints();
            double[] firstPoints = statisticsList.get(i).getFirstPoints();
            double deltaToU = (firstPoints[0] - lastPoints[lastPoints.length - 1]) / u;
            for (int l = Math.max(1, j + 1 - lastPoints.length); l <= u - 1; l++)
              resultCovariances[j] +=
                  (lastPoints[lastPoints.length - 1] + l * deltaToU)
                      * lastPoints[lastPoints.length - 1 - j + l];
            for (int l = 1; l <= Math.min(j, firstPoints.length); l++) {
              if (l <= j - u + 1)
                resultCovariances[j] +=
                    firstPoints[l - 1] * lastPoints[lastPoints.length - 1 + l + u - j - 1];
              else
                resultCovariances[j] +=
                    firstPoints[l - 1]
                        * (lastPoints[lastPoints.length - 1] + (l + u - j - 1) * deltaToU);
            }
          }
        }
      }
    }
    double[] autocorr = new double[p + 1];
    for (int j = 0; j <= p; j++) {
      autocorr[j] = resultCovariances[j] /= (finalCount - j);
    }

    // TODO: Yule-Walker Solver
    double[] epsilons = new double[p + 1];
    double[] kappas = new double[p + 1];
    double[][] alphas = new double[p + 1][p + 1];
    // alphas[i][j] denotes alpha_i^{(j)}
    epsilons[0] = resultCovariances[0];
    for (int i = 1; i <= p; i++) {
      double tmpSum = 0.0;
      for (int j = 1; j <= i - 1; j++) tmpSum += alphas[j][i - 1] * resultCovariances[i - j];
      kappas[i] = (resultCovariances[i] - tmpSum) / epsilons[i - 1];
      alphas[i][i] = kappas[i];
      if (i > 1) {
        for (int j = 1; j <= i - 1; j++)
          alphas[j][i] = alphas[j][i - 1] - kappas[i] * alphas[i - j][i - 1];
      }
      epsilons[i] = (1 - kappas[i] * kappas[i]) * epsilons[i - 1];
    }
    double[] coefficients = new double[p];
    System.out.print("Coefficients: ");
    for (int i = 1; i <= p; i++) {
      coefficients[i - 1] = alphas[i][p];
      System.out.print(String.format("%.4f", coefficients[i - 1]) + ", ");
    }
    System.out.println("\n");
    arAggrResult.setCoefficients(coefficients);
    statisticsList.clear();
  }
}
