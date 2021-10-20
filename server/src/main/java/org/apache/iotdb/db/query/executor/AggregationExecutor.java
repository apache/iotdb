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
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.VectorSeriesAggregateReader;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.db.utils.QueryUtils;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

@SuppressWarnings("java:S1135") // ignore todos
public class AggregationExecutor {

  private List<PartialPath> selectedSeries;
  protected List<TSDataType> dataTypes;
  protected List<String> aggregations;
  protected IExpression expression;
  protected boolean ascending;
  protected QueryContext context;
  protected AggregateResult[] aggregateResultList;

  /** aggregation batch calculation size. */
  private int aggregateFetchSize;

  protected AggregationExecutor(QueryContext context, AggregationPlan aggregationPlan) {
    this.selectedSeries = aggregationPlan.getDeduplicatedPaths();
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
        groupAggregationsBySeries(selectedSeries);
    // TODO-Cluster: group the paths by storage group to reduce communications
    List<StorageGroupProcessor> list =
        StorageEngine.getInstance().mergeLock(new ArrayList<>(pathToAggrIndexesMap.keySet()));

    // Attention: this method will REMOVE vector path from pathToAggrIndexesMap
    Map<PartialPath, List<List<Integer>>> vectorPathIndexesMap =
        groupVectorSeries(pathToAggrIndexesMap);
    try {
      for (Map.Entry<PartialPath, List<Integer>> entry : pathToAggrIndexesMap.entrySet()) {
        PartialPath seriesPath = entry.getKey();
        aggregateOneSeries(
            seriesPath,
            entry.getValue(),
            aggregationPlan.getAllMeasurementsInDevice(seriesPath.getDevice()),
            timeFilter);
      }
      for (Map.Entry<PartialPath, List<List<Integer>>> entry : vectorPathIndexesMap.entrySet()) {
        VectorPartialPath vectorSeries = (VectorPartialPath) entry.getKey();
        aggregateOneVectorSeries(
            vectorSeries,
            entry.getValue(),
            aggregationPlan.getAllMeasurementsInDevice(vectorSeries.getDevice()),
            timeFilter);
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
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
    boolean[] isAsc = new boolean[aggregateResultList.length];

    TSDataType tsDataType = dataTypes.get(indexes.get(0));
    for (int i : indexes) {
      // construct AggregateResult
      AggregateResult aggregateResult =
          AggregateResultFactory.getAggrResultByName(aggregations.get(i), tsDataType);
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
        null);

    int ascIndex = 0;
    int descIndex = 0;
    for (int i : indexes) {
      aggregateResultList[i] =
          isAsc[i]
              ? ascAggregateResultList.get(ascIndex++)
              : descAggregateResultList.get(descIndex++);
    }
  }

  protected void aggregateOneVectorSeries(
      VectorPartialPath seriesPath,
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

    aggregateOneVectorSeries(
        seriesPath,
        allMeasurementsInDevice,
        context,
        timeFilter,
        TSDataType.VECTOR,
        ascAggregateResultList,
        descAggregateResultList,
        null);

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
      TsFileFilter fileFilter)
      throws StorageEngineException, IOException, QueryProcessException {

    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, timeFilter);
    if (fileFilter != null) {
      QueryUtils.filterQueryDataSource(queryDataSource, fileFilter);
    }
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    if (ascAggregateResultList != null && !ascAggregateResultList.isEmpty()) {
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

  public static void aggregateOneVectorSeries(
      VectorPartialPath seriesPath,
      Set<String> measurements,
      QueryContext context,
      Filter timeFilter,
      TSDataType tsDataType,
      List<List<AggregateResult>> ascAggregateResultList,
      List<List<AggregateResult>> descAggregateResultList,
      TsFileFilter fileFilter)
      throws StorageEngineException, IOException, QueryProcessException {

    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, timeFilter);
    if (fileFilter != null) {
      QueryUtils.filterQueryDataSource(queryDataSource, fileFilter);
    }
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    if (!isAggregateResultEmpty(ascAggregateResultList)) {
      VectorSeriesAggregateReader seriesReader =
          new VectorSeriesAggregateReader(
              seriesPath,
              measurements,
              tsDataType,
              context,
              queryDataSource,
              timeFilter,
              null,
              null,
              true);
      aggregateFromVectorReader(seriesReader, ascAggregateResultList);
    }
    if (!isAggregateResultEmpty(descAggregateResultList)) {
      VectorSeriesAggregateReader seriesReader =
          new VectorSeriesAggregateReader(
              seriesPath,
              measurements,
              tsDataType,
              context,
              queryDataSource,
              timeFilter,
              null,
              null,
              false);
      aggregateFromVectorReader(seriesReader, descAggregateResultList);
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

  private static void aggregateFromVectorReader(
      VectorSeriesAggregateReader seriesReader, List<List<AggregateResult>> aggregateResultList)
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
            aggregateVectorPages(
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

  private static int aggregateVectorPages(
      VectorSeriesAggregateReader seriesReader,
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
    // group by path name
    Map<PartialPath, List<Integer>> pathToAggrIndexesMap =
        groupAggregationsBySeries(selectedSeries);
    Map<IReaderByTimestamp, List<Integer>> readerToAggrIndexesMap = new HashMap<>();
    List<StorageGroupProcessor> list = StorageEngine.getInstance().mergeLock(selectedSeries);
    try {
      for (int i = 0; i < selectedSeries.size(); i++) {
        PartialPath path = selectedSeries.get(i);
        List<Integer> indexes = pathToAggrIndexesMap.remove(path);
        if (indexes != null) {
          IReaderByTimestamp seriesReaderByTimestamp =
              getReaderByTime(path, queryPlan, dataTypes.get(i), context);
          readerToAggrIndexesMap.put(seriesReaderByTimestamp, indexes);
        }
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }

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
        QueryResourceManager.getInstance().getQueryDataSource(path, context, null),
        null,
        ascending);
  }

  /** calculate aggregation result with value filter. */
  private void aggregateWithValueFilter(
      TimeGenerator timestampGenerator,
      Map<IReaderByTimestamp, List<Integer>> readerToAggrIndexesMap)
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
      for (Entry<IReaderByTimestamp, List<Integer>> entry : readerToAggrIndexesMap.entrySet()) {
        int pathId = entry.getValue().get(0);
        // cache in timeGenerator
        if (cached.get(pathId)) {
          Object[] values = timestampGenerator.getValues(selectedSeries.get(pathId));
          for (Integer i : entry.getValue()) {
            aggregateResultList[i].updateResultUsingValues(timeArray, timeArrayLength, values);
          }
        } else {
          if (entry.getValue().size() == 1) {
            aggregateResultList[entry.getValue().get(0)].updateResultUsingTimestamps(
                timeArray, timeArrayLength, entry.getKey());
          } else {
            Object[] values = entry.getKey().getValuesInTimestamps(timeArray, timeArrayLength);
            for (Integer i : entry.getValue()) {
              aggregateResultList[i].updateResultUsingValues(timeArray, timeArrayLength, values);
            }
          }
        }
      }
    }
  }

  /**
   * using aggregate result data list construct QueryDataSet.
   *
   * @param aggregateResultList aggregate result list
   */
  private QueryDataSet constructDataSet(
      List<AggregateResult> aggregateResultList, AggregationPlan plan)
      throws QueryProcessException {
    SingleDataSet dataSet;
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

  /**
   * Merge same series and convert to series map. For example: Given: paths: s1, s2, s3, s1 and
   * aggregations: count, sum, count, sum. Then: pathToAggrIndexesMap: s1 -> 0, 3; s2 -> 1; s3 -> 2
   *
   * @param selectedSeries selected series
   * @return path to aggregation indexes map
   */
  private Map<PartialPath, List<Integer>> groupAggregationsBySeries(
      List<PartialPath> selectedSeries) {
    Map<PartialPath, List<Integer>> pathToAggrIndexesMap = new HashMap<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      PartialPath series = selectedSeries.get(i);
      pathToAggrIndexesMap.computeIfAbsent(series, key -> new ArrayList<>()).add(i);
    }
    return pathToAggrIndexesMap;
  }

  /**
   * Group all the subSensors of one vector into one VectorPartialPath and Remove vectorPartialPath
   * from pathToAggrIndexesMap. For example, input map: vector1[s1] -> [1, 3], vector1[s2] -> [2,4],
   * will return vector1[s1,s2], [[1,3], [2,4]]
   */
  private Map<PartialPath, List<List<Integer>>> groupVectorSeries(
      Map<PartialPath, List<Integer>> pathToAggrIndexesMap) {
    Map<PartialPath, List<List<Integer>>> result = new HashMap<>();
    Map<String, VectorPartialPath> temp = new HashMap<>();

    List<PartialPath> seriesPaths = new ArrayList<>(pathToAggrIndexesMap.keySet());
    for (PartialPath seriesPath : seriesPaths) {
      if (seriesPath instanceof VectorPartialPath) {
        List<Integer> indexes = pathToAggrIndexesMap.remove(seriesPath);
        VectorPartialPath groupPath = temp.get(seriesPath.getFullPath());
        if (groupPath == null) {
          groupPath = (VectorPartialPath) seriesPath.copy();
          temp.put(seriesPath.getFullPath(), groupPath);
          result.computeIfAbsent(groupPath, key -> new ArrayList<>()).add(indexes);
        } else {
          // groupPath is changed here so we update it
          List<List<Integer>> subIndexes = result.remove(groupPath);
          subIndexes.add(indexes);
          groupPath.addSubSensor(((VectorPartialPath) seriesPath).getSubSensorsList());
          result.put(groupPath, subIndexes);
        }
      }
    }
    return result;
  }
}
