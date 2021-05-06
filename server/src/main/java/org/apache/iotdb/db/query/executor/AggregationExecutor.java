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
import org.apache.iotdb.db.exception.layoutoptimize.LayoutNotExistException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.layoutoptimize.layoutholder.LayoutHolder;
import org.apache.iotdb.db.metadata.PartialPath;
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
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

@SuppressWarnings("java:S1135") // ignore todos
public class AggregationExecutor {

  private List<PartialPath> selectedSeries;
  protected List<TSDataType> dataTypes;
  protected List<String> aggregations;
  protected IExpression expression;
  protected boolean ascending;

  /** aggregation batch calculation size. */
  private int aggregateFetchSize;

  protected AggregationExecutor(AggregationPlan aggregationPlan) {
    this.selectedSeries = aggregationPlan.getDeduplicatedPaths();
    this.dataTypes = aggregationPlan.getDeduplicatedDataTypes();
    this.aggregations = aggregationPlan.getDeduplicatedAggregations();
    this.expression = aggregationPlan.getExpression();
    this.aggregateFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
    this.ascending = aggregationPlan.isAscending();
  }

  /**
   * execute aggregate function with only time filter or no filter.
   *
   * @param context query context
   */
  public QueryDataSet executeWithoutValueFilter(
      QueryContext context, AggregationPlan aggregationPlan)
      throws StorageEngineException, IOException, QueryProcessException {

    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }

    // TODO use multi-thread
    Map<PartialPath, List<Integer>> pathToAggrIndexesMap =
        groupAggregationsBySeries(selectedSeries);
    AggregateResult[] aggregateResultList = new AggregateResult[selectedSeries.size()];
    // TODO-Cluster: group the paths by storage group to reduce communications
    List<StorageGroupProcessor> list =
        StorageEngine.getInstance().mergeLock(new ArrayList<>(pathToAggrIndexesMap.keySet()));
    try {
      // adjust the query order according to the physical order
      Map<String, Set<PartialPath>> queryForEachDevice = new HashMap<>();
      Map<PartialPath, Map.Entry<PartialPath, List<Integer>>> entryMap = new HashMap<>();
      for (Map.Entry<PartialPath, List<Integer>> entry : pathToAggrIndexesMap.entrySet()) {
        if (!queryForEachDevice.containsKey(entry.getKey().getDevice())) {
          queryForEachDevice.put(entry.getKey().getDevice(), new HashSet<>());
        }
        queryForEachDevice.get(entry.getKey().getDevice()).add(entry.getKey());
        entryMap.put(entry.getKey(), entry);
      }
      LayoutHolder layoutHolder = LayoutHolder.getInstance();
      for (String deviceID : queryForEachDevice.keySet()) {
        if (!layoutHolder.hasLayoutForDevice(deviceID)) {
          layoutHolder.updateMetadata();
        }
        Set<PartialPath> pathSet = queryForEachDevice.get(deviceID);
        List<String> paths = layoutHolder.getMeasurementForDevice(deviceID);
        for (String pathID : paths) {
          try {
            PartialPath path = new PartialPath(deviceID, pathID);
            if (pathSet.contains(path)) {
              aggregateOneSeries(
                  entryMap.get(path),
                  aggregateResultList,
                  aggregationPlan.getAllMeasurementsInDevice(deviceID),
                  timeFilter,
                  context);
            }
          } catch (IllegalPathException e) {
            continue;
          }
        }
      }
      for (Map.Entry<PartialPath, List<Integer>> entry : pathToAggrIndexesMap.entrySet()) {
        aggregateOneSeries(
            entry,
            aggregateResultList,
            aggregationPlan.getAllMeasurementsInDevice(entry.getKey().getDevice()),
            timeFilter,
            context);
      }
    } catch (LayoutNotExistException e) {
      for (Map.Entry<PartialPath, List<Integer>> entry : pathToAggrIndexesMap.entrySet()) {
        aggregateOneSeries(
            entry,
            aggregateResultList,
            aggregationPlan.getAllMeasurementsInDevice(entry.getKey().getDevice()),
            timeFilter,
            context);
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }

    return constructDataSet(Arrays.asList(aggregateResultList), aggregationPlan);
  }

  /**
   * get aggregation result for one series
   *
   * @param pathToAggrIndexes entry of path to aggregation indexes map
   * @param timeFilter time filter
   * @param context query context
   */
  protected void aggregateOneSeries(
      Map.Entry<PartialPath, List<Integer>> pathToAggrIndexes,
      AggregateResult[] aggregateResultList,
      Set<String> measurements,
      Filter timeFilter,
      QueryContext context)
      throws IOException, QueryProcessException, StorageEngineException {
    List<AggregateResult> ascAggregateResultList = new ArrayList<>();
    List<AggregateResult> descAggregateResultList = new ArrayList<>();
    boolean[] isAsc = new boolean[aggregateResultList.length];

    PartialPath seriesPath = pathToAggrIndexes.getKey();
    TSDataType tsDataType = dataTypes.get(pathToAggrIndexes.getValue().get(0));

    for (int i : pathToAggrIndexes.getValue()) {
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
        measurements,
        context,
        timeFilter,
        tsDataType,
        ascAggregateResultList,
        descAggregateResultList,
        null);

    int ascIndex = 0;
    int descIndex = 0;
    for (int i : pathToAggrIndexes.getValue()) {
      aggregateResultList[i] =
          isAsc[i]
              ? ascAggregateResultList.get(ascIndex++)
              : descAggregateResultList.get(descIndex++);
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
      BatchData nextOverlappedPageData = seriesReader.nextPage();
      for (int i = 0; i < aggregateResultList.size(); i++) {
        if (!isCalculatedArray[i]) {
          AggregateResult aggregateResult = aggregateResultList.get(i);
          aggregateResult.updateResultFromPageData(nextOverlappedPageData);
          nextOverlappedPageData.resetBatchData();
          if (aggregateResult.hasFinalResult()) {
            isCalculatedArray[i] = true;
            remainingToCalculate--;
            if (remainingToCalculate == 0) {
              return 0;
            }
          }
        }
      }
    }
    return remainingToCalculate;
  }

  /**
   * execute aggregate function with value filter.
   *
   * @param context query context.
   */
  public QueryDataSet executeWithValueFilter(QueryContext context, AggregationPlan queryPlan)
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

    List<AggregateResult> aggregateResults = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      AggregateResult result =
          AggregateResultFactory.getAggrResultByName(
              aggregations.get(i), dataTypes.get(i), ascending);
      aggregateResults.add(result);
    }
    aggregateWithValueFilter(aggregateResults, timestampGenerator, readerToAggrIndexesMap);
    return constructDataSet(aggregateResults, queryPlan);
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
      List<AggregateResult> aggregateResults,
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
            aggregateResults.get(i).updateResultUsingValues(timeArray, timeArrayLength, values);
          }
        } else {
          if (entry.getValue().size() == 1) {
            aggregateResults
                .get(entry.getValue().get(0))
                .updateResultUsingTimestamps(timeArray, timeArrayLength, entry.getKey());
          } else {
            Object[] values = entry.getKey().getValuesInTimestamps(timeArray, timeArrayLength);
            for (Integer i : entry.getValue()) {
              aggregateResults.get(i).updateResultUsingValues(timeArray, timeArrayLength, values);
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
    RowRecord record = new RowRecord(0);
    for (AggregateResult resultData : aggregateResultList) {
      TSDataType dataType = resultData.getResultDataType();
      record.addField(resultData.getResult(), dataType);
    }

    SingleDataSet dataSet;
    if (plan.getLevel() >= 0) {
      Map<String, AggregateResult> finalPaths = plan.getAggPathByLevel();

      List<AggregateResult> mergedAggResults =
          FilePathUtils.mergeRecordByPath(plan, aggregateResultList, finalPaths);

      List<PartialPath> paths = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      for (int i = 0; i < mergedAggResults.size(); i++) {
        dataTypes.add(mergedAggResults.get(i).getResultDataType());
      }
      RowRecord curRecord = new RowRecord(0);
      for (AggregateResult resultData : mergedAggResults) {
        TSDataType dataType = resultData.getResultDataType();
        curRecord.addField(resultData.getResult(), dataType);
      }

      dataSet = new SingleDataSet(paths, dataTypes);
      dataSet.setRecord(curRecord);
    } else {
      dataSet = new SingleDataSet(selectedSeries, dataTypes);
      dataSet.setRecord(record);
    }

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
}
