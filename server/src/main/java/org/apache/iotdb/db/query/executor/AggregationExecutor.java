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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.pool.QueryTaskPoolManager;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregationExecutor {

  private List<Path> selectedSeries;
  private List<TSDataType> dataTypes;
  private List<String> aggregations;
  private IExpression expression;

  private static final QueryTaskPoolManager pool = QueryTaskPoolManager.getInstance();

  /**
   * aggregation batch calculation size.
   **/
  private int aggregateFetchSize;

  public AggregationExecutor(AggregationPlan aggregationPlan) {
    this.selectedSeries = aggregationPlan.getDeduplicatedPaths();
    this.dataTypes = aggregationPlan.getDeduplicatedDataTypes();
    this.aggregations = aggregationPlan.getDeduplicatedAggregations();
    this.expression = aggregationPlan.getExpression();
    this.aggregateFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
  }

  /**
   * Aggregate one series
   */
  class AggregationTask implements Callable<Pair<Path, List<AggregateResult>>> {

    // path to aggregation result indexes
    private Map.Entry<Path, List<Integer>> pathToAggrIndexes;
    private IAggregateReader reader;

    public AggregationTask(IAggregateReader reader, Map.Entry<Path, List<Integer>> pathToAggrIndexes) {
      this.reader = reader;
      this.pathToAggrIndexes = pathToAggrIndexes;
    }

    @Override
    public Pair<Path, List<AggregateResult>> call() throws QueryProcessException, IOException {
      return aggregateOneSeries();
    }

    /**
     * get aggregation result for one series
     *
     * @return AggregateResult list
     */
    private Pair<Path, List<AggregateResult>> aggregateOneSeries()
        throws IOException, QueryProcessException {
      List<AggregateResult> aggregateResultList = new ArrayList<>();
      List<Boolean> isCalculatedList = new ArrayList<>();
      Path seriesPath = pathToAggrIndexes.getKey();

      for (int i : pathToAggrIndexes.getValue()) {
        // construct AggregateResult
        AggregateResult aggregateResult = AggregateResultFactory
            .getAggrResultByName(aggregations.get(i), reader.getSeriesDataType());
        aggregateResultList.add(aggregateResult);
        isCalculatedList.add(false);
      }
      int remainingToCalculate = pathToAggrIndexes.getValue().size();

      while (reader.hasNextChunk()) {
        // cal by chunk statistics
        if (reader.canUseCurrentChunkStatistics()) {
          Statistics chunkStatistics = reader.currentChunkStatistics();
          for (int i = 0; i < aggregateResultList.size(); i++) {
            if (Boolean.FALSE.equals(isCalculatedList.get(i))) {
              AggregateResult aggregateResult = aggregateResultList.get(i);
              aggregateResult.updateResultFromStatistics(chunkStatistics);
              if (aggregateResult.isCalculatedAggregationResult()) {
                isCalculatedList.set(i, true);
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  return new Pair<>(seriesPath, aggregateResultList);
                }
              }
            }
          }
          reader.skipCurrentChunk();
          continue;
        }
        while (reader.hasNextPage()) {
          //cal by page statistics
          if (reader.canUseCurrentPageStatistics()) {
            Statistics pageStatistic = reader.currentPageStatistics();
            for (int i = 0; i < aggregateResultList.size(); i++) {
              if (Boolean.FALSE.equals(isCalculatedList.get(i))) {
                AggregateResult aggregateResult = aggregateResultList.get(i);
                aggregateResult.updateResultFromStatistics(pageStatistic);
                if (aggregateResult.isCalculatedAggregationResult()) {
                  isCalculatedList.set(i, true);
                  remainingToCalculate--;
                  if (remainingToCalculate == 0) {
                    return new Pair<>(seriesPath, aggregateResultList);
                  }
                }
              }
            }
            reader.skipCurrentPage();
            continue;
          }
          // cal by page data
          while (reader.hasNextOverlappedPage()) {
            BatchData nextOverlappedPageData = reader.nextOverlappedPage();
            for (int i = 0; i < aggregateResultList.size(); i++) {
              if (Boolean.FALSE.equals(isCalculatedList.get(i))) {
                AggregateResult aggregateResult = aggregateResultList.get(i);
                aggregateResult.updateResultFromPageData(nextOverlappedPageData);
                nextOverlappedPageData.resetBatchData();
                if (aggregateResult.isCalculatedAggregationResult()) {
                  isCalculatedList.set(i, true);
                  remainingToCalculate--;
                  if (remainingToCalculate == 0) {
                    return new Pair<>(seriesPath, aggregateResultList);
                  }
                }
              }
            }
          }
        }
      }
      return new Pair<>(seriesPath, aggregateResultList);
    }
  }

  /**
   * execute aggregate function with only time filter or no filter.
   *
   * @param context query context
   */
  public QueryDataSet executeWithoutValueFilter(QueryContext context)
      throws StorageEngineException, QueryProcessException {

    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }

    AggregateResult[] finalAggregateResults = new AggregateResult[selectedSeries.size()];
    Map<Path, List<Integer>> pathToAggrIndexesMap = groupAggregationsBySeries(selectedSeries);

    /*
     * submit AggregationTask for each series
     */
    List<Future<Pair<Path, List<AggregateResult>>>> futureList = new ArrayList<>();
    for (Map.Entry<Path, List<Integer>> pathToAggrIndex : pathToAggrIndexesMap.entrySet()) {

      Path seriesPath = pathToAggrIndex.getKey();
      // construct series reader without value filter
      QueryDataSource queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSource(seriesPath, context, timeFilter);
      // update filter by TTL
      timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

      TSDataType tsDataType = dataTypes.get(pathToAggrIndex.getValue().get(0));

      IAggregateReader seriesReader = new SeriesAggregateReader(pathToAggrIndex.getKey(),
          tsDataType, context, queryDataSource, timeFilter, null, null);

      Future<Pair<Path, List<AggregateResult>>> future = pool
          .submit(new AggregationTask(seriesReader, pathToAggrIndex));
      futureList.add(future);
    }

    /*
     * get AggregateResults for each series and put to final finalAggregateResults
     */
    for (Future<Pair<Path, List<AggregateResult>>> future: futureList) {
      try {
        Pair<Path, List<AggregateResult>> currentSeriesResults = future.get();
        // final result index of current series
        List<Integer> resultIndexList = pathToAggrIndexesMap.get(currentSeriesResults.left);
        List<AggregateResult> resultList = currentSeriesResults.right;

        // put current series results to final finalAggregateResults
        for (int i = 0; i < resultIndexList.size(); i++) {
          finalAggregateResults[resultIndexList.get(i)] = resultList.get(i);
        }
      } catch (Exception e) {
        throw new QueryProcessException(e.getMessage());
      }
    }
    return constructDataSet(Arrays.asList(finalAggregateResults));
  }


  /**
   * execute aggregate function with value filter.
   *
   * @param context query context.
   */
  public QueryDataSet executeWithValueFilter(QueryContext context)
      throws StorageEngineException, PathException, IOException {

    ServerTimeGenerator timestampGenerator = new ServerTimeGenerator(expression, context);
    List<IReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      Path path = selectedSeries.get(i);
      SeriesReaderByTimestamp seriesReaderByTimestamp = new SeriesReaderByTimestamp(path,
          dataTypes.get(i), context,
          QueryResourceManager.getInstance().getQueryDataSource(path, context, null), null);
      readersOfSelectedSeries.add(seriesReaderByTimestamp);
    }

    List<AggregateResult> aggregateResults = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      TSDataType type = dataTypes.get(i);
      AggregateResult result = AggregateResultFactory.getAggrResultByName(aggregations.get(i), type);
      aggregateResults.add(result);
    }
    aggregateWithValueFilter(aggregateResults, timestampGenerator, readersOfSelectedSeries);
    return constructDataSet(aggregateResults);
  }

  /**
   * calculate aggregation result with value filter.
   */
  private void aggregateWithValueFilter(List<AggregateResult> aggregateResults,
      ServerTimeGenerator timestampGenerator, List<IReaderByTimestamp> readersOfSelectedSeries)
      throws IOException {

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
      for (int i = 0; i < readersOfSelectedSeries.size(); i++) {
        aggregateResults.get(i).updateResultUsingTimestamps(timeArray, timeArrayLength,
            readersOfSelectedSeries.get(i));
      }
    }
  }

  /**
   * using aggregate result data list construct QueryDataSet.
   *
   * @param aggregateResultList aggregate result list
   */
  private QueryDataSet constructDataSet(List<AggregateResult> aggregateResultList) {
    RowRecord record = new RowRecord(0);
    for (AggregateResult resultData : aggregateResultList) {
      TSDataType dataType = resultData.getResultDataType();
      record.addField(resultData.getResult(), dataType);
    }

    SingleDataSet dataSet = new SingleDataSet(selectedSeries, dataTypes);
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
  private Map<Path, List<Integer>> groupAggregationsBySeries(List<Path> selectedSeries) {
    Map<Path, List<Integer>> pathToAggrIndexesMap = new HashMap<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      Path series = selectedSeries.get(i);
      List<Integer> indexList = pathToAggrIndexesMap
          .computeIfAbsent(series, key -> new ArrayList<>());
      indexList.add(i);
    }
    return pathToAggrIndexesMap;
  }
}
