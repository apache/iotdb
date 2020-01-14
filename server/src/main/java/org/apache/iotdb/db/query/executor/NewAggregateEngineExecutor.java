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
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.AggreResultDataPointReader;
import org.apache.iotdb.db.query.dataset.OldEngineDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.factory.AggreResultFactory;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesDataReaderWithoutValueFilter;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class NewAggregateEngineExecutor {

  private List<Path> selectedSeries;
  private List<TSDataType> dataTypes;
  private List<String> aggres;
  private IExpression expression;

  /**
   * aggregation batch calculation size.
   **/
  private int aggregateFetchSize;

  public NewAggregateEngineExecutor(AggregationPlan aggregationPlan) {
    this.selectedSeries = aggregationPlan.getDeduplicatedPaths();
    this.dataTypes = aggregationPlan.getDeduplicatedDataTypes();
    this.aggres = aggregationPlan.getDeduplicatedAggregations();
    this.expression = aggregationPlan.getExpression();
    this.aggregateFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
  }

  /**
   * execute aggregate function with only time filter or no filter.
   *
   * @param context query context
   */
  public QueryDataSet executeWithoutValueFilter(QueryContext context)
      throws StorageEngineException, IOException, QueryProcessException {

    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }

    List<AggreResultData> aggreResultDataList = new ArrayList<>();
    //TODO use multi-thread
    for (int i = 0; i < selectedSeries.size(); i++) {
      AggreResultData aggreResultData = aggregateOneSeries(i, timeFilter, context);
      aggreResultDataList.add(aggreResultData);
    }
    return constructDataSet(aggreResultDataList);
  }


  /**
   * get aggregation result for one series
   */
  private AggreResultData aggregateOneSeries(int i, Filter timeFilter, QueryContext context)
      throws IOException, QueryProcessException, StorageEngineException {

    // construct AggregateResult
    TSDataType tsDataType = dataTypes.get(i);
    AggregateResult aggregateResult = AggreResultFactory.getAggrResultByName(aggres.get(i), tsDataType);
    aggregateResult.init();

    // construct series reader without value filter
    SeriesDataReaderWithoutValueFilter seriesReader = new SeriesDataReaderWithoutValueFilter(
        selectedSeries.get(i), tsDataType, timeFilter, context);

    while (seriesReader.hasNextChunk()) {
      if (seriesReader.canUseChunkStatistics()) {
        Statistics chunkStatistics = seriesReader.currentChunkStatistics();
        aggregateResult.updateResultFromStatistics(chunkStatistics);
        if (aggregateResult.isCalculatedAggregationResult()) {
          return aggregateResult.getResult();
        }
        seriesReader.skipChunkData();
        continue;
      }
      while (seriesReader.hasNextPage()) {
        //cal by pageheader
        if (seriesReader.canUsePageStatistics()) {
          Statistics pageStatistic = seriesReader.currentChunkStatistics();
          aggregateResult.updateResultFromStatistics(pageStatistic);
          if (aggregateResult.isCalculatedAggregationResult()) {
            return aggregateResult.getResult();
          }
          seriesReader.skipPageData();
          continue;
        }
        //cal by pagedata
        while (seriesReader.hasNextBatch()) {
          aggregateResult.updateResultFromPageData(seriesReader.nextBatch());
          if (aggregateResult.isCalculatedAggregationResult()) {
            return aggregateResult.getResult();
          }
        }
      }
    }
    return aggregateResult.getResult();
  }


  /**
   * execute aggregate function with value filter.
   *
   * @param context query context.
   */
  public QueryDataSet executeWithValueFilter(QueryContext context)
      throws StorageEngineException, PathException, IOException {

    EngineTimeGenerator timestampGenerator = new EngineTimeGenerator(expression, context);
    List<IReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
    for (Path path : selectedSeries) {
      SeriesReaderByTimestamp seriesReaderByTimestamp = new SeriesReaderByTimestamp(path, context);
      readersOfSelectedSeries.add(seriesReaderByTimestamp);
    }

    List<AggregateResult> aggregateResults = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      TSDataType type = MManager.getInstance().getSeriesType(selectedSeries.get(i).getFullPath());
      AggregateResult function = AggreResultFactory.getAggrResultByName(aggres.get(i), type);
      function.init();
      aggregateResults.add(function);
    }
    List<AggreResultData> batchDataList = aggregateWithValueFilter(aggregateResults,
        timestampGenerator, readersOfSelectedSeries);
    return constructDataSet(batchDataList);
  }

  /**
   * calculation aggregate result with value filter.
   */
  private List<AggreResultData> aggregateWithValueFilter(
      List<AggregateResult> aggregateResults,
      EngineTimeGenerator timestampGenerator,
      List<IReaderByTimestamp> readersOfSelectedSeries)
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

    List<AggreResultData> aggreResultDataArrayList = new ArrayList<>();
    for (AggregateResult function : aggregateResults) {
      aggreResultDataArrayList.add(function.getResult());
    }
    return aggreResultDataArrayList;
  }

  /**
   * using aggregate result data list construct QueryDataSet.
   *
   * @param aggreResultDataList aggregate result data list
   */
  private QueryDataSet constructDataSet(List<AggreResultData> aggreResultDataList)
      throws IOException {
    List<TSDataType> dataTypes = new ArrayList<>();
    List<IPointReader> resultDataPointReaders = new ArrayList<>();
    for (AggreResultData resultData : aggreResultDataList) {
      dataTypes.add(resultData.getDataType());
      resultDataPointReaders.add(new AggreResultDataPointReader(resultData));
    }
    return new OldEngineDataSetWithoutValueFilter(selectedSeries, dataTypes,
        resultDataPointReaders);
  }
}
