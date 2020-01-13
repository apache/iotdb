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
import org.apache.iotdb.db.query.aggregation.impl.LastValueAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MaxTimeAggrFunc;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.AggreResultDataPointReader;
import org.apache.iotdb.db.query.dataset.OldEngineDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.factory.AggreFuncFactory;
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
  private List<String> aggres;
  private IExpression expression;

  /**
   * aggregation batch calculation size.
   **/
  private int aggregateFetchSize;

  /**
   * constructor.
   */
  public NewAggregateEngineExecutor(AggregationPlan aggregationPlan) {
    this.selectedSeries = aggregationPlan.getDeduplicatedPaths();
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

    List<SeriesDataReaderWithoutValueFilter> readersOfSequenceData = new ArrayList<>();
    List<AggregateResult> aggregateResults = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      // construct AggregateFunction
      TSDataType tsDataType = MManager.getInstance()
          .getSeriesType(selectedSeries.get(i).getFullPath());
      AggregateResult function = AggreFuncFactory.getAggrFuncByName(aggres.get(i), tsDataType);
      function.init();
      aggregateResults.add(function);

      // sequence reader for sealed tsfile, unsealed tsfile, memory
      SeriesDataReaderWithoutValueFilter newSeriesReaderWithoutValueFilter = new SeriesDataReaderWithoutValueFilter(
          selectedSeries.get(i), tsDataType, timeFilter, context);
      readersOfSequenceData.add(newSeriesReaderWithoutValueFilter);
    }

    List<AggreResultData> aggreResultDataList = new ArrayList<>();
    //TODO use multi-thread
    for (int i = 0; i < selectedSeries.size(); i++) {
      AggreResultData aggreResultData = aggregateWithoutValueFilter(aggregateResults.get(i),
          readersOfSequenceData.get(i));
      aggreResultDataList.add(aggreResultData);
    }
    return constructDataSet(aggreResultDataList);
  }

  /**
   * calculation aggregate result with only time filter or no filter for one series.
   *
   * @return one series aggregate result data
   */
  private AggreResultData aggregateWithoutValueFilter(AggregateResult function,
      SeriesDataReaderWithoutValueFilter newSeriesReader)
      throws IOException, QueryProcessException {
    if (function instanceof MaxTimeAggrFunc || function instanceof LastValueAggrFunc) {
      return handleLastMaxTimeWithOutTimeGenerator(function, newSeriesReader);
    }

    return getAggreResultData(function, newSeriesReader);
  }

  private AggreResultData getAggreResultData(AggregateResult function,
      SeriesDataReaderWithoutValueFilter newSeriesReader)
      throws IOException, QueryProcessException {
    while (newSeriesReader.hasNextChunk()) {
      if (newSeriesReader.canUseChunkStatistics()) {
        Statistics chunkStatistics = newSeriesReader.currentChunkStatistics();
        function.updateResultFromStatistics(chunkStatistics);
        if (function.isCalculatedAggregationResult()) {
          return function.getResult();
        }
        newSeriesReader.skipChunkData();
        continue;
      }
      while (newSeriesReader.hasNextPage()) {
        //cal by pageheader
        if (newSeriesReader.canUsePageStatistics()) {
          Statistics pageStatistic = newSeriesReader.currentChunkStatistics();
          function.updateResultFromStatistics(pageStatistic);
          if (function.isCalculatedAggregationResult()) {
            return function.getResult();
          }
          newSeriesReader.skipPageData();
          continue;
        }
        //cal by pagedata
        while (newSeriesReader.hasNextBatch()) {
          function.updateResultFromPageData(newSeriesReader.nextBatch());
          if (function.isCalculatedAggregationResult()) {
            return function.getResult();
          }
        }
      }
    }
    return function.getResult();
  }

  /**
   * handle last and max_time aggregate function with only time filter or no filter.
   *
   * @return BatchData-aggregate result
   */
  private AggreResultData handleLastMaxTimeWithOutTimeGenerator(AggregateResult function,
      SeriesDataReaderWithoutValueFilter newSeriesReader)
      throws IOException, QueryProcessException {
    return getAggreResultData(function, newSeriesReader);
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
      AggregateResult function = AggreFuncFactory.getAggrFuncByName(aggres.get(i), type);
      function.init();
      aggregateResults.add(function);
    }
    List<AggreResultData> batchDataList = aggregateWithValueFilter(aggregateResults,
        timestampGenerator,
        readersOfSelectedSeries);
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
