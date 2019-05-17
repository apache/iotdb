/**
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
package org.apache.iotdb.cluster.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.query.factory.ClusterSeriesReaderFactory;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.manager.coordinatornode.FilterGroupEntity;
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterSelectSeriesReader;
import org.apache.iotdb.cluster.query.timegenerator.ClusterTimeGenerator;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.aggregation.impl.LastAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MaxTimeAggrFunc;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.AggreResultDataPointReader;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithoutTimeGenerator;
import org.apache.iotdb.db.query.executor.AggregateEngineExecutor;
import org.apache.iotdb.db.query.factory.AggreFuncFactory;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

/**
 * Handle aggregation query and construct dataset in cluster
 */
public class ClusterAggregateEngineExecutor extends AggregateEngineExecutor {

  private ClusterRpcSingleQueryManager queryManager;
  private static final ClusterConfig CLUSTER_CONF = ClusterDescriptor.getInstance().getConfig();


  public ClusterAggregateEngineExecutor(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, ClusterRpcSingleQueryManager queryManager) {
    super(selectedSeries, aggres, expression);
    this.queryManager = queryManager;
  }

  @Override
  public QueryDataSet executeWithoutTimeGenerator(QueryContext context)
      throws FileNodeManagerException, IOException, PathErrorException, ProcessorException {
    Filter timeFilter = expression != null ? ((GlobalTimeExpression) expression).getFilter() : null;
    Map<Path, ClusterSelectSeriesReader> selectPathReaders = queryManager.getSelectSeriesReaders();

    List<Path> paths = new ArrayList<>();
    List<IPointReader> readers = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      Path path = selectedSeries.get(i);

      if (selectPathReaders.containsKey(path)) {
        ClusterSelectSeriesReader reader = selectPathReaders.get(path);
        readers.add(reader);
        dataTypes.add(reader.getDataType());
      } else {
        paths.add(path);
        // construct AggregateFunction
        TSDataType tsDataType = MManager.getInstance()
            .getSeriesType(path.getFullPath());
        AggregateFunction function = AggreFuncFactory.getAggrFuncByName(aggres.get(i), tsDataType);
        function.init();

        QueryDataSource queryDataSource = QueryResourceManager.getInstance()
            .getQueryDataSource(selectedSeries.get(i), context);

        // sequence reader for sealed tsfile, unsealed tsfile, memory
        SequenceDataReader sequenceReader;
        if (function instanceof MaxTimeAggrFunc || function instanceof LastAggrFunc) {
          sequenceReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), timeFilter,
              context, true);
        } else {
          sequenceReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), timeFilter,
              context, false);
        }

        // unseq reader for all chunk groups in unSeqFile, memory
        PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance()
            .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), timeFilter);

        AggreResultData aggreResultData = aggregateWithoutTimeGenerator(function,
            sequenceReader, unSeqMergeReader, timeFilter);

        dataTypes.add(aggreResultData.getDataType());
        readers.add(new AggreResultDataPointReader(aggreResultData));
      }
    }
    QueryResourceManager.getInstance()
        .beginQueryOfGivenQueryPaths(context.getJobId(), paths);

    return new EngineDataSetWithoutTimeGenerator(selectedSeries, dataTypes, readers);
  }

  /**
   * execute aggregate function with value filter.
   *
   * @param context query context.
   */
  @Override
  public QueryDataSet executeWithTimeGenerator(QueryContext context)
      throws FileNodeManagerException, PathErrorException, IOException, ProcessorException {

    /** add query token for query series which can handle locally **/
    List<Path> localQuerySeries = new ArrayList<>(selectedSeries);
    Set<Path> remoteQuerySeries = queryManager.getSelectSeriesReaders().keySet();
    localQuerySeries.removeAll(remoteQuerySeries);
    QueryResourceManager.getInstance()
        .beginQueryOfGivenQueryPaths(context.getJobId(), localQuerySeries);

    /** add query token for filter series which can handle locally **/
    Set<String> deviceIdSet = new HashSet<>();
    for (FilterGroupEntity filterGroupEntity : queryManager.getFilterGroupEntityMap().values()) {
      List<Path> remoteFilterSeries = filterGroupEntity.getFilterPaths();
      remoteFilterSeries.forEach(seriesPath -> deviceIdSet.add(seriesPath.getDevice()));
    }
    QueryResourceManager.getInstance()
        .beginQueryOfGivenExpression(context.getJobId(), expression, deviceIdSet);

    ClusterTimeGenerator timestampGenerator;
    List<EngineReaderByTimeStamp> readersOfSelectedSeries;
    try {
      timestampGenerator = new ClusterTimeGenerator(expression, context,
          queryManager);
      readersOfSelectedSeries = ClusterSeriesReaderFactory
          .createReadersByTimestampOfSelectedPaths(selectedSeries, context,
              queryManager);
    } catch (IOException ex) {
      throw new FileNodeManagerException(ex);
    }

    /** Get data type of select paths **/
    List<TSDataType> originDataTypes = new ArrayList<>();
    Map<Path, ClusterSelectSeriesReader> selectSeriesReaders = queryManager
        .getSelectSeriesReaders();
    for (Path path : selectedSeries) {
      try {
        if (selectSeriesReaders.containsKey(path)) {
          originDataTypes.add(selectSeriesReaders.get(path).getDataType());
        } else {
          originDataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));
        }
      } catch (PathErrorException e) {
        throw new FileNodeManagerException(e);
      }
    }

    List<AggregateFunction> aggregateFunctions = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      TSDataType type = originDataTypes.get(i);
      AggregateFunction function = AggreFuncFactory.getAggrFuncByName(aggres.get(i), type);
      function.init();
      aggregateFunctions.add(function);
    }
    List<AggreResultData> aggreResultDataList = aggregateWithTimeGenerator(aggregateFunctions,
        timestampGenerator,
        readersOfSelectedSeries);

    List<IPointReader> resultDataPointReaders = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (AggreResultData resultData : aggreResultDataList) {
      dataTypes.add(resultData.getDataType());
      resultDataPointReaders.add(new AggreResultDataPointReader(resultData));
    }
    return new EngineDataSetWithoutTimeGenerator(selectedSeries, dataTypes, resultDataPointReaders);
  }

  /**
   * calculation aggregate result with value filter.
   */
  @Override
  protected List<AggreResultData> aggregateWithTimeGenerator(
      List<AggregateFunction> aggregateFunctions,
      TimeGenerator timestampGenerator,
      List<EngineReaderByTimeStamp> readersOfSelectedSeries)
      throws IOException {

    while (timestampGenerator.hasNext()) {

      // generate timestamps for aggregate
      long[] timeArray = new long[aggregateFetchSize];
      List<Long> batchTimestamp = new ArrayList<>();
      int timeArrayLength = 0;
      for (int cnt = 0; cnt < aggregateFetchSize; cnt++) {
        if (!timestampGenerator.hasNext()) {
          break;
        }
        long time = timestampGenerator.next();
        timeArray[timeArrayLength++] = time;
        batchTimestamp.add(time);
      }

      // fetch all remote select series data by timestamp list.
      if (!batchTimestamp.isEmpty()) {
        try {
          queryManager.fetchBatchDataByTimestampForAllSelectPaths(batchTimestamp);
        } catch (RaftConnectionException e) {
          throw new IOException(e);
        }
      }

      // cal part of aggregate result
      for (int i = 0; i < readersOfSelectedSeries.size(); i++) {
        aggregateFunctions.get(i).calcAggregationUsingTimestamps(timeArray, timeArrayLength,
            readersOfSelectedSeries.get(i));
      }
    }

    List<AggreResultData> aggreResultDataArrayList = new ArrayList<>();
    for (AggregateFunction function : aggregateFunctions) {
      aggreResultDataArrayList.add(function.getResult());
    }
    return aggreResultDataArrayList;
  }
}
