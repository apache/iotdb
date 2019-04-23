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
package org.apache.iotdb.cluster.query.manager.querynode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.query.factory.ClusterSeriesReaderFactory;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterBatchReaderByTimestamp;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterBatchReaderWithoutTimeGenerator;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterFilterSeriesBatchReader;
import org.apache.iotdb.cluster.query.reader.querynode.IClusterBatchReader;
import org.apache.iotdb.cluster.query.reader.querynode.IClusterFilterSeriesBatchReader;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataByTimestampRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataByTimestampResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.ExecutorWithoutTimeGenerator;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;


public class ClusterLocalSingleQueryManager implements IClusterLocalSingleQueryManager {

  /**
   * Job id assigned by local QueryResourceManager
   */
  private long jobId;

  /**
   * Represents the number of query rounds, initial value is -1.
   */
  private long queryRound = -1;

  /**
   * Key is series full path, value is reader of select series
   */
  private Map<String, IClusterBatchReader> selectSeriesReaders = new HashMap<>();

  /**
   * Filter reader
   */
  private IClusterFilterSeriesBatchReader filterReader;

  /**
   * Key is series full path, value is data type of series
   */
  private Map<String, TSDataType> dataTypeMap = new HashMap<>();

  /**
   * Cached batch data result
   */
  private List<BatchData> cachedBatchDataResult = new ArrayList<>();

  private QueryProcessExecutor queryProcessExecutor = new OverflowQPExecutor();

  /**
   * Constructor of ClusterLocalSingleQueryManager
   */
  public ClusterLocalSingleQueryManager(long jobId) {
    this.jobId = jobId;
  }

  @Override
  public void createSeriesReader(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException, PathErrorException, FileNodeManagerException, ProcessorException, QueryFilterOptimizationException {
    QueryContext context = new QueryContext(jobId);
    Map<PathType, QueryPlan> queryPlanMap = request.getAllQueryPlan();
    if (queryPlanMap.containsKey(PathType.SELECT_PATH)) {
      QueryPlan plan = queryPlanMap.get(PathType.SELECT_PATH);
      if (plan instanceof GroupByPlan) {
        throw new UnsupportedOperationException();
      } else if (plan instanceof AggregationPlan) {
        throw new UnsupportedOperationException();
      } else {
        if (plan.getExpression() == null
            || plan.getExpression().getType() == ExpressionType.GLOBAL_TIME) {
          handleSelectReaderWithoutTimeGenerator(plan, context, response);
        } else {
          handleSelectReaderWithTimeGenerator(plan, context, response);
        }
      }
    }
    if (queryPlanMap.containsKey(PathType.FILTER_PATH)) {
      QueryPlan queryPlan = queryPlanMap.get(PathType.FILTER_PATH);
      handleFilterSeriesReader(queryPlan, context, response, PathType.FILTER_PATH);
    }
  }

  /**
   * Handle filter series reader
   *
   * @param plan filter series query plan
   */
  private void handleFilterSeriesReader(QueryPlan plan, QueryContext context,
      QuerySeriesDataResponse response, PathType pathType)
      throws PathErrorException, QueryFilterOptimizationException, FileNodeManagerException, ProcessorException, IOException {
    QueryDataSet queryDataSet = queryProcessExecutor
        .processQuery(plan, context);
    List<Path> paths = plan.getPaths();
    List<TSDataType> dataTypes = queryDataSet.getDataTypes();
    for (int i = 0; i < paths.size(); i++) {
      dataTypeMap.put(paths.get(i).getFullPath(), dataTypes.get(i));
    }
    response.getSeriesDataTypes().put(pathType, dataTypes);
    filterReader = new ClusterFilterSeriesBatchReader(queryDataSet, paths);
  }

  /**
   * Handle select series query with no filter or only global time filter
   *
   * @param plan plan query plan
   * @param context query context
   * @param response response for coordinator node
   */
  private void handleSelectReaderWithoutTimeGenerator(QueryPlan plan, QueryContext context,
      QuerySeriesDataResponse response)
      throws FileNodeManagerException {
    List<Path> paths = plan.getPaths();
    Filter timeFilter = null;
    if (plan.getExpression() != null) {
      timeFilter = ((GlobalTimeExpression) plan.getExpression()).getFilter();
    }
    List<TSDataType> dataTypes = new ArrayList<>();
    QueryResourceManager.getInstance()
        .beginQueryOfGivenQueryPaths(context.getJobId(), plan.getPaths());
    for (int i = 0; i < paths.size(); i++) {
      String fullPath = paths.get(i).getFullPath();
      IPointReader reader = ExecutorWithoutTimeGenerator
          .createSeriesReader(context, paths.get(i), dataTypes, timeFilter);
      selectSeriesReaders
          .put(fullPath, new ClusterBatchReaderWithoutTimeGenerator(dataTypes.get(i), reader));
      dataTypeMap.put(fullPath, dataTypes.get(i));
    }
    response.getSeriesDataTypes().put(PathType.SELECT_PATH, dataTypes);
  }

  /**
   * Handle select series query with value filter
   *
   * @param plan plan query plan
   * @param context query context
   * @param response response for coordinator node
   */
  private void handleSelectReaderWithTimeGenerator(QueryPlan plan, QueryContext context,
      QuerySeriesDataResponse response)
      throws PathErrorException, FileNodeManagerException, IOException {
    List<Path> paths = plan.getPaths();
    List<TSDataType> dataTypeList = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      Path path = paths.get(i);
      EngineReaderByTimeStamp readerByTimeStamp = ClusterSeriesReaderFactory
          .createReaderByTimeStamp(path, context);
      TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());
      selectSeriesReaders
          .put(path.getFullPath(), new ClusterBatchReaderByTimestamp(readerByTimeStamp, dataType));
      dataTypeMap.put(path.getFullPath(), dataType);
      dataTypeList.add(dataType);
    }
    response.getSeriesDataTypes().put(PathType.SELECT_PATH, dataTypeList);
  }

  @Override
  public void readBatchData(QuerySeriesDataRequest request, QuerySeriesDataResponse response)
      throws IOException {
    long targetQueryRounds = request.getQueryRounds();
    if (targetQueryRounds != this.queryRound) {
      this.queryRound = targetQueryRounds;
      PathType pathType = request.getPathType();
      List<String> paths = request.getSeriesPaths();
      List<BatchData> batchDataList;
      if (pathType == PathType.SELECT_PATH) {
        batchDataList = readSelectSeriesBatchData(paths);
      } else {
        batchDataList = readFilterSeriesBatchData();
      }
      cachedBatchDataResult = batchDataList;
    }
    response.setSeriesBatchData(cachedBatchDataResult);
  }

  @Override
  public void readBatchDataByTimestamp(QuerySeriesDataByTimestampRequest request,
      QuerySeriesDataByTimestampResponse response)
      throws IOException {
    List<String> fetchDataSeries = request.getFetchDataSeries();
    long targetQueryRounds = request.getQueryRounds();
    if (targetQueryRounds != this.queryRound) {
      this.queryRound = targetQueryRounds;
      List<BatchData> batchDataList = new ArrayList<>();
      for (String series : fetchDataSeries) {
        IClusterBatchReader reader = selectSeriesReaders.get(series);
        batchDataList.add(reader.nextBatch(request.getBatchTimestamp()));
      }
      cachedBatchDataResult = batchDataList;
    }
    response.setSeriesBatchData(cachedBatchDataResult);
  }

  /**
   * Read batch data of select series
   *
   * @param paths all series to query
   */
  private List<BatchData> readSelectSeriesBatchData(List<String> paths) throws IOException {
    List<BatchData> batchDataList = new ArrayList<>();
    for (String fullPath : paths) {
      batchDataList.add(selectSeriesReaders.get(fullPath).nextBatch());
    }
    return batchDataList;
  }

  /**
   * Read batch data of filter series
   *
   * @return batch data of all filter series
   */
  private List<BatchData> readFilterSeriesBatchData() throws IOException {
    return filterReader.nextBatchList();
  }

  @Override
  public void close() throws FileNodeManagerException {
    QueryResourceManager.getInstance().endQueryForGivenJob(jobId);
  }

  public long getJobId() {
    return jobId;
  }

  public long getQueryRound() {
    return queryRound;
  }

  public Map<String, IClusterBatchReader> getSelectSeriesReaders() {
    return selectSeriesReaders;
  }

  public IClusterFilterSeriesBatchReader getFilterReader() {
    return filterReader;
  }

  public Map<String, TSDataType> getDataTypeMap() {
    return dataTypeMap;
  }
}
