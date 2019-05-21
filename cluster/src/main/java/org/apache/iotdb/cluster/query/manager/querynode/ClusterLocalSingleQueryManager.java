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
import java.util.concurrent.ScheduledFuture;
import org.apache.iotdb.cluster.concurrent.pool.QueryTimerManager;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.query.factory.ClusterSeriesReaderFactory;
import org.apache.iotdb.cluster.query.reader.querynode.AbstractClusterSelectSeriesBatchReader;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterFillSelectSeriesBatchReader;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterFilterSeriesBatchReaderEntity;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterGroupBySelectSeriesBatchReaderEntity;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterSelectSeriesBatchReader;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterSelectSeriesBatchReaderByTimestamp;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterSelectSeriesBatchReaderEntity;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.InitSeriesReaderRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataByTimestampRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.InitSeriesReaderResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataByTimestampResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithOnlyTimeFilterDataSet;
import org.apache.iotdb.db.query.executor.AbstractExecutorWithoutTimeGenerator;
import org.apache.iotdb.db.query.executor.AggregateEngineExecutor;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.db.query.fill.PreviousFill;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClusterLocalSingleQueryManager implements IClusterLocalSingleQueryManager {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ClusterLocalSingleQueryManager.class);

  private String groupId;

  /**
   * Mark whether this manager has initialized or not.
   */
  private boolean isInit = false;

  /**
   * Timer of Query, if the time is up, close query resource.
   */
  private ScheduledFuture<?> queryTimer;

  /**
   * Job id assigned by local QueryResourceManager
   */
  private long jobId;

  /**
   * Represents the number of query rounds, initial value is -1.
   */
  private long queryRound = -1;

  /**
   * Select reader entity
   */
  private ClusterSelectSeriesBatchReaderEntity selectReaderEntity;

  /**
   * Select reader entity of group by query, which handle group by query with only time filter
   */
  private ClusterGroupBySelectSeriesBatchReaderEntity groupBySelectReaderEntity;

  /**
   * Filter reader entity
   */
  private ClusterFilterSeriesBatchReaderEntity filterReaderEntity;

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
    queryTimer = QueryTimerManager.getInstance()
        .execute(new QueryTimerRunnable(), ClusterConstant.QUERY_TIMEOUT_IN_QUERY_NODE);
  }

  @Override
  public InitSeriesReaderResponse createSeriesReader(InitSeriesReaderRequest request)
      throws IOException, PathErrorException, FileNodeManagerException, ProcessorException, QueryFilterOptimizationException, ClassNotFoundException {
    if (isInit) {
      throw new IOException(String
          .format("ClusterLocalSingleQueryManager has already initialized. Job id = %s", jobId));
    }
    isInit = true;
    this.groupId = request.getGroupID();
    InitSeriesReaderResponse response = new InitSeriesReaderResponse(groupId);
    QueryContext context = new QueryContext(jobId);
    Map<PathType, QueryPlan> queryPlanMap = request.getAllQueryPlan();
    if (queryPlanMap.containsKey(PathType.SELECT_PATH)) {
      selectReaderEntity = new ClusterSelectSeriesBatchReaderEntity();
      QueryPlan plan = queryPlanMap.get(PathType.SELECT_PATH);
      if (plan instanceof GroupByPlan) {
        handleGroupBySeriesReader(plan, context, response);
      } else if (plan instanceof AggregationPlan) {
        handleAggreSeriesReader(plan, context, response);
      } else if (plan instanceof FillQueryPlan) {
        handleFillSeriesReader(plan, context, response);
      } else {
        handleSelectSeriesReader(plan, context, response);
      }
    }
    if (queryPlanMap.containsKey(PathType.FILTER_PATH)) {
      QueryPlan queryPlan = queryPlanMap.get(PathType.FILTER_PATH);
      handleFilterSeriesReader(queryPlan, context, request, response, PathType.FILTER_PATH);
    }
    return response;
  }

  /**
   * Handle fill series reader
   *
   * @param queryPlan fill query plan
   */
  private void handleFillSeriesReader(QueryPlan queryPlan, QueryContext context,
      InitSeriesReaderResponse response)
      throws FileNodeManagerException, PathErrorException, IOException {
    FillQueryPlan fillQueryPlan = (FillQueryPlan) queryPlan;

    List<Path> selectedPaths = queryPlan.getPaths();
    List<TSDataType> dataTypes = new ArrayList<>();
    QueryResourceManager.getInstance().beginQueryOfGivenQueryPaths(jobId, selectedPaths);

    Map<TSDataType, IFill> typeIFillMap = fillQueryPlan.getFillType();
    for (Path path : selectedPaths) {
      QueryDataSource queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSource(path, context);
      TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());
      dataTypes.add(dataType);
      IFill fill;
      if (!typeIFillMap.containsKey(dataType)) {
        fill = new PreviousFill(dataType, fillQueryPlan.getQueryTime(), 0);
      } else {
        fill = typeIFillMap.get(dataType).copy(path);
      }
      fill.setDataType(dataType);
      fill.setQueryTime(fillQueryPlan.getQueryTime());
      fill.constructReaders(queryDataSource, context);
      selectReaderEntity.addPath(path.getFullPath());
      selectReaderEntity
          .addReaders(new ClusterFillSelectSeriesBatchReader(dataType, fill.getFillResult()));
      dataTypeMap.put(path.getFullPath(), dataType);
    }

    response.getSeriesDataTypes().put(PathType.SELECT_PATH, dataTypes);
  }


  /**
   * Handle aggregation series reader
   *
   * @param queryPlan fill query plan
   */
  private void handleGroupBySeriesReader(QueryPlan queryPlan, QueryContext context,
      InitSeriesReaderResponse response)
      throws FileNodeManagerException, PathErrorException, IOException, ProcessorException, QueryFilterOptimizationException {
    if (queryPlan.getExpression() == null
        || queryPlan.getExpression().getType() == ExpressionType.GLOBAL_TIME) {
      handleGroupBySeriesReaderWithoutTimeGenerator(queryPlan, context, response);
    } else {
      handleSelectReaderWithTimeGenerator(queryPlan, context, response);
    }
  }


  /**
   * Handle aggregation series reader without value filter
   *
   * @param queryPlan fill query plan
   */
  private void handleGroupBySeriesReaderWithoutTimeGenerator(QueryPlan queryPlan,
      QueryContext context,
      InitSeriesReaderResponse response)
      throws FileNodeManagerException, PathErrorException, IOException, ProcessorException, QueryFilterOptimizationException {
    QueryDataSet queryDataSet = queryProcessExecutor.processQuery(queryPlan, context);
    List<Path> paths = queryDataSet.getPaths();
    List<TSDataType> dataTypes = queryDataSet.getDataTypes();
    for (int i = 0; i < paths.size(); i++) {
      dataTypeMap.put(paths.get(i).getFullPath(), dataTypes.get(i));
    }
    groupBySelectReaderEntity = new ClusterGroupBySelectSeriesBatchReaderEntity(paths, dataTypes,
        (GroupByWithOnlyTimeFilterDataSet) queryDataSet);
    response.getSeriesDataTypes().put(PathType.SELECT_PATH, dataTypes);
  }

  /**
   * Handle aggregation series reader
   *
   * @param queryPlan fill query plan
   */
  private void handleAggreSeriesReader(QueryPlan queryPlan, QueryContext context,
      InitSeriesReaderResponse response)
      throws FileNodeManagerException, PathErrorException, IOException, ProcessorException {
    if (queryPlan.getExpression() == null
        || queryPlan.getExpression().getType() == ExpressionType.GLOBAL_TIME) {
      handleAggreSeriesReaderWithoutTimeGenerator(queryPlan, context, response);
    } else {
      handleSelectReaderWithTimeGenerator(queryPlan, context, response);
    }
  }

  /**
   * Handle aggregation series reader without value filter
   *
   * @param queryPlan fill query plan
   */
  private void handleAggreSeriesReaderWithoutTimeGenerator(QueryPlan queryPlan,
      QueryContext context,
      InitSeriesReaderResponse response)
      throws FileNodeManagerException, PathErrorException, IOException, ProcessorException {
    AggregationPlan fillQueryPlan = (AggregationPlan) queryPlan;

    List<Path> selectedPaths = fillQueryPlan.getPaths();
    QueryResourceManager.getInstance().beginQueryOfGivenQueryPaths(jobId, selectedPaths);

    AggregateEngineExecutor engineExecutor = new AggregateEngineExecutor(
        selectedPaths, fillQueryPlan.getAggregations(), fillQueryPlan.getExpression());

    List<IPointReader> readers = engineExecutor.constructAggreReadersWithoutTimeGenerator(context);

    List<TSDataType> dataTypes = engineExecutor.getDataTypes();

    for (int i = 0; i < selectedPaths.size(); i++) {
      Path path = selectedPaths.get(i);
      selectReaderEntity.addPath(path.getFullPath());
      selectReaderEntity.addReaders(
          new ClusterSelectSeriesBatchReader(dataTypes.get(i), readers.get(i)));
      dataTypeMap.put(path.getFullPath(), dataTypes.get(i));
    }

    response.getSeriesDataTypes().put(PathType.SELECT_PATH, dataTypes);
  }

  /**
   * Handle select series query
   *
   * @param plan plan query plan
   * @param context query context
   * @param response response for coordinator node
   */
  private void handleSelectSeriesReader(QueryPlan plan, QueryContext context,
      InitSeriesReaderResponse response)
      throws FileNodeManagerException, IOException, PathErrorException {
    if (plan.getExpression() == null
        || plan.getExpression().getType() == ExpressionType.GLOBAL_TIME) {
      handleSelectReaderWithoutTimeGenerator(plan, context, response);
    } else {
      handleSelectReaderWithTimeGenerator(plan, context, response);
    }
  }

  /**
   * Handle select series query with no filter or only global time filter
   *
   * @param plan plan query plan
   * @param context query context
   * @param response response for coordinator node
   */
  private void handleSelectReaderWithoutTimeGenerator(QueryPlan plan, QueryContext context,
      InitSeriesReaderResponse response)
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
      IPointReader reader = AbstractExecutorWithoutTimeGenerator
          .createSeriesReader(context, paths.get(i), dataTypes, timeFilter);
      selectReaderEntity.addPath(fullPath);
      selectReaderEntity.addReaders(new ClusterSelectSeriesBatchReader(dataTypes.get(i), reader));
      dataTypeMap.put(fullPath, dataTypes.get(i));
    }
    response.getSeriesDataTypes().put(PathType.SELECT_PATH, dataTypes);
  }

  /**
   * Handle filter series reader
   *
   * @param plan filter series query plan
   */
  private void handleFilterSeriesReader(QueryPlan plan, QueryContext context,
      InitSeriesReaderRequest request, InitSeriesReaderResponse response, PathType pathType)
      throws PathErrorException, QueryFilterOptimizationException, FileNodeManagerException, ProcessorException, IOException, ClassNotFoundException {
    QueryDataSet queryDataSet = queryProcessExecutor.processQuery(plan, context);
    List<Path> paths = plan.getPaths();
    List<TSDataType> dataTypes = queryDataSet.getDataTypes();
    for (int i = 0; i < paths.size(); i++) {
      dataTypeMap.put(paths.get(i).getFullPath(), dataTypes.get(i));
    }
    response.getSeriesDataTypes().put(pathType, dataTypes);
    filterReaderEntity = new ClusterFilterSeriesBatchReaderEntity(queryDataSet, paths,
        request.getFilterList());
  }

  /**
   * Handle select series query with value filter
   *
   * @param plan plan query plan
   * @param context query context
   * @param response response for coordinator node
   */
  private void handleSelectReaderWithTimeGenerator(QueryPlan plan, QueryContext context,
      InitSeriesReaderResponse response)
      throws PathErrorException, FileNodeManagerException, IOException {
    List<Path> paths = plan.getPaths();
    List<TSDataType> dataTypeList = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      Path path = paths.get(i);
      EngineReaderByTimeStamp readerByTimeStamp = ClusterSeriesReaderFactory
          .createReaderByTimeStamp(path, context);
      TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());
      selectReaderEntity.addPath(path.getFullPath());
      selectReaderEntity
          .addReaders(new ClusterSelectSeriesBatchReaderByTimestamp(readerByTimeStamp, dataType));
      dataTypeMap.put(path.getFullPath(), dataType);
      dataTypeList.add(dataType);
    }
    response.getSeriesDataTypes().put(PathType.SELECT_PATH, dataTypeList);
  }

  @Override
  public QuerySeriesDataResponse readBatchData(QuerySeriesDataRequest request)
      throws IOException {
    resetQueryTimer();
    QuerySeriesDataResponse response = new QuerySeriesDataResponse(request.getGroupID());
    long targetQueryRounds = request.getQueryRounds();
    if (targetQueryRounds != this.queryRound) {
      this.queryRound = targetQueryRounds;
      PathType pathType = request.getPathType();
      List<BatchData> batchDataList;
      if (pathType == PathType.SELECT_PATH) {
        // check whether it's a group by query with only time filter
        batchDataList =
            groupBySelectReaderEntity != null ? groupBySelectReaderEntity.nextBatchList()
                : readSelectSeriesBatchData(request.getSeriesPathIndexs());
      } else {
        batchDataList = readFilterSeriesBatchData();
      }
      cachedBatchDataResult = batchDataList;
    }
    response.setSeriesBatchData(cachedBatchDataResult);
    return response;
  }

  @Override
  public QuerySeriesDataByTimestampResponse readBatchDataByTimestamp(
      QuerySeriesDataByTimestampRequest request)
      throws IOException {
    resetQueryTimer();
    QuerySeriesDataByTimestampResponse response = new QuerySeriesDataByTimestampResponse(groupId);
    long targetQueryRounds = request.getQueryRounds();
    if (targetQueryRounds != this.queryRound) {
      this.queryRound = targetQueryRounds;
      List<AbstractClusterSelectSeriesBatchReader> readers = selectReaderEntity.getAllReaders();
      List<BatchData> batchDataList = new ArrayList<>();
      for (AbstractClusterSelectSeriesBatchReader reader : readers) {
        batchDataList.add(reader.nextBatch(request.getBatchTimestamp()));
      }
      cachedBatchDataResult = batchDataList;
    }
    response.setSeriesBatchData(cachedBatchDataResult);
    return response;
  }

  @Override
  public void resetQueryTimer() {
    queryTimer.cancel(false);
    queryTimer = QueryTimerManager.getInstance()
        .execute(new QueryTimerRunnable(), ClusterConstant.QUERY_TIMEOUT_IN_QUERY_NODE);
  }

  /**
   * Read batch data of select series by series index
   *
   * @param seriesIndexs all series index to query
   */
  private List<BatchData> readSelectSeriesBatchData(List<Integer> seriesIndexs) throws IOException {
    List<BatchData> batchDataList = new ArrayList<>();
    for (int index : seriesIndexs) {
      AbstractClusterSelectSeriesBatchReader reader = selectReaderEntity.getReaderByIndex(index);
      batchDataList.add(reader.nextBatch());
    }
    return batchDataList;
  }

  /**
   * Read batch data of filter series
   *
   * @return batch data of all filter series
   */
  private List<BatchData> readFilterSeriesBatchData() throws IOException {
    return filterReaderEntity.nextBatchList();
  }

  public String getGroupId() {
    return groupId;
  }

  @Override
  public void close() throws FileNodeManagerException {
    queryTimer.cancel(false);
    QueryResourceManager.getInstance().endQueryForGivenJob(jobId);
  }

  public long getJobId() {
    return jobId;
  }

  public long getQueryRound() {
    return queryRound;
  }

  public ClusterSelectSeriesBatchReaderEntity getSelectReaderEntity() {
    return selectReaderEntity;
  }

  public ClusterFilterSeriesBatchReaderEntity getFilterReaderEntity() {
    return filterReaderEntity;
  }

  public Map<String, TSDataType> getDataTypeMap() {
    return dataTypeMap;
  }

  public class QueryTimerRunnable implements Runnable {

    @Override
    public void run() {
      try {
        close();
      } catch (FileNodeManagerException e) {
        LOGGER.error(e.getMessage());
      }
    }
  }
}
