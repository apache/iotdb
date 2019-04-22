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
package org.apache.iotdb.cluster.query.manager.coordinatornode;

import com.alipay.sofa.jraft.entity.PeerId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.query.QueryType;
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterFilterSeriesReader;
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterSelectSeriesReader;
import org.apache.iotdb.cluster.query.utils.ClusterRpcReaderUtils;
import org.apache.iotdb.cluster.query.utils.QueryPlanPartitionUtils;
import org.apache.iotdb.cluster.rpc.raft.response.BasicQueryDataResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataByTimestampResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * Manage all remote series reader resource in a query resource in coordinator node.
 */
public class ClusterRpcSingleQueryManager implements IClusterRpcSingleQueryManager {

  /**
   * Query job id assigned by ClusterRpcQueryManager
   */
  private String taskId;

  /**
   * Represents the number of query rounds
   */
  private long queryRounds = 0;

  /**
   * Origin query plan parsed by QueryProcessor
   */
  private QueryPlan originQueryPlan;

  /**
   * Represent selected reader nodes, key is group id and value is selected peer id
   */
  private Map<String, PeerId> queryNodes = new HashMap<>();

  // select path resource
  /**
   * Query plans of select paths which are divided from queryPlan group by group id, it contains all
   * group id ,including local data group if it involves.
   */
  private Map<String, QueryPlan> selectPathPlans = new HashMap<>();

  /**
   * Key is group id (only contains remote group id), value is all select series in group id.
   */
  private Map<String, List<Path>> selectSeriesByGroupId = new HashMap<>();

  /**
   * Series reader of select paths (only contains remote series), key is series path , value is
   * reader
   */
  private Map<Path, ClusterSelectSeriesReader> selectSeriesReaders = new HashMap<>();

  // filter path resource
  /**
   * Query plans of filter paths which are divided from queryPlan group by group id, it contains all
   * * group id ,including local data group if it involves.
   */
  private Map<String, QueryPlan> filterPathPlans = new HashMap<>();

  /**
   * Key is group id (only contains remote group id), value is all filter series in group id
   */
  private Map<String, List<Path>> filterSeriesByGroupId = new HashMap<>();

  /**
   * Series reader of filter paths (only contains remote series), key is series path , value is
   * reader
   */
  private Map<Path, ClusterFilterSeriesReader> filterSeriesReaders = new HashMap<>();

  public ClusterRpcSingleQueryManager(String taskId,
      QueryPlan queryPlan) {
    this.taskId = taskId;
    this.originQueryPlan = queryPlan;
  }

  @Override
  public void initQueryResource(QueryType queryType, int readDataConsistencyLevel)
      throws PathErrorException, IOException, RaftConnectionException {
    switch (queryType) {
      case NO_FILTER:
        QueryPlanPartitionUtils.splitQueryPlanWithoutValueFilter(this);
        break;
      case GLOBAL_TIME:
        QueryPlanPartitionUtils.splitQueryPlanWithoutValueFilter(this);
        break;
      case FILTER:
        QueryPlanPartitionUtils.splitQueryPlanWithValueFilter(this);
        break;
      default:
        throw new UnsupportedOperationException();
    }
    initSeriesReader(readDataConsistencyLevel);
  }

  /**
   * Init series reader, complete all initialization with all remote query node of a specific data
   * group
   */
  private void initSeriesReader(int readDataConsistencyLevel)
      throws IOException, RaftConnectionException {
    for (Entry<String, QueryPlan> entry : selectPathPlans.entrySet()) {
      String groupId = entry.getKey();
      QueryPlan queryPlan = entry.getValue();
      if (!canHandleQueryLocally(groupId)) {
        PeerId randomPeer = RaftUtils.getRandomPeerID(groupId);
        queryNodes.put(groupId, randomPeer);
        Map<PathType, QueryPlan> allQueryPlan = new EnumMap<>(PathType.class);
        allQueryPlan.put(PathType.SELECT_PATH, queryPlan);
        if (filterPathPlans.containsKey(groupId)) {
          allQueryPlan.put(PathType.FILTER_PATH, queryPlan);
        }
        QuerySeriesDataResponse response = (QuerySeriesDataResponse) ClusterRpcReaderUtils
            .createClusterSeriesReader(groupId, randomPeer, readDataConsistencyLevel,
                allQueryPlan, taskId, queryRounds++);
        handleInitReaderResponse(groupId, allQueryPlan, response);
      } else {
        selectSeriesByGroupId.remove(groupId);
        if (filterPathPlans.containsKey(groupId)) {
          filterSeriesByGroupId.remove(groupId);
        }
      }
    }
  }

  /**
   * Handle response of initialization with remote query node
   */
  private void handleInitReaderResponse(String groupId, Map<PathType, QueryPlan> allQueryPlan,
      QuerySeriesDataResponse response) {
    /** create cluster series reader **/
    if (allQueryPlan.containsKey(PathType.SELECT_PATH)) {
      QueryPlan plan = allQueryPlan.get(PathType.SELECT_PATH);
      List<Path> paths = plan.getPaths();
      List<TSDataType> seriesType = response.getSeriesDataTypes().get(PathType.SELECT_PATH);
      for (int i = 0; i < paths.size(); i++) {
        Path seriesPath = paths.get(i);
        TSDataType dataType = seriesType.get(i);
        ClusterSelectSeriesReader seriesReader = new ClusterSelectSeriesReader(groupId, seriesPath,
            dataType, this);
        selectSeriesReaders.put(seriesPath, seriesReader);
      }
    }
    if (allQueryPlan.containsKey(PathType.FILTER_PATH)) {
      QueryPlan plan = allQueryPlan.get(PathType.FILTER_PATH);
      List<Path> paths = plan.getPaths();
      List<TSDataType> seriesType = response.getSeriesDataTypes().get(PathType.FILTER_PATH);
      for (int i = 0; i < paths.size(); i++) {
        Path seriesPath = paths.get(i);
        TSDataType dataType = seriesType.get(i);
        ClusterFilterSeriesReader seriesReader = new ClusterFilterSeriesReader(groupId, seriesPath,
            dataType, this);
        filterSeriesReaders.put(seriesPath, seriesReader);
      }
    }
  }

  @Override
  public void fetchBatchDataForSelectPaths(String groupId) throws RaftConnectionException {
    List<String> fetchDataSeries = new ArrayList<>();
    Map<String, List<Path>> seriesByGroupId;
    Map<Path, ClusterSelectSeriesReader> seriesReaders;
    seriesByGroupId = selectSeriesByGroupId;
    seriesReaders = selectSeriesReaders;
    if (seriesByGroupId.containsKey(groupId)) {
      List<Path> allFilterSeries = seriesByGroupId.get(groupId);
      for (Path series : allFilterSeries) {
        if (seriesReaders.get(series).enableFetchData()) {
          fetchDataSeries.add(series.getFullPath());
        }
      }
    }
    QuerySeriesDataResponse response = ClusterRpcReaderUtils
        .fetchBatchData(groupId, queryNodes.get(groupId), taskId, PathType.SELECT_PATH,
            fetchDataSeries,
            queryRounds++);
    handleFetchDataResponseForSelectPaths(fetchDataSeries, response);
  }

  @Override
  public void fetchBatchDataForFilterPaths(String groupId) throws RaftConnectionException {
    QuerySeriesDataResponse response = ClusterRpcReaderUtils
        .fetchBatchData(groupId, queryNodes.get(groupId), taskId, PathType.FILTER_PATH, null,
            queryRounds++);
    handleFetchDataResponseForFilterPaths(filterSeriesByGroupId.get(groupId), response);
  }


  @Override
  public void fetchBatchDataByTimestampForAllSelectPaths(List<Long> batchTimestamp)
      throws RaftConnectionException {
    for (Entry<String, List<Path>> entry : filterSeriesByGroupId.entrySet()) {
      String groupId = entry.getKey();
      List<String> fetchDataFilterSeries = new ArrayList<>();
      entry.getValue().forEach(path -> fetchDataFilterSeries.add(path.getFullPath()));
      QuerySeriesDataByTimestampResponse response = ClusterRpcReaderUtils
          .fetchBatchDataByTimestamp(groupId, queryNodes.get(groupId), taskId, queryRounds++,
              batchTimestamp);
      handleFetchDataByTimestampResponseForSelectPaths(fetchDataFilterSeries, response);
    }
  }

  /**
   * Handle response of fetching data, and add batch data to corresponding reader.
   */
  private void handleFetchDataByTimestampResponseForSelectPaths(List<String> fetchDataSeries,
      BasicQueryDataResponse response) {
    List<BatchData> batchDataList = response.getSeriesBatchData();
    for (int i = 0; i < fetchDataSeries.size(); i++) {
      String series = fetchDataSeries.get(i);
      BatchData batchData = batchDataList.get(i);
      selectSeriesReaders.get(new Path(series))
          .addBatchData(batchData, true);
    }
  }

  /**
   * Handle response of fetching data, and add batch data to corresponding reader.
   */
  private void handleFetchDataResponseForSelectPaths(List<String> fetchDataSeries,
      BasicQueryDataResponse response) {
    List<BatchData> batchDataList = response.getSeriesBatchData();
    for (int i = 0; i < fetchDataSeries.size(); i++) {
      String series = fetchDataSeries.get(i);
      BatchData batchData = batchDataList.get(i);
      selectSeriesReaders.get(new Path(series))
          .addBatchData(batchData, batchData.length() < ClusterConstant.BATCH_READ_SIZE);
    }
  }

  /**
   * Handle response of fetching data, and add batch data to corresponding reader.
   */
  private void handleFetchDataResponseForFilterPaths(List<Path> fetchDataSeries,
      QuerySeriesDataResponse response) {
    List<BatchData> batchDataList = response.getSeriesBatchData();
    boolean remoteDataFinish = true;
    for (int i = 0; i < batchDataList.size(); i++) {
      if (batchDataList.get(i).length() != 0) {
        remoteDataFinish = false;
        break;
      }
    }
    for (int i = 0; i < fetchDataSeries.size(); i++) {
      Path path = fetchDataSeries.get(i);
      BatchData batchData = batchDataList.get(i);
      filterSeriesReaders.get(path).addBatchData(batchData, remoteDataFinish);
    }
  }

  /**
   * Check whether coordinator can handle the query of a specific data group
   */
  private boolean canHandleQueryLocally(String groupId) {
    return QPExecutorUtils.canHandleQueryByGroupId(groupId);
  }

  @Override
  public QueryPlan getSelectPathQueryPlan(String fullPath) {
    return selectPathPlans.get(fullPath);
  }

  @Override
  public QueryPlan getFilterPathQueryPlan(String fullPath) {
    return filterPathPlans.get(fullPath);
  }

  @Override
  public void setDataGroupReaderNode(String groupId, PeerId readerNode) {
    queryNodes.put(groupId, readerNode);
  }

  @Override
  public PeerId getDataGroupReaderNode(String groupId) {
    return queryNodes.get(groupId);
  }

  @Override
  public void releaseQueryResource() throws RaftConnectionException {
    for (Entry<String, PeerId> entry : queryNodes.entrySet()) {
      String groupId = entry.getKey();
      PeerId queryNode = entry.getValue();
      ClusterRpcReaderUtils.releaseRemoteQueryResource(groupId, queryNode, taskId);
    }
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public long getQueryRounds() {
    return queryRounds;
  }

  public void setQueryRounds(long queryRounds) {
    this.queryRounds = queryRounds;
  }

  public QueryPlan getOriginQueryPlan() {
    return originQueryPlan;
  }

  public void setOriginQueryPlan(QueryPlan queryPlan) {
    this.originQueryPlan = queryPlan;
  }

  public Map<String, PeerId> getQueryNodes() {
    return queryNodes;
  }

  public void setQueryNodes(
      Map<String, PeerId> queryNodes) {
    this.queryNodes = queryNodes;
  }

  public Map<String, QueryPlan> getSelectPathPlans() {
    return selectPathPlans;
  }

  public void setSelectPathPlans(
      Map<String, QueryPlan> selectPathPlans) {
    this.selectPathPlans = selectPathPlans;
  }

  public Map<String, List<Path>> getSelectSeriesByGroupId() {
    return selectSeriesByGroupId;
  }

  public void setSelectSeriesByGroupId(
      Map<String, List<Path>> selectSeriesByGroupId) {
    this.selectSeriesByGroupId = selectSeriesByGroupId;
  }

  public Map<Path, ClusterSelectSeriesReader> getSelectSeriesReaders() {
    return selectSeriesReaders;
  }

  public void setSelectSeriesReaders(
      Map<Path, ClusterSelectSeriesReader> selectSeriesReaders) {
    this.selectSeriesReaders = selectSeriesReaders;
  }

  public Map<String, QueryPlan> getFilterPathPlans() {
    return filterPathPlans;
  }

  public void setFilterPathPlans(
      Map<String, QueryPlan> filterPathPlans) {
    this.filterPathPlans = filterPathPlans;
  }

  public Map<String, List<Path>> getFilterSeriesByGroupId() {
    return filterSeriesByGroupId;
  }

  public void setFilterSeriesByGroupId(
      Map<String, List<Path>> filterSeriesByGroupId) {
    this.filterSeriesByGroupId = filterSeriesByGroupId;
  }

  public Map<Path, ClusterFilterSeriesReader> getFilterSeriesReaders() {
    return filterSeriesReaders;
  }

  public void setFilterSeriesReaders(
      Map<Path, ClusterFilterSeriesReader> filterSeriesReaders) {
    this.filterSeriesReaders = filterSeriesReaders;
  }
}
