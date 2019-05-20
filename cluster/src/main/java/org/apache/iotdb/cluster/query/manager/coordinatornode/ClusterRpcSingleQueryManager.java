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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.query.QueryType;
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterFilterSeriesReader;
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterSelectSeriesReader;
import org.apache.iotdb.cluster.query.utils.ClusterRpcReaderUtils;
import org.apache.iotdb.cluster.query.utils.QueryPlanPartitionUtils;
import org.apache.iotdb.cluster.rpc.raft.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.CloseSeriesReaderRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.InitSeriesReaderRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataByTimestampRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.BasicQueryDataResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.InitSeriesReaderResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataByTimestampResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * Manage all remote series reader resource in a query resource in coordinator node.
 */
public class ClusterRpcSingleQueryManager implements IClusterRpcSingleQueryManager {

  /**
   * Statistic all usage of local data group.
   */
  private Set<String> dataGroupUsage = new HashSet<>();

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
   * Filter group entity group by data group, key is group id(only contain remote group id)
   */
  private Map<String, FilterGroupEntity> filterGroupEntityMap = new HashMap<>();

  private static final ClusterConfig CLUSTER_CONF = ClusterDescriptor.getInstance().getConfig();

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
      throws RaftConnectionException, IOException {
    // Init all series with data group of select series,if filter series has the same data group, init them together.
    for (Entry<String, QueryPlan> entry : selectPathPlans.entrySet()) {
      String groupId = entry.getKey();
      QueryPlan queryPlan = entry.getValue();
      if (!QPExecutorUtils.canHandleQueryByGroupId(groupId)) {
        Map<PathType, QueryPlan> allQueryPlan = new EnumMap<>(PathType.class);
        allQueryPlan.put(PathType.SELECT_PATH, queryPlan);
        List<Filter> filterList = new ArrayList<>();
        if (filterGroupEntityMap.containsKey(groupId)) {
          FilterGroupEntity filterGroupEntity = filterGroupEntityMap.get(groupId);
          allQueryPlan.put(PathType.FILTER_PATH, filterGroupEntity.getQueryPlan());
          filterList = filterGroupEntity.getFilters();
        }
        /** create request **/
        BasicRequest request = InitSeriesReaderRequest
            .createInitialQueryRequest(groupId, taskId, readDataConsistencyLevel,
                allQueryPlan, filterList);
        InitSeriesReaderResponse response = (InitSeriesReaderResponse) ClusterRpcReaderUtils
            .createClusterSeriesReader(groupId, request, this);
        handleInitReaderResponse(groupId, allQueryPlan, response);
      } else {
        dataGroupUsage.add(groupId);
        selectSeriesByGroupId.remove(groupId);
        if (filterGroupEntityMap.containsKey(groupId)) {
          filterGroupEntityMap.remove(groupId);
        }
      }
    }

    //Init series reader with data groups of filter series, which don't exist in data groups list of select series.
    for (Entry<String, FilterGroupEntity> entry : filterGroupEntityMap.entrySet()) {
      String groupId = entry.getKey();
      if (!selectPathPlans.containsKey(groupId)) {
        Map<PathType, QueryPlan> allQueryPlan = new EnumMap<>(PathType.class);
        FilterGroupEntity filterGroupEntity = filterGroupEntityMap.get(groupId);
        allQueryPlan.put(PathType.FILTER_PATH, filterGroupEntity.getQueryPlan());
        List<Filter> filterList = filterGroupEntity.getFilters();
        BasicRequest request = InitSeriesReaderRequest
            .createInitialQueryRequest(groupId, taskId, readDataConsistencyLevel,
                allQueryPlan, filterList);
        InitSeriesReaderResponse response = (InitSeriesReaderResponse) ClusterRpcReaderUtils
            .createClusterSeriesReader(groupId, request, this);
        handleInitReaderResponse(groupId, allQueryPlan, response);
      }
    }
  }

  /**
   * Handle response of initialization with remote query node
   */
  private void handleInitReaderResponse(String groupId, Map<PathType, QueryPlan> allQueryPlan,
      InitSeriesReaderResponse response) {
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
        if (!filterGroupEntityMap.containsKey(groupId)) {
          filterGroupEntityMap.put(groupId, new FilterGroupEntity(groupId));
        }
        filterGroupEntityMap.get(groupId).addFilterSeriesReader(seriesReader);
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
    BasicRequest request = QuerySeriesDataRequest
        .createFetchDataRequest(groupId, taskId, PathType.SELECT_PATH, fetchDataSeries,
            queryRounds++);
    QuerySeriesDataResponse response = (QuerySeriesDataResponse) ClusterRpcReaderUtils
        .handleQueryRequest(request, queryNodes.get(groupId), 0);
    handleFetchDataResponseForSelectPaths(fetchDataSeries, response);
  }

  @Override
  public void fetchBatchDataForFilterPaths(String groupId) throws RaftConnectionException {
    BasicRequest request = QuerySeriesDataRequest
        .createFetchDataRequest(groupId, taskId, PathType.FILTER_PATH, null, queryRounds++);
    QuerySeriesDataResponse response = (QuerySeriesDataResponse) ClusterRpcReaderUtils
        .handleQueryRequest(request, queryNodes.get(groupId), 0);

    handleFetchDataResponseForFilterPaths(groupId, response);
  }


  @Override
  public void fetchBatchDataByTimestampForAllSelectPaths(List<Long> batchTimestamp)
      throws RaftConnectionException {
    for (Entry<String, List<Path>> entry : selectSeriesByGroupId.entrySet()) {
      String groupId = entry.getKey();
      List<String> fetchDataFilterSeries = new ArrayList<>();
      entry.getValue().forEach(path -> fetchDataFilterSeries.add(path.getFullPath()));
      BasicRequest request = QuerySeriesDataByTimestampRequest
          .createRequest(groupId, queryRounds++, taskId, batchTimestamp, fetchDataFilterSeries);
      QuerySeriesDataByTimestampResponse response = (QuerySeriesDataByTimestampResponse) ClusterRpcReaderUtils
          .handleQueryRequest(request, queryNodes.get(groupId), 0);
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
          .addBatchData(batchData, batchData.length() < CLUSTER_CONF.getBatchReadSize());
    }
  }

  /**
   * Handle response of fetching data, and add batch data to corresponding reader.
   */
  private void handleFetchDataResponseForFilterPaths(String groupId,
      QuerySeriesDataResponse response) {
    FilterGroupEntity filterGroupEntity = filterGroupEntityMap.get(groupId);
    List<Path> fetchDataSeries = filterGroupEntity.getFilterPaths();
    List<BatchData> batchDataList = response.getSeriesBatchData();
    List<ClusterFilterSeriesReader> filterReaders = filterGroupEntity.getFilterSeriesReaders();
    boolean remoteDataFinish = true;
    for (int i = 0; i < batchDataList.size(); i++) {
      if (batchDataList.get(i).length() != 0) {
        remoteDataFinish = false;
        break;
      }
    }
    for (int i = 0; i < fetchDataSeries.size(); i++) {
      BatchData batchData = batchDataList.get(i);
      if (batchData.length() != 0) {
        filterReaders.get(i).addBatchData(batchData, remoteDataFinish);
      }
    }
  }

  @Override
  public QueryPlan getSelectPathQueryPlan(String fullPath) {
    return selectPathPlans.get(fullPath);
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
      BasicRequest request = CloseSeriesReaderRequest.createReleaseResourceRequest(groupId, taskId);
      ClusterRpcReaderUtils.handleQueryRequest(request, queryNode, 0);
    }
  }

  public Set<String> getDataGroupUsage() {
    return dataGroupUsage;
  }

  public void addDataGroupUsage(String groupId) {
    this.dataGroupUsage.add(groupId);
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

  public QueryPlan getOriginQueryPlan() {
    return originQueryPlan;
  }

  public void setQueryNode(String groupID, PeerId peerId) {
    this.queryNodes.put(groupID, peerId);
  }

  public Map<String, QueryPlan> getSelectPathPlans() {
    return selectPathPlans;
  }

  public Map<String, List<Path>> getSelectSeriesByGroupId() {
    return selectSeriesByGroupId;
  }

  public Map<Path, ClusterSelectSeriesReader> getSelectSeriesReaders() {
    return selectSeriesReaders;
  }

  public Map<String, FilterGroupEntity> getFilterGroupEntityMap() {
    return filterGroupEntityMap;
  }
}
