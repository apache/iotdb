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
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.query.reader.ClusterSeriesReader;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.apache.iotdb.cluster.utils.query.ClusterRpcReaderUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * Manage all remote series reader resource in coordinator node.
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
  private QueryPlan queryPlan;

  /**
   * Represent selected reader nodes, key is group id and value is selected peer id
   */
  private Map<String, PeerId> queryNodes = new HashMap<>();

  // select path resource
  /**
   * Query plans of select paths which are divided from queryPlan group by group id
   */
  private Map<String, QueryPlan> selectPathPlans = new HashMap<>();

  /**
   * Key is group id, value is all select series in group id
   */
  private Map<String, List<String>> selectSeriesByGroupId = new HashMap<>();

  /**
   * Series reader of select paths, key is series path , value is reader
   */
  private Map<String, ClusterSeriesReader> selectSeriesReaders = new HashMap<>();

  // filter path resource
  /**
   * Physical plans of filter paths which are divided from queryPlan group by group id
   */
  private Map<String, QueryPlan> filterPathPlans = new HashMap<>();

  /**
   * Key is group id, value is all filter series in group id
   */
  private Map<String, List<String>> filterSeriesByGroupId = new HashMap<>();

  /**
   * Series reader of filter paths, key is series path , value is reader
   */
  private Map<String, ClusterSeriesReader> filterSeriesReaders = new HashMap<>();

  public ClusterRpcSingleQueryManager(String taskId,
      QueryPlan queryPlan) {
    this.taskId = taskId;
    this.queryPlan = queryPlan;
  }

  @Override
  public void init(QueryType queryType, int readDataConsistencyLevel)
      throws PathErrorException, IOException, RaftConnectionException {
    switch (queryType) {
      case NO_FILTER:
        divideNoFilterPhysicalPlan();
        break;
      case GLOBAL_TIME:
        divideGlobalTimePhysicalPlan();
        break;
      case FILTER:
        divideFilterPhysicalPlan();
        break;
      default:
        throw new UnsupportedOperationException();
    }
    initSeriesReader(readDataConsistencyLevel);
  }

  public enum QueryType {
    NO_FILTER, GLOBAL_TIME, FILTER
  }

  /**
   * Divide no-fill type query plan by group id
   */
  private void divideNoFilterPhysicalPlan() throws PathErrorException {
    List<Path> selectPaths = queryPlan.getPaths();
    Map<String, List<Path>> selectPathsByGroupId = new HashMap<>();
    for (Path path : selectPaths) {
      String storageGroup = QPExecutorUtils.getStroageGroupByDevice(path.getDevice());
      String groupId = Router.getInstance().getGroupIdBySG(storageGroup);
      if (selectPathsByGroupId.containsKey(groupId)) {
        selectPathsByGroupId.put(groupId, new ArrayList<>());
        selectSeriesByGroupId.put(groupId, new ArrayList<>());
      }
      selectPathsByGroupId.get(groupId).add(path);
      selectSeriesByGroupId.get(groupId).add(path.getFullPath());
    }
    for (Entry<String, List<Path>> entry : selectPathsByGroupId.entrySet()) {
      String groupId = entry.getKey();
      List<Path> paths = entry.getValue();
      QueryPlan subQueryPlan = new QueryPlan();
      subQueryPlan.setProposer(queryPlan.getProposer());
      subQueryPlan.setPaths(paths);
      selectPathPlans.put(groupId, subQueryPlan);
    }
  }

  private void divideGlobalTimePhysicalPlan() {

  }

  private void divideFilterPhysicalPlan() {

  }

  /**
   * Init series reader, complete all initialization with all remote query node of a specific data
   * group
   */
  private void initSeriesReader(int readDataConsistencyLevel)
      throws IOException, RaftConnectionException {
    if (!selectPathPlans.isEmpty()) {
      for (Entry<String, QueryPlan> entry : selectPathPlans.entrySet()) {
        String groupId = entry.getKey();
        QueryPlan queryPlan = entry.getValue();
        if (!canHandleQueryLocally(groupId)) {
          PeerId randomPeer = RaftUtils.getRandomPeerID(groupId);
          queryNodes.put(groupId, randomPeer);
          Map<PathType, QueryPlan> allQueryPlan = new EnumMap<>(PathType.class);
          allQueryPlan.put(PathType.SELECT_PATH, queryPlan);
          QuerySeriesDataResponse response = (QuerySeriesDataResponse) ClusterRpcReaderUtils
              .createClusterSeriesReader(groupId, randomPeer, readDataConsistencyLevel,
                  allQueryPlan, taskId, queryRounds++);
          handleInitReaderResponse(groupId, allQueryPlan, response);
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
    for (Entry<PathType, QueryPlan> entry : allQueryPlan.entrySet()) {
      PathType pathType = entry.getKey();
      Map<String, ClusterSeriesReader> seriesReaderMap =
          pathType == PathType.SELECT_PATH ? selectSeriesReaders : filterSeriesReaders;
      QueryPlan plan = entry.getValue();
      List<Path> paths = plan.getPaths();
      List<TSDataType> seriesType = response.getSeriesDataTypes().get(pathType);
      List<BatchData> seriesBatchData = response.getSeriesBatchData().get(pathType);
      for (int i = 0; i < paths.size(); i++) {
        String seriesPath = paths.get(i).getFullPath();
        TSDataType dataType = seriesType.get(i);
        ClusterSeriesReader seriesReader = new ClusterSeriesReader(groupId, seriesPath,
            dataType, this);
        seriesReader.addBatchData(seriesBatchData.get(i));
        seriesReaderMap.put(seriesPath, seriesReader);
      }
    }
  }

  /**
   * Handle response of initial reader. In order to reduce the number of RPC communications,
   * fetching data from remote query node will fetch for all series in the same data group. If the
   * cached data for specific series is exceed limit, ignore this fetching data process.
   */
  @Override
  public void fetchData(String groupId) throws RaftConnectionException {
    Map<PathType, List<String>> fetchDataSeriesMap = new EnumMap<>(PathType.class);
    if (selectSeriesByGroupId.containsKey(groupId)) {
      List<String> allSelectSeries = selectSeriesByGroupId.get(groupId);
      List<String> fetchDataSeries = new ArrayList<>();
      for (String series : allSelectSeries) {
        if (selectSeriesReaders.get(series).enableFetchData()) {
          fetchDataSeries.add(series);
        }
      }
      fetchDataSeriesMap.put(PathType.SELECT_PATH, fetchDataSeries);
    }
    if (filterSeriesByGroupId.containsKey(groupId)) {
      List<String> fetchDataSeries = new ArrayList<>();
      List<String> allFilterSeries = filterSeriesByGroupId.get(groupId);
      for (String series : allFilterSeries) {
        if (filterSeriesReaders.get(series).enableFetchData()) {
          fetchDataSeries.add(series);
        }
      }
      fetchDataSeriesMap.put(PathType.FILTER_PATH, fetchDataSeries);
    }
    QuerySeriesDataResponse response = ClusterRpcReaderUtils
        .fetchBatchData(groupId, queryNodes.get(groupId), taskId, fetchDataSeriesMap,
            queryRounds++);
    handleFetchDataResponse(fetchDataSeriesMap, response);
  }

  /**
   * Handle response of fetch data, add batch data to corresponding reader.
   */
  private void handleFetchDataResponse(Map<PathType, List<String>> fetchDataSeriesMap,
      QuerySeriesDataResponse response) {
    for (Entry<PathType, List<String>> entry : fetchDataSeriesMap.entrySet()) {
      PathType pathType = entry.getKey();
      List<String> selectSeries = entry.getValue();
      Map<String, ClusterSeriesReader> seriesReaderMap =
          pathType == PathType.SELECT_PATH ? selectSeriesReaders : filterSeriesReaders;
      List<BatchData> batchDataList = response.getSeriesBatchData().get(pathType);
      for (int i = 0; i < selectSeries.size(); i++) {
        String series = selectSeries.get(i);
        BatchData batchData = batchDataList.get(i);
        seriesReaderMap.get(series).addBatchData(batchData);
      }
    }
  }

  /**
   * Check whether coordinator can handle the query of a specific data group
   */
  private boolean canHandleQueryLocally(String groupId) {
    return QPExecutorUtils.canHandleQueryByGroupId(groupId);
  }

  @Override
  public PhysicalPlan getSelectPathPhysicalPlan(String fullPath) {
    return selectPathPlans.get(fullPath);
  }

  @Override
  public PhysicalPlan getFilterPathPhysicalPlan(String fullPath) {
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

  public PhysicalPlan getQueryPlan() {
    return queryPlan;
  }

  public void setQueryPlan(QueryPlan queryPlan) {
    this.queryPlan = queryPlan;
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

  public Map<String, ClusterSeriesReader> getSelectSeriesReaders() {
    return selectSeriesReaders;
  }

  public void setSelectSeriesReaders(
      Map<String, ClusterSeriesReader> selectSeriesReaders) {
    this.selectSeriesReaders = selectSeriesReaders;
  }

  public Map<String, QueryPlan> getFilterPathPlans() {
    return filterPathPlans;
  }

  public void setFilterPathPlans(
      Map<String, QueryPlan> filterPathPlans) {
    this.filterPathPlans = filterPathPlans;
  }
}
