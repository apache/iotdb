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
package org.apache.iotdb.cluster.query.coordinatornode.manager;

import com.alipay.sofa.jraft.entity.PeerId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.query.coordinatornode.reader.ClusterSeriesReader;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.apache.iotdb.cluster.utils.query.ClusterRpcReaderUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.read.common.Path;

public class ClusterRpcSingleQueryManager implements IClusterRpcSingleQueryManager {

  /**
   * Query job id assigned by QueryResourceManager of coordinator node.
   */
  private long jobId;

  /**
   * Origin query plan parsed by QueryProcessor
   */
  private QueryPlan queryPlan;

  /**
   * Represent selected reader nodes, key is group id and value is selected peer id
   */
  private Map<String, PeerId> readerNodes = new HashMap<>();

  /**
   * Query plans of select paths which are divided from queryPlan group by group id
   */
  private Map<String, QueryPlan> selectPathPlans = new HashMap<>();

  private Map<String, ClusterSeriesReader> selectPathReaders = new HashMap<>();

  /**
   * Physical plans of filter paths which are divided from queryPlan group by group id
   */
  private Map<String, QueryPlan> filterPathPlans = new HashMap<>();

  private Map<String, ClusterSeriesReader> filterPathReaders = new HashMap<>();

  public ClusterRpcSingleQueryManager(long jobId,
      QueryPlan queryPlan) {
    this.jobId = jobId;
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
    initSelectedPathPlan(readDataConsistencyLevel);
    initFilterPathPlan(readDataConsistencyLevel);
  }

  public enum QueryType {
    NO_FILTER, GLOBAL_TIME, FILTER
  }

  /**
   * Divide no-fill type query plan by group id
   */
  private void divideNoFilterPhysicalPlan() throws PathErrorException {
    List<Path> selectPaths = queryPlan.getPaths();
    Map<String, List<Path>> pathsByGroupId = new HashMap<>();
    for (Path path : selectPaths) {
      String storageGroup = QPExecutorUtils.getStroageGroupByDevice(path.getDevice());
      String groupId = Router.getInstance().getGroupIdBySG(storageGroup);
      if (pathsByGroupId.containsKey(groupId)) {
        pathsByGroupId.put(groupId, new ArrayList<>());
      }
      pathsByGroupId.get(groupId).add(path);
    }
    for (Entry<String, List<Path>> entry : pathsByGroupId.entrySet()) {
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
   * Init select path
   */
  private void initSelectedPathPlan(int readDataConsistencyLevel)
      throws IOException, RaftConnectionException {
    if (!selectPathPlans.isEmpty()) {
      for (Entry<String, QueryPlan> entry : selectPathPlans.entrySet()) {
        String groupId = entry.getKey();
        QueryPlan queryPlan = entry.getValue();
        if (!canHandleQueryLocally(groupId)) {
          PeerId randomPeer = RaftUtils.getRandomPeerID(groupId);
          readerNodes.put(groupId, randomPeer);
          this.selectPathReaders = ClusterRpcReaderUtils
              .createClusterSeriesReader(groupId, randomPeer, queryPlan, readDataConsistencyLevel,
                  PathType.SELECT_PATH);
        }
      }
    }
  }

  private void initFilterPathPlan(int readDataConsistencyLevel) {
    if (!filterPathPlans.isEmpty()) {

    }
  }

  private boolean canHandleQueryLocally(String groupId){
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
    readerNodes.put(groupId, readerNode);
  }

  @Override
  public PeerId getDataGroupReaderNode(String groupId) {
    return readerNodes.get(groupId);
  }

  @Override
  public void releaseQueryResource() {

  }

  public long getJobId() {
    return jobId;
  }

  public void setJobId(long jobId) {
    this.jobId = jobId;
  }

  public PhysicalPlan getQueryPlan() {
    return queryPlan;
  }

  public void setQueryPlan(QueryPlan queryPlan) {
    this.queryPlan = queryPlan;
  }

  public Map<String, PeerId> getReaderNodes() {
    return readerNodes;
  }

  public void setReaderNodes(
      Map<String, PeerId> readerNodes) {
    this.readerNodes = readerNodes;
  }

  public Map<String, QueryPlan> getSelectPathPlans() {
    return selectPathPlans;
  }

  public void setSelectPathPlans(
      Map<String, QueryPlan> selectPathPlans) {
    this.selectPathPlans = selectPathPlans;
  }

  public Map<String, ClusterSeriesReader> getSelectPathReaders() {
    return selectPathReaders;
  }

  public void setSelectPathReaders(
      Map<String, ClusterSeriesReader> selectPathReaders) {
    this.selectPathReaders = selectPathReaders;
  }

  public Map<String, QueryPlan> getFilterPathPlans() {
    return filterPathPlans;
  }

  public void setFilterPathPlans(
      Map<String, QueryPlan> filterPathPlans) {
    this.filterPathPlans = filterPathPlans;
  }
}
