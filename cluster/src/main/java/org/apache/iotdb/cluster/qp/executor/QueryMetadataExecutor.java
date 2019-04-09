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
package org.apache.iotdb.cluster.qp.executor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.PeerId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.cluster.callback.SingleQPTask;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.ClusterQPExecutor;
import org.apache.iotdb.cluster.rpc.request.QueryMetadataInStringRequest;
import org.apache.iotdb.cluster.rpc.request.QueryStorageGroupRequest;
import org.apache.iotdb.cluster.rpc.request.QueryTimeSeriesRequest;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.response.QueryMetadataInStringResponse;
import org.apache.iotdb.cluster.rpc.response.QueryStorageGroupResponse;
import org.apache.iotdb.cluster.rpc.response.QueryTimeSeriesResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle < show timeseries <path> > logic
 */
public class QueryMetadataExecutor extends ClusterQPExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryMetadataExecutor.class);
  private static final String DOUB_SEPARATOR = "\\.";
  private static final char SINGLE_SEPARATOR = '.';

  public QueryMetadataExecutor() {
    super();
  }

  public Set<String> processStorageGroupQuery() throws InterruptedException {
    return queryStorageGroupLocally();
  }

  /**
   * Handle show timeseries <path> statement
   */
  public List<List<String>> processTimeSeriesQuery(String path)
      throws InterruptedException, PathErrorException, ProcessorException {
    List<List<String>> res = new ArrayList<>();
    List<String> storageGroupList = mManager.getAllFileNamesByPath(path);
    if (storageGroupList.isEmpty()) {
      return new ArrayList<>();
    } else {
      Map<String, Set<String>> groupIdSGMap = classifySGByGroupId(storageGroupList);
      for (Entry<String, Set<String>> entry : groupIdSGMap.entrySet()) {
        List<String> paths = getSubQueryPaths(entry.getValue(), path);
        String groupId = entry.getKey();
        handleTimseriesQuery(groupId, paths, res);
      }
    }
    return res;
  }

  /**
   * Get all query path in storage group relatively to query path
   */
  private List<String> getSubQueryPaths(Set<String> stoageGroupList, String queryPath) {
    List<String> paths = new ArrayList<>();
    for (String storageGroup : stoageGroupList) {
      if (storageGroup.length() >= queryPath.length()) {
        paths.add(storageGroup);
      } else {
        StringBuilder path = new StringBuilder();
        String[] storageGroupNodes = storageGroup.split(DOUB_SEPARATOR);
        String[] queryPathNodes = queryPath.split(DOUB_SEPARATOR);
        for(int  i = 0 ; i < queryPathNodes.length ; i++){
          if(i >= storageGroupNodes.length){
            path.append(queryPathNodes[i]).append(SINGLE_SEPARATOR);
          } else {
            path.append(storageGroupNodes[i]).append(SINGLE_SEPARATOR);
          }
        }
        paths.add(path.deleteCharAt(path.length()-1).toString());
      }
    }
    return paths;
  }

  /**
   * Handle query timeseries in one data group
   *
   * @param groupId data group id
   */
  private void handleTimseriesQuery(String groupId, List<String> pathList, List<List<String>> res)
      throws ProcessorException, InterruptedException {
    QueryTimeSeriesRequest request = new QueryTimeSeriesRequest(groupId,
        readMetadataConsistencyLevel, pathList);
    SingleQPTask task = new SingleQPTask(false, request);

    LOGGER.debug("Execute show timeseries {} statement for group {}.", pathList, groupId);
    /** Check if the plan can be executed locally. **/
    if (canHandleQueryByGroupId(groupId)) {
      LOGGER.debug("Execute show timeseries {} statement locally for group {}.", pathList, groupId);
      res.addAll(queryTimeSeriesLocally(pathList, groupId, task));
    } else {
      try {
        PeerId holder = RaftUtils.getRandomPeerID(groupId);
        res.addAll(queryTimeSeries(task, holder));
      } catch (RaftConnectionException e) {
        LOGGER.error(e.getMessage());
        throw new ProcessorException("Raft connection occurs error.", e);
      }
    }
  }

  public String processMetadataInStringQuery()
      throws InterruptedException, ProcessorException {
    Set<String> groupIdSet = router.getAllGroupId();

    List<String> metadataList = new ArrayList<>(groupIdSet.size());
    List<SingleQPTask> taskList = new ArrayList<>();
    for (String groupId : groupIdSet) {
      QueryMetadataInStringRequest request = new QueryMetadataInStringRequest(groupId,
          readMetadataConsistencyLevel);
      SingleQPTask task = new SingleQPTask(false, request);
      taskList.add(task);

      LOGGER.debug("Execute show metadata in string statement for group {}.", groupId);
      /** Check if the plan can be executed locally. **/
      if (canHandleQueryByGroupId(groupId)) {
        LOGGER.debug("Execute show metadata in string statement locally for group {}.", groupId);
        asyncQueryMetadataInStringLocally(groupId, task);
      } else {
        try {
          PeerId holder = RaftUtils.getRandomPeerID(groupId);
          asyncSendNonQueryTask(task, holder, 0);
        } catch (RaftConnectionException e) {
          LOGGER.error(e.getMessage());
          throw new ProcessorException("Raft connection occurs error.", e);
        }
      }
    }
    for (int i = 0; i < taskList.size(); i++) {
      SingleQPTask task = taskList.get(i);
      task.await();
      BasicResponse response = task.getResponse();
      if (response == null || !response.isSuccess()) {
        LOGGER.error("Execute show timeseries statement false.");
        throw new ProcessorException();
      }
      metadataList.add(((QueryMetadataInStringResponse)response).getMetadata());
    }
    return combineMetadataInStringList(metadataList);
  }

  /**
   * Handle "show timeseries <path>" statement
   *
   * @param pathList column path
   */
  private List<List<String>> queryTimeSeriesLocally(List<String> pathList, String groupId,
      SingleQPTask task)
      throws InterruptedException, ProcessorException {
    final byte[] reqContext = RaftUtils.createRaftRequestContext();
    DataPartitionRaftHolder dataPartitionHolder = RaftUtils.getDataPartitonRaftHolder(groupId);

    /** Check consistency level**/
    if (readMetadataConsistencyLevel == ClusterConstant.WEAK_CONSISTENCY_LEVEL) {
      QueryTimeSeriesResponse response = QueryTimeSeriesResponse
          .createEmptyResponse(groupId);
      try {
        for (String path : pathList) {
          response.addTimeSeries(mManager.getShowTimeseriesPath(path));
        }
      } catch (final PathErrorException e) {
        response = QueryTimeSeriesResponse.createErrorResponse(groupId, e.getMessage());
      }
      task.run(response);
    } else {
      ((RaftService) dataPartitionHolder.getService()).getNode()
          .readIndex(reqContext, new ReadIndexClosure() {

            @Override
            public void run(Status status, long index, byte[] reqCtx) {
              QueryTimeSeriesResponse response = QueryTimeSeriesResponse
                  .createEmptyResponse(groupId);
              if (status.isOk()) {
                try {
                  LOGGER.debug("start to read");
                  for (String path : pathList) {
                    response.addTimeSeries(mManager.getShowTimeseriesPath(path));
                  }
                } catch (final PathErrorException e) {
                  response = QueryTimeSeriesResponse.createErrorResponse(groupId, e.getMessage());
                }
              } else {
                response = QueryTimeSeriesResponse
                    .createErrorResponse(groupId, status.getErrorMsg());
              }
              task.run(response);
            }
          });
    }
    task.await();
    QueryTimeSeriesResponse response = (QueryTimeSeriesResponse) task.getResponse();
    if (response == null || !response.isSuccess()) {
      LOGGER.error("Execute show timeseries {} statement false.", pathList);
      throw new ProcessorException();
    }
    return response.getTimeSeries();
  }

  private List<List<String>> queryTimeSeries(SingleQPTask task, PeerId leader)
      throws InterruptedException, RaftConnectionException {
    BasicResponse response = asyncHandleNonQueryTaskGetRes(task, leader, 0);
    return response == null ? new ArrayList<>()
        : ((QueryTimeSeriesResponse) response).getTimeSeries();
  }

  /**
   * Handle "show storage group" statement locally
   *
   * @return Set of storage group name
   */
  private Set<String> queryStorageGroupLocally() throws InterruptedException {
    final byte[] reqContext = RaftUtils.createRaftRequestContext();
    QueryStorageGroupRequest request = new QueryStorageGroupRequest(
        ClusterConfig.METADATA_GROUP_ID, readMetadataConsistencyLevel);
    SingleQPTask task = new SingleQPTask(false, request);
    MetadataRaftHolder metadataHolder = (MetadataRaftHolder) server.getMetadataHolder();
    if (readMetadataConsistencyLevel == ClusterConstant.WEAK_CONSISTENCY_LEVEL) {
      QueryStorageGroupResponse response;
      try {
        response = QueryStorageGroupResponse
            .createSuccessResponse(metadataHolder.getFsm().getAllStorageGroups());
      } catch (final PathErrorException e) {
        response = QueryStorageGroupResponse.createErrorResponse(e.getMessage());
      }
      task.run(response);
    } else {
      ((RaftService) metadataHolder.getService()).getNode()
          .readIndex(reqContext, new ReadIndexClosure() {

            @Override
            public void run(Status status, long index, byte[] reqCtx) {
              QueryStorageGroupResponse response;
              if (status.isOk()) {
                try {
                  response = QueryStorageGroupResponse
                      .createSuccessResponse(metadataHolder.getFsm().getAllStorageGroups());
                } catch (final PathErrorException e) {
                  response = QueryStorageGroupResponse.createErrorResponse(e.getMessage());
                }
              } else {
                response = QueryStorageGroupResponse.createErrorResponse(status.getErrorMsg());
              }
              task.run(response);
            }
          });
    }
    task.await();
    return ((QueryStorageGroupResponse) task.getResponse()).getStorageGroups();
  }

  /**
   * Handle "show timeseries" statement
   */
  private void asyncQueryMetadataInStringLocally(String groupId, SingleQPTask task) {
    final byte[] reqContext = RaftUtils.createRaftRequestContext();
    DataPartitionRaftHolder dataPartitionHolder = (DataPartitionRaftHolder) server
        .getDataPartitionHolder(groupId);
    if (readMetadataConsistencyLevel == ClusterConstant.WEAK_CONSISTENCY_LEVEL) {
      QueryMetadataInStringResponse response = QueryMetadataInStringResponse
          .createSuccessResponse(groupId, mManager.getMetadataInString());
      response.addResult(true);
      task.run(response);
    } else {
      ((RaftService) dataPartitionHolder.getService()).getNode()
          .readIndex(reqContext, new ReadIndexClosure() {

            @Override
            public void run(Status status, long index, byte[] reqCtx) {
              QueryMetadataInStringResponse response;
              if (status.isOk()) {
                LOGGER.debug("start to read");
                response = QueryMetadataInStringResponse
                    .createSuccessResponse(groupId, mManager.getMetadataInString());
                response.addResult(true);
              } else {
                response = QueryMetadataInStringResponse
                    .createErrorResponse(groupId, status.getErrorMsg());
                response.addResult(false);
              }
              task.run(response);
            }
          });
    }
  }

  /**
   * Combine multiple metadata in String format into single String
   *
   * @return single String of all metadata
   */
  private String combineMetadataInStringList(List<String> metadataList) {
    return MManager.combineMetadataInStrings((String[]) metadataList.toArray());
  }
}
