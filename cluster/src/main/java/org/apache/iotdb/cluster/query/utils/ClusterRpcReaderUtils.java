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
package org.apache.iotdb.cluster.query.utils;

import com.alipay.sofa.jraft.entity.PeerId;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.task.QPTask.TaskState;
import org.apache.iotdb.cluster.qp.task.QueryTask;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.rpc.raft.NodeAsClient;
import org.apache.iotdb.cluster.rpc.raft.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataByTimestampRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataByTimestampResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * Utils for cluster reader which needs to acquire data from remote query node.
 */
public class ClusterRpcReaderUtils {

  /**
   * Count limit to redo a task
   */
  private static final int TASK_MAX_RETRY = ClusterDescriptor.getInstance().getConfig()
      .getQpTaskRedoCount();

  /**
   * Create cluster series reader
   *
   * @param peerId query node to fetch data
   * @param readDataConsistencyLevel consistency level of read data
   * @param taskId task id assigned by coordinator node
   * @param queryRounds represent the rounds of query
   */
  public static BasicResponse createClusterSeriesReader(String groupId, PeerId peerId,
      int readDataConsistencyLevel, Map<PathType, QueryPlan> allQueryPlan, String taskId,
      List<Filter> filterList, long queryRounds)
      throws IOException, RaftConnectionException {

    /** handle request **/
    BasicRequest request = QuerySeriesDataRequest
        .createInitialQueryRequest(groupId, taskId, readDataConsistencyLevel,
            allQueryPlan, filterList,queryRounds);
    return handleQueryRequest(request, peerId, 0);
  }

  /**
   * Send query request to remote node and return response
   *
   * @param request query request
   * @param peerId target remote query node
   * @param taskRetryNum retry num of the request
   * @return Response from remote query node
   */
  private static BasicResponse handleQueryRequest(BasicRequest request, PeerId peerId,
      int taskRetryNum)
      throws RaftConnectionException {
    if (taskRetryNum > TASK_MAX_RETRY) {
      throw new RaftConnectionException(
          String.format("Query request retries reach the upper bound %s",
              TASK_MAX_RETRY));
    }
    NodeAsClient nodeAsClient = RaftUtils.getRaftNodeAsClient();
    QueryTask queryTask = nodeAsClient.syncHandleRequest(request, peerId);
    if (queryTask.getState() == TaskState.FINISH) {
      return queryTask.getBasicResponse();
    } else {
      return handleQueryRequest(request, peerId, taskRetryNum + 1);
    }
  }

  public static QuerySeriesDataResponse fetchBatchData(String groupID, PeerId peerId, String taskId,
      PathType pathType, List<String> fetchDataSeries, long queryRounds)
      throws RaftConnectionException {
    BasicRequest request = QuerySeriesDataRequest
        .createFetchDataRequest(groupID, taskId, pathType, fetchDataSeries, queryRounds);
    return (QuerySeriesDataResponse) handleQueryRequest(request, peerId, 0);
  }

  public static QuerySeriesDataByTimestampResponse fetchBatchDataByTimestamp(String groupId,
      PeerId peerId, String taskId, long queryRounds, List<Long> batchTimestamp,
      List<String> fetchDataSeries)
      throws RaftConnectionException {
    BasicRequest request = QuerySeriesDataByTimestampRequest
        .createRequest(groupId, queryRounds, taskId, batchTimestamp, fetchDataSeries);
    return (QuerySeriesDataByTimestampResponse) handleQueryRequest(request, peerId, 0);
  }

  /**
   * Release remote query resources
   *
   * @param groupId data group id
   * @param peerId target query node
   * @param taskId unique task id
   */
  public static void releaseRemoteQueryResource(String groupId, PeerId peerId, String taskId)
      throws RaftConnectionException {

    BasicRequest request = QuerySeriesDataRequest.createReleaseResourceRequest(groupId, taskId);
    handleQueryRequest(request, peerId, 0);
  }
}
