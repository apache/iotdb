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
package org.apache.iotdb.cluster.utils.query;

import com.alipay.sofa.jraft.entity.PeerId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.task.QPTask.TaskState;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.query.reader.ClusterRpcBatchDataReader;
import org.apache.iotdb.cluster.query.reader.ClusterSeriesReader;
import org.apache.iotdb.cluster.rpc.raft.NodeAsClient;
import org.apache.iotdb.cluster.rpc.raft.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.raft.request.querydata.QuerySeriesDataRequest;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.raft.response.querydata.QuerySeriesDataResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;

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
   * Create cluster series reader and get the first batch data
   *
   * @param peerId query node to fetch data
   * @param queryPlan query plan to be executed in query node
   * @param readDataConsistencyLevel consistency level of read data
   * @param pathType path type
   * @param taskId task id assigned by coordinator node
   * @param queryRounds represent the rounds of query
   */
  public static Map<String, ClusterSeriesReader> createClusterSeriesReader(String groupId,
      PeerId peerId, QueryPlan queryPlan, int readDataConsistencyLevel, PathType pathType,
      String taskId, long queryRounds)
      throws IOException, RaftConnectionException {

    /** handle request **/
    List<PhysicalPlan> physicalPlanList = new ArrayList<>();
    physicalPlanList.add(queryPlan);
    BasicRequest request = new QuerySeriesDataRequest(groupId, taskId, readDataConsistencyLevel,
        physicalPlanList, pathType, queryRounds);
    QuerySeriesDataResponse response = (QuerySeriesDataResponse) handleQueryRequest(request, peerId,
        0);

    /** create cluster series reader **/
    Map<String, ClusterSeriesReader> allSeriesReader = new HashMap<>();
    List<Path> paths = queryPlan.getPaths();
    List<TSDataType> seriesType = response.getSeriesType();
    List<BatchData> seriesBatchData = response.getSeriesBatchData();
    for (int i =0 ; i < paths.size(); i++) {
      String seriesPath = paths.get(i).getFullPath();
      TSDataType dataType = seriesType.get(i);
      IBatchReader batchDataReader = new ClusterRpcBatchDataReader(peerId, taskId,
          pathType, seriesBatchData.get(i));
      ClusterSeriesReader seriesReader = new ClusterSeriesReader(batchDataReader, seriesPath,
          dataType);
      allSeriesReader.put(seriesPath, seriesReader);
    }
    return allSeriesReader;
  }

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
}
