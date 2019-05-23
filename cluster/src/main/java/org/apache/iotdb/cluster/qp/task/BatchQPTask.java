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
package org.apache.iotdb.cluster.qp.task;

import com.alipay.sofa.jraft.entity.PeerId;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.cluster.concurrent.pool.QPTaskThreadManager;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.executor.NonQueryExecutor;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.raft.response.nonquery.DataGroupNonQueryResponse;
import org.apache.iotdb.cluster.service.TSServiceClusterImpl.BatchResult;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute batch statement tasks. It's thread-safe.
 */
public class BatchQPTask extends MultiQPTask {


  private static final Logger LOGGER = LoggerFactory.getLogger(BatchQPTask.class);

  /**
   * Record the index of physical plans in a data group. The index means the position in result
   * String: group id
   */
  private Map<String, List<Integer>> planIndexMap;

  /**
   * Batch result array, mark the result type, which is in BatchResult
   */
  private int[] resultArray;

  /**
   * Batch result
   */
  private BatchResult batchResult;

  /**
   * Lock to update result
   */
  private ReentrantLock lock = new ReentrantLock();

  private NonQueryExecutor executor;


  public BatchQPTask(int taskNum, BatchResult result, Map<String, SingleQPTask> taskMap,
      Map<String, List<Integer>> planIndexMap) {
    super(false, taskNum, TaskType.BATCH);
    this.resultArray = result.getResultArray();
    this.batchResult = result;
    this.taskMap = taskMap;
    this.planIndexMap = planIndexMap;
    this.taskThreadMap = new HashMap<>();
  }

  /**
   * Process response
   *
   * @param basicResponse response from receiver
   */
  @Override
  public void receive(BasicResponse basicResponse) {
    lock.lock();
    try {
      String groupId = basicResponse.getGroupId();
      List<Boolean> results = basicResponse.getResults();
      List<Integer> indexList = planIndexMap.get(groupId);
      List<String> errorMsgList = ((DataGroupNonQueryResponse) basicResponse).getErrorMsgList();
      int errorMsgIndex = 0;
      for (int i = 0; i < indexList.size(); i++) {
        if (i >= results.size()) {
          resultArray[indexList.get(i)] = Statement.EXECUTE_FAILED;
          batchResult.addBatchErrorMessage(indexList.get(i), basicResponse.getErrorMsg());
        } else {
          if (results.get(i)) {
            resultArray[indexList.get(i)] = Statement.SUCCESS_NO_INFO;
          } else {
            resultArray[indexList.get(i)] = Statement.EXECUTE_FAILED;
            batchResult.addBatchErrorMessage(indexList.get(i), errorMsgList.get(errorMsgIndex++));
          }
        }
      }
      if (!basicResponse.isSuccess()) {
        batchResult.setAllSuccessful(false);
      }
    } finally {
      lock.unlock();
    }
    taskCountDownLatch.countDown();
  }

  public void executeBy(NonQueryExecutor executor) {
    this.executor = executor;

    for (Entry<String, SingleQPTask> entry : taskMap.entrySet()) {
      String groupId = entry.getKey();
      SingleQPTask subTask = entry.getValue();
      Future<?> taskThread;
      if (QPExecutorUtils.canHandleNonQueryByGroupId(groupId)) {
        taskThread = QPTaskThreadManager.getInstance()
            .submit(() -> executeLocalSubTask(subTask, groupId));
      } else {
        PeerId leader = RaftUtils.getLeaderPeerID(groupId);
        subTask.setTargetNode(leader);
        taskThread = QPTaskThreadManager.getInstance()
            .submit(() -> executeRpcSubTask(subTask, groupId));
      }
      taskThreadMap.put(groupId, taskThread);
    }
  }

  /**
   * Execute local sub task
   */
  private void executeLocalSubTask(QPTask subTask, String groupId) {
    try {
      executor.handleNonQueryRequestLocally(groupId, subTask);
      this.receive(subTask.getResponse());
    } catch (InterruptedException e) {
      LOGGER.error("Handle sub task locally failed.");
      this.receive(DataGroupNonQueryResponse.createErrorResponse(groupId, e.getMessage()));
    }
  }

  /**
   * Execute RPC sub task
   */
  private void executeRpcSubTask(SingleQPTask subTask, String groupId) {
    try {
      executor.syncHandleNonQueryTask(subTask);
      this.receive(subTask.getResponse());
    } catch (RaftConnectionException | InterruptedException e) {
      LOGGER.error("Async handle sub task failed.");
      this.receive(DataGroupNonQueryResponse.createErrorResponse(groupId, e.getMessage()));
    }
  }
}
