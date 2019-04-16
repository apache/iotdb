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
import org.apache.iotdb.cluster.concurrent.pool.QPTaskManager;
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
 * Execute batch statement task. It's thread-safe.
 */
public class BatchQPTask extends MultiQPTask {


  private static final Logger LOGGER = LoggerFactory.getLogger(BatchQPTask.class);

  /**
   * Record the index of physical plans in a data group. The index means the position in batchResult
   * String: group id
   */
  private Map<String, List<Integer>> planIndexMap;

  /**
   * Batch result
   */
  private int[] batchResult;

  /**
   * Mark if the batch is all successful.
   */
  private boolean isAllSuccessful;

  /**
   * Batch error message.
   */
  private String batchErrorMessage;

  /**
   * Lock to update result
   */
  private ReentrantLock lock = new ReentrantLock();

  private NonQueryExecutor executor;


  public BatchQPTask(int taskNum, BatchResult batchResult, Map<String, SingleQPTask> taskMap,
      Map<String, List<Integer>> planIndexMap) {
    super(false, taskNum, TaskType.BATCH);
    this.batchResult = batchResult.getResult();
    this.isAllSuccessful = batchResult.isAllSuccessful();
    this.batchErrorMessage = batchResult.getBatchErrorMessage();
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
  public void run(BasicResponse basicResponse) {
    lock.lock();
    try {
      String groupId = basicResponse.getGroupId();
      List<Boolean> results = basicResponse.getResults();
      List<Integer> indexList = planIndexMap.get(groupId);
      for (int i = 0; i < indexList.size(); i++) {
        if (i >= results.size()) {
          batchResult[indexList.get(i)] = Statement.EXECUTE_FAILED;
        } else {
          batchResult[indexList.get(i)] =
              results.get(i) ? Statement.SUCCESS_NO_INFO : Statement.EXECUTE_FAILED;
        }
      }
      if (!basicResponse.isSuccess()) {
        isAllSuccessful = false;
        batchErrorMessage = basicResponse.getErrorMsg();
      }
    } finally {
      lock.unlock();
    }
    taskCountDownLatch.countDown();
  }

  public void execute(NonQueryExecutor executor) {
    this.executor = executor;

    for (Entry<String, SingleQPTask> entry : taskMap.entrySet()) {
      String groupId = entry.getKey();
      SingleQPTask subTask = entry.getValue();
      Future<?> taskThread;
      if (QPExecutorUtils.canHandleNonQueryByGroupId(groupId)) {
        taskThread = QPTaskManager.getInstance()
            .submit(() -> executeLocalSubTask(subTask, groupId));
      } else {
        PeerId leader = RaftUtils.getLeaderPeerID(groupId);
        taskThread = QPTaskManager.getInstance()
            .submit(() -> executeRpcSubTask(subTask, leader, groupId));
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
      this.run(subTask.getResponse());
    } catch (InterruptedException e) {
      LOGGER.error("Handle sub task locally failed.");
      this.run(DataGroupNonQueryResponse.createErrorResponse(groupId, e.getMessage()));
    }
  }

  /**
   * Execute RPC sub task
   */
  private void executeRpcSubTask(SingleQPTask subTask, PeerId leader, String groupId) {
    try {
      executor.asyncHandleNonQueryTask(subTask, leader);
      this.run(subTask.getResponse());
    } catch (RaftConnectionException | InterruptedException e) {
      LOGGER.error("Async handle sub task failed.");
      this.run(DataGroupNonQueryResponse.createErrorResponse(groupId, e.getMessage()));
    }
  }

  public boolean isAllSuccessful() {
    return isAllSuccessful;
  }

  public void setAllSuccessful(boolean allSuccessful) {
    isAllSuccessful = allSuccessful;
  }

  public String getBatchErrorMessage() {
    return batchErrorMessage;
  }

  public void setBatchErrorMessage(String batchErrorMessage) {
    this.batchErrorMessage = batchErrorMessage;
  }
}
