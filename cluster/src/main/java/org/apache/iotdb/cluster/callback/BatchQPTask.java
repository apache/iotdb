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
package org.apache.iotdb.cluster.callback;

import com.alipay.sofa.jraft.entity.PeerId;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.executor.NonQueryExecutor;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.response.DataGroupNonQueryResponse;
import org.apache.iotdb.cluster.rpc.service.TSServiceClusterImpl.BatchResult;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute batch statement task. It's thread-safe.
 */
public class BatchQPTask extends QPTask {


  private static final Logger LOGGER = LoggerFactory.getLogger(BatchQPTask.class);

  /**
   * Each request is corresponding to a group id. String: group id
   */
  private Map<String, SingleQPTask> taskMap;

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

  public BatchQPTask(int taskNum, BatchResult batchResult, Map<String, SingleQPTask> taskMap,
      Map<String, List<Integer>> planIndexMap) {
    super(false, taskNum, TaskState.INITIAL);
    this.batchResult = batchResult.getResult();
    this.isAllSuccessful = batchResult.isAllSuccessful();
    this.batchErrorMessage = batchResult.getBatchErrorMessage();
    this.taskMap = taskMap;
    this.planIndexMap = planIndexMap;
  }

  public void execute(NonQueryExecutor executor) {
    for (Entry<String, SingleQPTask> entry : taskMap.entrySet()) {
      String groupId = entry.getKey();
      PeerId leader = RaftUtils.getTargetPeerID(groupId);
      SingleQPTask subTask = entry.getValue();
      Thread thread;
      if (executor.canHandleNonQueryByGroupId(groupId)) {
        thread = new Thread(() -> {
          try {
            executor.handleDataGroupRequestLocally(groupId, subTask);
            this.run(subTask.getResponse());
          } catch (InterruptedException e) {
            LOGGER.info("Handle sub task locally failed.");
            this.run(new DataGroupNonQueryResponse(groupId, false, null, e.toString()));
          }
        });
        thread.start();
      } else {
        thread = new Thread(() -> {
          try {
            executor.asyncHandleTask(subTask, leader);
            this.run(subTask.getResponse());
          } catch (RaftConnectionException | InterruptedException e) {
            LOGGER.info("Async handle sub task failed.");
            this.run(new DataGroupNonQueryResponse(groupId, false, null, e.toString()));
          }
        });
        thread.start();
      }
    }
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
      if(!basicResponse.isSuccess()){
        isAllSuccessful = false;
        batchErrorMessage = basicResponse.getErrorMsg();
      }
    } finally {
      lock.unlock();
    }
    taskCountDownLatch.countDown();
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
