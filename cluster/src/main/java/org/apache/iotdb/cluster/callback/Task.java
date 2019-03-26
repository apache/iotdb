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
import java.util.concurrent.CountDownLatch;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.rpc.bolt.NodeAsClient;
import org.apache.iotdb.cluster.rpc.bolt.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.bolt.response.BasicResponse;

public abstract class Task {

  /**
   * Task response
   */
  private BasicResponse response;

  /**
   * Task request
   */
  private BasicRequest request;
  /**
   * Whether this's a synchronization task or not.
   */
  private boolean isSyncTask;

  /**
   * Num of sub-task
   */
  private CountDownLatch taskNum;
  /**
   * Describe task type
   */
  private TaskState taskState;

  public Task(boolean isSyncTask, CountDownLatch taskNum, TaskState taskState) {
    this.isSyncTask = isSyncTask;
    this.taskNum = taskNum;
    this.taskState = taskState;
  }

  /**
   * Process response
   *
   * @param basicResponse response from receiver
   */
  public abstract void run(BasicResponse basicResponse) throws RaftConnectionException;

  /**
   * Redo the task if last task is not sent to leader
   *
   * @param request request to be sent
   * @param peerId leader node
   */
  public void redoTask(BasicRequest request, PeerId peerId) throws RaftConnectionException {
    NodeAsClient client = new NodeAsClient();
    if (isSyncTask) {
      client.syncHandleRequest(request, peerId, this);
    } else {
      client.asyncHandleRequest(request, peerId, this);
    }
  }

  public boolean isSyncTask() {
    return isSyncTask;
  }

  public void setSyncTask(boolean syncTask) {
    isSyncTask = syncTask;
  }

  public CountDownLatch getTaskNum() {
    return taskNum;
  }

  public void setTaskNum(CountDownLatch taskNum) {
    this.taskNum = taskNum;
  }

  public TaskState getTaskState() {
    return taskState;
  }

  public void setTaskState(TaskState taskState) {
    this.taskState = taskState;
  }


  public BasicResponse getResponse() {
    return response;
  }

  public void setResponse(BasicResponse response) {
    this.response = response;
  }

  public BasicRequest getRequest() {
    return request;
  }

  public void setRequest(BasicRequest request) {
    this.request = request;
  }

  public enum TaskState {
    INITIAL, REDIRECT, FINISH
  }
}
