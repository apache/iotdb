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

import org.apache.iotdb.cluster.rpc.raft.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process task(s) for only one raft group, which is used for operations except for querying data.
 */
public class SingleQPTask extends QPTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingleQPTask.class);

  private static final int TASK_NUM = 1;

  public SingleQPTask(boolean isSyncTask, BasicRequest request) {
    super(isSyncTask, TASK_NUM, TaskState.INITIAL, TaskType.SINGLE);
    this.request = request;
  }

  /**
   * Process response. If it's necessary to redirect leader, redo the task.
   */
  @Override
  public void receive(BasicResponse response) {
    if(taskState != TaskState.EXCEPTION) {
      this.response = response;
      if(response == null){
        LOGGER.error("Response is null");
      } else if (response.isRedirected()) {
        this.taskState = TaskState.REDIRECT;
      } else {
        this.taskState = TaskState.FINISH;
      }
    }
    this.taskCountDownLatch.countDown();
  }

  @Override
  public void shutdown() {
    if (taskCountDownLatch.getCount() != 0) {
      this.taskCountDownLatch.countDown();
    }
  }
}
