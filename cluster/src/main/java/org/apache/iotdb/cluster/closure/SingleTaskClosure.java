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
package org.apache.iotdb.cluster.closure;

import com.alipay.sofa.jraft.entity.PeerId;
import java.util.concurrent.CountDownLatch;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.rpc.bolt.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.bolt.response.BasicResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;

/**
 * Process single task.
 */
public class SingleTaskClosure extends TaskClosure {

  /**
   * Task response
   */
  private BasicResponse basicResponse;
  /**
   * Task request
   */
  private BasicRequest basicRequest;
  /**
   * Num of sub-task
   */
  private CountDownLatch countDownLatch;

  public SingleTaskClosure(CountDownLatch countDownLatch, BasicRequest basicRequest,
      boolean isSyncTask) {
    this.countDownLatch = countDownLatch;
    this.basicRequest = basicRequest;
    setSyncTask(isSyncTask);
  }

  /**
   * Process response. If it's necessary to redirect leader, redo the task.
   */
  @Override
  public void run(BasicResponse response) throws RaftConnectionException {
    if (!response.isSuccess() && response.isRedirected()) {
      PeerId peerId;
      try {
        peerId = RaftUtils.getLeader(basicRequest.getGroupID());
      } catch (Exception e) {
        countDownLatch.countDown();
        throw new RaftConnectionException(
            String.format("Can not connect to the raft group %s", basicRequest.getGroupID()));
      }
      redoTask(basicRequest, peerId);
    }
    basicResponse = response;
    countDownLatch.countDown();
  }

  public BasicResponse getBasicResponse() {
    return basicResponse;
  }

  public void setBasicResponse(BasicResponse basicResponse) {
    this.basicResponse = basicResponse;
  }

}
