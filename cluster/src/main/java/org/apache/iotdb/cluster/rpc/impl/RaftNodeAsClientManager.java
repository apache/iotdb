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
package org.apache.iotdb.cluster.rpc.impl;

import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.cluster.callback.QPTask;
import org.apache.iotdb.cluster.callback.QPTask.TaskState;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.rpc.NodeAsClient;
import org.apache.iotdb.cluster.rpc.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.response.DataGroupNonQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage resource of @NodeAsClient
 */
public class RaftNodeAsClientManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeAsClientManager.class);

  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();
  /**
   * Timeout limit for a task, the unit is milliseconds
   */
  private static final int TASK_TIMEOUT_MS = CLUSTER_CONFIG.getTaskTimeoutMs();

  /**
   * Max valid number of @NodeAsClient usage, represent the number can run simultaneously at the
   * same time
   */
  private static final int MAX_VALID_CLIENT_NUM = CLUSTER_CONFIG.getMaxNumOfInnerRpcClient();

  /**
   * Max request number in queue
   */
  private static final int MAX_QUEUE_CLIENT_NUM = CLUSTER_CONFIG.getMaxNumOfInnerRpcClient();

  /**
   * RaftNodeAsClient singleton
   */
  private final RaftNodeAsClient client = new RaftNodeAsClient();

  /**
   * Number of clients in use
   */
  private AtomicInteger validClientNum = new AtomicInteger(0);

  /**
   * Number of requests for clients in queue
   */
  private int queueClientNum = 0;

  /**
   * Lock to update validClientNum
   */
  private ReentrantLock numLock = new ReentrantLock();

  private RaftNodeAsClientManager(){

  }

  /**
   * Try to get client, return null if num of queue client exceeds threshold.
   */
  public RaftNodeAsClient getRaftNodeAsClient() {
    try {
      numLock.lock();
      if (validClientNum.get() < MAX_VALID_CLIENT_NUM) {
        validClientNum.incrementAndGet();
        return client;
      }
      if (queueClientNum >= MAX_QUEUE_CLIENT_NUM) {
        return null;
      }
      queueClientNum++;
    }finally {
      numLock.unlock();
    }
    return tryToGetClient();
  }

  /**
   * Check whether it can get the client
   */
  private RaftNodeAsClient tryToGetClient() {
    for(;;){
      if(validClientNum.get() < MAX_VALID_CLIENT_NUM){
        try{
          numLock.lock();
          if(validClientNum.get() < MAX_VALID_CLIENT_NUM){
            validClientNum.incrementAndGet();
            queueClientNum--;
            return client;
          }
        }finally {
          numLock.unlock();
        }
      }
    }
  }

  public void releaseClient() {
    validClientNum.decrementAndGet();
  }

  public void init(){
    client.init();
  }

  public void shutdown(){
    client.shutdown();
  }

  public static final RaftNodeAsClientManager getInstance() {
    return RaftNodeAsClientManager.ClientManagerHolder.INSTANCE;
  }

  private static class ClientManagerHolder {

    private static final RaftNodeAsClientManager INSTANCE = new RaftNodeAsClientManager();

    private ClientManagerHolder() {

    }
  }

  /**
   * Implement NodeAsClient with Raft Service
   *
   * @see org.apache.iotdb.cluster.rpc.NodeAsClient
   */
  public class RaftNodeAsClient implements NodeAsClient {

    /**
     * Rpc Service Client
     */
    private BoltCliClientService boltClientService;

    private RaftNodeAsClient(){
    }

    private void init(){
      boltClientService = new BoltCliClientService();
      boltClientService.init(new CliOptions());
    }

    @Override
    public void asyncHandleRequest(BasicRequest request, PeerId leader,
        QPTask qpTask)
        throws RaftConnectionException {
      LOGGER.debug("Node as client to send request to leader: {}", leader);
      try {
        boltClientService.getRpcClient()
            .invokeWithCallback(leader.getEndpoint().toString(), request,
                new InvokeCallback() {

                  @Override
                  public void onResponse(Object result) {
                    BasicResponse response = (BasicResponse) result;
                    releaseClient();
                    qpTask.run(response);
                  }

                  @Override
                  public void onException(Throwable e) {
                    LOGGER.error("Bolt rpc client occurs errors when handling Request", e);
                    qpTask.setTaskState(TaskState.EXCEPTION);
                    releaseClient();
                    qpTask.run(DataGroupNonQueryResponse
                        .createErrorInstance(request.getGroupID(), e.getMessage()));
                  }

                  @Override
                  public Executor getExecutor() {
                    return null;
                  }
                }, TASK_TIMEOUT_MS);
      } catch (RemotingException | InterruptedException e) {
        LOGGER.error(e.getMessage());
        qpTask.setTaskState(TaskState.EXCEPTION);
        releaseClient();
        qpTask.run(DataGroupNonQueryResponse
            .createErrorInstance(request.getGroupID(), e.getMessage()));
        throw new RaftConnectionException(e);
      }
    }

    @Override
    public void syncHandleRequest(BasicRequest request, PeerId leader,
        QPTask qpTask)
        throws RaftConnectionException {
      try {
        BasicResponse response = (BasicResponse) boltClientService.getRpcClient()
            .invokeSync(leader.getEndpoint().toString(), request, TASK_TIMEOUT_MS);
        qpTask.run(response);
      } catch (RemotingException | InterruptedException e) {
        qpTask.setTaskState(TaskState.EXCEPTION);
        qpTask.run(DataGroupNonQueryResponse
            .createErrorInstance(request.getGroupID(), e.getMessage()));
        throw new RaftConnectionException(e);
      } finally {
        releaseClient();
      }
    }

    /**
     * Shut down client
     */
    @Override
    public void shutdown() {
      boltClientService.shutdown();
    }

  }

}
