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
package org.apache.iotdb.cluster.rpc.raft.impl;

import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import java.util.LinkedList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.cluster.qp.task.QPTask.TaskState;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.task.SingleQPTask;
import org.apache.iotdb.cluster.rpc.raft.NodeAsClient;
import org.apache.iotdb.cluster.rpc.raft.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.apache.iotdb.cluster.qp.task.QueryTask;
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
  private static final int TASK_TIMEOUT_MS = CLUSTER_CONFIG.getQpTaskTimeout();

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
   * RaftNodeAsClient list
   */
  private final LinkedList<RaftNodeAsClient> clientList = new LinkedList<>();

  /**
   * Number of clients in use
   */
  private AtomicInteger clientNumInUse = new AtomicInteger(0);

  /**
   * Number of requests for clients in queue
   */
  private int queueClientNum = 0;

  /**
   * Lock to update clientNumInUse
   */
  private ReentrantLock resourceLock = new ReentrantLock();

  /**
   * Mark whether system is shutting down
   */
  private volatile boolean isShuttingDown;

  /**
   * Interval of thread sleep, unit is millisecond.
   */
  private static final int THREAD_SLEEP_INTERVAL = 10;

  private RaftNodeAsClientManager() {

  }

  public void init() {
    isShuttingDown = false;
  }

  /**
   * Try to get clientList, return null if num of queue clientList exceeds threshold.
   */
  public RaftNodeAsClient getRaftNodeAsClient() throws RaftConnectionException {
    try {
      resourceLock.lock();
      if (queueClientNum >= MAX_QUEUE_CLIENT_NUM) {
        throw new RaftConnectionException(String
            .format("Raft inner rpc clients have reached the max numbers %s",
                CLUSTER_CONFIG.getMaxNumOfInnerRpcClient() + CLUSTER_CONFIG
                    .getMaxQueueNumOfInnerRpcClient()));
      }
      checkShuttingDown();
      if (clientNumInUse.get() < MAX_VALID_CLIENT_NUM) {
        clientNumInUse.incrementAndGet();
        return getClient();
      }
      queueClientNum++;
    } finally {
      resourceLock.unlock();
    }
    return tryToGetClient();
  }

  private void checkShuttingDown() throws RaftConnectionException {
    if (isShuttingDown) {
      throw new RaftConnectionException(
          "Reject to provide RaftNodeAsClient client because cluster system is shutting down");
    }
  }

  /**
   * Check whether it can get the clientList
   */
  private RaftNodeAsClient tryToGetClient() throws RaftConnectionException {
    for (; ; ) {
      if (clientNumInUse.get() < MAX_VALID_CLIENT_NUM) {
        resourceLock.lock();
        try {
          checkShuttingDown();
          if (clientNumInUse.get() < MAX_VALID_CLIENT_NUM) {
            clientNumInUse.incrementAndGet();
            queueClientNum--;
            return getClient();
          }
        } catch (RaftConnectionException e) {
          queueClientNum--;
          throw new RaftConnectionException(e);
        } finally {
          resourceLock.unlock();
        }
      }
      try {
        Thread.sleep(THREAD_SLEEP_INTERVAL);
      } catch (InterruptedException e) {
        throw new RaftConnectionException("An error occurred when trying to get NodeAsClient", e);
      }
    }
  }

  /**
   * No-safe method, get client
   */
  private RaftNodeAsClient getClient() {
    if (clientList.isEmpty()) {
      return new RaftNodeAsClient();
    } else {
      return clientList.removeFirst();
    }
  }

  /**
   * Release usage of a client
   */
  public void releaseClient(RaftNodeAsClient client) {
    resourceLock.lock();
    try {
      clientNumInUse.decrementAndGet();
      clientList.addLast(client);
    } finally {
      resourceLock.unlock();
    }
  }

  public void shutdown() throws InterruptedException {
    isShuttingDown = true;
    while (clientNumInUse.get() != 0 && queueClientNum != 0) {
      // wait until releasing all usage of clients.
      Thread.sleep(THREAD_SLEEP_INTERVAL);
    }
    while (!clientList.isEmpty()) {
      clientList.removeFirst().shutdown();
    }
  }

  /**
   * Get client number in use
   */
  public int getClientNumInUse() {
    return clientNumInUse.get();
  }

  /**
   * Get client number in queue
   */
  public int getClientNumInQueue() {
    return queueClientNum;
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
   * @see NodeAsClient
   */
  public class RaftNodeAsClient implements NodeAsClient {

    /**
     * Rpc Service Client
     */
    private BoltCliClientService boltClientService;

    private RaftNodeAsClient() {
      init();
    }

    private void init() {
      boltClientService = new BoltCliClientService();
      boltClientService.init(new CliOptions());
    }

    @Override
    public void asyncHandleRequest(BasicRequest request, PeerId leader,
        SingleQPTask qpTask)
        throws RaftConnectionException {
      LOGGER.debug("Node as client to send request to leader: {}", leader);
      try {
        boltClientService.getRpcClient()
            .invokeWithCallback(leader.getEndpoint().toString(), request,
                new InvokeCallback() {

                  @Override
                  public void onResponse(Object result) {
                    BasicResponse response = (BasicResponse) result;
                    releaseClient(RaftNodeAsClient.this);
                    qpTask.run(response);
                  }

                  @Override
                  public void onException(Throwable e) {
                    LOGGER.error("Bolt rpc client occurs errors when handling Request", e);
                    qpTask.setTaskState(TaskState.EXCEPTION);
                    releaseClient(RaftNodeAsClient.this);
                    qpTask.run(null);
                  }

                  @Override
                  public Executor getExecutor() {
                    return null;
                  }
                }, TASK_TIMEOUT_MS);
      } catch (RemotingException | InterruptedException e) {
        LOGGER.error(e.getMessage());
        qpTask.setTaskState(TaskState.EXCEPTION);
        releaseClient(RaftNodeAsClient.this);
        boltClientService.disconnect(leader.getEndpoint());
        qpTask.run(null);
        throw new RaftConnectionException(e);
      }
    }

    @Override
    public QueryTask syncHandleRequest(BasicRequest request, PeerId peerId) {
      try {
        BasicResponse response = (BasicResponse) boltClientService.getRpcClient()
            .invokeSync(peerId.getEndpoint().toString(), request, TASK_TIMEOUT_MS);
        return new QueryTask(response, TaskState.FINISH);
      } catch (RemotingException | InterruptedException e) {
        return new QueryTask(null, TaskState.EXCEPTION);
      } finally {
        releaseClient(RaftNodeAsClient.this);
      }
    }

    /**
     * Shut down clientList
     */
    @Override
    public void shutdown() {
      boltClientService.shutdown();
    }

  }

}
