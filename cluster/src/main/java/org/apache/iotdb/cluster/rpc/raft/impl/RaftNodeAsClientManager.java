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
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import java.util.LinkedList;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.cluster.concurrent.pool.NodeAsClientThreadManager;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.task.QPTask.TaskState;
import org.apache.iotdb.cluster.qp.task.SingleQPTask;
import org.apache.iotdb.cluster.rpc.raft.NodeAsClient;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.apache.iotdb.db.exception.ProcessorException;
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
   * Max request number in queue
   */
  private static final int MAX_QUEUE_TASK_NUM = CLUSTER_CONFIG.getMaxQueueNumOfQPTask();

  /**
   * Node as client thread pool manager
   */
  private static final NodeAsClientThreadManager THREAD_POOL_MANAGER = NodeAsClientThreadManager
      .getInstance();

  /**
   * QPTask queue list
   */
  private final LinkedList<SingleQPTask> taskQueue = new LinkedList<>();

  /**
   * Lock to update clientNumInUse
   */
  private Lock resourceLock = new ReentrantLock();

  /**
   * Condition to get client
   */
  private Condition resourceCondition = resourceLock.newCondition();

  /**
   * Mark whether system is shutting down
   */
  private volatile boolean isShuttingDown;

  private RaftNodeAsClientManager() {

  }

  public void init() {
    isShuttingDown = false;
    for (int i = 0; i < CLUSTER_CONFIG.getConcurrentInnerRpcClientThread(); i++) {
      THREAD_POOL_MANAGER.execute(() -> {
        RaftNodeAsClient client = new RaftNodeAsClient();
        while (true) {
          consumeQPTask(client);
          if (Thread.currentThread().isInterrupted()) {
            break;
          }
        }
        client.shutdown();
      });
    }
  }

  /**
   * Produce qp task to be executed.
   */
  public void produceQPTask(SingleQPTask qpTask) throws RaftConnectionException {
    resourceLock.lock();
    try {
      checkShuttingDown();
      if (taskQueue.size() >= MAX_QUEUE_TASK_NUM) {
        throw new RaftConnectionException(String
            .format("Raft inner rpc clients have reached the max numbers %s",
                CLUSTER_CONFIG.getConcurrentInnerRpcClientThread() + CLUSTER_CONFIG
                    .getMaxQueueNumOfQPTask()));
      }
      taskQueue.addLast(qpTask);
      resourceCondition.signal();
    } finally {
      resourceLock.unlock();
    }
  }

  /**
   * Consume qp task
   */
  private void consumeQPTask(RaftNodeAsClient client) {
    resourceLock.lock();
    try {
      while (taskQueue.isEmpty()) {
        if (Thread.currentThread().isInterrupted()) {
          return;
        }
        resourceCondition.await();
      }
      client.asyncHandleRequest(taskQueue.removeFirst());
    } catch (InterruptedException e) {
      LOGGER.error("An error occurred when await for ResourceContidion", e);
    } finally {
      resourceLock.unlock();
    }
  }

  private void checkShuttingDown() throws RaftConnectionException {
    if (isShuttingDown) {
      throw new RaftConnectionException(
          "Reject to execute QPTask because cluster system is shutting down");
    }
  }

  public void shutdown() throws ProcessorException {
    isShuttingDown = true;
    THREAD_POOL_MANAGER.close(true, ClusterConstant.CLOSE_THREAD_POOL_BLOCK_TIMEOUT);
  }

  /**
   * Get qp task number in queue
   */
  public int getQPTaskNumInQueue() {
    return taskQueue.size();
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
    public void asyncHandleRequest(SingleQPTask qpTask) {
      LOGGER.debug("Node as client to send request to leader: {}", qpTask.getTargetNode());
      try {
        boltClientService.getRpcClient()
            .invokeWithCallback(qpTask.getTargetNode().getEndpoint().toString(),
                qpTask.getRequest(),
                new InvokeCallback() {

                  @Override
                  public void onResponse(Object result) {
                    BasicResponse response = (BasicResponse) result;
                    qpTask.receive(response);
                  }

                  @Override
                  public void onException(Throwable e) {
                    LOGGER.error("Bolt rpc client occurs errors when handling Request", e);
                    qpTask.setTaskState(TaskState.EXCEPTION);
                    qpTask.receive(null);
                  }

                  @Override
                  public Executor getExecutor() {
                    return null;
                  }
                }, TASK_TIMEOUT_MS);
      } catch (RemotingException | InterruptedException e) {
        LOGGER.error(e.getMessage());
        qpTask.setTaskState(TaskState.RAFT_CONNECTION_EXCEPTION);
        qpTask.receive(null);
      }
    }

    /**
     * Shut down taskQueue
     */
    @Override
    public void shutdown() {
      boltClientService.shutdown();
    }

  }

}
