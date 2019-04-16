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
package org.apache.iotdb.cluster.qp.executor;

import com.alipay.sofa.jraft.entity.PeerId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.exception.ConsistencyLevelException;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.task.QPTask;
import org.apache.iotdb.cluster.qp.task.QPTask.TaskState;
import org.apache.iotdb.cluster.qp.task.SingleQPTask;
import org.apache.iotdb.cluster.rpc.raft.NodeAsClient;
import org.apache.iotdb.cluster.rpc.raft.impl.RaftNodeAsClientManager;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractQPExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractQPExecutor.class);

  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();

  protected Router router = Router.getInstance();

  protected MManager mManager = MManager.getInstance();

  protected final Server server = Server.getInstance();

  /**
   * The task in progress.
   */
  protected ThreadLocal<QPTask> currentTask = new ThreadLocal<>();

  /**
   * Count limit to redo a single task
   */
  private static final int TASK_MAX_RETRY = CLUSTER_CONFIG.getQpTaskRedoCount();

  /**
   * ReadMetadataConsistencyLevel: 1 Strong consistency, 2 Weak consistency
   */
  protected int readMetadataConsistencyLevel = CLUSTER_CONFIG.getReadMetadataConsistencyLevel();

  /**
   * ReadDataConsistencyLevel: 1 Strong consistency, 2 Weak consistency
   */
  private int readDataConsistencyLevel = CLUSTER_CONFIG.getReadDataConsistencyLevel();

  /**
   * Async handle QPTask by QPTask and leader id
   *
   * @param task request QPTask
   * @param leader leader of the target raft group
   * @param taskRetryNum Number of QPTask retries due to timeout and redirected.
   * @return basic response
   */
  protected BasicResponse asyncHandleNonQuerySingleTaskGetRes(SingleQPTask task, PeerId leader,
      int taskRetryNum)
      throws InterruptedException, RaftConnectionException {
    asyncSendNonQuerySingleTask(task, leader, taskRetryNum);
    return syncGetNonQueryRes(task, leader, taskRetryNum);
  }

  /**
   * Asynchronous send rpc task via client
   *  @param task rpc task
   * @param leader leader node of the group
   * @param taskRetryNum Retry time of the task
   */
  protected void asyncSendNonQuerySingleTask(SingleQPTask task, PeerId leader, int taskRetryNum)
      throws RaftConnectionException {
    if (taskRetryNum >= TASK_MAX_RETRY) {
      throw new RaftConnectionException(String.format("QPTask retries reach the upper bound %s",
          TASK_MAX_RETRY));
    }
    NodeAsClient client = RaftUtils.getRaftNodeAsClient();
    /** Call async method **/
    client.asyncHandleRequest(task.getRequest(), leader, task);
  }

  /**
   * Synchronous get task response. If it's redirected or status is exception, the task needs to be
   * resent. Note: If status is Exception, it marks that an exception occurred during the task is
   * being sent instead of executed.
   *  @param task rpc task
   * @param leader leader node of the group
   * @param taskRetryNum Retry time of the task
   */
  private BasicResponse syncGetNonQueryRes(SingleQPTask task, PeerId leader, int taskRetryNum)
      throws InterruptedException, RaftConnectionException {
    task.await();
    if (task.getTaskState() != TaskState.FINISH) {
      if (task.getTaskState() == TaskState.REDIRECT) {
        /** redirect to the right leader **/
        leader = PeerId.parsePeer(task.getResponse().getLeaderStr());
        LOGGER.debug("Redirect leader: {}, group id = {}", leader, task.getRequest().getGroupID());
        RaftUtils.updateRaftGroupLeader(task.getRequest().getGroupID(), leader);
      }
      task.resetTask();
      return asyncHandleNonQuerySingleTaskGetRes(task, leader, taskRetryNum + 1);
    }
    return task.getResponse();
  }

  public void shutdown() {
    if (currentTask.get() != null) {
      currentTask.get().shutdown();
    }
  }

  public void setReadMetadataConsistencyLevel(int level) throws ConsistencyLevelException {
    if (level <= ClusterConstant.MAX_CONSISTENCY_LEVEL) {
      this.readMetadataConsistencyLevel = level;
    } else {
      throw new ConsistencyLevelException(String.format("Consistency level %d not support", level));
    }
  }

  public void setReadDataConsistencyLevel(int level) throws ConsistencyLevelException {
    if (level <= ClusterConstant.MAX_CONSISTENCY_LEVEL) {
      this.readDataConsistencyLevel = level;
    } else {
      throw new ConsistencyLevelException(String.format("Consistency level %d not support", level));
    }
  }

  public int getReadMetadataConsistencyLevel() {
    return readMetadataConsistencyLevel;
  }

  public int getReadDataConsistencyLevel() {
    return readDataConsistencyLevel;
  }
}
