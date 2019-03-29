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
package org.apache.iotdb.cluster.qp;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import org.apache.iotdb.cluster.callback.Task;
import org.apache.iotdb.cluster.callback.Task.TaskState;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.rpc.NodeAsClient;
import org.apache.iotdb.cluster.rpc.impl.RaftNodeAsClient;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ClusterQPExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterQPExecutor.class);
  protected static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();
  protected Router router = Router.getInstance();
  protected PhysicalNode localNode = new PhysicalNode(CLUSTER_CONFIG.getIp(),
      CLUSTER_CONFIG.getPort());
  protected OverflowQPExecutor qpExecutor = new OverflowQPExecutor();
  protected MManager mManager = MManager.getInstance();
  /**
   * Rpc Service Client
   */
  protected BoltCliClientService cliClientService;

  /**
   * Count limit to redo a single task
   */
  protected static final int TASK_MAX_RETRY = CLUSTER_CONFIG.getTaskRedoCount();
  /**
   * Number of subtask in task segmentation
   */
  protected static int SUB_TASK_NUM = 1;

  /**
   * Get Storage Group Name by device name
   */
  public String getStroageGroupByDevice(String device) throws PathErrorException {
    String storageGroup;
    try {
      storageGroup = MManager.getInstance().getFileNameByPath(device);
    } catch (PathErrorException e) {
      throw new PathErrorException(String.format("File level of %s doesn't exist.", device));
    }
    return storageGroup;
  }

  /**
   * Get raft group id by storage group name
   */
  public String getGroupIdBySG(String storageGroup) {
    return router.getGroupID(router.routeGroup(storageGroup));
  }

  /**
   * Verify if the command can execute in local. 1. If this node belongs to the storage group 2. If
   * this node is leader.
   */
  public boolean canHandle(String storageGroup) {
    if (router.containPhysicalNode(storageGroup, localNode)) {
      String groupId = getGroupIdBySG(storageGroup);
      if (RaftUtils.convertPeerId(RaftUtils.getTargetPeerID(groupId)).equals(localNode)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Async handle task by task and leader id
   *
   * @param task request task
   * @param leader leader of the target raft group
   * @param taskRetryNum Number of task retries due to timeout and redirected.
   * @return basic response
   */
  public BasicResponse asyncHandleTaskGetRes(Task task, PeerId leader, int taskRetryNum)
      throws InterruptedException, RaftConnectionException {
    if (taskRetryNum >= TASK_MAX_RETRY) {
      throw new RaftConnectionException(String.format("Task retries reach the upper bound %s",
          TASK_MAX_RETRY));
    }
    NodeAsClient client = new RaftNodeAsClient();
    /** Call async method **/
    client.asyncHandleRequest(cliClientService, task.getRequest(), leader, task);
    task.await();
    if (task.getTaskState() != TaskState.FINISH) {
      if (task.getTaskState() == TaskState.REDIRECT) {
        /** redirect to the right leader **/
        leader = PeerId.parsePeer(task.getResponse().getLeaderStr());
        LOGGER.info("Redirect leader: {}, group id = {}" , leader, task.getRequest().getGroupID());
        RaftUtils.updateRaftGroupLeader(task.getRequest().getGroupID(), leader);
      }
      task.resetTask();
      return asyncHandleTaskGetRes(task, leader, taskRetryNum + 1);
    }
    return task.getResponse();
  }

  public void shutdown() {
    cliClientService.shutdown();
  }
}
