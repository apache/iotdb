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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.cluster.qp.callback.QPTask;
import org.apache.iotdb.cluster.qp.callback.QPTask.TaskState;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.exception.ConsistencyLevelException;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
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

public abstract class ClusterQPExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterQPExecutor.class);

  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();

  /**
   * Raft as client manager.
   */
  private static final RaftNodeAsClientManager CLIENT_MANAGER = RaftNodeAsClientManager
      .getInstance();

  protected Router router = Router.getInstance();

  private PhysicalNode localNode = new PhysicalNode(CLUSTER_CONFIG.getIp(),
      CLUSTER_CONFIG.getPort());

  protected MManager mManager = MManager.getInstance();

  protected final Server server = Server.getInstance();

  /**
   * The task in progress.
   */
  protected QPTask currentTask;

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
   * Get Storage Group Name by device name
   */
  protected String getStroageGroupByDevice(String device) throws PathErrorException {
    String storageGroup;
    try {
      storageGroup = MManager.getInstance().getFileNameByPath(device);
    } catch (PathErrorException e) {
      throw new PathErrorException(String.format("File level of %s doesn't exist.", device));
    }
    return storageGroup;
  }

  /**
   * Get all Storage Group Names by path
   */
  public List<String> getAllStroageGroupsByPath(String path) throws PathErrorException {
    List<String> storageGroupList;
    try {
      storageGroupList = mManager.getAllFileNamesByPath(path);
    } catch (PathErrorException e) {
      throw new PathErrorException(String.format("File level of %s doesn't exist.", path));
    }
    return storageGroupList;
  }

  /**
   * Classify the input storage group list by which data group it belongs to.
   *
   * @return key is groupId, value is all SGs belong to this data group
   */
  protected Map<String, Set<String>> classifySGByGroupId(List<String> sgList) {
    Map<String, Set<String>> map = new HashMap<>();
    for (int i = 0; i < sgList.size(); i++) {
      String sg = sgList.get(i);
      String groupId = getGroupIdBySG(sg);
      if (map.containsKey(groupId)) {
        map.get(groupId).add(sg);
      } else {
        Set<String> set = new HashSet<>();
        set.add(sg);
        map.put(groupId, set);
      }
    }
    return map;
  }

  /**
   * Get raft group id by storage group name
   */
  protected String getGroupIdBySG(String storageGroup) {
    return router.getGroupID(router.routeGroup(storageGroup));
  }

  /**
   * Check if the non query command can execute in local. 1. If this node belongs to the storage
   * group 2. If this node is leader.
   */
  public boolean canHandleNonQueryByGroupId(String groupId) {
    boolean canHandle = false;
    if(groupId.equals(ClusterConfig.METADATA_GROUP_ID)){
      canHandle = ((MetadataRaftHolder) (server.getMetadataHolder())).getFsm().isLeader();
    }else {
      if (router.containPhysicalNodeByGroupId(groupId, localNode) && RaftUtils
          .getPhysicalNodeFrom(RaftUtils.getLeaderPeerID(groupId)).equals(localNode)) {
        canHandle = true;
      }
    }
    return canHandle;
  }

  /**
   * Check if the query command can execute in local. Check if this node belongs to the group id
   */
  protected boolean canHandleQueryByGroupId(String groupId) {
    return router.containPhysicalNodeByGroupId(groupId, localNode);
  }

  /**
   * Async handle QPTask by QPTask and leader id
   *
   * @param task request QPTask
   * @param leader leader of the target raft group
   * @param taskRetryNum Number of QPTask retries due to timeout and redirected.
   * @return basic response
   */
  protected BasicResponse asyncHandleNonQueryTaskGetRes(QPTask task, PeerId leader,
      int taskRetryNum)
      throws InterruptedException, RaftConnectionException {
    asyncSendNonQueryTask(task, leader, taskRetryNum);
    return asyncGetNonQueryRes(task, leader, taskRetryNum);
  }

  /**
   * Asynchronous send rpc task via client
   *
   * @param task rpc task
   * @param leader leader node of the group
   * @param taskRetryNum Retry time of the task
   */
  public void asyncSendNonQueryTask(QPTask task, PeerId leader, int taskRetryNum)
      throws RaftConnectionException {
    if (taskRetryNum >= TASK_MAX_RETRY) {
      throw new RaftConnectionException(String.format("QPTask retries reach the upper bound %s",
          TASK_MAX_RETRY));
    }
    NodeAsClient client = getRaftNodeAsClient();
    /** Call async method **/
    client.asyncHandleRequest(task.getRequest(), leader, task);
  }

  /**
   * try to get raft rpc client
   */
  private NodeAsClient getRaftNodeAsClient() throws RaftConnectionException {
    return CLIENT_MANAGER.getRaftNodeAsClient();
  }

  /**
   * Asynchronous get task response. If it's redirected or status is exception, the task needs to be
   * resent. Note: If status is Exception, it marks that an exception occurred during the task is
   * being sent instead of executed.
   *
   * @param task rpc task
   * @param leader leader node of the group
   * @param taskRetryNum Retry time of the task
   */
  private BasicResponse asyncGetNonQueryRes(QPTask task, PeerId leader, int taskRetryNum)
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
      return asyncHandleNonQueryTaskGetRes(task, leader, taskRetryNum + 1);
    }
    return task.getResponse();
  }

  public void shutdown() {
    if (currentTask != null) {
      currentTask.shutdown();
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
