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
package org.apache.iotdb.cluster.utils;

import com.alipay.sofa.jraft.util.OnlyForTest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;

/**
 * Utils for QP executor
 */
public class QPExecutorUtils {

  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();

  private static final Router router = Router.getInstance();

  private static final PhysicalNode localNode = new PhysicalNode(CLUSTER_CONFIG.getIp(),
      CLUSTER_CONFIG.getPort());

  private static final  MManager mManager = MManager.getInstance();

  private static final Server server = Server.getInstance();

  /**
   * Get Storage Group Name by device name
   */
  public static String getStroageGroupByDevice(String device) throws PathErrorException {
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
  public static List<String> getAllStroageGroupsByPath(String path) throws PathErrorException {
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
  public static Map<String, Set<String>> classifySGByGroupId(List<String> sgList) {
    Map<String, Set<String>> map = new HashMap<>();
    for (int i = 0; i < sgList.size(); i++) {
      String sg = sgList.get(i);
      String groupId = router.getGroupIdBySG(sg);
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
   * Check if the non query command can execute in local. 1. If this node belongs to the storage
   * group 2. If this node is leader.
   */
  public static boolean canHandleNonQueryByGroupId(String groupId) {
    boolean canHandle = false;
    if(groupId.equals(ClusterConfig.METADATA_GROUP_ID)){
      canHandle = ((MetadataRaftHolder) (server.getMetadataHolder())).getFsm().isLeader();
    }else {
      if (checkDataGroupLeader(groupId)) {
        canHandle = true;
      }
    }
    return canHandle;
  }

  /**
   * Check whether local node is leader of data group.
   *
   * @param groupId data group
   * @return true if local node is leader of data group, else return false
   */
  public static boolean checkDataGroupLeader(String groupId) {
    boolean isLeader = false;
    if (router.containPhysicalNodeByGroupId(groupId, localNode) && RaftUtils
        .getPhysicalNodeFrom(RaftUtils.getLeaderPeerID(groupId)).equals(localNode)) {
      isLeader = true;
    }
    return isLeader;
  }

  /**
   * Check if the query command can execute in local. Check if this node belongs to the group id
   */
  public static boolean canHandleQueryByGroupId(String groupId) {
    return router.containPhysicalNodeByGroupId(groupId, localNode);
  }

  /**
   * Get group id by device
   *
   * @param device device
   */
  public static String getGroupIdByDevice(String device) throws PathErrorException {
    String storageGroup = QPExecutorUtils.getStroageGroupByDevice(device);
    String groupId = Router.getInstance().getGroupIdBySG(storageGroup);
    return groupId;
  }

  /**
   * Change address of local node for test
   */
  @OnlyForTest
  public static void setLocalNodeAddr(String ip, int port) {
    localNode.setIp(ip);
    localNode.setPort(port);
  }
}
