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
package org.apache.iotdb.cluster.utils.hash;

import com.alipay.sofa.jraft.util.OnlyForTest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.ErrorConfigureExecption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster router, it's responsible for hash mapping and routing to specified data groups
 */
public class Router {

  private static final Logger LOGGER = LoggerFactory.getLogger(Router.class);

  /**
   * A local cache to store Which nodes do a storage group correspond to
   */
  private Map<String, PhysicalNode[]> sgRouter = new ConcurrentHashMap<>();

  /**
   * Key is the first node of the group, value is all physical node groups which contain this node
   */
  private Map<PhysicalNode, PhysicalNode[][]> dataPartitionCache = new HashMap<>();

  /**
   * Key is the first node of the group, value is group id.
   */
  private Map<PhysicalNode, String> nodeMapGroupIdCache = new HashMap<>();

  /**
   * Key is group id, value is the first node of the group.
   */
  private Map<String, PhysicalNode> groupIdMapNodeCache = new HashMap<>();

  /**
   * Data group name prefix
   */
  public static final String DATA_GROUP_STR = "data-group-";

  private HashFunction hashFunction = new MD5Hash();

  private final SortedMap<Integer, PhysicalNode> physicalRing = new TreeMap<>();
  private final SortedMap<Integer, VirtualNode> virtualRing = new TreeMap<>();

  private static class RouterHolder {

    private static final Router INSTANCE = new Router();
  }

  private Router() {
    init();
  }

  public static final Router getInstance() {
    return RouterHolder.INSTANCE;
  }

  /**
   * Change this method to public for test, you should not invoke this method explicitly.
   */
  public void init() {
    init(ClusterDescriptor.getInstance().getConfig());
  }

  public void init(ClusterConfig config) {
    reset();
    String[] hosts = config.getNodes();
    int replicator = config.getReplication();
    int numOfVirtualNodes = config.getNumOfVirtualNodes();
    createHashRing(hosts, numOfVirtualNodes);
    PhysicalNode[] nodes = physicalRing.values().toArray(new PhysicalNode[0]);
    int len = nodes.length;
    for (int i = 0; i < len; i++) {
      PhysicalNode first = nodes[i];
      if (len < replicator) {
        throw new ErrorConfigureExecption(String.format("Replicator number %d is greater "
            + "than cluster number %d", replicator, len));
      } else if (len == replicator) {
        PhysicalNode[][] val = new PhysicalNode[1][len];
        nodeMapGroupIdCache.put(first, DATA_GROUP_STR + "0");
        groupIdMapNodeCache.put(DATA_GROUP_STR + "0", first);
        for (int j = 0; j < len; j++) {
          val[0][j] = nodes[(i + j) % len];
        }
        dataPartitionCache.put(first, val);
      }  else {
        PhysicalNode[][] val = new PhysicalNode[replicator][replicator];
        nodeMapGroupIdCache.put(first, DATA_GROUP_STR + i);
        groupIdMapNodeCache.put(DATA_GROUP_STR + i, first);
        for (int j = 0; j < replicator; j++) {
          for (int k = 0; k < replicator; k++) {
            val[j][k] = nodes[(i - j + k + len) % len];
          }
        }
        dataPartitionCache.put(first, val);
      }
    }
  }

  private void createHashRing(String[] hosts, int numOfVirtualNodes){
    for (String host : hosts) {
      String[] values = host.split(":");
      PhysicalNode node = new PhysicalNode(values[0].trim(), Integer.parseInt(values[1].trim()));
      addNode(node, numOfVirtualNodes);
    }
  }

  /**
   * Calculate the physical nodes corresponding to the replications where a data point is located
   *
   * @param storageGroupName storage group
   */
  public PhysicalNode[] routeGroup(String storageGroupName) {
    if (sgRouter.containsKey(storageGroupName)) {
      return sgRouter.get(storageGroupName);
    }
    PhysicalNode node = routeNode(storageGroupName);
    PhysicalNode[] nodes = dataPartitionCache.get(node)[0];
    sgRouter.put(storageGroupName, nodes);
    return nodes;
  }

  public String getGroupID(PhysicalNode[] nodes) {
    return nodeMapGroupIdCache.get(nodes[0]);
  }

  public PhysicalNode[][] getGroupsNodes(String ip, int port) {
    return this.getGroupsNodes(new PhysicalNode(ip, port));
  }

  /**
   * Add a new node to cluster
   */
  private void addNode(PhysicalNode node, int virtualNum) {
    physicalRing.put(hashFunction.hash(node.getKey()), node);
    for (int i = 0; i < virtualNum; i++) {
      VirtualNode vNode = new VirtualNode(i, node);
      virtualRing.put(hashFunction.hash(vNode.getKey()), vNode);
    }
  }

  /**
   * For a storage group, compute the nearest physical node on the hash ring
   */
  public PhysicalNode routeNode(String objectKey) {
    int hashVal = hashFunction.hash(objectKey);
    SortedMap<Integer, VirtualNode> tailMap = virtualRing.tailMap(hashVal);
    Integer nodeHashVal = !tailMap.isEmpty() ? tailMap.firstKey() : virtualRing.firstKey();
    return virtualRing.get(nodeHashVal).getPhysicalNode();
  }


  /**
   * For a given physical, how many groups does it belong to
   *
   * @param node first node of a group
   */
  private PhysicalNode[][] getGroupsNodes(PhysicalNode node) {
    return dataPartitionCache.get(node);
  }

  private void reset() {
    physicalRing.clear();
    virtualRing.clear();
    sgRouter.clear();
    dataPartitionCache.clear();
    nodeMapGroupIdCache.clear();
  }

  @OnlyForTest
  public void showPhysicalRing() {
    for (Entry<Integer, PhysicalNode> entry : physicalRing.entrySet()) {
      LOGGER.info("{}-{}", entry.getKey(), entry.getValue().getKey());
    }
  }

  @OnlyForTest
  public void showVirtualRing() {
    for (Entry<Integer, VirtualNode> entry : virtualRing.entrySet()) {
      LOGGER.info("{}-{}", entry.getKey(), entry.getValue().getKey());
    }
  }

  public boolean containPhysicalNodeBySG(String storageGroup, PhysicalNode node) {
    PhysicalNode[] nodes = routeGroup(storageGroup);
    return Arrays.asList(nodes).contains(node);
  }

  public boolean containPhysicalNodeByGroupId(String groupId, PhysicalNode node) {
    PhysicalNode[] nodes = getNodesByGroupId(groupId);
    return Arrays.asList(nodes).contains(node);
  }

  /**
   * Show physical nodes by group id.
   */
  public void showPhysicalNodes(String groupId) {
    PhysicalNode[] physicalPlans = getNodesByGroupId(groupId);
    for (PhysicalNode node : physicalPlans) {
      LOGGER.info("{}", node);
    }
  }

  /**
   * Get Physical node by group id.
   */
  public PhysicalNode[] getNodesByGroupId(String groupId) {
    PhysicalNode node = groupIdMapNodeCache.get(groupId);
    return dataPartitionCache.get(node)[0];
  }

  public Set<String> getAllGroupId() {
    return groupIdMapNodeCache.keySet();
  }

  public SortedMap<Integer, PhysicalNode> getPhysicalRing() {
    return physicalRing;
  }

  public SortedMap<Integer, VirtualNode> getVirtualRing() {
    return virtualRing;
  }
}
