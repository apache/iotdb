
package org.apache.iotdb.cluster.utils.hash;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.ErrorConfigureExecption;

public class Router {

  // Replication number
  private int replicator;
  private HashFunction hashFunction = new MD5Hash();
  private final SortedMap<Integer, PhysicalNode> physicalRing = new TreeMap<>();
  private final SortedMap<Integer, VirtualNode> virtualRing = new TreeMap<>();

  // A local cache to store Which nodes do a storage group correspond to
  private Map<String, PhysicalNode[]> router = new ConcurrentHashMap<>();
  private Map<PhysicalNode, PhysicalNode[][]> dataPartitionCache = new HashMap<>();
  private Map<PhysicalNode, String> groupIdCache = new HashMap<>();
  public static final String DATA_GROUP_STR = "data-group-";

  private static class RouterHolder {

    private static final Router INSTANCE = new Router();
  }

  private Router() {
    init();
  }

  public static final Router getInstance() {
    return RouterHolder.INSTANCE;
  }

  // change this method to public for test, you should not invoke this method explicitly.
  public void init() {
    reset();
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    String[] ipList = config.getNodes();
    this.replicator = config.getReplication();
    int port = config.getPort();
    int numOfVirtulaNodes = config.getNumOfVirtulaNodes();
    for (String ip : ipList) {
      PhysicalNode node = new PhysicalNode(ip, port);
      addNode(node, numOfVirtulaNodes);
    }
    PhysicalNode[] nodes = physicalRing.values().toArray(new PhysicalNode[physicalRing.size()]);
    int len = nodes.length;
    for (int i = 0; i < len; i++) {
      PhysicalNode first = nodes[i];
      if (len < replicator) {
        throw new ErrorConfigureExecption(String.format("Replicator number %d is greater "
            + "than cluster number %d", replicator, len));
      } else if (len == replicator) {
        PhysicalNode[][] val = new PhysicalNode[1][len];
        groupIdCache.put(first, DATA_GROUP_STR + "0");
        for (int j = 0; j < len; j++) {
          val[0][j] = nodes[(i + j) % len];
        }
        dataPartitionCache.put(first, val);
      } else {
        PhysicalNode[][] val = new PhysicalNode[replicator][replicator];
        groupIdCache.put(first, DATA_GROUP_STR + i);
        for (int j = 0; j < replicator; j++) {
          for (int k = 0; k < replicator; k++) {
            val[j][k] = nodes[(i - j + k + len) % len];
          }
        }
        dataPartitionCache.put(first, val);
      }
    }
  }

  // Calculate the physical nodes corresponding to the replications
  // where a data point is located
  public PhysicalNode[] routeGroup(String objectKey) {
    if (router.containsKey(objectKey)) {
      return router.get(objectKey);
    }
    PhysicalNode node = routeNode(objectKey);
    PhysicalNode[] nodes = dataPartitionCache.get(node)[0];
    router.put(objectKey, nodes);
    return nodes;
  }

  public String getGroupID(PhysicalNode[] nodes) {
    return groupIdCache.get(nodes[0]);
  }

  public PhysicalNode[][] generateGroups(String ip, int port) {
    return this.generateGroups(new PhysicalNode(ip, port));
  }

  private void addNode(PhysicalNode node, int virtualNum) {
    physicalRing.put(hashFunction.hash(node.getKey()), node);
    for (int i = 0; i < virtualNum; i++) {
      VirtualNode vNode = new VirtualNode(i, node);
      virtualRing.put(hashFunction.hash(vNode.getKey()), vNode);
    }
  }

  //Only for test
  // For a storage group, compute the nearest physical node on the hash ring
  public PhysicalNode routeNode(String objectKey) {
    int hashVal = hashFunction.hash(objectKey);
    SortedMap<Integer, VirtualNode> tailMap = virtualRing.tailMap(hashVal);
    Integer nodeHashVal = !tailMap.isEmpty() ? tailMap.firstKey() : virtualRing.firstKey();
    return virtualRing.get(nodeHashVal).getPhysicalNode();
  }

  // For a given physical, how many groups does it belong to
  private PhysicalNode[][] generateGroups(PhysicalNode node) {
    return dataPartitionCache.get(node);
  }

  private void reset() {
    physicalRing.clear();
    virtualRing.clear();
    router.clear();
    dataPartitionCache.clear();
    groupIdCache.clear();
  }

  // only for test
  public void showPhysicalRing() {
    for (Entry<Integer, PhysicalNode> entry : physicalRing.entrySet()) {
      System.out.println(String.format("%d-%s", entry.getKey(), entry.getValue().getKey()));
    }
  }

  //only for test
  public void showVirtualRing() {
    for (Entry<Integer, VirtualNode> entry : virtualRing.entrySet()) {
      System.out.println(String.format("%d-%s", entry.getKey(), entry.getValue().getKey()));
    }
  }

  public boolean containPhysicalNode(String storageGroup, PhysicalNode node) {
    PhysicalNode[] nodes = routeGroup(storageGroup);
    return Arrays.asList(nodes).contains(node);
  }
}
