
package org.apache.iotdb.cluster.utils;

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
  private final int numOfVirtulaNodes = 2;
  private HashFunction hashFunction = new MD5Hash();
  private final SortedMap<Integer, PhysicalNode> physicalRing = new TreeMap<>();
  private final SortedMap<Integer, VirtualNode> virtualRing = new TreeMap<>();

  // A local cache to store Which nodes do a storage group correspond to
  private Map<String, PhysicalNode[]> router = new ConcurrentHashMap<>();
  private Map<PhysicalNode, PhysicalNode[][]> dataPartitionCache = new HashMap<>();

  private static class RouterHolder {

    private static final Router INSTANCE = new Router();
  }

  private Router() {
    init();
  }

  public static final Router getInstance() {
    return RouterHolder.INSTANCE;
  }

  public void init() {
    reset();
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    String[] ipList = config.getNodes();
    this.replicator = config.getReplication();
    int port = config.getPort();
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
        for (int j = 0; j < len; j++) {
          val[0][j] = nodes[(i + j) % len];
        }
        dataPartitionCache.put(first, val);
      } else {
        PhysicalNode[][] val = new PhysicalNode[replicator][replicator];
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

  // For a storage group, compute the nearest physical node on the VRing
  private PhysicalNode routeNode(String objectKey) {
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
  }

  public void showPhysicalRing() {
    for (Entry<Integer, PhysicalNode> entry : physicalRing.entrySet()) {
      System.out.println(String.format("%d-%s", entry.getKey(), entry.getValue().getKey()));
    }
  }
}
