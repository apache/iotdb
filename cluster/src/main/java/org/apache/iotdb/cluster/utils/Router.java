
package org.apache.iotdb.cluster.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.iotdb.cluster.exception.ErrorConfigureExecption;

public class Router {

  private List<PhysicalNode> nodes = new ArrayList<>();
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
    // TODO get form config file
    // String[] ipList = {"192.168.130.1", "192.168.130.2", "192.168.130.3"};
    // this.replicator = replicator;
    // int port = 7777;
//    for (String ip : ipList) {
//      nodes.add(new PhysicalNode(ip, port));
//    }
    init();
  }

  public static final Router getInstance() {
    return RouterHolder.INSTANCE;
  }

  private void init() {
    reset();
    for (PhysicalNode node : nodes) {
      addNode(node, this.numOfVirtulaNodes);
    }
    PhysicalNode[] nodes = (PhysicalNode[]) physicalRing.values().toArray();
    int len = nodes.length;
    for (int i = 0; i < len; i++) {
      PhysicalNode first = nodes[i];
      if (len < replicator) {
        throw new ErrorConfigureExecption(String.format("Replicator number %d is greater "
            + "than cluster number %d", replicator, len));
      } else if (len == replicator) {
        PhysicalNode[][] val = new PhysicalNode[1][len];
        for (int j = 0; j < len; j++) {
          val[0][j] = nodes[i + j % len];
        }
        dataPartitionCache.put(first, val);
      } else {
        PhysicalNode[][] val = new PhysicalNode[replicator][replicator];
        for (int j = 0; j < replicator; j++) {
          for (int k = 0; k < replicator; k++) {
            val[j][k] = nodes[(i - j + k) % len];
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

  private void reset(){
    physicalRing.clear();
    virtualRing.clear();
    router.clear();
    dataPartitionCache.clear();
  }
}
