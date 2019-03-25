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
package org.apache.iotdb.cluster.org.apache.iotdb.cluster.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class Router {
  private List<PhysicalNode> nodes = new ArrayList<>();
  // Replication number
  private final int replicator;
  private final int numOfVirtulaNodes = 2;
  private HashFunction hashFunction = new MD5Hash();
  private final SortedMap<Integer, PhysicalNode> physicalRing = new TreeMap<>();
  private final SortedMap<Integer, VirtualNode> virtualRing = new TreeMap<>();


  // A local cache to store Which nodes do a storage group correspond to
  private Map<String, PhysicalNode[]> router = new ConcurrentHashMap<>();
  private Map<PhysicalNode, PhysicalNode[][]> dataPartitionCache = new HashMap<>();

  public Router(){
    // TODO get replicator form config file
    String[] ipList = {"192.168.130.1", "192.168.130.2", "192.168.130.3",};
    this.replicator = 3;
    int port = 7777;

    for(String ip: ipList){
      nodes.add(new PhysicalNode(ip, port));
    }
    init();
  }

  private void init(){
    for(PhysicalNode node : nodes){
      addNode(node, this.numOfVirtulaNodes);
    }

  }

  private void addNode(PhysicalNode node, int virtualNum){
    physicalRing.put(hashFunction.hash(node.getKey()), node);
    for(int i = 0; i < virtualNum; i++){
      VirtualNode vNode = new VirtualNode(i, node);
      virtualRing.put(hashFunction.hash(vNode.getKey()), vNode);
    }
  }

  // For a storage group, compute the nearest physical node on the VRing
  private PhysicalNode routeNode(String objectKey){
//    if(router.containsKey(objectKey)){
//      return router.get(objectKey);
//    }
//    int hashVal = hashFunction.hash(objectKey);
//    SortedMap<Integer,VirtualNode> tailMap = virtualRing.tailMap(hashVal);
//    Integer nodeHashVal = !tailMap.isEmpty() ? tailMap.firstKey() : virtualRing.firstKey();
//    PhysicalNode node = virtualRing.get(nodeHashVal).getPhysicalNode();
//    router.put(objectKey, node);
//    return node;
    return null;
  }

  // Calculate the physical nodes corresponding to the replications where a data point is located
  public PhysicalNode[] routeGroup(String objectKey){
    return router.get(objectKey);
  }

  public PhysicalNode[][] generateGroups(String ip, int port){
    return this.generateGroups(new PhysicalNode(ip, port));
  }

  // For a given physical, how many groups does it belong to
  private PhysicalNode[][] generateGroups(PhysicalNode node){
    return dataPartitionCache.get(node);
  }



  private class PhysicalNode{
    final String ip;
    final int port;

    PhysicalNode(String ip, int port){
      this.ip = ip;
      this.port = port;
    }

    String getKey(){
      return String.format("%s:%d", ip, port);
    }

  }
  private class VirtualNode {
    private final int replicaIndex;
    private final PhysicalNode physicalNode;

    VirtualNode(int replicaIndex, PhysicalNode physicalNode){
      this.replicaIndex = replicaIndex;
      this.physicalNode = physicalNode;
    }

    PhysicalNode getPhysicalNode(){
      return this.physicalNode;
    }

    String getKey(){
      return String.format("%s-%d", physicalNode.getKey(), replicaIndex);
    }

  }

  private class MD5Hash implements HashFunction{
    MessageDigest instance;

    MD5Hash() {
      try {
        instance = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
      }
    }

    public synchronized int hash(String key) {
      instance.reset();
      instance.update(key.getBytes());
      byte[] digest = instance.digest();

      int h = 0;
      for (int i = 0; i < 4; i++) {
        h <<= 8;
        h |= ((int) digest[i]) & 0xFF;
      }
      return h;
    }
  }
}
