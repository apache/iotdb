/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.member;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.rpc.thrift.VNode;
import org.apache.iotdb.cluster.server.heartbeat.HeartBeatThread;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGroupMember extends RaftMember implements TSDataService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(DataGroupMember.class);

  private TSDataService.AsyncClient.Factory clientFactory;

  private VNode thisVNode;
  private PartitionGroup vNodes;
  // the data port of each node in this group
  private Map<Node, Integer> dataPortMap = new ConcurrentHashMap<>();

  private DataGroupMember(TProtocolFactory factory, PartitionGroup nodes, VNode thisVNode,
      LogManager logManager) throws IOException {
    this.thisNode = thisVNode.getPNode();
    this.thisVNode = thisVNode;
    this.logManager = logManager;
    allNodes = new ArrayList<>();
    clientFactory = new TSDataService.AsyncClient.Factory(new TAsyncClientManager(), factory);
    queryProcessor = new QueryProcessor(new QueryProcessExecutor());
    this.vNodes = nodes;
    for (VNode vNode : vNodes) {
      allNodes.add(vNode.getPNode());
    }
  }

  @Override
  void initLogManager() {
    // log manager a constructor parameter
  }

  @Override
  public void start() throws TTransportException {
    super.start();
    heartBeatService.submit(new HeartBeatThread(this));
  }

  @Override
  AsyncClient getAsyncClient(TNonblockingTransport transport) {
    return clientFactory.getAsyncClient(transport);
  }

  @Override
  public AsyncClient connectNode(Node node) {
    if (node.equals(thisNode)) {
      return null;
    }

    AsyncClient client = null;
    try {
      client = getAsyncClient(new TNonblockingSocket(node.getIp(), getDataPort(node),
          CONNECTION_TIME_OUT_MS));
    } catch (IOException e) {
      logger.warn("Cannot connect to node {}", node, e);
    }
    return client;
  }

  private int getDataPort(Node node) {
    Integer dataPort = dataPortMap.get(node);
    if (dataPort == null) {
      for (VNode vNode : vNodes) {
        if (vNode.getPNode().equals(node)) {
          dataPort = node.getDataPorts().get(vNode.getSerialNum());
          dataPortMap.put(node, dataPort);
        }
      }
    }
    return dataPort;
  }

  /**
   * The first node (on the hash ring) in this data group is the header. It determines the duty
   * (what range on the ring do the group take responsibility for) of the group and although other
   * nodes in this may change, this node is unchangeable unless the data group is dismissed. It
   * is also the identifier of this data group.
   */
  public VNode getHeader() {
    return vNodes.get(0);
  }

  public static class Factory {
    private TProtocolFactory protocolFactory;
    private LogManager logManager;

    Factory(TProtocolFactory protocolFactory, LogManager logManager) {
      this.protocolFactory = protocolFactory;
      this.logManager = logManager;
    }

    public DataGroupMember create(PartitionGroup partitionGroup, VNode thisVNode)
        throws IOException {
      return new DataGroupMember(protocolFactory, partitionGroup, thisVNode, logManager);
    }
  }

  /**
   * Try to add a VNode into this group
   * @param node
   */
  public synchronized void addNode(VNode node) {
    synchronized (allNodes) {
      List<Node> allNodeList = (List<Node>) allNodes;
      VNode firstNode = vNodes.get(0);
      VNode lastNode = vNodes.get(vNodes.size() - 1);


      boolean crossTail = lastNode.hash < firstNode.hash;
    }
  }
}
