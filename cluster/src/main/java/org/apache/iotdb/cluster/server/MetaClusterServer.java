/*
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
package org.apache.iotdb.cluster.server;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncProcessor;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.nodetool.ClusterMonitor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.RegisterManager;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * MetaCluster manages the whole cluster's metadata, such as what nodes are in the cluster and
 * the data partition. Each node has one MetaClusterServer instance,
 * the single-node IoTDB instance is started-up at the same time.
 */
public class MetaClusterServer extends RaftServer implements TSMetaService.AsyncIface {

  // each node only contains one MetaGroupMember
  private MetaGroupMember member;
  private IoTDB ioTDB;
  // to register the ClusterMonitor that helps monitoring the cluster
  private RegisterManager registerManager = new RegisterManager();

  public MetaClusterServer() throws QueryProcessException {
    super();
    member = new MetaGroupMember(protocolFactory, thisNode);
    // TODO-Cluster#355: check the initial cluster size and refuse to start when the size <
    //  #replication
  }

  /**
   * Besides the standard RaftServer start-up, the IoTDB instance, a MetaGroupMember and the
   * ClusterMonitor are also started.
   * @throws TTransportException
   * @throws StartupException
   */
  @Override
  public void start() throws TTransportException, StartupException {
    super.start();
    ioTDB = new IoTDB();
    ioTDB.active();
    member.start();
    registerManager.register(ClusterMonitor.INSTANCE);
  }

  /**
   * Also stops the IoTDB instance, the MetaGroupMember and the ClusterMonitor.
   */
  @Override
  public void stop() {
    super.stop();
    ioTDB.stop();
    ioTDB = null;
    member.stop();
    registerManager.deregisterAll();
  }

  /**
   * Build a initial cluster with other nodes on the seed list.
   */
  public void buildCluster() {
    member.buildCluster();
  }

  /**
   * Pick up a node from the seed list and send a join request to it.
   * @return whether the node has joined the cluster.
   */
  public boolean joinCluster() {
    return member.joinCluster();
  }

  /**
   * MetaClusterServer uses the meta port to create the socket.
   * @return
   * @throws TTransportException
   */
  @Override
  TNonblockingServerSocket getServerSocket() throws TTransportException {
    return  new TNonblockingServerSocket(new InetSocketAddress(config.getLocalIP(),
        config.getLocalMetaPort()), connectionTimeoutInMS);
  }

  @Override
  String getClientThreadPrefix() {
    return "MetaClientThread-";
  }

  @Override
  String getServerClientName() {
    return "MetaServerThread-";
  }

  @Override
  AsyncProcessor getProcessor() {
    // this one is from TSMetaIService
    return new AsyncProcessor(this);
  }

  // Request forwarding. There is only one MetaGroupMember each node, so all requests will be
  // directly sent to that member. See the methods in MetaGroupMember for details

  @Override
  public void addNode(Node node, AsyncMethodCallback resultHandler) {
    member.addNode(node, resultHandler);
  }

  @Override
  public void sendHeartBeat(HeartBeatRequest request, AsyncMethodCallback resultHandler) {
    member.sendHeartBeat(request, resultHandler);
  }

  @Override
  public void startElection(ElectionRequest electionRequest, AsyncMethodCallback resultHandler) {
    member.startElection(electionRequest, resultHandler);
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {
    member.appendEntries(request, resultHandler);
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
    member.appendEntry(request, resultHandler);
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback resultHandler) {
    member.sendSnapshot(request, resultHandler);
  }

  @Override
  public void executeNonQueryPlan(ExecutNonQueryReq request,
      AsyncMethodCallback<TSStatus> resultHandler) {
    member.executeNonQueryPlan(request, resultHandler);
  }

  @Override
  public void requestCommitIndex(Node header, AsyncMethodCallback<Long> resultHandler) {
    member.requestCommitIndex(header, resultHandler);
  }

  @Override
  public void pullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) {
    member.pullTimeSeriesSchema(request, resultHandler);
  }

  @Override
  public void checkAlive(AsyncMethodCallback<Node> resultHandler) {
    member.checkAlive(resultHandler);
  }

  @Override
  public void readFile(String filePath, long offset, int length, Node header,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    member.readFile(filePath, offset ,length, header, resultHandler);
  }

  @Override
  public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
    member.queryNodeStatus(resultHandler);
  }

  public MetaGroupMember getMember() {
    return member;
  }

  @Override
  public void removeNode(Node node, AsyncMethodCallback<Long> resultHandler) {
    member.removeNode(node, resultHandler);
  }

  @Override
  public void exile(AsyncMethodCallback<Void> resultHandler) {
    member.exile(resultHandler);
  }
}
