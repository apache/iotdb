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

import java.io.IOException;
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
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.RegisterManager;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * MetaCluster manages the whole cluster metadata, such as what nodes are in the cluster and
 * the data partition. Each node has one metaClusterServer instance,
 * the single-node IoTDB instance is startup at the same time.
 */
public class MetaClusterServer extends RaftServer implements TSMetaService.AsyncIface {

  // each node only contains one MetaGroupMember
  private MetaGroupMember member;
  private IoTDB ioTDB;
  private RegisterManager registerManager = new RegisterManager();

  public MetaClusterServer() throws IOException {
    super();
    member = new MetaGroupMember(protocolFactory, thisNode);
    // TODO-Cluster#355: check the initial cluster size and refuse to start when the size <
    //  #replication
  }

  @Override
  public void start() throws TTransportException, StartupException {
    super.start();
    ioTDB = new IoTDB();
    ioTDB.active();
    member.start();
    registerManager.register(ClusterMonitor.INSTANCE);
  }

  @Override
  public void stop() {
    super.stop();
    ioTDB.stop();
    ioTDB = null;
    member.stop();
    registerManager.deregisterAll();
  }

  public void buildCluster() {
    member.buildCluster();
  }

  public boolean joinCluster() {
    return member.joinCluster();
  }

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
    return new AsyncProcessor(this);
  }

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
