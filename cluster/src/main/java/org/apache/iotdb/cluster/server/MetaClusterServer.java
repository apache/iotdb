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

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.exception.ConfigInconsistentException;
import org.apache.iotdb.cluster.exception.StartUpCheckFailureException;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.cluster.metadata.MetaPuller;
import org.apache.iotdb.cluster.rpc.thrift.*;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncProcessor;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.Processor;
import org.apache.iotdb.cluster.server.heartbeat.MetaHeartbeatServer;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.service.MetaAsyncService;
import org.apache.iotdb.cluster.server.service.MetaSyncService;
import org.apache.iotdb.cluster.utils.nodetool.ClusterMonitor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.RegisterManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * MetaCluster manages the whole cluster's metadata, such as what nodes are in the cluster and the
 * data partition. Each node has one MetaClusterServer instance, the single-node IoTDB instance is
 * started-up at the same time.
 */
public class MetaClusterServer extends RaftServer
    implements TSMetaService.AsyncIface, TSMetaService.Iface {
  private static Logger logger = LoggerFactory.getLogger(MetaClusterServer.class);

  // each node only contains one MetaGroupMember
  private MetaGroupMember member;
  private Coordinator coordinator;
  // the single-node IoTDB instance
  private IoTDB ioTDB;
  // to register the ClusterMonitor that helps monitoring the cluster
  private RegisterManager registerManager = new RegisterManager();
  private MetaAsyncService asyncService;
  private MetaSyncService syncService;
  private MetaHeartbeatServer metaHeartbeatServer;

  public MetaClusterServer() throws QueryProcessException {
    super();
    metaHeartbeatServer = new MetaHeartbeatServer(thisNode, this);
    coordinator = new Coordinator();
    member = new MetaGroupMember(protocolFactory, thisNode, coordinator);
    coordinator.setMetaGroupMember(member);
    asyncService = new MetaAsyncService(member);
    syncService = new MetaSyncService(member);
    MetaPuller.getInstance().init(member);
  }

  /**
   * Besides the standard RaftServer start-up, the IoTDB instance, a MetaGroupMember and the
   * ClusterMonitor are also started.
   *
   * @throws TTransportException
   * @throws StartupException
   */
  @Override
  public void start() throws TTransportException, StartupException {
    super.start();
    metaHeartbeatServer.start();
    ioTDB = new IoTDB();
    IoTDB.setMetaManager(CMManager.getInstance());
    IoTDB.setClusterMode();
    ((CMManager) IoTDB.metaManager).setMetaGroupMember(member);
    ((CMManager) IoTDB.metaManager).setCoordinator(coordinator);
    ioTDB.active();
    member.start();
    // JMX based DBA API
    registerManager.register(ClusterMonitor.INSTANCE);
  }

  /** Also stops the IoTDB instance, the MetaGroupMember and the ClusterMonitor. */
  @Override
  public void stop() {
    if (ioTDB == null) {
      return;
    }
    metaHeartbeatServer.stop();
    super.stop();
    ioTDB.stop();
    ioTDB = null;
    member.stop();
    registerManager.deregisterAll();
  }

  /** Build a initial cluster with other nodes on the seed list. */
  public void buildCluster() throws ConfigInconsistentException, StartUpCheckFailureException {
    member.buildCluster();
  }

  /**
   * Pick up a node from the seed list and send a join request to it.
   *
   * @return whether the node has joined the cluster.
   */
  public void joinCluster() throws ConfigInconsistentException, StartUpCheckFailureException {
    member.joinCluster();
  }

  /**
   * MetaClusterServer uses the meta port to create the socket.
   *
   * @return the TServerTransport
   * @throws TTransportException if create the socket fails
   */
  @Override
  TServerTransport getServerSocket() throws TTransportException {
    logger.info(
        "[{}] Cluster node will listen {}:{}",
        getServerClientName(),
        config.getInternalIp(),
        config.getInternalMetaPort());
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      return new TNonblockingServerSocket(
          new InetSocketAddress(config.getInternalIp(), config.getInternalMetaPort()),
          getConnectionTimeoutInMS());
    } else {
      return new TServerSocket(
          new InetSocketAddress(config.getInternalIp(), config.getInternalMetaPort()));
    }
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
  TProcessor getProcessor() {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      return new AsyncProcessor<>(this);
    } else {
      return new Processor<>(this);
    }
  }

  // Request forwarding. There is only one MetaGroupMember each node, so all requests will be
  // directly sent to that member. See the methods in MetaGroupMember for details

  @Override
  public void addNode(Node node, StartUpStatus startUpStatus, AsyncMethodCallback resultHandler) {
    asyncService.addNode(node, startUpStatus, resultHandler);
  }

  @Override
  public void sendHeartbeat(HeartBeatRequest request, AsyncMethodCallback resultHandler) {
    asyncService.sendHeartbeat(request, resultHandler);
  }

  @Override
  public void startElection(ElectionRequest electionRequest, AsyncMethodCallback resultHandler) {
    asyncService.startElection(electionRequest, resultHandler);
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {
    asyncService.appendEntries(request, resultHandler);
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
    asyncService.appendEntry(request, resultHandler);
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback resultHandler) {
    asyncService.sendSnapshot(request, resultHandler);
  }

  @Override
  public void executeNonQueryPlan(
      ExecutNonQueryReq request, AsyncMethodCallback<TSStatus> resultHandler) {
    asyncService.executeNonQueryPlan(request, resultHandler);
  }

  @Override
  public void requestCommitIndex(
      RaftNode header, AsyncMethodCallback<RequestCommitIndexResponse> resultHandler) {
    asyncService.requestCommitIndex(header, resultHandler);
  }

  @Override
  public void checkAlive(AsyncMethodCallback<Node> resultHandler) {
    asyncService.checkAlive(resultHandler);
  }

  @Override
  public void collectMigrationStatus(AsyncMethodCallback<ByteBuffer> resultHandler) {
    asyncService.collectMigrationStatus(resultHandler);
  }

  @Override
  public void readFile(
      String filePath, long offset, int length, AsyncMethodCallback<ByteBuffer> resultHandler) {
    asyncService.readFile(filePath, offset, length, resultHandler);
  }

  @Override
  public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
    asyncService.queryNodeStatus(resultHandler);
  }

  public MetaGroupMember getMember() {
    return member;
  }

  @Override
  public void checkStatus(
      StartUpStatus startUpStatus, AsyncMethodCallback<CheckStatusResponse> resultHandler) {
    asyncService.checkStatus(startUpStatus, resultHandler);
  }

  @Override
  public void removeNode(Node node, AsyncMethodCallback<Long> resultHandler) {
    asyncService.removeNode(node, resultHandler);
  }

  @Override
  public void exile(ByteBuffer removeNodeLog, AsyncMethodCallback<Void> resultHandler) {
    asyncService.exile(removeNodeLog, resultHandler);
  }

  @Override
  public void matchTerm(
      long index, long term, RaftNode header, AsyncMethodCallback<Boolean> resultHandler) {
    asyncService.matchTerm(index, term, header, resultHandler);
  }

  @Override
  public AddNodeResponse addNode(Node node, StartUpStatus startUpStatus) throws TException {
    return syncService.addNode(node, startUpStatus);
  }

  @Override
  public CheckStatusResponse checkStatus(StartUpStatus startUpStatus) {
    return syncService.checkStatus(startUpStatus);
  }

  @Override
  public long removeNode(Node node) throws TException {
    return syncService.removeNode(node);
  }

  @Override
  public void exile(ByteBuffer removeNodeLog) {
    syncService.exile(removeNodeLog);
  }

  @Override
  public TNodeStatus queryNodeStatus() {
    return syncService.queryNodeStatus();
  }

  @Override
  public Node checkAlive() {
    return syncService.checkAlive();
  }

  @Override
  public ByteBuffer collectMigrationStatus() {
    return syncService.collectMigrationStatus();
  }

  @Override
  public HeartBeatResponse sendHeartbeat(HeartBeatRequest request) {
    return syncService.sendHeartbeat(request);
  }

  @Override
  public long startElection(ElectionRequest request) {
    return syncService.startElection(request);
  }

  @Override
  public long appendEntries(AppendEntriesRequest request) throws TException {
    return syncService.appendEntries(request);
  }

  @Override
  public long appendEntry(AppendEntryRequest request) throws TException {
    return syncService.appendEntry(request);
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request) throws TException {
    syncService.sendSnapshot(request);
  }

  @Override
  public TSStatus executeNonQueryPlan(ExecutNonQueryReq request) throws TException {
    return syncService.executeNonQueryPlan(request);
  }

  @Override
  public RequestCommitIndexResponse requestCommitIndex(RaftNode header) throws TException {
    return syncService.requestCommitIndex(header);
  }

  @Override
  public ByteBuffer readFile(String filePath, long offset, int length) throws TException {
    return syncService.readFile(filePath, offset, length);
  }

  @Override
  public boolean matchTerm(long index, long term, RaftNode header) {
    return syncService.matchTerm(index, term, header);
  }

  @Override
  public void removeHardLink(String hardLinkPath) throws TException {
    syncService.removeHardLink(hardLinkPath);
  }

  @Override
  public void removeHardLink(String hardLinkPath, AsyncMethodCallback<Void> resultHandler) {
    asyncService.removeHardLink(hardLinkPath, resultHandler);
  }

  @Override
  public void handshake(Node sender) {
    syncService.handshake(sender);
  }

  @Override
  public void handshake(Node sender, AsyncMethodCallback<Void> resultHandler) {
    asyncService.handshake(sender, resultHandler);
  }

  @TestOnly
  public void setMetaGroupMember(MetaGroupMember metaGroupMember) {
    this.member = metaGroupMember;
  }

  @TestOnly
  public IoTDB getIoTDB() {
    return ioTDB;
  }
}
