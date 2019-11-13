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
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncProcessor;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaCluster manages cluster metadata, such as what nodes are in the cluster and data partition.
 */
public class MetaClusterServer extends RaftServer implements TSMetaService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(MetaClusterServer.class);

  // each node only contains one MetaGroupMember
  private MetaGroupMember member;
  private IoTDB ioTDB;
  private PartitionTable partitionTable;


  public MetaClusterServer() throws IOException {
    super();
    member = new MetaGroupMember(protocolFactory);
    member.loadNodes();
    // TODO-Cluster: check the initial cluster size and refuse to start when the size < #replication
  }

  private void buildDataGroups() {
    PartitionGroup[] partitionGroups = partitionTable.getPartitionGroups();

  }

  @Override
  public void start() throws TTransportException {
    super.start();
    ioTDB = new IoTDB();
    ioTDB.active();
    member.start();
  }

  @Override
  void stop() {
    super.stop();
    ioTDB.stop();
    ioTDB = null;
    member.stop();
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
}
