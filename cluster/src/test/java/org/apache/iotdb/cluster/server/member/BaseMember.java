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

package org.apache.iotdb.cluster.server.member;

import org.apache.iotdb.cluster.client.DataClientProvider;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.common.TestAsyncDataClient;
import org.apache.iotdb.cluster.common.TestAsyncMetaClient;
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestPartitionedLogManager;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.log.applier.DataLogApplier;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.manage.RaftLogManager;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.cluster.metadata.MetaPuller;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.RegisterManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.SchemaUtils;

import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class BaseMember {

  public static AtomicLong dummyResponse = new AtomicLong(Response.RESPONSE_AGREE);

  Map<Node, DataGroupMember> dataGroupMemberMap;
  private Map<Node, MetaGroupMember> metaGroupMemberMap;
  PartitionGroup allNodes;
  protected MetaGroupMember testMetaMember;
  protected Coordinator coordinator;
  RaftLogManager metaLogManager;
  PartitionTable partitionTable;
  PlanExecutor planExecutor;
  protected ExecutorService testThreadPool;

  private List<String> prevUrls;
  private long prevLeaderWait;
  private boolean prevUseAsyncServer;
  private int preLogBufferSize;
  private boolean prevUseAsyncApplier;
  private boolean prevEnableWAL;

  private int syncLeaderMaxWait;
  private long heartBeatInterval;

  @Before
  public void setUp() throws Exception {
    prevUseAsyncApplier = ClusterDescriptor.getInstance().getConfig().isUseAsyncApplier();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncApplier(false);
    prevUseAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    preLogBufferSize = ClusterDescriptor.getInstance().getConfig().getRaftLogBufferSize();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    ClusterDescriptor.getInstance().getConfig().setRaftLogBufferSize(4096);
    testThreadPool = Executors.newFixedThreadPool(4);
    prevLeaderWait = RaftMember.getWaitLeaderTimeMs();
    prevEnableWAL = IoTDBDescriptor.getInstance().getConfig().isEnableWal();
    IoTDBDescriptor.getInstance().getConfig().setEnableWal(false);
    RaftMember.setWaitLeaderTimeMs(10);

    syncLeaderMaxWait = RaftServer.getSyncLeaderMaxWaitMs();
    heartBeatInterval = RaftServer.getHeartBeatIntervalMs();

    RaftServer.setSyncLeaderMaxWaitMs(100);
    RaftServer.setHeartBeatIntervalMs(100);

    allNodes = new PartitionGroup();
    for (int i = 0; i < 100; i += 10) {
      allNodes.add(TestUtils.getNode(i));
    }

    partitionTable = new SlotPartitionTable(allNodes, TestUtils.getNode(0));

    dataGroupMemberMap = new HashMap<>();
    metaGroupMemberMap = new HashMap<>();
    metaLogManager = new TestLogManager(1);
    testMetaMember = getMetaGroupMember(TestUtils.getNode(0));

    coordinator = new Coordinator(testMetaMember);
    testMetaMember.setCoordinator(coordinator);

    for (Node node : allNodes) {
      // pre-create data members
      getDataGroupMember(node);
    }

    IoTDB.setMetaManager(CMManager.getInstance());
    CMManager.getInstance().setMetaGroupMember(testMetaMember);
    CMManager.getInstance().setCoordinator(coordinator);

    EnvironmentUtils.envSetUp();
    prevUrls = ClusterDescriptor.getInstance().getConfig().getSeedNodeUrls();
    List<String> testUrls = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Node node = TestUtils.getNode(i);
      testUrls.add(node.getInternalIp() + ":" + node.getMetaPort());
    }
    ClusterDescriptor.getInstance().getConfig().setSeedNodeUrls(testUrls);

    for (int i = 0; i < 10; i++) {
      try {
        IoTDB.metaManager.setStorageGroup(new PartialPath(TestUtils.getTestSg(i)));
        for (int j = 0; j < 20; j++) {
          SchemaUtils.registerTimeseries(TestUtils.getTestTimeSeriesSchema(i, j));
        }
      } catch (MetadataException e) {
        // ignore
      }
    }
    planExecutor = new PlanExecutor();

    testMetaMember.setPartitionTable(partitionTable);
    MetaPuller.getInstance().init(testMetaMember);
  }

  @After
  public void tearDown() throws Exception {
    testMetaMember.stop();
    metaLogManager.close();
    for (DataGroupMember member : dataGroupMemberMap.values()) {
      member.stop();
      member.closeLogManager();
    }
    dataGroupMemberMap.clear();
    for (MetaGroupMember member : metaGroupMemberMap.values()) {
      member.stop();
      member.closeLogManager();
    }
    metaGroupMemberMap.clear();
    RegisterManager.setDeregisterTimeOut(100);
    EnvironmentUtils.cleanEnv();
    ClusterDescriptor.getInstance().getConfig().setSeedNodeUrls(prevUrls);
    new File(MetaGroupMember.PARTITION_FILE_NAME).delete();
    new File(MetaGroupMember.NODE_IDENTIFIER_FILE_NAME).delete();
    RaftMember.setWaitLeaderTimeMs(prevLeaderWait);
    testThreadPool.shutdownNow();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(prevUseAsyncServer);
    ClusterDescriptor.getInstance().getConfig().setRaftLogBufferSize(preLogBufferSize);
    ClusterDescriptor.getInstance().getConfig().setUseAsyncApplier(prevUseAsyncApplier);
    IoTDBDescriptor.getInstance().getConfig().setEnableWal(prevEnableWAL);

    RaftServer.setSyncLeaderMaxWaitMs(syncLeaderMaxWait);
    RaftServer.setHeartBeatIntervalMs(heartBeatInterval);
  }

  DataGroupMember getDataGroupMember(Node node) {
    return dataGroupMemberMap.computeIfAbsent(node, this::newDataGroupMember);
  }

  private DataGroupMember newDataGroupMember(Node node) {
    DataGroupMember newMember =
        new TestDataGroupMember(node, partitionTable.getHeaderGroup(node)) {

          @Override
          public boolean syncLeader(RaftMember.CheckConsistency checkConsistency) {
            return true;
          }

          @Override
          public long appendEntry(AppendEntryRequest request) {
            return Response.RESPONSE_AGREE;
          }

          @Override
          public AsyncClient getAsyncClient(Node node) {
            try {
              return new TestAsyncDataClient(node, dataGroupMemberMap);
            } catch (IOException e) {
              return null;
            }
          }

          @Override
          public AsyncClient getAsyncClient(Node node, boolean activatedOnly) {
            try {
              return new TestAsyncDataClient(node, dataGroupMemberMap);
            } catch (IOException e) {
              return null;
            }
          }

          @Override
          public AsyncClient getSendLogAsyncClient(Node node) {
            return getAsyncClient(node);
          }
        };
    newMember.setThisNode(node);
    newMember.setMetaGroupMember(testMetaMember);
    newMember.setLeader(node);
    newMember.setCharacter(NodeCharacter.LEADER);
    newMember.setLogManager(
        getLogManager(partitionTable.getHeaderGroup(TestUtils.getNode(0)), newMember));

    newMember.setAppendLogThreadPool(testThreadPool);
    return newMember;
  }

  private PartitionedSnapshotLogManager getLogManager(
      PartitionGroup partitionGroup, DataGroupMember dataGroupMember) {
    return new TestPartitionedLogManager(
        new DataLogApplier(testMetaMember, dataGroupMember),
        testMetaMember.getPartitionTable(),
        partitionGroup.getHeader(),
        FileSnapshot.Factory.INSTANCE) {
      @Override
      public void takeSnapshot() {}
    };
  }

  protected MetaGroupMember getMetaGroupMember(Node node) throws QueryProcessException {
    return metaGroupMemberMap.computeIfAbsent(node, this::newMetaGroupMember);
  }

  private MetaGroupMember newMetaGroupMember(Node node) {
    MetaGroupMember ret =
        new TestMetaGroupMember() {

          @Override
          public DataGroupMember getLocalDataMember(Node header, Object request) {
            return getDataGroupMember(header);
          }

          @Override
          public DataGroupMember getLocalDataMember(Node header) {
            return getDataGroupMember(header);
          }

          @Override
          public AsyncClient getAsyncClient(Node node) {
            try {
              return new TestAsyncMetaClient(null, null, node, null) {
                @Override
                public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
                  new Thread(() -> resultHandler.onComplete(new TNodeStatus())).start();
                }
              };
            } catch (IOException e) {
              return null;
            }
          }

          @Override
          public AsyncClient getSendLogAsyncClient(Node node) {
            return getAsyncClient(node);
          }
        };
    ret.setThisNode(node);
    ret.setCoordinator(new Coordinator());
    ret.setPartitionTable(partitionTable);
    ret.setAllNodes(allNodes);
    ret.setLogManager(metaLogManager);
    ret.setLeader(node);
    ret.setCharacter(NodeCharacter.LEADER);
    ret.setAppendLogThreadPool(testThreadPool);
    ret.setClientProvider(
        new DataClientProvider(new Factory()) {
          @Override
          public AsyncDataClient getAsyncDataClient(Node node, int timeout) throws IOException {
            return new TestAsyncDataClient(node, dataGroupMemberMap);
          }
        });
    return ret;
  }
}
