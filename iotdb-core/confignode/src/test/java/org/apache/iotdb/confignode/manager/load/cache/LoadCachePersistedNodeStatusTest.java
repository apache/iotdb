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

package org.apache.iotdb.confignode.manager.load.cache;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RegisterAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateNodeStatusPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.cache.node.AINodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.BaseNodeCache;
import org.apache.iotdb.confignode.manager.load.cache.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoadCachePersistedNodeStatusTest {

  private static final int SELF_ID = ConfigNodeHeartbeatCache.CURRENT_NODE_ID;
  private static final int CONFIG_NODE_ID = SELF_ID + 100;
  private static final int DATA_NODE_ID = SELF_ID + 101;
  private static final int REMOVING_DATA_NODE_ID = SELF_ID + 102;
  private static final int AI_NODE_ID = SELF_ID + 103;

  private NodeInfo nodeInfo;
  private ConsensusManager consensusManager;
  private IManager configManager;
  private NodeManager nodeManager;
  private LoadCache loadCache;

  @Before
  public void setUp() throws Exception {
    nodeInfo = spy(new NodeInfo());
    configManager = mock(IManager.class);
    nodeManager = mock(NodeManager.class);
    consensusManager = mock(ConsensusManager.class);
    ClusterSchemaManager schemaManager = mock(ClusterSchemaManager.class);

    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getConsensusManager()).thenReturn(consensusManager);
    when(configManager.getClusterSchemaManager()).thenReturn(schemaManager);
    when(schemaManager.getDatabaseNames(null)).thenReturn(Collections.emptyList());
    when(nodeManager.getRegisteredConfigNodes())
        .thenReturn(
            Arrays.asList(
                new TConfigNodeLocation(
                    SELF_ID, new TEndPoint("127.0.0.1", 11000), new TEndPoint("127.0.0.1", 12000)),
                new TConfigNodeLocation(
                    CONFIG_NODE_ID,
                    new TEndPoint("127.0.0.1", 11001),
                    new TEndPoint("127.0.0.1", 12001))));
    when(nodeManager.getRegisteredDataNodes())
        .thenReturn(
            Arrays.asList(
                new TDataNodeConfiguration()
                    .setLocation(new TDataNodeLocation().setDataNodeId(DATA_NODE_ID)),
                new TDataNodeConfiguration()
                    .setLocation(new TDataNodeLocation().setDataNodeId(REMOVING_DATA_NODE_ID))));
    when(nodeManager.getRegisteredAINodes()).thenReturn(Collections.emptyList());
    for (TConfigNodeLocation node : nodeManager.getRegisteredConfigNodes()) {
      nodeInfo.applyConfigNode(new ApplyConfigNodePlan(node));
    }
    for (TDataNodeConfiguration node : nodeManager.getRegisteredDataNodes()) {
      nodeInfo.registerDataNode(new RegisterDataNodePlan(node));
    }
    when(consensusManager.write(any(UpdateNodeStatusPlan.class)))
        .thenAnswer(invocation -> nodeInfo.applyNodeStatusPlan(invocation.getArgument(0)));
    loadCache = new LoadCache();
    loadCache.setNodeStatusPersistence(nodeInfo::getPersistedNodeStatus, consensusManager::write);
  }

  @Test
  public void testInitializationRestoresStickyStatusesAndKeepsLeaderRunning() throws Exception {
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(CONFIG_NODE_ID, NodeStatus.Stopped));
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    nodeInfo.applyNodeStatusPlan(
        new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(SELF_ID, NodeStatus.Stopped));

    loadCache.initHeartbeatCache(configManager);

    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(CONFIG_NODE_ID));
    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(SELF_ID));
    Assert.assertTrue(loadCache.getNodeHeartbeatUnreadyReasons().isEmpty());
    verify(nodeInfo).getPersistedNodeStatus(CONFIG_NODE_ID);
    verify(nodeInfo).getPersistedNodeStatus(DATA_NODE_ID);
    verify(nodeInfo).getPersistedNodeStatus(REMOVING_DATA_NODE_ID);
    verify(nodeInfo, never()).getPersistedNodeStatus(SELF_ID);

    loadCache.updateNodeStatistics();

    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(CONFIG_NODE_ID));
    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(SELF_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(SELF_ID));
  }

  @Test
  public void testRestoredStatusDoesNotCreateHeartbeatSamples() throws Exception {
    for (NodeStatus status : Arrays.asList(NodeStatus.Stopped, NodeStatus.Removing)) {
      for (BaseNodeCache cache :
          Arrays.asList(
              new DataNodeHeartbeatCache(DATA_NODE_ID),
              new ConfigNodeHeartbeatCache(CONFIG_NODE_ID),
              new AINodeHeartbeatCache(AI_NODE_ID))) {
        cache.initPersistence(
            new Pair<>(status, null), (ignoredCache, ignoredStatus, ignoredReason) -> success());
        Assert.assertEquals(status, cache.getNodeStatus());
        Assert.assertFalse(cache.hasHeartbeatSample());
        cache.updateNodeStatistics();
        Assert.assertEquals(status, cache.getNodeStatus());
        Assert.assertFalse(cache.hasHeartbeatSample());
      }
    }
  }

  @Test
  public void testRestoredReadOnlyRequiresHeartbeatAndUnknownClearsPersistence() throws Exception {
    Pair<NodeStatus, String> persisted = new Pair<>(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);
    for (BaseNodeCache cache :
        Arrays.asList(
            new DataNodeHeartbeatCache(DATA_NODE_ID),
            new ConfigNodeHeartbeatCache(CONFIG_NODE_ID),
            new AINodeHeartbeatCache(AI_NODE_ID))) {
      cache.initPersistence(persisted, (ignoredCache, ignoredStatus, ignoredReason) -> success());
      Assert.assertEquals(NodeStatus.ReadOnly, cache.getNodeStatus());
      Assert.assertEquals(NodeStatus.DISK_FULL, cache.getNodeStatusReason());
      Assert.assertFalse(cache.hasHeartbeatSample());
    }

    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(CONFIG_NODE_ID, NodeStatus.Stopped));
    nodeInfo.applyNodeStatusPlan(
        new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    nodeInfo.applyNodeStatusPlan(
        new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.ReadOnly, NodeStatus.DISK_FULL));
    loadCache.initHeartbeatCache(configManager);
    Assert.assertEquals(NodeStatus.ReadOnly, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.DISK_FULL, loadCache.getNodeStatusReason(DATA_NODE_ID));
    Assert.assertEquals(
        Collections.singletonList("nodes=[" + DATA_NODE_ID + "]"),
        loadCache.getNodeHeartbeatUnreadyReasons());

    // Restoring durable metadata is not evidence that the node is still reachable.
    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.Unknown, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertNull(loadCache.getNodeStatusReason(DATA_NODE_ID));
    Assert.assertNull(nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(
        Collections.singletonList("nodes=[" + DATA_NODE_ID + "]"),
        loadCache.getNodeHeartbeatUnreadyReasons());
    verify(consensusManager).write(new UpdateNodeStatusPlan(DATA_NODE_ID, null));
  }

  @Test
  public void testReadOnlyReasonChangesArePersistedAndRunningClearsRecord() throws Exception {
    loadCache.initHeartbeatCache(configManager);
    // Each heartbeat replaces the previous reason, including a lower-priority or absent reason.
    for (String reason :
        Arrays.asList(NodeStatus.STOPPING, NodeStatus.MANUAL, NodeStatus.DISK_FULL, null)) {
      loadCache.cacheDataNodeHeartbeatSample(
          DATA_NODE_ID, readOnlySample(System.nanoTime(), reason));
      Assert.assertTrue(loadCache.updateNodeStatistics());
      Assert.assertEquals(NodeStatus.ReadOnly, loadCache.getNodeStatus(DATA_NODE_ID));
      Assert.assertEquals(reason, loadCache.getNodeStatusReason(DATA_NODE_ID));
      Assert.assertEquals(
          new Pair<>(NodeStatus.ReadOnly, reason), nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));

      // Repeating the same status and reason must not append another consensus log.
      loadCache.cacheDataNodeHeartbeatSample(
          DATA_NODE_ID, readOnlySample(System.nanoTime(), reason));
      Assert.assertTrue(loadCache.updateNodeStatistics());
      verify(consensusManager, times(1))
          .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.ReadOnly, reason));
    }

    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertNull(loadCache.getNodeStatusReason(DATA_NODE_ID));
    Assert.assertNull(nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    verify(consensusManager).write(new UpdateNodeStatusPlan(DATA_NODE_ID, null));
  }

  @Test
  public void testLiveHeartbeatClearsStoppedStatusForBothNodeTypes() throws Exception {
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(CONFIG_NODE_ID, NodeStatus.Stopped));
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);

    loadCache.cacheConfigNodeHeartbeatSample(
        CONFIG_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    loadCache.updateNodeStatistics();

    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(CONFIG_NODE_ID));
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(CONFIG_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(DATA_NODE_ID));
  }

  @Test
  public void testRevivalUpdatesStatisticsWhileClearIsRetried() throws Exception {
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    doReturn(failure())
        .doAnswer(invocation -> nodeInfo.applyNodeStatusPlan(invocation.getArgument(0)))
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, null));
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));

    Assert.assertFalse(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));

    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(DATA_NODE_ID));
    verify(consensusManager, times(2)).write(new UpdateNodeStatusPlan(DATA_NODE_ID, null));
  }

  @Test
  public void testClearDoesNotRecheckThePreviousPersistedStatus() throws Exception {
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    // CLEAR executes the caller's decision even if the applied value has changed.
    doAnswer(
            invocation -> {
              nodeInfo.applyNodeStatusPlan(
                  new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Removing));
              return nodeInfo.applyNodeStatusPlan(invocation.getArgument(0));
            })
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, null));

    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertNull(nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
  }

  @Test
  public void testReadOnlyRevivalPublishesReasonWhilePersistenceIsRetried() throws Exception {
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    doReturn(failure())
        .doAnswer(invocation -> nodeInfo.applyNodeStatusPlan(invocation.getArgument(0)))
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.ReadOnly, NodeStatus.MANUAL));
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, readOnlySample(System.nanoTime(), NodeStatus.MANUAL));

    Assert.assertFalse(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.ReadOnly, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.MANUAL, loadCache.getNodeStatusReason(DATA_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));

    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.ReadOnly, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.MANUAL, loadCache.getNodeStatusReason(DATA_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.ReadOnly, NodeStatus.MANUAL),
        nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    verify(consensusManager, times(2))
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.ReadOnly, NodeStatus.MANUAL));

    // A new leader restores the committed reason before receiving its first heartbeat.
    loadCache.initHeartbeatCache(configManager);
    Assert.assertEquals(NodeStatus.ReadOnly, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.MANUAL, loadCache.getNodeStatusReason(DATA_NODE_ID));
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, readOnlySample(System.nanoTime(), NodeStatus.MANUAL));
    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.ReadOnly, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.MANUAL, loadCache.getNodeStatusReason(DATA_NODE_ID));
  }

  @Test
  public void testReadOnlyHeartbeatIsNotFilteredByCacheInitializationTime() throws Exception {
    long previousLeaderTimestamp = System.nanoTime();
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, readOnlySample(previousLeaderTimestamp, NodeStatus.MANUAL));

    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.ReadOnly, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.MANUAL, loadCache.getNodeStatusReason(DATA_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.ReadOnly, NodeStatus.MANUAL),
        nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    verify(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.ReadOnly, NodeStatus.MANUAL));
  }

  @Test
  public void testStoppingReasonIsClearedByShutdownAndCannotOverrideRemoving() throws Exception {
    nodeInfo.applyNodeStatusPlan(
        new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    loadCache.initHeartbeatCache(configManager);
    for (int nodeId : Arrays.asList(DATA_NODE_ID, REMOVING_DATA_NODE_ID)) {
      loadCache.cacheDataNodeHeartbeatSample(
          nodeId, readOnlySample(System.nanoTime(), NodeStatus.STOPPING));
    }
    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.ReadOnly, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.STOPPING, loadCache.getNodeStatusReason(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertNull(loadCache.getNodeStatusReason(REMOVING_DATA_NODE_ID));

    for (int nodeId : Arrays.asList(DATA_NODE_ID, REMOVING_DATA_NODE_ID)) {
      Assert.assertEquals(
          success().getCode(),
          loadCache.trySetNodeStatus(nodeId, NodeStatus.Stopped, null, false).getCode());
      Assert.assertNull(loadCache.getNodeStatusReason(nodeId));
    }
    Assert.assertEquals(
        new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Removing, null),
        nodeInfo.getPersistedNodeStatus(REMOVING_DATA_NODE_ID));
  }

  @Test
  public void testStoppedStatisticsUpdateAfterFailedPersistenceReturns() throws Exception {
    loadCache.initHeartbeatCache(configManager);
    loadCache.trySetNodeStatus(DATA_NODE_ID, NodeStatus.Running, null, false);
    CountDownLatch persistenceStarted = new CountDownLatch(1);
    CountDownLatch finishPersistence = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              persistenceStarted.countDown();
              Assert.assertTrue(finishPersistence.await(5, TimeUnit.SECONDS));
              return failure();
            })
        .doAnswer(invocation -> nodeInfo.applyNodeStatusPlan(invocation.getArgument(0)))
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<TSStatus> update =
          executor.submit(
              () -> loadCache.trySetNodeStatus(DATA_NODE_ID, NodeStatus.Stopped, null, false));
      Assert.assertTrue(persistenceStarted.await(5, TimeUnit.SECONDS));
      Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
      Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(DATA_NODE_ID));
      finishPersistence.countDown();
      Assert.assertEquals(failure().getCode(), update.get(5, TimeUnit.SECONDS).getCode());
      Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
      Assert.assertNull(nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));

      // Even when the latest sample becomes Unknown, statistics retain the stop for retry.
      loadCache.cacheDataNodeHeartbeatSample(
          DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Unknown));
      Assert.assertTrue(loadCache.updateNodeStatistics());
      Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
      Assert.assertEquals(
          new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
      verify(consensusManager, times(2))
          .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    } finally {
      finishPersistence.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testHeartbeatAndStatisticsWaitUntilShutdownPersistenceCompletes() throws Exception {
    loadCache.initHeartbeatCache(configManager);
    loadCache.trySetNodeStatus(DATA_NODE_ID, NodeStatus.Running, null, false);
    NodeHeartbeatSample delayedRunning = new NodeHeartbeatSample(NodeStatus.Running);
    CountDownLatch persistenceStarted = new CountDownLatch(1);
    CountDownLatch finishPersistence = new CountDownLatch(1);
    CountDownLatch updatesStarted = new CountDownLatch(2);
    doAnswer(
            invocation -> {
              persistenceStarted.countDown();
              Assert.assertTrue(finishPersistence.await(10, TimeUnit.SECONDS));
              return nodeInfo.applyNodeStatusPlan(invocation.getArgument(0));
            })
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    ExecutorService executor = Executors.newFixedThreadPool(3);
    try {
      Future<TSStatus> stopped =
          executor.submit(
              () -> loadCache.trySetNodeStatus(DATA_NODE_ID, NodeStatus.Stopped, null, false));
      Assert.assertTrue(persistenceStarted.await(5, TimeUnit.SECONDS));
      Future<?> heartbeat =
          executor.submit(
              () -> {
                updatesStarted.countDown();
                loadCache.cacheDataNodeHeartbeatSample(DATA_NODE_ID, delayedRunning);
              });
      Future<Boolean> statistics =
          executor.submit(
              () -> {
                updatesStarted.countDown();
                return loadCache.updateNodeStatistics();
              });
      Assert.assertTrue(updatesStarted.await(5, TimeUnit.SECONDS));
      for (Future<?> pending : Arrays.asList(heartbeat, statistics)) {
        try {
          pending.get(100, TimeUnit.MILLISECONDS);
          Assert.fail("The node update must wait for shutdown persistence");
        } catch (TimeoutException expected) {
          // Both operations must wait, while the last published status remains readable.
        }
      }
      Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
      Assert.assertNull(nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
      verify(consensusManager, times(1))
          .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));

      finishPersistence.countDown();
      Assert.assertEquals(success().getCode(), stopped.get(5, TimeUnit.SECONDS).getCode());
      heartbeat.get(5, TimeUnit.SECONDS);
      Assert.assertTrue(statistics.get(5, TimeUnit.SECONDS));
      Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
      Assert.assertEquals(
          new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));

      loadCache.cacheDataNodeHeartbeatSample(
          DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
      Assert.assertTrue(loadCache.updateNodeStatistics());
      Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
      Assert.assertNull(nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    } finally {
      finishPersistence.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testHeartbeatIsNotFilteredByCacheInitializationTime() throws Exception {
    long oldSampleTimestamp = System.nanoTime() - 1;
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(oldSampleTimestamp, NodeStatus.Running));
    loadCache.updateNodeStatistics();

    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertNull(nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    verify(consensusManager).write(new UpdateNodeStatusPlan(DATA_NODE_ID, null));
  }

  @Test
  public void testForcingAnotherNodeDoesNotClearRemovingStatus() throws Exception {
    nodeInfo.applyNodeStatusPlan(
        new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    loadCache.initHeartbeatCache(configManager);
    loadCache.cacheDataNodeHeartbeatSample(
        REMOVING_DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));

    Assert.assertEquals(
        success().getCode(),
        loadCache.trySetNodeStatus(DATA_NODE_ID, NodeStatus.Stopped, null, false).getCode());

    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Removing, null),
        nodeInfo.getPersistedNodeStatus(REMOVING_DATA_NODE_ID));
    verify(consensusManager, never())
        .write(
            argThat(
                plan ->
                    plan instanceof UpdateNodeStatusPlan
                        && ((UpdateNodeStatusPlan) plan).getNodeId() == REMOVING_DATA_NODE_ID));

    // An explicit rollback of that node still clears its durable Removing marker.
    Assert.assertEquals(
        success().getCode(),
        loadCache
            .trySetNodeStatus(REMOVING_DATA_NODE_ID, NodeStatus.Running, null, true)
            .getCode());
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(REMOVING_DATA_NODE_ID));
  }

  @Test
  public void testExplicitRollbackRestoresOfflineAndRunningStatuses() throws Exception {
    for (NodeStatus rollbackStatus :
        Arrays.asList(
            NodeStatus.Unknown, NodeStatus.Stopped, NodeStatus.Running, NodeStatus.ReadOnly)) {
      nodeInfo.applyNodeStatusPlan(
          new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
      loadCache.initHeartbeatCache(configManager);

      Assert.assertEquals(
          success().getCode(),
          loadCache
              .trySetNodeStatus(
                  REMOVING_DATA_NODE_ID,
                  rollbackStatus,
                  rollbackStatus == NodeStatus.ReadOnly ? NodeStatus.MANUAL : null,
                  true)
              .getCode());
      Assert.assertEquals(rollbackStatus, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
      Assert.assertEquals(
          rollbackStatus == NodeStatus.ReadOnly ? NodeStatus.MANUAL : null,
          loadCache.getNodeStatusReason(REMOVING_DATA_NODE_ID));
      Assert.assertEquals(
          rollbackStatus.isPersistentStatus()
              ? new Pair<>(
                  rollbackStatus, rollbackStatus == NodeStatus.ReadOnly ? NodeStatus.MANUAL : null)
              : null,
          nodeInfo.getPersistedNodeStatus(REMOVING_DATA_NODE_ID));

      // Ordinary statistics updates must preserve the successfully committed rollback.
      Assert.assertTrue(loadCache.updateNodeStatistics());
      Assert.assertEquals(rollbackStatus, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
      Assert.assertEquals(
          rollbackStatus == NodeStatus.ReadOnly ? NodeStatus.MANUAL : null,
          loadCache.getNodeStatusReason(REMOVING_DATA_NODE_ID));
    }
  }

  @Test
  public void testFailedExplicitRollbackUpdatesStatisticsAndRetriesPeriodically() throws Exception {
    nodeInfo.applyNodeStatusPlan(
        new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    loadCache.initHeartbeatCache(configManager);
    doReturn(failure())
        .doAnswer(invocation -> nodeInfo.applyNodeStatusPlan(invocation.getArgument(0)))
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, null));

    Assert.assertEquals(
        failure().getCode(),
        loadCache
            .trySetNodeStatus(REMOVING_DATA_NODE_ID, NodeStatus.Running, null, true)
            .getCode());
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Removing, null),
        nodeInfo.getPersistedNodeStatus(REMOVING_DATA_NODE_ID));

    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(REMOVING_DATA_NODE_ID));
    verify(consensusManager, times(2)).write(new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, null));
    verify(consensusManager, never())
        .write(new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
  }

  @Test
  public void testShutdownReportCannotOverrideRemovingStatus() throws Exception {
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(CONFIG_NODE_ID, NodeStatus.Removing));
    nodeInfo.applyNodeStatusPlan(
        new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    loadCache.initHeartbeatCache(configManager);

    for (int nodeId : Arrays.asList(CONFIG_NODE_ID, REMOVING_DATA_NODE_ID)) {
      Assert.assertEquals(
          success().getCode(),
          loadCache.trySetNodeStatus(nodeId, NodeStatus.Stopped, null, false).getCode());
      Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(nodeId));
      Assert.assertEquals(
          new Pair<>(NodeStatus.Removing, null), nodeInfo.getPersistedNodeStatus(nodeId));
      verify(consensusManager, never()).write(new UpdateNodeStatusPlan(nodeId, NodeStatus.Stopped));
    }
  }

  @Test
  public void testStatisticsFailureDoesNotPreventUpdatingOtherNodes() throws Exception {
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(CONFIG_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    doReturn(failure()).when(consensusManager).write(new UpdateNodeStatusPlan(DATA_NODE_ID, null));
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    loadCache.cacheConfigNodeHeartbeatSample(
        CONFIG_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));

    Assert.assertFalse(loadCache.updateNodeStatistics());

    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(CONFIG_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(CONFIG_NODE_ID));
    verify(consensusManager, never()).write(new UpdateNodeStatusPlan(SELF_ID, null));
    verify(consensusManager, never()).write(new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, null));
  }

  @Test
  public void testRepeatedCacheCreationPreservesPendingShutdown() throws Exception {
    loadCache.initHeartbeatCache(configManager);
    Assert.assertEquals(
        success().getCode(),
        loadCache.trySetNodeStatus(CONFIG_NODE_ID, NodeStatus.Running, null, false).getCode());
    Assert.assertFalse(loadCache.checkAndSetHeartbeatProcessing(CONFIG_NODE_ID));
    CountDownLatch persistenceStarted = new CountDownLatch(1);
    CountDownLatch finishPersistence = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              persistenceStarted.countDown();
              Assert.assertTrue(finishPersistence.await(10, TimeUnit.SECONDS));
              return nodeInfo.applyNodeStatusPlan(invocation.getArgument(0));
            })
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(CONFIG_NODE_ID, NodeStatus.Stopped));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<TSStatus> shutdown =
          executor.submit(
              () -> loadCache.trySetNodeStatus(CONFIG_NODE_ID, NodeStatus.Stopped, null, false));
      Assert.assertTrue(persistenceStarted.await(5, TimeUnit.SECONDS));
      // A recovered AddConfigNodeProcedure may repeat REGISTER_SUCCESS after registration applied.
      loadCache.createNodeHeartbeatCache(NodeType.ConfigNode, CONFIG_NODE_ID);
      finishPersistence.countDown();
      Assert.assertEquals(success().getCode(), shutdown.get(5, TimeUnit.SECONDS).getCode());
      Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(CONFIG_NODE_ID));
      Assert.assertEquals(
          new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(CONFIG_NODE_ID));
      Assert.assertTrue(loadCache.checkAndSetHeartbeatProcessing(CONFIG_NODE_ID));

      Assert.assertTrue(loadCache.updateNodeStatistics());
      Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(CONFIG_NODE_ID));
      Assert.assertEquals(
          new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(CONFIG_NODE_ID));
      verify(consensusManager, never()).write(new UpdateNodeStatusPlan(CONFIG_NODE_ID, null));
    } finally {
      finishPersistence.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testDiscardedCacheCannotClearNewRemovingMarker() throws Exception {
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    Field field = LoadCache.class.getDeclaredField("nodeCacheMap");
    field.setAccessible(true);
    Map<Integer, BaseNodeCache> caches = (Map<Integer, BaseNodeCache>) field.get(loadCache);
    BaseNodeCache discarded = caches.get(DATA_NODE_ID);
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Removing));
    loadCache.initHeartbeatCache(configManager);
    Assert.assertEquals(
        failure().getCode(), discarded.trySetNodeStatus(NodeStatus.Running, null, true).getCode());
    Assert.assertEquals(
        new Pair<>(NodeStatus.Removing, null), nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(DATA_NODE_ID));
    verify(consensusManager, never()).write(new UpdateNodeStatusPlan(DATA_NODE_ID, null));
  }

  @Test
  public void testReadOnlyHeartbeatReplacesStoppedButNotRemoving() throws Exception {
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    nodeInfo.applyNodeStatusPlan(
        new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    loadCache.initHeartbeatCache(configManager);
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.ReadOnly));
    loadCache.cacheDataNodeHeartbeatSample(
        REMOVING_DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.ReadOnly));
    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.ReadOnly, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.ReadOnly, null), nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Removing, null),
        nodeInfo.getPersistedNodeStatus(REMOVING_DATA_NODE_ID));
  }

  @Test
  public void testAINodeRestoresStoppedAndRemovingAcrossLeaderChange() throws Exception {
    registerAINode();
    loadCache.initHeartbeatCache(configManager);
    for (NodeStatus status : Arrays.asList(NodeStatus.Stopped, NodeStatus.Removing)) {
      Assert.assertEquals(
          success().getCode(),
          loadCache.trySetNodeStatus(AI_NODE_ID, status, null, false).getCode());
      Assert.assertEquals(new Pair<>(status, null), nodeInfo.getPersistedNodeStatus(AI_NODE_ID));
      loadCache.initHeartbeatCache(configManager);
      Assert.assertEquals(status, loadCache.getNodeStatus(AI_NODE_ID));
      Assert.assertTrue(loadCache.updateNodeStatistics());
      Assert.assertEquals(status, loadCache.getNodeStatus(AI_NODE_ID));
    }
    loadCache.cacheAINodeHeartbeatSample(AI_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(AI_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Removing, null), nodeInfo.getPersistedNodeStatus(AI_NODE_ID));
  }

  @Test
  public void testAINodeUpdatesStatisticsOnPersistenceFailureAndRetries() throws Exception {
    registerAINode();
    loadCache.initHeartbeatCache(configManager);
    doReturn(failure())
        .doAnswer(invocation -> nodeInfo.applyNodeStatusPlan(invocation.getArgument(0)))
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(AI_NODE_ID, NodeStatus.Stopped));
    Assert.assertEquals(
        failure().getCode(),
        loadCache.trySetNodeStatus(AI_NODE_ID, NodeStatus.Stopped, null, false).getCode());
    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(AI_NODE_ID));
    Assert.assertNull(nodeInfo.getPersistedNodeStatus(AI_NODE_ID));
    loadCache.cacheAINodeHeartbeatSample(AI_NODE_ID, new NodeHeartbeatSample(NodeStatus.Unknown));
    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(AI_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(AI_NODE_ID));

    doReturn(failure())
        .doAnswer(invocation -> nodeInfo.applyNodeStatusPlan(invocation.getArgument(0)))
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(AI_NODE_ID, null));
    loadCache.cacheAINodeHeartbeatSample(AI_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    Assert.assertFalse(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(AI_NODE_ID));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(AI_NODE_ID));
    Assert.assertTrue(loadCache.updateNodeStatistics());
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(AI_NODE_ID));
    Assert.assertNull(nodeInfo.getPersistedNodeStatus(AI_NODE_ID));
  }

  @Test
  public void testDelayedAINodeHeartbeatDoesNotRecreateRemovedCache() throws Exception {
    registerAINode();
    loadCache.initHeartbeatCache(configManager);
    loadCache.removeNodeCache(AI_NODE_ID);
    loadCache.cacheAINodeHeartbeatSample(AI_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    Assert.assertFalse(loadCache.getCurrentNodeStatisticsMap().containsKey(AI_NODE_ID));
  }

  private void registerAINode() {
    TAINodeConfiguration node =
        new TAINodeConfiguration().setLocation(new TAINodeLocation().setAiNodeId(AI_NODE_ID));
    nodeInfo.registerAINode(new RegisterAINodePlan(node));
    when(nodeManager.getRegisteredAINodes()).thenReturn(Collections.singletonList(node));
  }

  @Test
  public void testOlderHeartbeatCannotUndoShutdownReport() throws Exception {
    loadCache.initHeartbeatCache(configManager);
    NodeHeartbeatSample delayedRunning = new NodeHeartbeatSample(NodeStatus.Running);
    Assert.assertEquals(
        success().getCode(),
        loadCache.trySetNodeStatus(DATA_NODE_ID, NodeStatus.Stopped, null, false).getCode());
    for (NodeHeartbeatSample olderHeartbeat :
        Arrays.asList(
            delayedRunning,
            new NodeHeartbeatSample(
                System.nanoTime() - TimeUnit.DAYS.toNanos(1), NodeStatus.Running))) {
      loadCache.cacheDataNodeHeartbeatSample(DATA_NODE_ID, olderHeartbeat);
      loadCache.updateNodeStatistics();
      Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
      Assert.assertEquals(
          new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    }
  }

  @Test
  public void testBlockedNodeWriteDoesNotHoldOtherNodeCacheLock() throws Exception {
    loadCache.initHeartbeatCache(configManager);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(
            i -> {
              entered.countDown();
              Assert.assertTrue(release.await(10, TimeUnit.SECONDS));
              return nodeInfo.applyNodeStatusPlan(i.getArgument(0));
            })
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<TSStatus> blocked =
          executor.submit(
              () -> loadCache.trySetNodeStatus(DATA_NODE_ID, NodeStatus.Stopped, null, true));
      Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
      Future<TSStatus> independent =
          executor.submit(
              () ->
                  loadCache.trySetNodeStatus(
                      REMOVING_DATA_NODE_ID, NodeStatus.Removing, null, true));
      Assert.assertEquals(success().getCode(), independent.get(5, TimeUnit.SECONDS).getCode());
      Assert.assertEquals(
          new Pair<>(NodeStatus.Removing, null),
          nodeInfo.getPersistedNodeStatus(REMOVING_DATA_NODE_ID));
      Assert.assertFalse(blocked.isDone());
      release.countDown();
      Assert.assertEquals(success().getCode(), blocked.get(5, TimeUnit.SECONDS).getCode());
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  private static NodeHeartbeatSample readOnlySample(long timestamp, String reason) {
    return new NodeHeartbeatSample(
        new TDataNodeHeartbeatResp()
            .setHeartbeatTimestamp(timestamp)
            .setStatus(NodeStatus.ReadOnly.getStatus())
            .setStatusReason(reason));
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Test
  public void testLateShutdownReportConvergesAfterRestartedNodeHeartbeat() throws Exception {
    loadCache.initHeartbeatCache(configManager);
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    loadCache.updateNodeStatistics();
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    // Shutdown reports carry no DataNode incarnation. Document the current temporary Stopped
    // result, then require a fresh heartbeat from the new process to durably clear that marker.
    Assert.assertEquals(
        success().getCode(),
        loadCache.trySetNodeStatus(DATA_NODE_ID, NodeStatus.Stopped, null, false).getCode());
    Assert.assertEquals(
        new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(DATA_NODE_ID));
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    loadCache.updateNodeStatistics();
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(DATA_NODE_ID));
  }

  private static TSStatus failure() {
    return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
  }
}
