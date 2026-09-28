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

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateNodeStatusPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateNodeStatusPlan.Operation;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoadCacheNodeStatusTest {
  private static final int NODE_ID = 1;

  private NodeInfo nodeInfo;
  private LoadCache loadCache;
  private ConsensusManager consensusManager;

  @Before
  public void setUp() throws Exception {
    nodeInfo = new NodeInfo();
    nodeInfo.registerDataNode(
        new RegisterDataNodePlan(
            new TDataNodeConfiguration()
                .setLocation(new TDataNodeLocation().setDataNodeId(NODE_ID))));
    IManager manager = mock(IManager.class, RETURNS_DEEP_STUBS);
    consensusManager = mock(ConsensusManager.class);
    when(manager.getConsensusManager()).thenReturn(consensusManager);
    when(consensusManager.write(any(UpdateNodeStatusPlan.class)))
        .thenAnswer(invocation -> nodeInfo.applyNodeStatusPlan(invocation.getArgument(0)));
    when(manager.getNodeManager()).thenReturn(new NodeManager(manager, nodeInfo));
    when(manager.getClusterSchemaManager().getDatabaseNames(null))
        .thenReturn(Collections.emptyList());
    loadCache = new LoadCache();
    loadCache.setNodeStatusPersistence(nodeInfo::getPersistedNodeStatus, consensusManager::write);
    loadCache.initHeartbeatCache(manager);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLoadManagerSuppliesPersistenceBeforeConsensusInitialization() throws Exception {
    ConfigManager manager = new ConfigManager();
    try {
      NodeInfo info = manager.getNodeManager().getNodeInfo();
      info.registerDataNode(
          new RegisterDataNodePlan(
              new TDataNodeConfiguration()
                  .setLocation(new TDataNodeLocation().setDataNodeId(NODE_ID))));
      LoadCache cache = manager.getLoadManager().getLoadCache();
      cache.initHeartbeatCache(manager);

      // Use the real methods supplied by LoadManager, replacing only consensus submission.
      Field consensusField = ConfigManager.class.getDeclaredField("consensusManager");
      consensusField.setAccessible(true);
      ((AtomicReference<ConsensusManager>) consensusField.get(manager)).set(consensusManager);
      doThrow(new ConsensusException("quorum unavailable"))
          .when(consensusManager)
          .write(any(UpdateNodeStatusPlan.class));
      TSStatus result = cache.trySetNodeStatus(NODE_ID, NodeStatus.Stopped, false);
      assertFailure(result);
      assertEquals("quorum unavailable", result.getMessage());
      assertEquals(NodeStatus.Stopped, cache.getNodeStatus(NODE_ID));
      assertNull(info.getPersistedNodeStatus(NODE_ID));

      doAnswer(invocation -> info.applyNodeStatusPlan(invocation.getArgument(0)))
          .when(consensusManager)
          .write(any(UpdateNodeStatusPlan.class));
      assertTrue(cache.updateNodeStatistics());
      assertEquals(NodeStatus.Stopped, info.getPersistedNodeStatus(NODE_ID));
      assertSuccess(cache.trySetNodeStatus(NODE_ID, NodeStatus.Stopped, false));
      verify(consensusManager, times(2))
          .write(new UpdateNodeStatusPlan(NODE_ID, Operation.SET_STOPPED));
    } finally {
      manager.close();
    }
  }

  @Test
  public void testOnlyDurableTransitionsWriteConsensus() throws Exception {
    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Running, false));
    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Unknown, false));
    verify(consensusManager, never()).write(any());

    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    assertEquals(NodeStatus.Stopped, nodeInfo.getPersistedNodeStatus(NODE_ID));
    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    verify(consensusManager, times(1)).write(any());

    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Removing, true));
    assertEquals(NodeStatus.Removing, nodeInfo.getPersistedNodeStatus(NODE_ID));
    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Running, true));
    assertNull(nodeInfo.getPersistedNodeStatus(NODE_ID));
    verify(consensusManager, times(3)).write(any());
  }

  @Test
  public void testFailedWriteIsRetriedAndClearIsDurable() throws Exception {
    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Running, false));
    doReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()))
        .doAnswer(invocation -> nodeInfo.applyNodeStatusPlan(invocation.getArgument(0)))
        .when(consensusManager)
        .write(any(UpdateNodeStatusPlan.class));
    assertFailure(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    assertNull(nodeInfo.getPersistedNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(NODE_ID));
    // A later connection failure must retain the stop and retry its uncommitted record.
    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Unknown, false));
    assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Stopped, nodeInfo.getPersistedNodeStatus(NODE_ID));
    verify(consensusManager, times(2))
        .write(new UpdateNodeStatusPlan(NODE_ID, Operation.SET_STOPPED));

    doThrow(new ConsensusException("test consensus failure"))
        .doAnswer(invocation -> nodeInfo.applyNodeStatusPlan(invocation.getArgument(0)))
        .when(consensusManager)
        .write(any(UpdateNodeStatusPlan.class));
    assertFailure(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Running, false));
    assertEquals(NodeStatus.Stopped, nodeInfo.getPersistedNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Running, loadCache.getNodeStatus(NODE_ID));
    assertTrue(loadCache.updateNodeStatistics());
    assertEquals(NodeStatus.Running, loadCache.getNodeStatus(NODE_ID));
    assertNull(nodeInfo.getPersistedNodeStatus(NODE_ID));
  }

  private static void assertSuccess(TSStatus status) {
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @Test
  public void testCommittedWriteWithLostResponseIsNotClearedByUnknown() throws Exception {
    doAnswer(
            i -> {
              nodeInfo.applyNodeStatusPlan(i.getArgument(0));
              throw new ConsensusException("response lost after commit");
            })
        .when(consensusManager)
        .write(any(UpdateNodeStatusPlan.class));
    assertFailure(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    assertEquals(NodeStatus.Stopped, nodeInfo.getPersistedNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(NODE_ID));
    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Unknown, false));
    assertTrue(loadCache.updateNodeStatistics());
    assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Stopped, nodeInfo.getPersistedNodeStatus(NODE_ID));
    verify(consensusManager, times(1)).write(any());
  }

  @Test
  public void testOrdinaryUpdatesKeepRemovingWithoutQuorumButRollbackRequiresIt() throws Exception {
    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Removing, true));
    clearInvocations(consensusManager);
    doThrow(new ConsensusException("quorum unavailable")).when(consensusManager).write(any());
    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Removing, true));
    for (NodeStatus observed :
        new NodeStatus[] {NodeStatus.Unknown, NodeStatus.Stopped, NodeStatus.Running}) {
      assertSuccess(loadCache.trySetNodeStatus(NODE_ID, observed, false));
      assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(NODE_ID));
    }
    verify(consensusManager, never()).write(any());
    assertFailure(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Stopped, true));
    assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(NODE_ID));
    assertFailure(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Running, true));
    assertEquals(NodeStatus.Removing, nodeInfo.getPersistedNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Running, loadCache.getNodeStatus(NODE_ID));
    assertFalse(loadCache.updateNodeStatistics());
    assertEquals(NodeStatus.Running, loadCache.getNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Removing, nodeInfo.getPersistedNodeStatus(NODE_ID));

    doAnswer(invocation -> nodeInfo.applyNodeStatusPlan(invocation.getArgument(0)))
        .when(consensusManager)
        .write(any(UpdateNodeStatusPlan.class));
    assertTrue(loadCache.updateNodeStatistics());
    assertEquals(NodeStatus.Running, loadCache.getNodeStatus(NODE_ID));
    assertNull(nodeInfo.getPersistedNodeStatus(NODE_ID));
    verify(consensusManager, never())
        .write(new UpdateNodeStatusPlan(NODE_ID, Operation.SET_REMOVING));
  }

  @Test
  public void testConnectionFailureKeepsStoppedUntilExplicitReset() throws Exception {
    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    clearInvocations(consensusManager);

    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Unknown, false));
    assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Stopped, nodeInfo.getPersistedNodeStatus(NODE_ID));
    verify(consensusManager, never()).write(any());

    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Unknown, true));
    assertEquals(NodeStatus.Unknown, loadCache.getNodeStatus(NODE_ID));
    assertNull(nodeInfo.getPersistedNodeStatus(NODE_ID));
    loadCache.updateNodeStatistics();
    assertEquals(NodeStatus.Unknown, loadCache.getNodeStatus(NODE_ID));
  }

  private static void assertFailure(TSStatus status) {
    assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
  }

  @Test
  public void testCommittedClearWithLostResponseIsNotRevertedByStatistics() throws Exception {
    assertSuccess(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Removing, true));
    clearInvocations(consensusManager);
    doAnswer(
            i -> {
              nodeInfo.applyNodeStatusPlan(i.getArgument(0));
              throw new ConsensusException("clear response lost");
            })
        .when(consensusManager)
        .write(any(UpdateNodeStatusPlan.class));
    assertFailure(loadCache.trySetNodeStatus(NODE_ID, NodeStatus.Running, true));
    assertNull(nodeInfo.getPersistedNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Running, loadCache.getNodeStatus(NODE_ID));
    assertTrue(loadCache.updateNodeStatistics());
    assertEquals(NodeStatus.Running, loadCache.getNodeStatus(NODE_ID));
    assertNull(nodeInfo.getPersistedNodeStatus(NODE_ID));
    verify(consensusManager, times(1)).write(any());
    verify(consensusManager, never())
        .write(new UpdateNodeStatusPlan(NODE_ID, Operation.SET_REMOVING));
  }
}
