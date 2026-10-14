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
package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.request.AsyncRequestManager;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateNodeStatusPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RemoveDataNodeHandler;
import org.apache.iotdb.confignode.procedure.state.RemoveDataNodeState;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/** Real procedure/handler/cache/NodeInfo, with faults only at the RPC and consensus boundaries. */
public class RemoveDataNodePersistenceTest {
  private static final int FIRST = 100;
  private static final int SECOND = 101;
  private final Map<Integer, NodeStatus> remoteStatuses = new HashMap<>();
  private final Map<Integer, Boolean> failRpc = new HashMap<>();
  private final Map<Integer, Boolean> failStop = new HashMap<>();
  private NodeInfo nodeInfo;
  private LoadCache cache;
  private ConfigManager manager;
  private ConsensusManager consensus;
  private RemoveDataNodeHandler handler;
  private ConfigNodeProcedureEnv env;
  private Field clientManagerField;
  private Object originalClientManager;
  private List<TDataNodeLocation> nodes;
  private boolean failStatusWrite;
  private boolean applyRpcBeforeFailure;
  private boolean failDeleteWrite;
  private boolean throwDeleteWrite;
  private int stopRequests;

  @Before
  public void setUp() throws Exception {
    nodes = Arrays.asList(location(FIRST), location(SECOND));
    nodeInfo = new NodeInfo();
    manager = mock(ConfigManager.class, RETURNS_DEEP_STUBS);
    consensus = mock(ConsensusManager.class);
    when(manager.getConsensusManager()).thenReturn(consensus);
    when(consensus.isLeader()).thenReturn(true);
    when(consensus.write(any(RemoveDataNodePlan.class)))
        .thenAnswer(
            i -> {
              if (throwDeleteWrite) {
                throw new ConsensusException("injected delete failure");
              }
              return failDeleteWrite ? failure() : nodeInfo.removeDataNode(i.getArgument(0));
            });
    for (TDataNodeLocation node : nodes) {
      nodeInfo.registerDataNode(
          new RegisterDataNodePlan(new TDataNodeConfiguration().setLocation(node)));
      remoteStatuses.put(node.getDataNodeId(), NodeStatus.Running);
    }
    when(manager.getNodeManager()).thenReturn(new NodeManager(manager, nodeInfo));
    when(manager.getClusterSchemaManager().getDatabaseNames(null))
        .thenReturn(Collections.emptyList());
    when(manager.getPartitionManager().getAllReplicaSets()).thenReturn(Collections.emptyList());
    when(manager.getPartitionManager().getAllReplicaSets(anyInt()))
        .thenReturn(Collections.emptyList());
    cache = new LoadCache();
    cache.setNodeStatusPersistence(
        nodeInfo::getPersistedNodeStatus,
        plan -> failStatusWrite ? failure() : nodeInfo.applyNodeStatusPlan(plan));
    cache.initHeartbeatCache(manager);
    for (TDataNodeLocation node : nodes) {
      assertSuccess(cache.trySetNodeStatus(node.getDataNodeId(), NodeStatus.Running, null, true));
    }
    LoadManager loadManager = mock(LoadManager.class);
    when(manager.getLoadManager()).thenReturn(loadManager);
    when(loadManager.trySetNodeStatus(anyInt(), any(), any(), anyBoolean()))
        .thenAnswer(
            i ->
                cache.trySetNodeStatus(
                    i.getArgument(0), i.getArgument(1), i.getArgument(2), i.getArgument(3)));
    when(loadManager.getNodeStatus(anyInt()))
        .thenAnswer(i -> cache.getNodeStatus(i.getArgument(0)));
    doAnswer(
            i -> {
              cache.removeNodeCache(i.getArgument(0));
              return null;
            })
        .when(loadManager)
        .removeNodeCache(anyInt());

    // Keep the real request context, handler and retry loop. Restore the singleton after each test.
    IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> transport =
        mock(IClientManager.class);
    for (TDataNodeLocation node : nodes) {
      int id = node.getDataNodeId();
      AsyncDataNodeInternalServiceClient client = mock(AsyncDataNodeInternalServiceClient.class);
      when(transport.borrowClient(node.getInternalEndPoint())).thenReturn(client);
      doAnswer(
              i -> {
                AsyncMethodCallback<TSStatus> callback = i.getArgument(1);
                if (failRpc.getOrDefault(id, false)) {
                  if (applyRpcBeforeFailure) {
                    remoteStatuses.put(id, NodeStatus.parse(i.getArgument(0)));
                  }
                  callback.onComplete(failure());
                } else {
                  remoteStatuses.put(id, NodeStatus.parse(i.getArgument(0)));
                  callback.onComplete(success());
                }
                return null;
              })
          .when(client)
          .setSystemStatus(anyString(), any());
      doAnswer(
              i -> {
                stopRequests++;
                AsyncMethodCallback<TSStatus> callback = i.getArgument(0);
                callback.onComplete(failStop.getOrDefault(id, false) ? failure() : success());
                return null;
              })
          .when(client)
          .stopAndClearDataNode(any());
    }
    clientManagerField = AsyncRequestManager.class.getDeclaredField("clientManager");
    clientManagerField.setAccessible(true);
    originalClientManager =
        clientManagerField.get(CnToDnInternalServiceAsyncRequestManager.getInstance());
    clientManagerField.set(CnToDnInternalServiceAsyncRequestManager.getInstance(), transport);
    handler = spy(new RemoveDataNodeHandler(manager));
    // Region migration and broadcast are independent of the status persistence under test.
    doReturn(Collections.emptyList()).when(handler).selectedRegionMigrationPlans(anyList());
    doNothing().when(handler).broadcastDataNodeStatusChange(anyList());
    env = mock(ConfigNodeProcedureEnv.class);
    when(env.getConfigManager()).thenReturn(manager);
    when(env.getRemoveDataNodeHandler()).thenReturn(handler);
  }

  @After
  public void tearDown() throws Exception {
    if (clientManagerField != null && originalClientManager != null) {
      clientManagerField.set(
          CnToDnInternalServiceAsyncRequestManager.getInstance(), originalClientManager);
    }
  }

  @Test
  public void testPreparePersistsRemoving() throws Exception {
    change(FIRST, NodeStatus.Removing);
    assertStates(FIRST, NodeStatus.Removing, NodeStatus.Removing, NodeStatus.Removing);
  }

  @Test
  public void testPrepareRpcFailureStillPersistsRemoving() throws Exception {
    failRpc.put(FIRST, true);
    TestProcedure procedure =
        procedure(NodeStatus.Running, RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE);
    procedure.step(env);
    assertEquals(RemoveDataNodeState.BROADCAST_DISABLE_DATA_NODE, procedure.state());
    assertStates(FIRST, NodeStatus.Running, NodeStatus.Removing, NodeStatus.Removing);
  }

  @Test
  public void testOfflineRemovalAndRetryDoNotRequireRpcSuccess() throws Exception {
    for (NodeStatus offline : Arrays.asList(NodeStatus.Unknown, NodeStatus.Stopped)) {
      assertSuccess(cache.trySetNodeStatus(FIRST, offline, null, true));
      remoteStatuses.put(FIRST, offline);
      failRpc.put(FIRST, true);
      change(FIRST, NodeStatus.Removing);
      assertStates(FIRST, offline, NodeStatus.Removing, NodeStatus.Removing);
      change(FIRST, NodeStatus.Removing);
      assertStates(FIRST, offline, NodeStatus.Removing, NodeStatus.Removing);
    }
  }

  @Test
  public void testOfflineRemovalStillRequiresPersistence() throws Exception {
    assertSuccess(cache.trySetNodeStatus(FIRST, NodeStatus.Stopped, null, true));
    failRpc.put(FIRST, true);
    failStatusWrite = true;
    expectChangeFailure(NodeStatus.Removing);
    assertEquals(NodeStatus.Removing, cache.getNodeStatus(FIRST));
    assertEquals(new Pair<>(NodeStatus.Stopped, null), nodeInfo.getPersistedNodeStatus(FIRST));
    failStatusWrite = false;
    change(FIRST, NodeStatus.Removing);
    assertEquals(new Pair<>(NodeStatus.Removing, null), nodeInfo.getPersistedNodeStatus(FIRST));
  }

  @Test
  public void testOfflineCompensationDoesNotRequireRpcSuccess() throws Exception {
    retainRegionOn(FIRST);
    for (NodeStatus offline : Arrays.asList(NodeStatus.Unknown, NodeStatus.Stopped)) {
      failRpc.put(FIRST, false);
      change(FIRST, NodeStatus.Removing);
      failRpc.put(FIRST, true);
      assertTrue(procedure(offline, RemoveDataNodeState.STOP_DATA_NODE).finish(env));
      assertStates(
          FIRST, NodeStatus.Removing, offline, offline == NodeStatus.Stopped ? offline : null);
    }
  }

  @Test
  public void testInitialPrepareCommitFailureRetriesWithoutRecheckingCapacity() throws Exception {
    for (int index = nodes.size(); index <= NodeInfo.getMinimumDataNode(); index++) {
      nodeInfo.registerDataNode(
          new RegisterDataNodePlan(
              new TDataNodeConfiguration().setLocation(location(FIRST + index))));
    }
    cache.initHeartbeatCache(manager);
    for (TDataNodeConfiguration node : nodeInfo.getRegisteredDataNodes()) {
      assertSuccess(
          cache.trySetNodeStatus(
              node.getLocation().getDataNodeId(), NodeStatus.Running, null, true));
    }
    when(manager
            .getLoadManager()
            .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.ReadOnly))
        .thenAnswer(
            i -> cache.filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.ReadOnly));

    TestProcedure procedure =
        procedure(NodeStatus.Running, RemoveDataNodeState.REGION_REPLICA_CHECK);
    failStatusWrite = true;
    procedure.step(env);
    procedure.step(env);
    assertFalse(procedure.isFailed());
    assertEquals(RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE, procedure.state());
    assertStates(FIRST, NodeStatus.Removing, NodeStatus.Removing, null);

    failStatusWrite = false;
    procedure.step(env);
    assertEquals(RemoveDataNodeState.BROADCAST_DISABLE_DATA_NODE, procedure.state());
    assertStates(FIRST, NodeStatus.Removing, NodeStatus.Removing, NodeStatus.Removing);
  }

  @Test
  public void testPrepareCommitFailureUpdatesCacheAndRetries() throws Exception {
    failStatusWrite = true;
    TestProcedure procedure =
        procedure(NodeStatus.Running, RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE);
    procedure.step(env);
    assertFalse(procedure.isFailed());
    assertEquals(RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE, procedure.state());
    assertStates(FIRST, NodeStatus.Removing, NodeStatus.Removing, null);
    failStatusWrite = false;
    procedure.step(env);
    assertEquals(RemoveDataNodeState.BROADCAST_DISABLE_DATA_NODE, procedure.state());
    assertStates(FIRST, NodeStatus.Removing, NodeStatus.Removing, NodeStatus.Removing);
  }

  @Test
  public void testPrepareCommitFailureStopsAfterRetryThreshold() throws Exception {
    failStatusWrite = true;
    TestProcedure procedure =
        procedure(NodeStatus.Running, RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE);
    for (int attempt = 0; attempt < 5; attempt++) {
      procedure.step(env);
      assertFalse(procedure.isFailed());
      assertEquals(RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE, procedure.state());
    }
    procedure.step(env);
    assertTrue(procedure.isFailed());
    assertStates(FIRST, NodeStatus.Removing, NodeStatus.Removing, null);
  }

  @Test
  public void testAppliedRpcWithLostResponseMustStillPersistIntent() throws Exception {
    applyRpcBeforeFailure = true;
    failRpc.put(FIRST, true);
    TestProcedure procedure =
        procedure(NodeStatus.Running, RemoveDataNodeState.REMOVE_DATA_NODE_PREPARE);
    procedure.step(env);
    assertStates(FIRST, NodeStatus.Removing, NodeStatus.Removing, NodeStatus.Removing);
    assertEquals(RemoveDataNodeState.BROADCAST_DISABLE_DATA_NODE, procedure.state());
  }

  @Test
  public void testPreparePartialRpcFailurePreservesPerNodeResults() throws Exception {
    failRpc.put(SECOND, true);
    Map<Integer, NodeStatus> desired = new HashMap<>();
    desired.put(FIRST, NodeStatus.Removing);
    desired.put(SECOND, NodeStatus.Removing);
    handler.changeDataNodeStatus(nodes, desired);
    assertStates(FIRST, NodeStatus.Removing, NodeStatus.Removing, NodeStatus.Removing);
    assertStates(SECOND, NodeStatus.Running, NodeStatus.Removing, NodeStatus.Removing);
  }

  @Test
  public void testRpcFailureOnRetryStillRequiresPersistence() throws Exception {
    failStatusWrite = true;
    expectChangeFailure(NodeStatus.Removing);
    failRpc.put(FIRST, true);
    expectChangeFailure(NodeStatus.Removing);
    assertStates(FIRST, NodeStatus.Removing, NodeStatus.Removing, null);
    failStatusWrite = false;
    change(FIRST, NodeStatus.Removing);
    assertStates(FIRST, NodeStatus.Removing, NodeStatus.Removing, NodeStatus.Removing);
  }

  @Test
  public void testCompensationRestoresRunning() throws Exception {
    assertCompensation(NodeStatus.Running);
  }

  @Test
  public void testCompensationRestoresReadOnly() throws Exception {
    assertCompensation(NodeStatus.ReadOnly);
    assertEquals(NodeStatus.MANUAL, cache.getNodeStatusReason(FIRST));
  }

  @Test
  public void testCompensationRestoresUnknown() throws Exception {
    assertCompensation(NodeStatus.Unknown);
  }

  @Test
  public void testCompensationRestoresStopped() throws Exception {
    assertCompensation(NodeStatus.Stopped);
  }

  @Test
  public void testCompensationRpcFailureMustNotFinishProcedure() throws Exception {
    change(FIRST, NodeStatus.Removing);
    retainRegionOn(FIRST);
    failRpc.put(FIRST, true);
    for (NodeStatus original : Arrays.asList(NodeStatus.Running, NodeStatus.ReadOnly)) {
      assertFalse(
          "Failed compensation must not finish",
          procedure(original, RemoveDataNodeState.STOP_DATA_NODE).finish(env));
      assertStates(FIRST, NodeStatus.Removing, NodeStatus.Removing, NodeStatus.Removing);
    }
  }

  @Test
  public void testCompensationCommitFailureRemainsRetryable() throws Exception {
    change(FIRST, NodeStatus.Removing);
    retainRegionOn(FIRST);
    failStatusWrite = true;
    TestProcedure procedure = procedure(NodeStatus.Running, RemoveDataNodeState.STOP_DATA_NODE);
    assertFalse(procedure.finish(env));
    assertStates(FIRST, NodeStatus.Running, NodeStatus.Running, NodeStatus.Removing);
    failStatusWrite = false;
    assertTrue(procedure.finish(env));
    assertStates(FIRST, NodeStatus.Running, NodeStatus.Running, null);
  }

  @Test
  public void testDeleteErrorMustNotStopOrForgetRegisteredNode() throws Exception {
    failDeleteWrite = true;
    assertDeleteFailure();
  }

  @Test
  public void testProcedureExecutorCanActuallyRetryCompensationCommitFailure() throws Exception {
    change(FIRST, NodeStatus.Removing);
    retainRegionOn(FIRST);
    failStatusWrite = true;
    TestProcedure procedure = procedure(NodeStatus.Running, RemoveDataNodeState.STOP_DATA_NODE);
    procedure.step(env);
    assertEquals(
        "The state-machine executor must retain the failed compensation step",
        RemoveDataNodeState.STOP_DATA_NODE,
        procedure.state());
    failStatusWrite = false;
    procedure.step(env);
    assertStates(FIRST, NodeStatus.Running, NodeStatus.Running, null);
  }

  @Test
  public void testDeleteExceptionMustNotStopOrForgetRegisteredNode() throws Exception {
    throwDeleteWrite = true;
    assertDeleteFailure();
  }

  @Test
  public void testSuccessfulDeleteCannotBeResurrectedByLateStatus() throws Exception {
    change(FIRST, NodeStatus.Removing);
    assertTrue(procedure(NodeStatus.Running, RemoveDataNodeState.STOP_DATA_NODE).finish(env));
    assertFalse(
        nodeInfo.getRegisteredDataNodes().stream()
            .anyMatch(n -> n.getLocation().getDataNodeId() == FIRST));
    assertNull(nodeInfo.getPersistedNodeStatus(FIRST));
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(FIRST, NodeStatus.Removing));
    assertNull(nodeInfo.getPersistedNodeStatus(FIRST));
    assertTrue(stopRequests > 0);
  }

  @Test
  public void testFailedRemoteStopCannotResurrectDeletedNode() throws Exception {
    failStop.put(FIRST, true);
    testSuccessfulDeleteCannotBeResurrectedByLateStatus();
  }

  @Test
  public void testPartialRemovalKeepsSuccessfulAndCompensatedResultsSeparate() throws Exception {
    change(FIRST, NodeStatus.Removing);
    change(SECOND, NodeStatus.Removing);
    retainRegionOn(SECOND);
    Map<Integer, NodeStatus> originals = new HashMap<>();
    originals.put(FIRST, NodeStatus.Running);
    originals.put(SECOND, NodeStatus.ReadOnly);
    TestProcedure procedure =
        new TestProcedure(nodes, originals, RemoveDataNodeState.STOP_DATA_NODE);
    assertTrue(procedure.finish(env));
    assertNull(nodeInfo.getPersistedNodeStatus(FIRST));
    assertEquals(1, nodeInfo.getRegisteredDataNodes().size());
    assertStates(SECOND, NodeStatus.ReadOnly, NodeStatus.ReadOnly, NodeStatus.ReadOnly);
  }

  private void assertDeleteFailure() throws Exception {
    change(FIRST, NodeStatus.Removing);
    boolean finished =
        procedure(NodeStatus.Running, RemoveDataNodeState.STOP_DATA_NODE).finish(env);
    assertEquals("No remote stop before durable unregister", 0, stopRequests);
    assertFalse("Failed unregister must remain retryable", finished);
    assertEquals(NodeStatus.Removing, cache.getNodeStatus(FIRST));
    assertEquals(new Pair<>(NodeStatus.Removing, null), nodeInfo.getPersistedNodeStatus(FIRST));
    assertEquals(2, nodeInfo.getRegisteredDataNodes().size());
  }

  private void assertCompensation(NodeStatus original) throws Exception {
    change(FIRST, NodeStatus.Removing);
    retainRegionOn(FIRST);
    assertTrue(
        "Compensation should complete for " + original,
        procedure(original, RemoveDataNodeState.STOP_DATA_NODE).finish(env));
    assertStates(FIRST, original, original, original.isPersistentStatus() ? original : null);
  }

  private void retainRegionOn(int id) {
    when(manager.getPartitionManager().getAllReplicaSets())
        .thenReturn(
            Collections.singletonList(
                new TRegionReplicaSet(
                    new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
                    Collections.singletonList(location(id)))));
  }

  private void change(int id, NodeStatus status) throws Exception {
    handler.changeDataNodeStatus(
        Collections.singletonList(location(id)), Collections.singletonMap(id, status));
  }

  private void expectChangeFailure(NodeStatus status) throws Exception {
    try {
      change(FIRST, status);
      fail("Unconfirmed status transition must fail");
    } catch (IOException expected) {
      assertNotNull(expected.getMessage());
    }
  }

  private void assertStates(int id, NodeStatus remote, NodeStatus published, NodeStatus persisted) {
    assertEquals("DataNode", remote, remoteStatuses.get(id));
    assertEquals("LoadCache", published, cache.getNodeStatus(id));
    assertEquals(
        "NodeInfo",
        persisted == null
            ? null
            : new Pair<>(persisted, persisted == NodeStatus.ReadOnly ? NodeStatus.MANUAL : null),
        nodeInfo.getPersistedNodeStatus(id));
  }

  private TestProcedure procedure(NodeStatus original, RemoveDataNodeState initial) {
    return new TestProcedure(
        Collections.singletonList(location(FIRST)),
        Collections.singletonMap(FIRST, original),
        initial);
  }

  private static TDataNodeLocation location(int id) {
    return new TDataNodeLocation()
        .setDataNodeId(id)
        .setInternalEndPoint(new TEndPoint("127.0.0.1", 10000 + id))
        .setClientRpcEndPoint(new TEndPoint("127.0.0.1", 11000 + id));
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static TSStatus failure() {
    return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
  }

  private static void assertSuccess(TSStatus status) {
    assertEquals(success().getCode(), status.getCode());
  }

  private static class TestProcedure extends RemoveDataNodesProcedure {
    private final RemoveDataNodeState initial;

    private TestProcedure(
        List<TDataNodeLocation> nodes,
        Map<Integer, NodeStatus> originals,
        RemoveDataNodeState initial) {
      super(nodes, originals);
      this.initial = initial;
    }

    @Override
    protected RemoveDataNodeState getInitialState() {
      return initial;
    }

    private void step(ConfigNodeProcedureEnv env) throws InterruptedException {
      execute(env);
    }

    private RemoveDataNodeState state() {
      return getCurrentState();
    }

    private boolean finish(ConfigNodeProcedureEnv env) {
      return executeFromState(env, RemoveDataNodeState.STOP_DATA_NODE) == Flow.NO_MORE_STATE;
    }
  }
}
