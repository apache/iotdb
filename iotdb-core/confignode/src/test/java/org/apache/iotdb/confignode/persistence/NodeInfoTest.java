/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RegisterAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RemoveAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateNodeStatusPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateVersionInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Map;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class NodeInfoTest {

  private static NodeInfo nodeInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() throws StartupException {
    nodeInfo = new NodeInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    nodeInfo.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws TException, IOException {
    registerConfigNodes();
    registerDataNodes();
    nodeInfo.registerAINode(new RegisterAINodePlan(generateTAINodeConfiguration()));
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(10001, NodeStatus.Stopped));
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Removing));
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(10004, NodeStatus.Stopped));
    nodeInfo.applyNodeStatusPlan(new UpdateNodeStatusPlan(10006, NodeStatus.Removing));
    Assert.assertTrue(nodeInfo.processTakeSnapshot(snapshotDir));

    NodeInfo nodeInfo1 = new NodeInfo();
    nodeInfo1.processLoadSnapshot(snapshotDir);
    Assert.assertEquals(nodeInfo, nodeInfo1);
    Assert.assertEquals(
        new Pair<>(NodeStatus.Stopped, null), nodeInfo1.getPersistedNodeStatus(10001));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Removing, null), nodeInfo1.getPersistedNodeStatus(10003));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Stopped, null), nodeInfo1.getPersistedNodeStatus(10004));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Removing, null), nodeInfo1.getPersistedNodeStatus(10006));
  }

  @Test
  public void testLoadSnapshotWithoutNodeStatuses() throws TException, IOException {
    NodeInfo oldNodeInfo = new NodeInfo();
    oldNodeInfo.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3)));
    oldNodeInfo.updateVersionInfo(
        new UpdateVersionInfoPlan(new TNodeVersionInfo("2.0", "build"), 10003));
    File oldSnapshotDir = new File(snapshotDir, "without-node-statuses");
    Assert.assertTrue(oldSnapshotDir.mkdirs());
    Assert.assertTrue(oldNodeInfo.processTakeSnapshot(oldSnapshotDir));
    // The previous format ends immediately before the new, empty node-status map size.
    try (RandomAccessFile snapshot =
        new RandomAccessFile(new File(oldSnapshotDir, "node_info.bin"), "rw")) {
      snapshot.setLength(snapshot.length() - Integer.BYTES);
    }

    NodeInfo restored = new NodeInfo();
    restored.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3)));
    restored.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Removing));
    restored.processLoadSnapshot(oldSnapshotDir);
    Assert.assertEquals(oldNodeInfo, restored);
    Assert.assertTrue(restored.getPersistedNodeStatuses().isEmpty());
    Assert.assertEquals(new TNodeVersionInfo("2.0", "build"), restored.getVersionInfo(10003));
  }

  @Test
  public void testSetOperationsReplacePersistedStatus() {
    NodeInfo info = new NodeInfo();
    info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3)));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Stopped));
    Assert.assertEquals(new Pair<>(NodeStatus.Stopped, null), info.getPersistedNodeStatus(10003));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Removing));
    Assert.assertEquals(new Pair<>(NodeStatus.Removing, null), info.getPersistedNodeStatus(10003));
    Pair<NodeStatus, String> status = info.getPersistedNodeStatus(10003);
    status.left = NodeStatus.ReadOnly;
    status.right = NodeStatus.MANUAL;
    Assert.assertEquals(new Pair<>(NodeStatus.Removing, null), info.getPersistedNodeStatus(10003));
    Map<Integer, Pair<NodeStatus, String>> snapshot = info.getPersistedNodeStatuses();
    snapshot.get(10003).left = NodeStatus.ReadOnly;
    snapshot.get(10003).right = NodeStatus.MANUAL;
    Assert.assertEquals(new Pair<>(NodeStatus.Removing, null), info.getPersistedNodeStatus(10003));
    snapshot.clear();
    Assert.assertEquals(new Pair<>(NodeStatus.Removing, null), info.getPersistedNodeStatus(10003));

    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Stopped));
    Assert.assertEquals(new Pair<>(NodeStatus.Stopped, null), info.getPersistedNodeStatus(10003));
    info.clear();
    Assert.assertTrue(info.getPersistedNodeStatuses().isEmpty());
  }

  @Test
  public void testReadOnlyReasonsSurviveSnapshot() throws TException, IOException {
    NodeInfo info = new NodeInfo();
    String[] reasons = {
      null,
      NodeStatus.MANUAL,
      NodeStatus.DISK_FULL,
      NodeStatus.STOPPING,
      NodeStatus.UNRECOVERABLE_ERROR + ": failed to recover root.test-0"
    };
    for (int i = 0; i < reasons.length; i++) {
      info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3 + i)));
      info.applyNodeStatusPlan(
          new UpdateNodeStatusPlan(10003 + i, NodeStatus.ReadOnly, reasons[i]));
    }
    File directory = new File(snapshotDir, "readonly-reasons");
    Assert.assertTrue(directory.mkdirs());
    Assert.assertTrue(info.processTakeSnapshot(directory));

    NodeInfo restored = new NodeInfo();
    restored.processLoadSnapshot(directory);
    Assert.assertEquals(info, restored);
    for (int i = 0; i < reasons.length; i++) {
      Assert.assertEquals(
          new Pair<>(NodeStatus.ReadOnly, reasons[i]), restored.getPersistedNodeStatus(10003 + i));
    }
  }

  @Test
  public void testStoppedAndRemovingReasonsSurviveSnapshot() throws TException, IOException {
    NodeInfo info = new NodeInfo();
    info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3)));
    info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(4)));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Stopped, "stop reason"));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10004, NodeStatus.Removing, "remove reason"));
    File directory = new File(snapshotDir, "stopped-removing-reasons");
    Assert.assertTrue(directory.mkdirs());
    Assert.assertTrue(info.processTakeSnapshot(directory));

    NodeInfo restored = new NodeInfo();
    restored.processLoadSnapshot(directory);
    Assert.assertEquals(info, restored);
    Assert.assertEquals(
        new Pair<>(NodeStatus.Stopped, "stop reason"), restored.getPersistedNodeStatus(10003));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Removing, "remove reason"), restored.getPersistedNodeStatus(10004));
  }

  @Test
  public void testReadOnlyReasonReplacementAndStatusChange() {
    NodeInfo info = new NodeInfo();
    info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3)));
    // Plans contain the caller's final decision, including lower-priority or absent reasons.
    for (String reason : new String[] {NodeStatus.MANUAL, NodeStatus.DISK_FULL, null}) {
      info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.ReadOnly, reason));
      Assert.assertEquals(
          new Pair<>(NodeStatus.ReadOnly, reason), info.getPersistedNodeStatus(10003));
    }
    info.applyNodeStatusPlan(
        new UpdateNodeStatusPlan(10003, NodeStatus.ReadOnly, NodeStatus.MANUAL));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Stopped));
    Assert.assertEquals(new Pair<>(NodeStatus.Stopped, null), info.getPersistedNodeStatus(10003));
  }

  @Test
  public void testNodeRemovalDiscardsReadOnlyReason() {
    NodeInfo info = new NodeInfo();
    info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3)));
    UpdateNodeStatusPlan readOnly =
        new UpdateNodeStatusPlan(10003, NodeStatus.ReadOnly, NodeStatus.MANUAL);
    info.applyNodeStatusPlan(readOnly);
    info.removeDataNode(
        new RemoveDataNodePlan(Collections.singletonList(generateTDataNodeLocation(3))));
    Assert.assertNull(info.getPersistedNodeStatus(10003));
    info.applyNodeStatusPlan(readOnly);
    Assert.assertNull(info.getPersistedNodeStatus(10003));
  }

  @Test
  public void testClearRemovesAnyPersistedStatusAndAllowsRetry() {
    NodeInfo info = new NodeInfo();
    info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3)));
    for (NodeStatus status :
        new NodeStatus[] {NodeStatus.Stopped, NodeStatus.Removing, NodeStatus.ReadOnly}) {
      info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, status, NodeStatus.MANUAL));
      Assert.assertNotNull(info.getPersistedNodeStatus(10003));

      UpdateNodeStatusPlan clear = new UpdateNodeStatusPlan(10003, null);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), info.applyNodeStatusPlan(clear).getCode());
      Assert.assertNull(info.getPersistedNodeStatus(10003));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), info.applyNodeStatusPlan(clear).getCode());
      Assert.assertNull(info.getPersistedNodeStatus(10003));
    }
  }

  @Test
  public void testNodeRemovalDiscardsStatusAndIgnoresDelayedUpdates() {
    NodeInfo info = new NodeInfo();
    TConfigNodeLocation configNode =
        new TConfigNodeLocation(
            10001, new TEndPoint("127.0.0.1", 22201), new TEndPoint("127.0.0.1", 22301));
    info.applyConfigNode(new ApplyConfigNodePlan(configNode));
    info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3)));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10001, NodeStatus.Stopped));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Removing));
    TAINodeConfiguration aiNode = generateTAINodeConfiguration();
    info.registerAINode(new RegisterAINodePlan(aiNode));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10006, NodeStatus.Removing));

    info.removeConfigNode(new RemoveConfigNodePlan(configNode));
    info.removeDataNode(
        new RemoveDataNodePlan(Collections.singletonList(generateTDataNodeLocation(3))));
    info.removeAINode(new RemoveAINodePlan(aiNode.getLocation()));
    Assert.assertTrue(info.getPersistedNodeStatuses().isEmpty());

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10001, NodeStatus.Stopped)).getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Removing)).getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10006, NodeStatus.Stopped)).getCode());
    Assert.assertTrue(info.getPersistedNodeStatuses().isEmpty());
  }

  private static TAINodeConfiguration generateTAINodeConfiguration() {
    return new TAINodeConfiguration(
        new TAINodeLocation(10006, new TEndPoint("127.0.0.1", 10810)), new TNodeResource(8, 1024L));
  }

  private void registerConfigNodes() {
    for (int i = 0; i < 3; i++) {
      ApplyConfigNodePlan applyConfigNodePlan =
          new ApplyConfigNodePlan(
              new TConfigNodeLocation(
                  10000 + i,
                  new TEndPoint("127.0.0.1", 22200 + i),
                  new TEndPoint("127.0.0.1", 22300 + i)));
      nodeInfo.applyConfigNode(applyConfigNodePlan);
    }
  }

  @Test
  public void testTruncatedStatusPayloadMustFailSnapshotLoad() throws Exception {
    for (NodeStatus status : new NodeStatus[] {NodeStatus.Removing, NodeStatus.ReadOnly}) {
      NodeInfo info = new NodeInfo();
      info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3)));
      info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, status, NodeStatus.MANUAL));
      File directory = new File(snapshotDir, "truncated-status-payload-" + status);
      Assert.assertTrue(directory.mkdirs());
      Assert.assertTrue(info.processTakeSnapshot(directory));
      try (RandomAccessFile file =
          new RandomAccessFile(new File(directory, "node_info.bin"), "rw")) {
        file.setLength(file.length() - 2);
      }
      Assert.assertThrows(IOException.class, () -> new NodeInfo().processLoadSnapshot(directory));
    }
  }

  @Test
  public void testClearedStatusesStayClearedAfterSnapshotAndLaterPlans() throws Exception {
    NodeInfo info = new NodeInfo();
    info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3)));
    info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(4)));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Stopped));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10004, NodeStatus.Removing));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, null));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10004, null));
    File directory = new File(snapshotDir, "cleared-statuses");
    Assert.assertTrue(directory.mkdirs());
    Assert.assertTrue(info.processTakeSnapshot(directory));
    NodeInfo restored = new NodeInfo();
    restored.processLoadSnapshot(directory);
    Assert.assertTrue(restored.getPersistedNodeStatuses().isEmpty());
    // Replay the entries after that snapshot in their original order.
    restored.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Stopped));
    restored.applyNodeStatusPlan(new UpdateNodeStatusPlan(10004, NodeStatus.Removing));
    restored.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, null));
    Assert.assertNull(restored.getPersistedNodeStatus(10003));
    Assert.assertEquals(
        new Pair<>(NodeStatus.Removing, null), restored.getPersistedNodeStatus(10004));
  }

  @Test
  public void testRejoinedNodeDoesNotInheritRemovedNodeStatus() {
    NodeInfo info = new NodeInfo();
    info.registerDataNode(new RegisterDataNodePlan(generateTDataNodeConfiguration(3)));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Removing));
    info.removeDataNode(
        new RemoveDataNodePlan(Collections.singletonList(generateTDataNodeLocation(3))));
    int newId = info.generateNextNodeId();
    TDataNodeConfiguration rejoined = generateTDataNodeConfiguration(3);
    rejoined.getLocation().setDataNodeId(newId);
    info.registerDataNode(new RegisterDataNodePlan(rejoined));
    info.applyNodeStatusPlan(new UpdateNodeStatusPlan(10003, NodeStatus.Stopped));
    Assert.assertNotEquals(10003, newId);
    Assert.assertNull(info.getPersistedNodeStatus(newId));
    Assert.assertNull(info.getPersistedNodeStatus(10003));
  }

  private void registerDataNodes() {
    for (int i = 3; i < 6; i++) {
      RegisterDataNodePlan registerDataNodePlan =
          new RegisterDataNodePlan(generateTDataNodeConfiguration(i));
      nodeInfo.registerDataNode(registerDataNodePlan);
    }
  }

  private TDataNodeConfiguration generateTDataNodeConfiguration(int flag) {
    TDataNodeLocation location = generateTDataNodeLocation(flag);
    TNodeResource resource = new TNodeResource(16, 34359738368L);
    return new TDataNodeConfiguration(location, resource);
  }

  private TDataNodeLocation generateTDataNodeLocation(int flag) {
    return new TDataNodeLocation(
        10000 + flag,
        new TEndPoint("127.0.0.1", 6600 + flag),
        new TEndPoint("127.0.0.1", 7700 + flag),
        new TEndPoint("127.0.0.1", 8800 + flag),
        new TEndPoint("127.0.0.1", 9900 + flag),
        new TEndPoint("127.0.0.1", 11000 + flag));
  }
}
