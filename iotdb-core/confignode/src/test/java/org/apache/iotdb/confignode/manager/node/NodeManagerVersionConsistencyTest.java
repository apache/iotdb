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

package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeManagerVersionConsistencyTest {

  private static final TNodeVersionInfo EXPECTED_VERSION =
      new TNodeVersionInfo("2.0.0", "current-build");

  private NodeInfo nodeInfo;
  private NodeManager nodeManager;
  private Map<Integer, TNodeVersionInfo> versions;

  @Before
  public void setUp() {
    nodeInfo = mock(NodeInfo.class);
    nodeManager = new NodeManager(mock(IManager.class), nodeInfo);
    when(nodeInfo.getRegisteredConfigNodes())
        .thenReturn(
            Arrays.asList(
                new TConfigNodeLocation().setConfigNodeId(0),
                new TConfigNodeLocation().setConfigNodeId(1)));
    when(nodeInfo.getRegisteredDataNodes())
        .thenReturn(
            Collections.singletonList(
                new TDataNodeConfiguration()
                    .setLocation(new TDataNodeLocation().setDataNodeId(2))));
    versions = new HashMap<>();
    for (int nodeId = 0; nodeId < 3; nodeId++) {
      versions.put(nodeId, EXPECTED_VERSION.deepCopy());
    }
    when(nodeInfo.getNodeVersionInfo()).thenReturn(versions);
  }

  @Test
  public void testAllRegisteredConfigNodesMatchExpectedVersion() {
    assertTrue(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
  }

  @Test
  public void testConfigNodeVersionMismatch() {
    versions.put(1, new TNodeVersionInfo("1.0.0", "previous-build"));

    assertFalse(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
  }

  @Test
  public void testDataNodeVersionDoesNotParticipate() {
    versions.put(2, new TNodeVersionInfo("1.0.0", "previous-build"));

    assertTrue(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));

    versions.put(2, new TNodeVersionInfo(EXPECTED_VERSION.getVersion(), "another-build"));

    assertTrue(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
  }

  @Test
  public void testMissingDataNodeVersionDoesNotParticipate() {
    versions.remove(2);

    assertTrue(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
  }

  @Test
  public void testUnknownOrIncompleteDataNodeVersionDoesNotParticipate() {
    for (String invalidValue : Arrays.asList(null, "", " ", "UNKNOWN", "unknown")) {
      versions.put(2, new TNodeVersionInfo(invalidValue, EXPECTED_VERSION.getBuildInfo()));
      assertTrue(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
      versions.put(2, new TNodeVersionInfo(EXPECTED_VERSION.getVersion(), invalidValue));
      assertTrue(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
    }
  }

  @Test
  public void testSameVersionWithDifferentBuildIsRejected() {
    versions.put(1, new TNodeVersionInfo(EXPECTED_VERSION.getVersion(), "another-build"));

    assertFalse(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
  }

  @Test
  public void testMissingRegisteredConfigNodeVersionIsRejected() {
    for (int nodeId : Arrays.asList(0, 1)) {
      TNodeVersionInfo previous = versions.remove(nodeId);
      assertFalse(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
      versions.put(nodeId, previous);
    }
  }

  @Test
  public void testNullVersionEntryIsRejected() {
    versions.put(1, null);

    assertFalse(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
  }

  @Test
  public void testUnknownOrIncompleteExpectedVersionIsRejected() {
    assertFalse(nodeManager.isConfigNodeVersionConsistentWith(null));
    for (String invalidValue : Arrays.asList(null, "", " ", "UNKNOWN", "unknown")) {
      assertFalse(
          nodeManager.isConfigNodeVersionConsistentWith(
              new TNodeVersionInfo(invalidValue, EXPECTED_VERSION.getBuildInfo())));
      assertFalse(
          nodeManager.isConfigNodeVersionConsistentWith(
              new TNodeVersionInfo(EXPECTED_VERSION.getVersion(), invalidValue)));
    }
  }

  @Test
  public void testUnknownOrIncompleteRegisteredVersionIsRejected() {
    for (String invalidValue : Arrays.asList(null, "", " ", "UNKNOWN", "unknown")) {
      versions.put(1, new TNodeVersionInfo(invalidValue, EXPECTED_VERSION.getBuildInfo()));
      assertFalse(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
      versions.put(1, new TNodeVersionInfo(EXPECTED_VERSION.getVersion(), invalidValue));
      assertFalse(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
    }
  }

  @Test
  public void testConsistentMetadataMustMatchExpectedVersion() {
    versions.replaceAll((nodeId, version) -> new TNodeVersionInfo("1.0.0", "previous-build"));

    assertFalse(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
  }

  @Test
  public void testAINodeVersionDoesNotParticipate() {
    versions.put(3, new TNodeVersionInfo("AINode-version", "AINode-build"));

    assertTrue(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
  }

  @Test
  public void testEmptyConfigNodeMembershipIsRejected() {
    when(nodeInfo.getRegisteredConfigNodes()).thenReturn(Collections.emptyList());

    assertFalse(nodeManager.isConfigNodeVersionConsistentWith(EXPECTED_VERSION));
  }
}
