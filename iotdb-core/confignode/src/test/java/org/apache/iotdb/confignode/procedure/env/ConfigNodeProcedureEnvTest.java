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

package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.procedure.scheduler.ProcedureScheduler;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfigNodeProcedureEnvTest {

  private static final int DATA_NODE_ID = 1;

  private final ConfigManager configManager = mock(ConfigManager.class);
  private final LoadManager loadManager = mock(LoadManager.class);
  private final ConfigNodeProcedureEnv env =
      new ConfigNodeProcedureEnv(configManager, mock(ProcedureScheduler.class));

  @Before
  public void setUp() {
    when(configManager.getLoadManager()).thenReturn(loadManager);
  }

  @Test
  public void testStoppedNodeIsNotRuntimeActiveWriter() {
    when(loadManager.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.Stopped);

    // A Stopped node is handled like Unknown: it can not serve as an active runtime writer.
    Assert.assertFalse(env.isRuntimeActiveWriterNode(DATA_NODE_ID));
  }

  @Test
  public void testUnknownNodeIsNotRuntimeActiveWriter() {
    when(loadManager.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.Unknown);

    Assert.assertFalse(env.isRuntimeActiveWriterNode(DATA_NODE_ID));
  }

  @Test
  public void testRemovingNodeIsNotRuntimeActiveWriter() {
    when(loadManager.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.Removing);

    Assert.assertFalse(env.isRuntimeActiveWriterNode(DATA_NODE_ID));
  }

  @Test
  public void testRunningNodeIsRuntimeActiveWriter() {
    when(loadManager.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.Running);

    Assert.assertTrue(env.isRuntimeActiveWriterNode(DATA_NODE_ID));
  }

  @Test
  public void testReadOnlyNodeIsRuntimeActiveWriter() {
    when(loadManager.getNodeStatus(DATA_NODE_ID)).thenReturn(NodeStatus.ReadOnly);

    // ReadOnly still responds and can serve as a runtime writer, unlike Unknown/Stopped/Removing.
    Assert.assertTrue(env.isRuntimeActiveWriterNode(DATA_NODE_ID));
  }

  @Test
  public void testNegativeNodeIdIsNotRuntimeActiveWriter() {
    // The node id guard dominates the status check: even a Running status can not turn an
    // invalid node id into an active writer.
    when(loadManager.getNodeStatus(-1)).thenReturn(NodeStatus.Running);

    Assert.assertFalse(env.isRuntimeActiveWriterNode(-1));
  }
}
