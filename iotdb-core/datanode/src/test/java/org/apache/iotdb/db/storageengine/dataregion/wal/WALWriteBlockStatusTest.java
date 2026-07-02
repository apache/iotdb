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

package org.apache.iotdb.db.storageengine.dataregion.wal;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class WALWriteBlockStatusTest {

  @Test
  public void testRunningNodeTurnsReadOnlyWhenWalBlocked() {
    CommonConfig commonConfig = mockCommonConfig(NodeStatus.Running, null);

    WALWriteBlockStatus.updateStatus(commonConfig, true);

    assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    assertEquals(WALWriteBlockStatus.WAL_BLOCKED, commonConfig.getStatusReason());
  }

  @Test
  public void testWalBlockedReadOnlyNodeRecovers() {
    CommonConfig commonConfig =
        mockCommonConfig(NodeStatus.ReadOnly, WALWriteBlockStatus.WAL_BLOCKED);

    WALWriteBlockStatus.updateStatus(commonConfig, false);

    assertEquals(NodeStatus.Running, commonConfig.getNodeStatus());
    assertNull(commonConfig.getStatusReason());
  }

  @Test
  public void testOtherReadOnlyReasonIsNotOverwrittenOrRecovered() {
    CommonConfig commonConfig = mockCommonConfig(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);

    WALWriteBlockStatus.updateStatus(commonConfig, true);
    assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    assertEquals(NodeStatus.DISK_FULL, commonConfig.getStatusReason());

    WALWriteBlockStatus.updateStatus(commonConfig, false);
    assertEquals(NodeStatus.ReadOnly, commonConfig.getNodeStatus());
    assertEquals(NodeStatus.DISK_FULL, commonConfig.getStatusReason());
  }

  private CommonConfig mockCommonConfig(NodeStatus initialStatus, String initialStatusReason) {
    AtomicReference<NodeStatus> status = new AtomicReference<>(initialStatus);
    AtomicReference<String> statusReason = new AtomicReference<>(initialStatusReason);
    CommonConfig commonConfig = Mockito.mock(CommonConfig.class);

    Mockito.when(commonConfig.getNodeStatus()).thenAnswer(invocation -> status.get());
    Mockito.when(commonConfig.getStatusReason()).thenAnswer(invocation -> statusReason.get());
    Mockito.doAnswer(
            invocation -> {
              status.set(invocation.getArgument(0));
              statusReason.set(null);
              return null;
            })
        .when(commonConfig)
        .setNodeStatus(Mockito.any(NodeStatus.class));
    Mockito.doAnswer(
            invocation -> {
              statusReason.set(invocation.getArgument(0));
              return null;
            })
        .when(commonConfig)
        .setStatusReason(Mockito.any());
    return commonConfig;
  }
}
