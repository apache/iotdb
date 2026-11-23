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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ShowAvailableUrlsTaskTest {
  @Test
  public void testOnlyRunningAndReadOnlyNodesAreReturned() throws Exception {
    List<TDataNodeInfo> nodes =
        Arrays.stream(NodeStatus.values())
            .map(
                status ->
                    new TDataNodeInfo()
                        .setStatus(status.getStatus())
                        .setStatusReason("reason")
                        .setRpcAddresss("127.0.0.1")
                        .setRpcPort(6667 + status.ordinal()))
            .collect(Collectors.toList());
    TsBlock result = execute(nodes);
    assertEquals(2, result.getPositionCount());
    assertEquals("127.0.0.1", result.getColumn(0).getBinary(0).toString());
    assertEquals(6667 + NodeStatus.Running.ordinal(), result.getColumn(1).getInt(0));
    assertEquals(6667 + NodeStatus.ReadOnly.ordinal(), result.getColumn(1).getInt(1));
  }

  @Test
  public void testNoOnlineNodes() throws Exception {
    assertEquals(
        0,
        execute(
                Arrays.asList(
                    new TDataNodeInfo().setStatus(NodeStatus.Unknown.getStatus()),
                    new TDataNodeInfo().setStatus(NodeStatus.Stopped.getStatus()),
                    new TDataNodeInfo().setStatus(NodeStatus.Removing.getStatus())))
            .getPositionCount());
    assertEquals(0, execute(Collections.emptyList()).getPositionCount());
    assertEquals(0, execute(null).getPositionCount());
  }

  @Test
  public void testReadOnlyStoppingIsExcludedButOtherReasonsRemainReadable() throws Exception {
    List<TDataNodeInfo> nodes =
        Arrays.asList(
            readOnlyNode(6667, NodeStatus.STOPPING),
            readOnlyNode(6668, NodeStatus.DISK_FULL),
            readOnlyNode(6669, NodeStatus.MANUAL),
            readOnlyNode(6670, null));
    TsBlock result = execute(nodes);
    assertEquals(3, result.getPositionCount());
    for (int i = 0; i < 3; i++) {
      assertEquals(6668 + i, result.getColumn(1).getInt(i));
    }
  }

  private TDataNodeInfo readOnlyNode(int port, String reason) {
    return new TDataNodeInfo()
        .setStatus(NodeStatus.ReadOnly.getStatus())
        .setStatusReason(reason)
        .setRpcAddresss("127.0.0.1")
        .setRpcPort(port);
  }

  private TsBlock execute(List<TDataNodeInfo> nodes) throws Exception {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    ShowAvailableUrlsTask.buildTsBlock(
        new TShowDataNodesResp().setDataNodesInfoList(nodes), future);
    return future.get().getResultSet();
  }
}
