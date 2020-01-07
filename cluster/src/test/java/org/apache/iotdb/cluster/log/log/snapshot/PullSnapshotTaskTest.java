/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.log.snapshot;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.client.ClientPool;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.common.TestSnapshot;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.snapshot.PullSnapshotTask;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Before;
import org.junit.Test;

public class PullSnapshotTaskTest {

  private ClientPool clientPool = new ClientPool(null);
  private Map<Integer, Snapshot> snapshotMap = new HashMap<>();
  private DataGroupMember newMember = new DataGroupMember() {
    @Override
    public AsyncClient connectNode(Node node) {
      try {
        return new DataClient(new TBinaryProtocol.Factory(), new TAsyncClientManager(),
            node, clientPool) {
          @Override
          public void pullSnapshot(PullSnapshotRequest request,
              AsyncMethodCallback<PullSnapshotResp> resultHandler) {
            new Thread(() -> {
              PullSnapshotResp resp = new PullSnapshotResp();
              Map<Integer, ByteBuffer> snapshotBytes = new HashMap<>();
              for (Entry<Integer, Snapshot> entry : snapshotMap.entrySet()) {
                snapshotBytes.put(entry.getKey(), entry.getValue().serialize());
              }
              resp.setSnapshotBytes(snapshotBytes);
              resultHandler.onComplete(resp);
            }).start();
          }
        };
      } catch (IOException e) {
        return null;
      }
    }
  };

  @Before
  public void setUp() {
    for (int i = 0; i < 10; i++) {
      snapshotMap.put(i, new TestSnapshot(i));
    }
  }

  @Test
  public void test() {
    List<Integer> slots = new ArrayList<>();
    List<Node> previousHolders = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      slots.add(i);
      previousHolders.add(TestUtils.getNode(i + 1));
    }
    PullSnapshotTask<TestSnapshot> task = new PullSnapshotTask<>(TestUtils.getNode(0), slots,
        newMember, previousHolders, TestSnapshot::new);
    Map<Integer, TestSnapshot> result = task.call();
    assertEquals(snapshotMap, result);
  }
}