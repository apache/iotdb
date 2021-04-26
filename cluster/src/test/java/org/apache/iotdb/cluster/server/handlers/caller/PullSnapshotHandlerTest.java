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

package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestSnapshot;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PullSnapshotHandlerTest {

  @Test
  public void testSnapshot() throws InterruptedException {
    AtomicReference<Map<Integer, TestSnapshot>> result = new AtomicReference<>();
    Node owner = TestUtils.getNode(1);
    List<Integer> slots = new ArrayList<>();
    Map<Integer, TestSnapshot> snapshotMap = new HashMap<>();
    Map<Integer, ByteBuffer> snapshotBufferMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      slots.add(i);
      TestSnapshot snapshot = new TestSnapshot(i);
      snapshotMap.put(i, snapshot);
      snapshotBufferMap.put(i, snapshot.serialize());
    }

    PullSnapshotHandler<TestSnapshot> handler =
        new PullSnapshotHandler<>(result, owner, slots, TestSnapshot.Factory.INSTANCE);
    synchronized (result) {
      new Thread(
              () -> {
                PullSnapshotResp resp = new PullSnapshotResp();
                resp.setSnapshotBytes(snapshotBufferMap);
                handler.onComplete(resp);
              })
          .start();
      result.wait();
    }
    assertEquals(snapshotMap, result.get());
  }

  @Test
  public void testError() throws InterruptedException {
    AtomicReference<Map<Integer, TestSnapshot>> result = new AtomicReference<>();
    Node owner = TestUtils.getNode(1);
    List<Integer> slots = new ArrayList<>();
    PullSnapshotHandler<TestSnapshot> handler =
        new PullSnapshotHandler<>(result, owner, slots, TestSnapshot.Factory.INSTANCE);
    synchronized (result) {
      new Thread(() -> handler.onError(new TestException())).start();
      result.wait();
    }
    assertNull(result.get());
  }
}
