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

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.DeserializedBatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.util.TestEntry;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IoTConsensusServerImplTest {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSyncLogBecomesReadOnlyWhileWaiting() throws Exception {
    final ScheduledExecutorService backgroundTaskService =
        Executors.newSingleThreadScheduledExecutor();
    final AtomicBoolean readOnly = new AtomicBoolean();
    final TestStateMachine stateMachine =
        new TestStateMachine() {
          @Override
          public boolean isReadOnly() {
            return readOnly.get();
          }
        };
    final Peer thisNode = new Peer(new DataRegionId(1), 0, new TEndPoint("127.0.0.1", 6667));
    final IoTConsensusServerImpl server =
        new IoTConsensusServerImpl(
            temporaryFolder.getRoot().getAbsolutePath(),
            thisNode,
            new TreeSet<>(Collections.singletonList(thisNode)),
            stateMachine,
            backgroundTaskService,
            null,
            null,
            IoTConsensusConfig.newBuilder()
                .setReplication(
                    IoTConsensusConfig.Replication.newBuilder()
                        .setMaxWaitingTimeForWaitBatchInMs(100)
                        .build())
                .build());
    final DeserializedBatchIndexedConsensusRequest request =
        new DeserializedBatchIndexedConsensusRequest(1, 2, 2);
    for (int index = 1; index <= 2; index++) {
      request.add(
          new IndexedConsensusRequest(
              index, index, Collections.singletonList(new TestEntry(index, thisNode))));
    }
    final AtomicReference<TSStatus> status = new AtomicReference<>();
    final Thread syncLogThread = new Thread(() -> status.set(server.syncLog(1, request)));
    try {
      assertFalse(server.isReadOnly());
      syncLogThread.start();
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (syncLogThread.getState() != Thread.State.TIMED_WAITING
          && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(Thread.State.TIMED_WAITING, syncLogThread.getState());
      readOnly.set(true);
      syncLogThread.join(TimeUnit.SECONDS.toMillis(15));
      assertFalse(syncLogThread.isAlive());
      assertEquals(2, status.get().getSubStatusSize());
      assertEquals(
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(),
          status.get().getSubStatus().get(0).getCode());
      assertEquals(
          StatusUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY).getMessage(),
          status.get().getSubStatus().get(0).getMessage());
      assertEquals(
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(),
          status.get().getSubStatus().get(1).getCode());
      assertTrue(stateMachine.getRequestSet().isEmpty());

      // The sender must be able to retry the complete batch after the replica becomes writable.
      readOnly.set(false);
      server.reloadConsensusConfig(
          IoTConsensusConfig.newBuilder()
              .setReplication(
                  IoTConsensusConfig.Replication.newBuilder()
                      .setMaxPendingBatchesNum(1)
                      .setMaxWaitingTimeForWaitBatchInMs(100)
                      .build())
              .build());
      final TSStatus retryStatus = server.syncLog(1, request);
      assertTrue(
          retryStatus.getSubStatus().stream()
              .allMatch(s -> s.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      assertEquals(2, stateMachine.getRequestSet().size());
    } finally {
      syncLogThread.interrupt();
      syncLogThread.join(TimeUnit.SECONDS.toMillis(5));
      backgroundTaskService.shutdownNow();
    }
  }
}
