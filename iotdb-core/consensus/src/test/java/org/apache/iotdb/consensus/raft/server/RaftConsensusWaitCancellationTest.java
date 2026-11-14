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

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.raft.RaftConsensus;
import org.apache.iotdb.consensus.raft.server.RecordingStateMachine.StringRequest;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Facade wait cancellation must release only the waiter, never the committed command. */
public class RaftConsensusWaitCancellationTest {
  private static final DataRegionId GROUP = new DataRegionId(74);
  private final Map<String, String> savedProperties = new LinkedHashMap<>();
  private final CountDownLatch enteredApply = new CountDownLatch(1);
  private final CountDownLatch releaseApply = new CountDownLatch(1);
  private final CountDownLatch returned = new CountDownLatch(1);
  private final AtomicReference<TSStatus> result = new AtomicReference<>();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final AtomicBoolean interruptedAtReturn = new AtomicBoolean();
  private RaftConsensus consensus;
  private RecordingStateMachine machine;
  private File directory;
  private Thread caller;

  @Test
  public void facadeTimeoutReleasesWaiterAndCommittedCommandStillApplies() throws Exception {
    start(500);
    startBlockedWrite();
    assertTrue(returned.await(5, TimeUnit.SECONDS));
    assertNull(failure.get());
    assertEquals(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(), result.get().getCode());
    assertEquals("consensus write timed out", result.get().getMessage());
    assertFalse(interruptedAtReturn.get());
    assertWaiterGoneThenApplyContinues();
  }

  @Test
  public void facadeInterruptionReleasesWaiterAndPreservesInterruptFlag() throws Exception {
    start(5000);
    startBlockedWrite();
    caller.interrupt();
    assertTrue(returned.await(2, TimeUnit.SECONDS));
    assertNull(failure.get());
    assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), result.get().getCode());
    assertEquals("interrupted", result.get().getMessage());
    assertTrue(interruptedAtReturn.get());
    assertWaiterGoneThenApplyContinues();
  }

  private void start(long timeoutMs) throws Exception {
    property("raft.write_timeout_ms", Long.toString(timeoutMs));
    property("raft.log.cache_bytes", "0");
    property("raft.election_timeout_min_ms", "100");
    property("raft.election_timeout_max_ms", "200");
    property("raft.first_election_timeout_min_ms", "50");
    property("raft.first_election_timeout_max_ms", "100");
    property("raft.heartbeat_interval_ms", "20");
    directory = Files.createTempDirectory("raft-wait-cancel-").toFile();
    machine =
        new RecordingStateMachine() {
          @Override
          public TSStatus write(IConsensusRequest request) {
            if ("blocked".equals(((StringRequest) request).value)) {
              enteredApply.countDown();
              try {
                if (!releaseApply.await(10, TimeUnit.SECONDS)) {
                  throw new AssertionError("test did not release committed application");
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("client interruption reached the applier", e);
              }
            }
            return super.write(request);
          }
        };
    TEndPoint endpoint;
    try (ServerSocket port = new ServerSocket(0)) {
      endpoint = new TEndPoint("127.0.0.1", port.getLocalPort());
      consensus =
          (RaftConsensus)
              ConsensusFactory.getConsensusImpl(
                      RaftConsensus.CLASS_NAME,
                      ConsensusConfig.newBuilder()
                          .setThisNodeId(1)
                          .setThisNode(endpoint)
                          .setStorageDir(directory.getAbsolutePath())
                          .setConsensusGroupType(TConsensusGroupType.DataRegion)
                          .build(),
                      group -> machine)
                  .orElseThrow(() -> new IllegalStateException("cannot construct Raft"));
    }
    consensus.start();
    consensus.createLocalPeer(GROUP, Collections.singletonList(new Peer(GROUP, 1, endpoint)));
    await(() -> consensus.isLeaderReady(GROUP));
  }

  private void startBlockedWrite() throws Exception {
    caller =
        new Thread(
            () -> {
              try {
                result.set(consensus.write(GROUP, new StringRequest("blocked")));
                interruptedAtReturn.set(Thread.currentThread().isInterrupted());
              } catch (Throwable t) {
                failure.set(t);
              } finally {
                returned.countDown();
              }
            },
            "raft-cancel-test-client");
    caller.start();
    assertTrue(enteredApply.await(5, TimeUnit.SECONDS));
    assertEquals(1, requests().size());
    assertEquals(1, requests().boundSize());
    assertTrue(machine.applied.isEmpty());
  }

  private void assertWaiterGoneThenApplyContinues() throws Exception {
    assertEquals(0, requests().size());
    assertEquals(0, requests().boundSize());
    assertTrue(machine.applied.isEmpty());
    releaseApply.countDown();
    await(() -> machine.applied.contains("blocked"));
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        consensus.write(GROUP, new StringRequest("after")).getCode());
    assertEquals(Arrays.asList("blocked", "after"), machine.snapshotApplied());
    assertNull(consensus.getGroup(GROUP).failure());
    assertEquals(0, requests().boundSize());
  }

  private ClientRequestTable requests() throws Exception {
    Field requests = RaftGroupServer.class.getDeclaredField("requests");
    requests.setAccessible(true);
    return (ClientRequestTable) requests.get(consensus.getGroup(GROUP));
  }

  private void property(String key, String value) {
    savedProperties.put(key, System.getProperty(key));
    System.setProperty(key, value);
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertTrue("timed out waiting for application/leadership", condition.getAsBoolean());
  }

  @After
  public void tearDown() throws Exception {
    releaseApply.countDown();
    if (caller != null) {
      caller.join(3000);
    }
    try {
      if (consensus != null) {
        consensus.stop();
      }
    } finally {
      if (directory != null) {
        org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(directory);
      }
      savedProperties.forEach(
          (key, value) -> {
            if (value == null) {
              System.clearProperty(key);
            } else {
              System.setProperty(key, value);
            }
          });
    }
    assertFalse("client thread survived cleanup", caller != null && caller.isAlive());
  }
}
