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

package org.apache.iotdb.consensus.traft.paper.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.RequestSemantics;
import org.apache.iotdb.consensus.common.request.RequestSemanticsProvider;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.raft.server.RecordingStateMachine;
import org.apache.iotdb.consensus.traft.TRaftConsensus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** A one-voter append can apply before returning the identity used to bind its client wait. */
public class TRaftLocalRequestTest {
  private static final DataRegionId GROUP = new DataRegionId(735);
  private final Map<String, String> previous = new LinkedHashMap<>();
  private File directory;
  private TEndPoint endpoint;
  private TRaftConsensus consensus;
  private Machine machine;

  private static final class Request extends RecordingStateMachine.StringRequest
      implements RequestSemanticsProvider {
    final long timestamp;

    Request(String value, long timestamp) {
      super(value);
      this.timestamp = timestamp;
    }

    @Override
    public RequestSemantics getRequestSemantics() {
      return RequestSemantics.data(timestamp);
    }
  }

  private static final class Machine extends RecordingStateMachine {
    final AtomicInteger bytes = new AtomicInteger();
    final AtomicInteger live = new AtomicInteger();
    volatile IConsensusRequest lastLive;

    @Override
    public IConsensusRequest deserializeRequest(IConsensusRequest request) {
      if (request instanceof ByteBufferConsensusRequest) {
        bytes.incrementAndGet();
      } else {
        live.incrementAndGet();
        lastLive = request;
      }
      return super.deserializeRequest(request);
    }
  }

  private void property(String name, String value) {
    previous.put(name, System.getProperty(name));
    System.setProperty(name, value);
  }

  @Before
  public void setUp() throws Exception {
    property("raft.election_timeout_min_ms", "100");
    property("raft.election_timeout_max_ms", "200");
    property("raft.heartbeat_interval_ms", "20");
    property("raft.log_cache_bytes", "0");
    property("raft.write_timeout_ms", "2000");
    property("traft.partition_max_entries", "16");
    property("traft.partition_max_ms", "0");
    directory = Files.createTempDirectory("traft-local-request-").toFile();
    try (ServerSocket socket = new ServerSocket(0)) {
      endpoint = new TEndPoint("127.0.0.1", socket.getLocalPort());
    }
    start();
    consensus.createLocalPeer(GROUP, Collections.singletonList(new Peer(GROUP, 1, endpoint)));
    awaitLeader();
  }

  private void start() throws Exception {
    machine = new Machine();
    consensus =
        new TRaftConsensus(
            ConsensusConfig.newBuilder()
                .setThisNodeId(1)
                .setThisNode(endpoint)
                .setStorageDir(directory.getAbsolutePath())
                .setConsensusGroupType(TConsensusGroupType.DataRegion)
                .build(),
            id -> machine);
    consensus.start();
  }

  private void awaitLeader() throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!consensus.isLeaderReady(GROUP) && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue("A single voter must finish its recovery quorum", consensus.isLeaderReady(GROUP));
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (consensus != null) {
        consensus.stop();
      }
    } finally {
      if (directory != null) {
        FileUtils.deleteFileOrDirectory(directory);
      }
      previous.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
    }
  }

  @Test
  public void synchronousDataAndBoundaryApplyBindTheirOwnLiveRequestAndRestartFallsBack()
      throws Exception {
    Request high = new Request("synchronous-data", 20);
    Request low = new Request("synchronous-boundary", -1);
    for (Request request : Arrays.asList(high, low)) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), consensus.write(GROUP, request).getCode());
      assertSame(request, machine.lastLive);
    }
    assertEquals(2, machine.live.get());
    assertEquals(0, machine.bytes.get());
    assertEquals(0, machine.remoteApplied.get());
    assertEquals(Arrays.asList(high.value, low.value), machine.snapshotApplied());
    consensus.stop();
    consensus = null;
    start();
    awaitLeader();
    assertEquals(Arrays.asList(high.value, low.value), machine.snapshotApplied());
    assertEquals("No live object survives a process restart", 0, machine.live.get());
    assertEquals(2, machine.bytes.get());
    assertEquals(
        "Older creation terms replay as remote consensus input", 2, machine.remoteApplied.get());
  }
}
