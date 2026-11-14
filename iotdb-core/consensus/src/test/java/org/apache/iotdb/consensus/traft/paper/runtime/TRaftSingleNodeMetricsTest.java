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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.RequestSemantics;
import org.apache.iotdb.consensus.common.request.RequestSemanticsProvider;
import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.raft.server.RecordingStateMachine;
import org.apache.iotdb.consensus.traft.TRaftConsensus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TRaftSingleNodeMetricsTest {
  @Test
  public void synchronousCommitStopsTimingBeforeSlowApply() throws Exception {
    Map<String, String> previous = new LinkedHashMap<>();
    property(previous, "raft.election_timeout_min_ms", "100");
    property(previous, "raft.election_timeout_max_ms", "200");
    property(previous, "raft.heartbeat_interval_ms", "20");
    property(previous, "traft.partition_max_entries", "8");
    property(previous, "traft.partition_max_ms", "0");
    File directory = Files.createTempDirectory("traft-single-metrics-").toFile();
    TRaftConsensus consensus = null;
    try {
      int port;
      try (ServerSocket socket = new ServerSocket(0)) {
        port = socket.getLocalPort();
      }
      DataRegionId group = new DataRegionId(734);
      TEndPoint endpoint = new TEndPoint("127.0.0.1", port);
      RecordingStateMachine machine =
          new RecordingStateMachine() {
            @Override
            public TSStatus write(IConsensusRequest request) {
              try {
                Thread.sleep(150);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
              }
              return super.write(request);
            }
          };
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
      consensus.createLocalPeer(group, Collections.singletonList(new Peer(group, 1, endpoint)));
      TRaftConsensus running = consensus;
      await(() -> running.isLeaderReady(group));
      long commits = ResearchMetrics.value("traft.leader_commit.count");
      long applies = ResearchMetrics.value("traft.leader_apply.count");
      long commitBefore = ResearchMetrics.value("traft.leader_commit.sum_nanos");
      long applyBefore = ResearchMetrics.value("traft.leader_apply.sum_nanos");
      TSStatus result = consensus.write(group, new TimedRequest());
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.getCode());
      await(() -> ResearchMetrics.value("traft.leader_apply.sum_nanos") > applyBefore);
      assertEquals(commits + 1, ResearchMetrics.value("traft.leader_commit.count"));
      assertEquals(applies + 1, ResearchMetrics.value("traft.leader_apply.count"));
      long commitNanos = ResearchMetrics.value("traft.leader_commit.sum_nanos") - commitBefore;
      long applyNanos = ResearchMetrics.value("traft.leader_apply.sum_nanos") - applyBefore;
      assertTrue(
          "Commit must exclude the 150 ms application delay",
          applyNanos - commitNanos >= TimeUnit.MILLISECONDS.toNanos(100));
      assertEquals(Collections.singletonList("slow-apply"), machine.applied);
    } finally {
      try {
        if (consensus != null) {
          consensus.stop();
        }
      } finally {
        FileUtils.deleteFileOrDirectory(directory);
        previous.forEach(
            (key, value) -> {
              if (value == null) {
                System.clearProperty(key);
              } else {
                System.setProperty(key, value);
              }
            });
      }
    }
  }

  private static void property(Map<String, String> previous, String key, String value) {
    previous.put(key, System.getProperty(key));
    System.setProperty(key, value);
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue("Timed out waiting for single-node metrics", condition.getAsBoolean());
  }

  private static final class TimedRequest extends RecordingStateMachine.StringRequest
      implements RequestSemanticsProvider {
    TimedRequest() {
      super("slow-apply");
    }

    @Override
    public RequestSemantics getRequestSemantics() {
      return RequestSemantics.data(11);
    }
  }
}
