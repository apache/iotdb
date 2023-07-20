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
package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.consensus.exception.RatisUnderRecoveryException;

import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecoverReadTest {

  private static class SlowRecoverStateMachine extends TestUtils.IntegerCounter {
    private final TimeDuration stallDuration;
    private final AtomicBoolean stallApply = new AtomicBoolean(false);

    private SlowRecoverStateMachine(TimeDuration stallDuration) {
      this.stallDuration = stallDuration;
    }

    @Override
    public TSStatus write(IConsensusRequest request) {
      if (stallApply.get()) {
        try {
          stallDuration.sleep();
        } catch (InterruptedException e) {
          System.out.println("Interrupted when stalling for write operations");
          Thread.currentThread().interrupt();
        }
      }
      return super.write(request);
    }

    @Override
    public boolean takeSnapshot(File snapshotDir) {
      // allow no snapshot to ensure the raft log be replayed
      return false;
    }

    void setStall() {
      stallApply.set(true);
    }
  }

  private TestUtils.MiniCluster miniCluster;

  @Before
  public void setUp() throws Exception {
    final TestUtils.MiniClusterFactory factory = new TestUtils.MiniClusterFactory();
    miniCluster =
        factory
            .setSMProvider(
                () -> new SlowRecoverStateMachine(TimeDuration.valueOf(500, TimeUnit.MILLISECONDS)))
            .setRatisConfig(
                RatisConfig.newBuilder()
                    .setLog(
                        RatisConfig.Log.newBuilder()
                            .setPurgeUptoSnapshotIndex(false)
                            .setPreserveNumsWhenPurge(1024)
                            .build())
                    .setRead(
                        RatisConfig.Read.newBuilder()
                            .setReadTimeout(TimeDuration.valueOf(30, TimeUnit.SECONDS))
                            .build())
                    .setRpc(
                        RatisConfig.Rpc.newBuilder()
                            .setFirstElectionTimeoutMin(TimeDuration.valueOf(1, TimeUnit.SECONDS))
                            .setFirstElectionTimeoutMax(TimeDuration.valueOf(2, TimeUnit.SECONDS))
                            .build())
                    .build())
            .create();
    miniCluster.start();
  }

  @After
  public void tearUp() throws Exception {
    miniCluster.cleanUp();
  }

  /* mimics the situation before this patch */
  @Test
  public void inconsistentReadAfterRestart() throws Exception {
    final ConsensusGroupId gid = miniCluster.getGid();
    final List<Peer> members = miniCluster.getPeers();

    miniCluster.getServers().forEach(s -> s.createPeer(gid, members));

    // first write 10 ops
    TestUtils.write(miniCluster.getServer(0), gid, 10);
    Assert.assertEquals(10, TestUtils.read(miniCluster.getServer(0), gid));

    // set apply stall time
    miniCluster.getStateMachines().stream()
        .map(SlowRecoverStateMachine.class::cast)
        .forEach(SlowRecoverStateMachine::setStall);

    // shutdown and restart the cluster
    miniCluster.restart();

    // manually set the canServe flag to true to mimic original implementation
    miniCluster.getServer(0).allowStaleRead(gid);

    Assert.assertNotEquals(10, TestUtils.read(miniCluster.getServer(0), gid));
  }

  @Test
  public void consistentRead() throws Exception {
    final ConsensusGroupId gid = miniCluster.getGid();
    final List<Peer> members = miniCluster.getPeers();

    miniCluster.getServers().forEach(s -> s.createPeer(gid, members));

    // first write 10 ops
    TestUtils.write(miniCluster.getServer(0), gid, 10);
    Assert.assertEquals(10, TestUtils.read(miniCluster.getServer(0), gid));

    miniCluster.getStateMachines().stream()
        .map(SlowRecoverStateMachine.class::cast)
        .forEach(SlowRecoverStateMachine::setStall);

    // shutdown and restart the cluster
    miniCluster.restart();

    // wait an active leader to serve linearizable read requests
    miniCluster.waitUntilActiveLeader();

    Assert.assertEquals(10, TestUtils.read(miniCluster.getServer(0), gid));
  }

  @Test
  public void consistentReadFailedWithNoLeader() throws Exception {
    final ConsensusGroupId gid = miniCluster.getGid();
    final List<Peer> members = miniCluster.getPeers();

    miniCluster.getServers().forEach(s -> s.createPeer(gid, members));

    // first write 10 ops
    TestUtils.write(miniCluster.getServer(0), gid, 10);
    Assert.assertEquals(10, TestUtils.read(miniCluster.getServer(0), gid));

    miniCluster.getStateMachines().stream()
        .map(SlowRecoverStateMachine.class::cast)
        .forEach(SlowRecoverStateMachine::setStall);

    // shutdown and restart the cluster
    miniCluster.restart();

    // query during redo: get exception that ratis is under recovery
    final ConsensusReadResponse readResponse = TestUtils.doRead(miniCluster.getServer(0), gid);
    Assert.assertTrue(readResponse.getException() instanceof RatisUnderRecoveryException);
  }

  @Test
  public void consistentReadTimeout() throws Exception {
    final ConsensusGroupId gid = miniCluster.getGid();
    final List<Peer> members = miniCluster.getPeers();

    miniCluster.getServers().forEach(s -> s.createPeer(gid, members));

    // first write 30 ops
    TestUtils.write(miniCluster.getServer(0), gid, 100);
    Assert.assertEquals(100, TestUtils.read(miniCluster.getServer(0), gid));

    miniCluster.getStateMachines().stream()
        .map(SlowRecoverStateMachine.class::cast)
        .forEach(SlowRecoverStateMachine::setStall);

    // shutdown and restart the cluster
    miniCluster.restart();

    // wait until active leader to serve read index requests
    miniCluster.waitUntilActiveLeader();

    // query during redo: get exception that ratis is under recovery
    final ConsensusReadResponse readResponse = TestUtils.doRead(miniCluster.getServer(0), gid);
    Assert.assertTrue(readResponse.getException() instanceof RatisUnderRecoveryException);
  }
}
