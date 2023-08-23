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
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.RatisUnderRecoveryException;

import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecoverReadTest {
  private static final Logger logger = LoggerFactory.getLogger(RecoverReadTest.class);
  private static final TimeDuration stallDuration =
      TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);

  private static class SlowRecoverStateMachine extends TestUtils.IntegerCounter {
    private final TimeDuration stallDuration;
    private final AtomicBoolean stallApply = new AtomicBoolean(false);

    private SlowRecoverStateMachine(TimeDuration stallDuration) {
      this.stallDuration = stallDuration;
    }

    private SlowRecoverStateMachine(TimeDuration stallDuration, boolean isStall) {
      this.stallDuration = stallDuration;
      this.stallApply.set(isStall);
    }

    @Override
    public TSStatus write(IConsensusRequest request) {
      if (stallApply.get()) {
        try {
          stallDuration.sleep();
          logger.info("Apply i={} when recovering", integer.get());
        } catch (InterruptedException e) {
          logger.warn("Interrupted when stalling for write operations", e);
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
  }

  private TestUtils.MiniCluster miniCluster;

  @Before
  public void setUp() throws Exception {
    logger.info("[RECOVER TEST] start setting up the test env");
    final TestUtils.MiniClusterFactory factory = new TestUtils.MiniClusterFactory();
    miniCluster =
        factory
            .setSMProvider(() -> new SlowRecoverStateMachine(stallDuration))
            .setRatisConfig(
                RatisConfig.newBuilder()
                    .setLog(
                        RatisConfig.Log.newBuilder()
                            .setPurgeUptoSnapshotIndex(false)
                            .setPreserveNumsWhenPurge(1024)
                            .build())
                    .setRead(
                        RatisConfig.Read.newBuilder()
                            .setReadTimeout(TimeDuration.valueOf(20, TimeUnit.SECONDS))
                            .build())
                    .setRpc(
                        RatisConfig.Rpc.newBuilder()
                            .setFirstElectionTimeoutMin(TimeDuration.valueOf(1, TimeUnit.SECONDS))
                            .setFirstElectionTimeoutMax(TimeDuration.valueOf(2, TimeUnit.SECONDS))
                            .setRequestTimeout(TimeDuration.valueOf(20, TimeUnit.SECONDS))
                            .build())
                    .build())
            .create();
    miniCluster.start();
    logger.info("[RECOVER TEST] end setting up the test env");
  }

  @After
  public void tearUp() throws Exception {
    logger.info("[RECOVER TEST] start tearing down the test env");
    miniCluster.cleanUp();
    logger.info("[RECOVER TEST] end tearing down the test env");
  }

  /* mimics the situation before this patch */
  @Test
  public void inconsistentReadAfterRestart() throws Exception {
    final ConsensusGroupId gid = miniCluster.getGid();
    final List<Peer> members = miniCluster.getPeers();

    for (RatisConsensus s : miniCluster.getServers()) {
      s.createLocalPeer(gid, members);
    }

    // first write 10 ops
    TestUtils.write(miniCluster.getServer(0), gid, 10);
    Assert.assertEquals(10, TestUtils.read(miniCluster.getServer(0), gid));

    // stop the cluster
    miniCluster.stop();

    // set stall when restart
    miniCluster.resetSMProviderBeforeRestart(
        () -> new SlowRecoverStateMachine(stallDuration, true));

    // restart the cluster
    miniCluster.restart();

    // manually set the canServe flag to true to mimic original implementation
    miniCluster.getServer(0).allowStaleRead(gid);

    Assert.assertNotEquals(10, TestUtils.read(miniCluster.getServer(0), gid));
  }

  @Test
  public void consistentRead() throws Exception {
    final ConsensusGroupId gid = miniCluster.getGid();
    final List<Peer> members = miniCluster.getPeers();

    for (RatisConsensus s : miniCluster.getServers()) {
      s.createLocalPeer(gid, members);
    }

    // first write 10 ops
    TestUtils.write(miniCluster.getServer(0), gid, 10);
    Assert.assertEquals(10, TestUtils.read(miniCluster.getServer(0), gid));

    // stop the cluster
    miniCluster.stop();

    // set stall when restart
    miniCluster.resetSMProviderBeforeRestart(
        () -> new SlowRecoverStateMachine(stallDuration, true));

    // restart the cluster
    miniCluster.restart();

    // wait an active leader to serve linearizable read requests
    miniCluster.waitUntilActiveLeader();

    // try max 3 minutes
    final long startTs = System.currentTimeMillis();
    DataSet resp;
    try {
      resp = TestUtils.doRead(miniCluster.getServer(0), gid);
    } catch (ConsensusException e) {
      final long timeElapsed = System.currentTimeMillis() - startTs;
      if (timeElapsed > 1000 * 60 * 3) { // 3 min
        Assert.fail("Linearizable read failed after 3 minutes, last exception seen: " + e);
      }
      Thread.sleep(100);
      logger.info("linearizable read failed  when restart, retrying: ", e);
      resp = TestUtils.doRead(miniCluster.getServer(0), gid);
    }

    Assert.assertEquals(10, ((TestUtils.TestDataSet) resp).getNumber());
  }

  @Test
  public void consistentReadFailedWithNoLeader() throws Exception {
    final ConsensusGroupId gid = miniCluster.getGid();
    final List<Peer> members = miniCluster.getPeers();

    for (RatisConsensus s : miniCluster.getServers()) {
      s.createLocalPeer(gid, members);
    }

    // first write 10 ops
    TestUtils.write(miniCluster.getServer(0), gid, 10);
    Assert.assertEquals(10, TestUtils.read(miniCluster.getServer(0), gid));

    // stop the cluster
    miniCluster.stop();

    // set stall when restart
    miniCluster.resetSMProviderBeforeRestart(
        () -> new SlowRecoverStateMachine(stallDuration, true));

    // restart the cluster
    miniCluster.restart();

    // query during redo: get exception that ratis is under recovery
    try {
      TestUtils.doRead(miniCluster.getServer(0), gid);
      Assert.fail();
    } catch (ConsensusException e) {
      Assert.assertTrue(e instanceof RatisUnderRecoveryException);
    }
  }

  @Test
  public void consistentReadTimeout() throws Exception {
    final ConsensusGroupId gid = miniCluster.getGid();
    final List<Peer> members = miniCluster.getPeers();

    for (RatisConsensus s : miniCluster.getServers()) {
      s.createLocalPeer(gid, members);
    }

    // first write 30 ops
    TestUtils.write(miniCluster.getServer(0), gid, 50);
    Assert.assertEquals(50, TestUtils.read(miniCluster.getServer(0), gid));

    // stop the cluster
    miniCluster.stop();

    // set stall when restart
    miniCluster.resetSMProviderBeforeRestart(
        () -> new SlowRecoverStateMachine(stallDuration, true));

    // restart the cluster
    miniCluster.restart();

    // wait until active leader to serve read index requests
    miniCluster.waitUntilActiveLeader();

    // query during redo: get exception that ratis is under recovery
    try {
      TestUtils.doRead(miniCluster.getServer(0), gid);
      Assert.fail();
    } catch (ConsensusException e) {
      Assert.assertTrue(e instanceof RatisUnderRecoveryException);
    }
  }
}
