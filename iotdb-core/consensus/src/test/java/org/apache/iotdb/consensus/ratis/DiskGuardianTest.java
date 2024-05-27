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

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.consensus.ratis.utils.Retriable;

import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class DiskGuardianTest {

  private static final Logger logger = LoggerFactory.getLogger(DiskGuardian.class);

  private TestUtils.MiniCluster miniCluster;

  private void buildMiniCluster(boolean periodicSnapshot, boolean thresholdSnapshot) {
    final TestUtils.MiniClusterFactory factory = new TestUtils.MiniClusterFactory();

    final RatisConfig config =
        RatisConfig.newBuilder()
            .setSnapshot(
                RatisConfig.Snapshot.newBuilder()
                    .setCreationGap(1L)
                    .setAutoTriggerEnabled(false)
                    .build())
            .setImpl(
                RatisConfig.Impl.newBuilder()
                    .setForceSnapshotInterval(periodicSnapshot ? 5 : -1)
                    .setCheckAndTakeSnapshotInterval(5)
                    .setRaftLogSizeMaxThreshold(thresholdSnapshot ? 1 : 1024 * 1024 * 1024)
                    .build())
            .build();

    miniCluster = factory.setRatisConfig(config).setGid(new SchemaRegionId(1)).create();
  }

  @After
  public void tearUp() throws Exception {
    miniCluster.cleanUp();
  }

  @Test
  public void testForceSnapshot() throws Exception {
    testSnapshotImpl(true, false);
  }

  @Test
  public void testThresholdSnapshot() throws Exception {
    testSnapshotImpl(false, true);
  }

  private void testSnapshotImpl(boolean periodic, boolean threshold) throws Exception {
    buildMiniCluster(periodic, threshold);
    miniCluster.start();
    final ConsensusGroupId gid = miniCluster.getGid();
    final List<Peer> members = miniCluster.getPeers();
    for (RatisConsensus s : miniCluster.getServers()) {
      s.createLocalPeer(gid, members);
    }

    miniCluster.waitUntilActiveLeaderElectedAndReady();
    miniCluster.writeManySerial(0, 10);
    Assert.assertFalse(miniCluster.hasSnapshot(gid, 0));
    Retriable.attemptUntilTrue(
        () -> miniCluster.hasSnapshot(gid, 0),
        12,
        TimeDuration.valueOf(5, TimeUnit.SECONDS),
        "should take snapshot",
        logger);
    Assert.assertTrue(miniCluster.hasSnapshot(gid, 0));
  }
}
