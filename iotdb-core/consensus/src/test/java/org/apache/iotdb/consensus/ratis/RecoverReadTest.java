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
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusResponse;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecoverReadTest {

  private static class SlowRecoverStateMachine extends TestUtils.IntegerCounter {
    private final TimeDuration stallDuration;
    private AtomicBoolean isRecover = new AtomicBoolean(false);
    public SlowRecoverStateMachine(TimeDuration stallDuration) {
      this.stallDuration = stallDuration;
    }
    @Override
    public TSStatus write(IConsensusRequest request) {
      if (isRecover.get()) {
          try {
          stallDuration.wait();
        } catch (InterruptedException e) {
          System.out.println("Interrupted when stalling for write operations");
          Thread.currentThread().interrupt();
        }
      }
      return super.write(request);
    }

    @Override
    public void loadSnapshot(File latestSnapshotRootDir) {
      isRecover.set(true);
      super.loadSnapshot(latestSnapshotRootDir);
    }
  }

  private TestUtils.MiniCluster miniCluster;

  @Before
  public void setUp() throws Exception {
    final TestUtils.MiniClusterFactory factory = new TestUtils.MiniClusterFactory();
    miniCluster = factory
        .setSMProvider(() -> new SlowRecoverStateMachine(TimeDuration.ONE_SECOND))
        .setRatisConfig(RatisConfig.newBuilder()
            .setLog(RatisConfig.Log.newBuilder()
                  .setPurgeUptoSnapshotIndex(true)
                .build()).build())
        .create();
    miniCluster.start();
  }
  @After
  public void tearUp() throws Exception {
    //miniCluster.cleanUp();
  }

  @Test
  public void consistentReadBeforeRedo() throws Exception {
    final ConsensusGroupId gid = miniCluster.getGid();
    final List<Peer> members = miniCluster.getPeers();

    miniCluster.getServer(0).createPeer(gid, members.subList(0,1));
//    miniCluster.getServer(1).createPeer(gid, members);
//    miniCluster.getServer(2).createPeer(gid, members);

    // first write 5 ops
    TestUtils.write(miniCluster.getServer(0), gid, 5);
    Assert.assertEquals(5, TestUtils.read(miniCluster.getServer(0), gid));

    // take a snapshot
    final ConsensusGenericResponse snapshotResp = miniCluster.getServer(0).triggerSnapshot(gid);
    Assert.assertTrue(snapshotResp.isSuccess());

    // write another 5 ops
    TestUtils.write(miniCluster.getServer(0), gid, 5);
    Assert.assertEquals(10, TestUtils.read(miniCluster.getServer(0), gid));

//    // restart the cluster
//    miniCluster.restart();
//
//    Assert.assertEquals(10, TestUtils.read(miniCluster.getServer(0), gid));
  }
}
