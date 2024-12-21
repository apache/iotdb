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
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.ConsensusGroup;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;

import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RatisConsensusTest {
  private static final Logger logger = LoggerFactory.getLogger(RatisConsensusTest.class);

  private ConsensusGroupId gid;
  private List<Peer> peers;
  private List<RatisConsensus> servers;
  private List<IStateMachine> stateMachines;
  private ConsensusGroup group;

  private TestUtils.MiniCluster miniCluster;
  private final ExecutorService writeExecutor = Executors.newFixedThreadPool(2);

  private final RatisConfig config =
      RatisConfig.newBuilder()
          .setLog(
              RatisConfig.Log.newBuilder()
                  .setPurgeUptoSnapshotIndex(true)
                  .setPurgeGap(10)
                  .setUnsafeFlushEnabled(false)
                  .build())
          .setSnapshot(
              RatisConfig.Snapshot.newBuilder()
                  .setAutoTriggerThreshold(100)
                  .setCreationGap(10)
                  .build())
          .setRpc(
              RatisConfig.Rpc.newBuilder()
                  .setFirstElectionTimeoutMin(TimeDuration.valueOf(1, TimeUnit.SECONDS))
                  .setFirstElectionTimeoutMax(TimeDuration.valueOf(4, TimeUnit.SECONDS))
                  .setTimeoutMin(TimeDuration.valueOf(1, TimeUnit.SECONDS))
                  .setTimeoutMax(TimeDuration.valueOf(4, TimeUnit.SECONDS))
                  .build())
          .setImpl(
              RatisConfig.Impl.newBuilder()
                  .setRaftLogSizeMaxThreshold(1)
                  .setCheckAndTakeSnapshotInterval(4)
                  .build())
          .build();

  @Before
  public void setUp() throws IOException {
    miniCluster = new TestUtils.MiniClusterFactory().setRatisConfig(config).create();
    miniCluster.start();
    gid = miniCluster.getGid();
    servers = miniCluster.getServers();
    group = miniCluster.getGroup();
    peers = miniCluster.getPeers();
    stateMachines = miniCluster.getStateMachines();
  }

  @After
  public void tearDown() throws IOException {
    writeExecutor.shutdown();
    miniCluster.cleanUp();
  }

  @Test
  public void basicConsensus3Copy() throws Exception {
    servers.get(0).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(1).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(2).createLocalPeer(group.getGroupId(), group.getPeers());

    miniCluster.waitUntilActiveLeaderElectedAndReady();

    doConsensus(0, 10, 10);
  }

  @Test
  public void addMemberToGroup() throws Exception {
    List<Peer> original = peers.subList(0, 1);

    Assert.assertEquals(0, servers.get(0).getReplicationNum(group.getGroupId()));
    servers.get(0).createLocalPeer(group.getGroupId(), original);
    Assert.assertEquals(1, servers.get(0).getReplicationNum(group.getGroupId()));
    doConsensus(0, 10, 10);

    Assert.assertThrows(
        ConsensusGroupAlreadyExistException.class,
        () -> servers.get(0).createLocalPeer(group.getGroupId(), original));

    // add 2 members
    servers.get(1).createLocalPeer(group.getGroupId(), Collections.emptyList());
    servers.get(0).addRemotePeer(group.getGroupId(), peers.get(1));
    Assert.assertEquals(2, servers.get(1).getReplicationNum(group.getGroupId()));

    servers.get(2).createLocalPeer(group.getGroupId(), Collections.emptyList());
    servers.get(0).addRemotePeer(group.getGroupId(), peers.get(2));
    Assert.assertEquals(3, servers.get(1).getReplicationNum(group.getGroupId()));

    miniCluster.waitUntilActiveLeaderElectedAndReady();

    doConsensus(0, 10, 20);

    for (int i = 0; i < 3; i++) {
      if (servers.get(i).isLeaderReady(gid)) {
        Assert.assertEquals(
            3, ((TestUtils.IntegerCounter) stateMachines.get(i)).getConfiguration().size());
      }
    }
  }

  @Test
  public void removeMemberFromGroup() throws Exception {
    Assert.assertThrows(
        ConsensusGroupNotExistException.class,
        () -> servers.get(0).deleteLocalPeer(group.getGroupId()));

    servers.get(0).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(1).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(2).createLocalPeer(group.getGroupId(), group.getPeers());

    miniCluster.waitUntilActiveLeaderElectedAndReady();
    doConsensus(0, 10, 10);

    servers.get(0).transferLeader(gid, peers.get(0));
    Assert.assertEquals(3, servers.get(0).getReplicationNum(gid));
    servers.get(0).removeRemotePeer(gid, peers.get(1));
    Assert.assertEquals(2, servers.get(0).getReplicationNum(gid));
    servers.get(1).deleteLocalPeer(gid);
    servers.get(0).removeRemotePeer(gid, peers.get(2));
    Assert.assertEquals(1, servers.get(0).getReplicationNum(gid));
    servers.get(2).deleteLocalPeer(gid);

    miniCluster.waitUntilActiveLeaderElectedAndReady();
    doConsensus(0, 10, 20);
  }

  @Test
  public void oneMemberGroupChange() throws Exception {
    Assert.assertThrows(
        ConsensusGroupNotExistException.class,
        () -> servers.get(0).addRemotePeer(group.getGroupId(), peers.get(0)));

    servers.get(0).createLocalPeer(group.getGroupId(), peers.subList(0, 1));
    doConsensus(0, 10, 10);

    servers.get(1).createLocalPeer(group.getGroupId(), Collections.emptyList());
    servers.get(0).addRemotePeer(group.getGroupId(), peers.get(1));
    Assert.assertThrows(
        PeerAlreadyInConsensusGroupException.class,
        () -> servers.get(0).addRemotePeer(group.getGroupId(), peers.get(1)));

    servers.get(0).transferLeader(group.getGroupId(), peers.get(1));

    servers.get(1).removeRemotePeer(group.getGroupId(), peers.get(0));
    Assert.assertThrows(
        PeerNotInConsensusGroupException.class,
        () -> servers.get(1).removeRemotePeer(group.getGroupId(), peers.get(0)));

    Assert.assertEquals(servers.get(1).getLeader(gid).getNodeId(), peers.get(1).getNodeId());

    servers.get(0).deleteLocalPeer(group.getGroupId());
    Assert.assertThrows(
        ConsensusGroupNotExistException.class,
        () -> servers.get(0).deleteLocalPeer(group.getGroupId()));
  }

  @Test
  public void crashAndStart() throws Exception {
    servers.get(0).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(1).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(2).createLocalPeer(group.getGroupId(), group.getPeers());

    miniCluster.waitUntilActiveLeaderElectedAndReady();
    // 200 operation will trigger snapshot & purge
    doConsensus(0, 200, 200);

    miniCluster.stop();
    miniCluster.restart();

    miniCluster.waitUntilActiveLeaderElectedAndReady();
    doConsensus(0, 10, 210);
  }

  @Test
  public void transferLeader() throws Exception {
    servers.get(0).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(1).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(2).createLocalPeer(group.getGroupId(), group.getPeers());

    doConsensus(0, 10, 10);

    final int oldLeader = servers.get(0).getLeader(group.getGroupId()).getNodeId();
    final int newLeader = (oldLeader + 1) % miniCluster.getServers().size();
    logger.debug("old leader is {} and new leader is {}", oldLeader, newLeader);

    try {
      servers.get(0).transferLeader(group.getGroupId(), peers.get(newLeader));
    } catch (ConsensusException e) {
      logger.error("Failed to transfer snapshot:", e);
      Assert.fail();
    }

    // After the transferLeader request, the new peer will start leader election, need to wait here
    miniCluster.waitUntilActiveLeaderElectedAndReady();

    final Peer newLeaderPeer = servers.get(0).getLeader(group.getGroupId());
    Assert.assertNotNull(newLeaderPeer);
    Assert.assertEquals(newLeader, newLeaderPeer.getNodeId());
  }

  @Test
  public void transferLeaderFailed() throws Exception {
    servers.get(0).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(1).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(2).createLocalPeer(group.getGroupId(), group.getPeers());

    doConsensus(0, 10, 10);

    final int leader = servers.get(0).getLeader(gid).getNodeId();
    final int f1 = (leader + 1) % 3;
    servers.get(f1).stop();
    doConsensus(leader, 10, 20);

    // transfer will fail since the server 2 is down
    Assert.assertThrows(
        ConsensusException.class,
        () -> {
          servers.get(leader).transferLeader(gid, peers.get(f1));
        });
  }

  @Test
  public void transferSnapshot() throws Exception {
    servers.get(0).createLocalPeer(gid, peers.subList(0, 1));

    doConsensus(0, 10, 10);
    servers.get(0).triggerSnapshot(gid, false);

    servers.get(1).createLocalPeer(gid, peers.subList(1, 2));
    servers.get(0).addRemotePeer(gid, peers.get(1));

    miniCluster.waitUntilActiveLeaderElectedAndReady();
    doConsensus(1, 10, 20);
  }

  @Test
  public void forceSnapshot() throws Exception {
    servers.get(0).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(1).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(2).createLocalPeer(group.getGroupId(), group.getPeers());

    doConsensus(0, 1, 1); // not enough logs to auto trigger snapshot
    try {
      servers.get(0).triggerSnapshot(group.getGroupId(), true);
    } catch (ConsensusException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertTrue(miniCluster.hasSnapshot(group.getGroupId(), 0));
  }

  @Test
  public void parsingAndConstructIDs() throws Exception {
    servers.get(0).createLocalPeer(gid, peers.subList(0, 1));
    doConsensus(0, 10, 10);

    String regionDir = servers.get(0).getRegionDirFromConsensusGroupId(gid);
    try {
      File regionDirFile = new File(regionDir);
      Assert.assertTrue(regionDirFile.exists());
    } catch (Exception e) {
      Assert.fail();
    }
  }

  private void doConsensus(int serverIndex, int count, int target) throws Exception {
    miniCluster.writeManyParallel(writeExecutor, serverIndex, count);
    Assert.assertEquals(target, miniCluster.mustRead(serverIndex));
  }
}
