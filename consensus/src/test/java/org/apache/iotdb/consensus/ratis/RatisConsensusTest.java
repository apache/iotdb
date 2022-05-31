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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.ConsensusGroup;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RatisConsensusTest {

  private ConsensusGroupId gid;
  private List<Peer> peers;
  private List<File> peersStorage;
  private List<IConsensus> servers;
  private List<TestUtils.IntegerCounter> stateMachines;
  private ConsensusGroup group;
  CountDownLatch latch;

  private void makeServers() throws IOException {
    for (int i = 0; i < 3; i++) {
      stateMachines.add(new TestUtils.IntegerCounter());
      int finalI = i;
      servers.add(
          ConsensusFactory.getConsensusImpl(
                  ConsensusFactory.RatisConsensus,
                  peers.get(i).getEndpoint(),
                  peersStorage.get(i),
                  groupId -> stateMachines.get(finalI))
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              ConsensusFactory.CONSTRUCT_FAILED_MSG,
                              ConsensusFactory.RatisConsensus))));
      servers.get(i).start();
    }
  }

  @Before
  public void setUp() throws IOException {
    gid = new DataRegionId(1);
    peers = new ArrayList<>();
    peers.add(new Peer(gid, new TEndPoint("127.0.0.1", 6000)));
    peers.add(new Peer(gid, new TEndPoint("127.0.0.1", 6001)));
    peers.add(new Peer(gid, new TEndPoint("127.0.0.1", 6002)));
    peersStorage = new ArrayList<>();
    peersStorage.add(new File("target" + java.io.File.separator + "1"));
    peersStorage.add(new File("target" + java.io.File.separator + "2"));
    peersStorage.add(new File("target" + java.io.File.separator + "3"));
    for (File dir : peersStorage) {
      dir.mkdirs();
    }
    group = new ConsensusGroup(gid, peers);
    servers = new ArrayList<>();
    stateMachines = new ArrayList<>();
    makeServers();
  }

  @After
  public void tearDown() throws IOException {
    for (int i = 0; i < 3; i++) {
      servers.get(i).stop();
    }
    for (File file : peersStorage) {
      FileUtils.deleteFully(file);
    }
  }

  private int getLeaderOrdinal() {
    return servers.get(0).getLeader(gid).getEndpoint().port - 6000;
  }

  @Test
  public void basicConsensus() throws Exception {

    // 1. Add a new group
    servers.get(0).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(1).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(2).addConsensusGroup(group.getGroupId(), group.getPeers());

    // 2. Do Consensus 10
    doConsensus(servers.get(0), group.getGroupId(), 10, 10);

    // pick a random leader
    int leader = 1;
    int follower1 = (leader + 1) % 3;
    int follower2 = (leader + 2) % 3;

    // 3. Remove two Peers from Group (peer 0 and peer 2)
    // transfer the leader to peer1
    servers.get(follower1).transferLeader(gid, peers.get(leader));
    // Assert.assertTrue(servers.get(1).isLeader(gid));
    // first use removePeer to inform the group leader of configuration change
    servers.get(leader).removePeer(gid, peers.get(follower1));
    servers.get(leader).removePeer(gid, peers.get(follower2));
    // then use removeConsensusGroup to clean up removed Consensus-Peer's states
    servers.get(follower1).removeConsensusGroup(gid);
    servers.get(follower2).removeConsensusGroup(gid);
    Assert.assertEquals(
        servers.get(leader).getLeader(gid).getEndpoint(), peers.get(leader).getEndpoint());
    Assert.assertEquals(
        stateMachines.get(leader).getLeaderEndpoint(), peers.get(leader).getEndpoint());
    Assert.assertEquals(stateMachines.get(leader).getConfiguration().size(), 1);
    Assert.assertEquals(stateMachines.get(leader).getConfiguration().get(0), peers.get(leader));

    // 4. try consensus again with one peer
    doConsensus(servers.get(leader), gid, 10, 20);

    // 5. add two peers back
    // first notify these new peers, let them initialize
    servers.get(follower1).addConsensusGroup(gid, peers);
    servers.get(follower2).addConsensusGroup(gid, peers);
    // then use addPeer to inform the group leader of configuration change
    servers.get(leader).addPeer(gid, peers.get(follower1));
    servers.get(leader).addPeer(gid, peers.get(follower2));
    Assert.assertEquals(stateMachines.get(leader).getConfiguration().size(), 3);

    // 6. try consensus with all 3 peers
    doConsensus(servers.get(2), gid, 10, 30);

    // pick a random leader
    leader = 0;
    follower1 = (leader + 1) % 3;
    follower2 = (leader + 2) % 3;
    // 7. again, group contains only peer0
    servers.get(0).transferLeader(group.getGroupId(), peers.get(leader));
    servers
        .get(leader)
        .changePeer(group.getGroupId(), Collections.singletonList(peers.get(leader)));
    servers.get(follower1).removeConsensusGroup(group.getGroupId());
    servers.get(follower2).removeConsensusGroup(group.getGroupId());
    Assert.assertEquals(
        stateMachines.get(leader).getLeaderEndpoint(), peers.get(leader).getEndpoint());
    Assert.assertEquals(stateMachines.get(leader).getConfiguration().size(), 1);
    Assert.assertEquals(stateMachines.get(leader).getConfiguration().get(0), peers.get(leader));

    // 8. try consensus with only peer0
    doConsensus(servers.get(leader), gid, 10, 40);

    // 9. shutdown all the servers
    for (IConsensus consensus : servers) {
      consensus.stop();
    }
    servers.clear();

    // 10. start again and verify the snapshot
    makeServers();
    servers.get(0).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(1).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(2).addConsensusGroup(group.getGroupId(), group.getPeers());
    doConsensus(servers.get(0), gid, 10, 50);
  }

  private void doConsensus(IConsensus consensus, ConsensusGroupId gid, int count, int target)
      throws Exception {

    latch = new CountDownLatch(count);
    // do write
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    for (int i = 0; i < count; i++) {
      executorService.submit(
          () -> {
            ByteBuffer incr = ByteBuffer.allocate(4);
            incr.putInt(1);
            incr.flip();
            ByteBufferConsensusRequest incrReq = new ByteBufferConsensusRequest(incr);

            ConsensusWriteResponse response = consensus.write(gid, incrReq);
            if (response.getException() != null) {
              response.getException().printStackTrace(System.out);
            }
            Assert.assertEquals(response.getStatus().getCode(), 200);
            latch.countDown();
          });
    }

    executorService.shutdown();

    // wait at most 60s for write to complete, otherwise fail the test
    Assert.assertTrue(latch.await(60, TimeUnit.SECONDS));

    ByteBuffer get = ByteBuffer.allocate(4);
    get.putInt(2);
    get.flip();
    ByteBufferConsensusRequest getReq = new ByteBufferConsensusRequest(get);

    // wait at most 60s to discover a valid leader
    long start = System.currentTimeMillis();
    IConsensus leader = null;
    while (leader == null) {
      long current = System.currentTimeMillis();
      if ((current - start) > 60 * 1000) {
        break;
      }
      for (int i = 0; i < 3; i++) {
        if (servers.get(i).isLeader(gid)) {
          leader = servers.get(i);
        }
      }
    }
    Assert.assertNotNull(leader);

    // Check we reached a consensus
    ConsensusReadResponse response = leader.read(gid, getReq);
    TestUtils.TestDataSet result = (TestUtils.TestDataSet) response.getDataset();
    Assert.assertEquals(target, result.getNumber());
  }
}
