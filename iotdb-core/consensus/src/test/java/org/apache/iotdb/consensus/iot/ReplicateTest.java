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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.ConsensusGroup;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.iot.util.TestEntry;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ReplicateTest {

  private static final long CHECK_POINT_GAP = 500;
  private final Logger logger = LoggerFactory.getLogger(ReplicateTest.class);

  private final ConsensusGroupId gid = new DataRegionId(1);

  private int basePort = 9000;

  private final List<Peer> peers =
      Arrays.asList(
          new Peer(gid, 1, new TEndPoint("127.0.0.1", basePort - 2)),
          new Peer(gid, 2, new TEndPoint("127.0.0.1", basePort - 1)),
          new Peer(gid, 3, new TEndPoint("127.0.0.1", basePort)));

  private final List<File> peersStorage =
      Arrays.asList(
          new File("target" + File.separator + "1"),
          new File("target" + File.separator + "2"),
          new File("target" + File.separator + "3"));

  private final ConsensusGroup group = new ConsensusGroup(gid, peers);
  private final List<IoTConsensus> servers = new ArrayList<>();
  private final List<TestStateMachine> stateMachines = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    for (File file : peersStorage) {
      file.mkdirs();
      stateMachines.add(new TestStateMachine());
    }
    initServer();
  }

  @After
  public void tearDown() throws Exception {
    stopServer();
    for (File file : peersStorage) {
      FileUtils.deleteFully(file);
    }
  }

  private void initServer() throws IOException {
    Assume.assumeTrue(checkPortAvailable());
    try {
      for (int i = 0; i < peers.size(); i++) {
        int finalI = i;
        servers.add(
            (IoTConsensus)
                ConsensusFactory.getConsensusImpl(
                        ConsensusFactory.IOT_CONSENSUS,
                        ConsensusConfig.newBuilder()
                            .setThisNodeId(peers.get(i).getNodeId())
                            .setThisNode(peers.get(i).getEndpoint())
                            .setStorageDir(peersStorage.get(i).getAbsolutePath())
                            .setConsensusGroupType(TConsensusGroupType.DataRegion)
                            .build(),
                        groupId -> stateMachines.get(finalI))
                    .orElseThrow(
                        () ->
                            new IllegalArgumentException(
                                String.format(
                                    ConsensusFactory.CONSTRUCT_FAILED_MSG,
                                    ConsensusFactory.IOT_CONSENSUS))));
        servers.get(i).recordCorrectPeerListBeforeStarting(Collections.singletonMap(gid, peers));
      }
      for (int i = 0; i < peers.size(); i++) {
        servers.get(i).start();
      }
    } catch (IOException e) {
      if (e.getCause() instanceof StartupException) {
        // just succeed when can not bind socket
        logger.info("Can not start IoTConsensus because", e);
        Assume.assumeTrue(false);
      } else {
        logger.error("Failed because", e);
        Assert.fail("Failed because " + e.getMessage());
      }
    }
  }

  private void stopServer() {
    servers.parallelStream().forEach(IoTConsensus::stop);
    servers.clear();
  }

  /**
   * The three nodes use the requests in the queue to replicate the requests to the other two nodes.
   */
  @Test
  public void replicateUsingQueueTest()
      throws IOException, InterruptedException, ConsensusException {
    logger.info("Start ReplicateUsingQueueTest");
    servers.get(0).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(1).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(2).createLocalPeer(group.getGroupId(), group.getPeers());

    Assert.assertEquals(0, servers.get(0).getImpl(gid).getSearchIndex());
    Assert.assertEquals(0, servers.get(1).getImpl(gid).getSearchIndex());
    Assert.assertEquals(0, servers.get(2).getImpl(gid).getSearchIndex());

    for (int i = 0; i < CHECK_POINT_GAP; i++) {
      servers.get(0).write(gid, new TestEntry(i, peers.get(0)));
      servers.get(1).write(gid, new TestEntry(i, peers.get(1)));
      servers.get(2).write(gid, new TestEntry(i, peers.get(2)));
      Assert.assertEquals(i + 1, servers.get(0).getImpl(gid).getSearchIndex());
      Assert.assertEquals(i + 1, servers.get(1).getImpl(gid).getSearchIndex());
      Assert.assertEquals(i + 1, servers.get(2).getImpl(gid).getSearchIndex());
    }

    for (int i = 0; i < 3; i++) {
      long start = System.currentTimeMillis();
      while (servers.get(i).getImpl(gid).getMinSyncIndex() < CHECK_POINT_GAP) {
        long current = System.currentTimeMillis();
        if ((current - start) > 60 * 1000) {
          Assert.fail("Unable to replicate entries");
        }
        Thread.sleep(100);
      }
    }

    Assert.assertEquals(CHECK_POINT_GAP, servers.get(0).getImpl(gid).getMinSyncIndex());
    Assert.assertEquals(CHECK_POINT_GAP, servers.get(1).getImpl(gid).getMinSyncIndex());
    Assert.assertEquals(CHECK_POINT_GAP, servers.get(2).getImpl(gid).getMinSyncIndex());
    Assert.assertEquals(CHECK_POINT_GAP * 3, stateMachines.get(0).getRequestSet().size());
    Assert.assertEquals(CHECK_POINT_GAP * 3, stateMachines.get(1).getRequestSet().size());
    Assert.assertEquals(CHECK_POINT_GAP * 3, stateMachines.get(2).getRequestSet().size());
    Assert.assertEquals(stateMachines.get(0).getData(), stateMachines.get(1).getData());
    Assert.assertEquals(stateMachines.get(2).getData(), stateMachines.get(1).getData());

    try {
      stopServer();
      initServer();

      checkPeerList(servers.get(0).getImpl(gid));
      checkPeerList(servers.get(1).getImpl(gid));
      checkPeerList(servers.get(2).getImpl(gid));

      Assert.assertEquals(CHECK_POINT_GAP, servers.get(0).getImpl(gid).getSearchIndex());
      Assert.assertEquals(CHECK_POINT_GAP, servers.get(1).getImpl(gid).getSearchIndex());
      Assert.assertEquals(CHECK_POINT_GAP, servers.get(2).getImpl(gid).getSearchIndex());

      for (int i = 0; i < 3; i++) {
        long start = System.currentTimeMillis();
        while (servers.get(i).getImpl(gid).getMinSyncIndex() < CHECK_POINT_GAP) {
          long current = System.currentTimeMillis();
          if ((current - start) > 60 * 1000) {
            Assert.fail("Unable to recover entries");
          }
          Thread.sleep(100);
        }
      }

      Assert.assertEquals(CHECK_POINT_GAP, servers.get(0).getImpl(gid).getMinSyncIndex());
      Assert.assertEquals(CHECK_POINT_GAP, servers.get(1).getImpl(gid).getMinSyncIndex());
      Assert.assertEquals(CHECK_POINT_GAP, servers.get(2).getImpl(gid).getMinSyncIndex());

    } catch (IOException e) {
      if (e.getCause() instanceof StartupException) {
        // just succeed when can not bind socket
        logger.info("Can not start IoTConsensus because", e);
      } else {
        logger.error("Failed because", e);
        Assert.fail("Failed because " + e.getMessage());
      }
    }
  }

  /**
   * First, suspend one node to test that the request replication between the two alive nodes is ok,
   * then restart all nodes to lose state in the queue, and test using WAL replication to make all
   * nodes finally consistent.
   */
  @Test
  public void replicateUsingWALTest() throws IOException, InterruptedException, ConsensusException {
    logger.info("Start ReplicateUsingWALTest");
    servers.get(0).createLocalPeer(group.getGroupId(), group.getPeers());
    servers.get(1).createLocalPeer(group.getGroupId(), group.getPeers());

    Assert.assertEquals(0, servers.get(0).getImpl(gid).getSearchIndex());
    Assert.assertEquals(0, servers.get(1).getImpl(gid).getSearchIndex());

    for (int i = 0; i < CHECK_POINT_GAP; i++) {
      servers.get(0).write(gid, new TestEntry(i, peers.get(0)));
      servers.get(1).write(gid, new TestEntry(i, peers.get(1)));
      Assert.assertEquals(i + 1, servers.get(0).getImpl(gid).getSearchIndex());
      Assert.assertEquals(i + 1, servers.get(1).getImpl(gid).getSearchIndex());
    }

    Assert.assertEquals(0, servers.get(0).getImpl(gid).getMinSyncIndex());
    Assert.assertEquals(0, servers.get(1).getImpl(gid).getMinSyncIndex());

    try {
      stopServer();
      initServer();

      servers.get(2).createLocalPeer(group.getGroupId(), group.getPeers());
      checkPeerList(servers.get(0).getImpl(gid));
      checkPeerList(servers.get(1).getImpl(gid));
      checkPeerList(servers.get(2).getImpl(gid));

      Assert.assertEquals(CHECK_POINT_GAP, servers.get(0).getImpl(gid).getSearchIndex());
      Assert.assertEquals(CHECK_POINT_GAP, servers.get(1).getImpl(gid).getSearchIndex());
      Assert.assertEquals(0, servers.get(2).getImpl(gid).getSearchIndex());

      for (int i = 0; i < 2; i++) {
        long start = System.currentTimeMillis();
        // should be [CHECK_POINT_GAP, CHECK_POINT_GAP * 2 - 1] after
        // replicating all entries
        while (servers.get(i).getImpl(gid).getMinSyncIndex() < CHECK_POINT_GAP) {
          long current = System.currentTimeMillis();
          if ((current - start) > 60 * 1000) {
            logger.error("{}", servers.get(i).getImpl(gid).getMinSyncIndex());
            Assert.fail("Unable to replicate entries");
          }
          Thread.sleep(100);
        }
      }

      Assert.assertEquals(CHECK_POINT_GAP * 2, stateMachines.get(0).getRequestSet().size());
      Assert.assertEquals(CHECK_POINT_GAP * 2, stateMachines.get(1).getRequestSet().size());
      Assert.assertEquals(CHECK_POINT_GAP * 2, stateMachines.get(2).getRequestSet().size());

      Assert.assertEquals(stateMachines.get(0).getData(), stateMachines.get(1).getData());
      Assert.assertEquals(stateMachines.get(2).getData(), stateMachines.get(1).getData());
    } catch (IOException e) {
      if (e.getCause() instanceof StartupException) {
        // just succeed when can not bind socket
        logger.info("Can not start IoTConsensus because", e);
      } else {
        logger.error("Failed because", e);
        Assert.fail("Failed because " + e.getMessage());
      }
    }
  }

  @Test
  public void parsingAndConstructIDTest() throws Exception {
    logger.info("Start ParsingAndConstructIDTest");
    servers.get(0).createLocalPeer(group.getGroupId(), group.getPeers());
    for (int i = 0; i < CHECK_POINT_GAP; i++) {
      servers.get(0).write(gid, new TestEntry(i, peers.get(0)));
    }

    String regionDir = servers.get(0).getRegionDirFromConsensusGroupId(gid);
    try {
      File regionDirFile = new File(regionDir);
      Assert.assertTrue(regionDirFile.exists());
    } catch (Exception e) {
      Assert.fail();
    }
  }

  private boolean checkPortAvailable() {
    for (Peer peer : this.peers) {
      try (ServerSocket ignored = new ServerSocket(peer.getEndpoint().port)) {
        logger.info("check port {} success for node {}", peer.getEndpoint().port, peer.getNodeId());
      } catch (IOException e) {
        logger.error("check port {} failed for node {}", peer.getEndpoint().port, peer.getNodeId());
        return false;
      }
    }
    return true;
  }

  private void checkPeerList(IoTConsensusServerImpl iotServerImpl) {
    Assert.assertEquals(
        peers.stream().map(Peer::getNodeId).collect(Collectors.toSet()),
        iotServerImpl.getConfiguration().stream().map(Peer::getNodeId).collect(Collectors.toSet()));
  }
}
