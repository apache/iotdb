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
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class StabilityTest {

  private final ConsensusGroupId dataRegionId = new DataRegionId(1);

  private final File storageDir = new File("target" + java.io.File.separator + "stability");

  private IoTConsensus consensusImpl;

  private final int basePort = 6667;

  public void constructConsensus() throws IOException {
    consensusImpl =
        (IoTConsensus)
            ConsensusFactory.getConsensusImpl(
                    ConsensusFactory.IOT_CONSENSUS,
                    ConsensusConfig.newBuilder()
                        .setThisNodeId(1)
                        .setThisNode(new TEndPoint("0.0.0.0", basePort))
                        .setStorageDir(storageDir.getAbsolutePath())
                        .setConsensusGroupType(TConsensusGroupType.DataRegion)
                        .build(),
                    gid -> new TestStateMachine())
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            String.format(
                                ConsensusFactory.CONSTRUCT_FAILED_MSG,
                                ConsensusFactory.IOT_CONSENSUS)));
    consensusImpl.start();
  }

  @Before
  public void setUp() throws Exception {
    FileUtils.deleteFully(storageDir);
    constructConsensus();
  }

  @After
  public void tearDown() throws IOException {
    consensusImpl.stop();
    FileUtils.deleteFully(storageDir);
  }

  @Test
  public void allTest() throws Exception {
    addConsensusGroup();
    removeConsensusGroup();
    peerTest();
    transferLeader();
    snapshotTest();
  }

  public void addConsensusGroup() {
    try {
      Assert.assertEquals(0, consensusImpl.getReplicationNum(dataRegionId));
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort))));
      Assert.assertEquals(1, consensusImpl.getReplicationNum(dataRegionId));
    } catch (ConsensusException e) {
      Assert.fail();
    }

    try {
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort))));
      Assert.fail();
    } catch (ConsensusException e) {
      assertTrue(e instanceof ConsensusGroupAlreadyExistException);
    }

    try {
      consensusImpl.createLocalPeer(dataRegionId, Collections.emptyList());
      Assert.fail();
    } catch (ConsensusException e) {
      assertTrue(e instanceof IllegalPeerNumException);
    }

    try {
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.1", basePort))));
      Assert.fail();
    } catch (ConsensusException e) {
      assertTrue(e instanceof IllegalPeerEndpointException);
    }

    try {
      consensusImpl.deleteLocalPeer(dataRegionId);
    } catch (ConsensusException e) {
      Assert.fail();
    }
  }

  public void removeConsensusGroup() {
    try {
      consensusImpl.deleteLocalPeer(dataRegionId);
      Assert.fail();
    } catch (ConsensusException e) {
      assertTrue(e instanceof ConsensusGroupNotExistException);
    }

    try {
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort))));
      Assert.assertEquals(1, consensusImpl.getReplicationNum(dataRegionId));
      consensusImpl.deleteLocalPeer(dataRegionId);
      Assert.assertEquals(0, consensusImpl.getReplicationNum(dataRegionId));
    } catch (ConsensusException e) {
      Assert.fail();
    }
  }

  public void peerTest() throws Exception {
    consensusImpl.createLocalPeer(
        dataRegionId,
        Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort))));

    consensusImpl.deleteLocalPeer(dataRegionId);

    consensusImpl.stop();
    consensusImpl = null;

    constructConsensus();
    consensusImpl.createLocalPeer(
        dataRegionId,
        Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort))));
    consensusImpl.deleteLocalPeer(dataRegionId);
  }

  public void transferLeader() {
    try {
      consensusImpl.transferLeader(
          dataRegionId, new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort)));
      Assert.fail("Can't transfer leader in IoTConsensus.");
    } catch (ConsensusException e) {
      // not handle
    }
  }

  public void snapshotTest() throws ConsensusException {
    consensusImpl.createLocalPeer(
        dataRegionId,
        Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort))));
    consensusImpl.triggerSnapshot(dataRegionId, false);

    File dataDir = new File(IoTConsensus.buildPeerDir(storageDir, dataRegionId));

    File[] versionFiles1 =
        dataDir.listFiles((dir, name) -> name.startsWith(IoTConsensusServerImpl.SNAPSHOT_DIR_NAME));
    Assert.assertNotNull(versionFiles1);
    Assert.assertEquals(1, versionFiles1.length);

    consensusImpl.triggerSnapshot(dataRegionId, false);

    consensusImpl.triggerSnapshot(dataRegionId, false);

    File[] versionFiles2 =
        dataDir.listFiles((dir, name) -> name.startsWith(IoTConsensusServerImpl.SNAPSHOT_DIR_NAME));
    Assert.assertNotNull(versionFiles2);
    Assert.assertEquals(1, versionFiles2.length);
    Assert.assertNotEquals(versionFiles1[0].getName(), versionFiles2[0].getName());
    consensusImpl.deleteLocalPeer(dataRegionId);
  }

  @Test
  public void recordAndResetPeerListTest() throws Exception {
    try {
      Assert.assertEquals(0, consensusImpl.getReplicationNum(dataRegionId));
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort))));
      Assert.assertEquals(1, consensusImpl.getReplicationNum(dataRegionId));
      Assert.assertEquals(1, consensusImpl.getImpl(dataRegionId).getConfiguration().size());
    } catch (ConsensusException e) {
      Assert.fail();
    }
    consensusImpl.stop();

    // test add sync channel
    Map<ConsensusGroupId, List<Peer>> correctPeers = new HashMap<>();
    List<Peer> peerList1And2 = new ArrayList<>();
    peerList1And2.add(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort)));
    peerList1And2.add(new Peer(dataRegionId, 2, new TEndPoint("0.0.0.0", basePort)));
    correctPeers.put(dataRegionId, peerList1And2);
    consensusImpl.recordCorrectPeerListBeforeStarting(correctPeers);
    consensusImpl.start();
    Assert.assertEquals(2, consensusImpl.getImpl(dataRegionId).getConfiguration().size());
    consensusImpl.stop();

    // test remove sync channel
    List<Peer> peerList1 = new ArrayList<>();
    peerList1.add(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort)));
    correctPeers.put(dataRegionId, peerList1);
    consensusImpl.recordCorrectPeerListBeforeStarting(correctPeers);
    consensusImpl.start();
    Assert.assertEquals(1, consensusImpl.getImpl(dataRegionId).getConfiguration().size());
    consensusImpl.stop();

    // test remove invalid peer
    List<Peer> peerList2 = new ArrayList<>();
    peerList2.add(new Peer(dataRegionId, 2, new TEndPoint("0.0.0.0", basePort)));
    correctPeers.put(dataRegionId, peerList2);
    consensusImpl.recordCorrectPeerListBeforeStarting(correctPeers);
    consensusImpl.start();
    Assert.assertNull(consensusImpl.getImpl(dataRegionId));
  }
}
