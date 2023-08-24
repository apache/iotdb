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
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
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
import java.util.Collections;

import static org.junit.Assert.assertTrue;

public class StabilityTest {

  private final ConsensusGroupId dataRegionId = new DataRegionId(1);

  private final File storageDir = new File("target" + java.io.File.separator + "stability");

  private IConsensus consensusImpl;

  private final int basePort = 6667;

  public void constructConsensus() throws IOException {
    consensusImpl =
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
    snapshotUpgradeTest();
  }

  public void addConsensusGroup() {
    try {
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667))));
    } catch (ConsensusException e) {
      Assert.fail();
    }

    try {
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667))));
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
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.1", 6667))));
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
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667))));
      consensusImpl.deleteLocalPeer(dataRegionId);
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
          dataRegionId, new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667)));
      Assert.fail("Can't transfer leader in SimpleConsensus.");
    } catch (ConsensusException e) {
      // not handle
    }
  }

  public void snapshotTest() throws ConsensusException {
    consensusImpl.createLocalPeer(
        dataRegionId,
        Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort))));
    consensusImpl.triggerSnapshot(dataRegionId);

    File dataDir = new File(IoTConsensus.buildPeerDir(storageDir, dataRegionId));

    File[] versionFiles1 =
        dataDir.listFiles((dir, name) -> name.startsWith(IoTConsensusServerImpl.SNAPSHOT_DIR_NAME));
    Assert.assertNotNull(versionFiles1);
    Assert.assertEquals(1, versionFiles1.length);

    consensusImpl.triggerSnapshot(dataRegionId);

    consensusImpl.triggerSnapshot(dataRegionId);

    File[] versionFiles2 =
        dataDir.listFiles((dir, name) -> name.startsWith(IoTConsensusServerImpl.SNAPSHOT_DIR_NAME));
    Assert.assertNotNull(versionFiles2);
    Assert.assertEquals(1, versionFiles2.length);
    Assert.assertNotEquals(versionFiles1[0].getName(), versionFiles2[0].getName());
    consensusImpl.deleteLocalPeer(dataRegionId);
  }

  public void snapshotUpgradeTest() throws Exception {
    consensusImpl.createLocalPeer(
        dataRegionId,
        Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", basePort))));
    consensusImpl.triggerSnapshot(dataRegionId);
    long oldSnapshotIndex = System.currentTimeMillis();
    String oldSnapshotDirName =
        String.format(
            "%s_%s_%d",
            IoTConsensusServerImpl.SNAPSHOT_DIR_NAME, dataRegionId.getId(), oldSnapshotIndex);
    File regionDir = new File(storageDir, "1_1");
    File oldSnapshotDir = new File(regionDir, oldSnapshotDirName);
    if (oldSnapshotDir.exists()) {
      FileUtils.deleteFully(oldSnapshotDir);
    }
    if (!oldSnapshotDir.mkdirs()) {
      throw new ConsensusGroupModifyPeerException(
          String.format("%s: cannot mkdir for snapshot", dataRegionId));
    }
    consensusImpl.triggerSnapshot(dataRegionId);
    Assert.assertFalse(oldSnapshotDir.exists());

    File dataDir = new File(IoTConsensus.buildPeerDir(storageDir, dataRegionId));

    File[] snapshotFiles =
        dataDir.listFiles((dir, name) -> name.startsWith(IoTConsensusServerImpl.SNAPSHOT_DIR_NAME));
    Assert.assertNotNull(snapshotFiles);
    Assert.assertEquals(1, snapshotFiles.length);
    Assert.assertEquals(
        oldSnapshotIndex + 1,
        Long.parseLong(snapshotFiles[0].getName().replaceAll(".*[^\\d](?=(\\d+))", "")));
    consensusImpl.deleteLocalPeer(dataRegionId);
  }
}
