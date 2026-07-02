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

package org.apache.iotdb.confignode.manager.consensus;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.exception.AddPeerException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConsensusManagerTest {

  private final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();

  private String originalSystemDir;
  private String originalConsensusDir;
  private String originalInternalAddress;
  private int originalInternalPort;
  private int originalConsensusPort;
  private TEndPoint originalSeedConfigNode;
  private File testDir;

  @Before
  public void setUp() throws IOException {
    originalSystemDir = conf.getSystemDir();
    originalConsensusDir = conf.getConsensusDir();
    originalInternalAddress = conf.getInternalAddress();
    originalInternalPort = conf.getInternalPort();
    originalConsensusPort = conf.getConsensusPort();
    originalSeedConfigNode = conf.getSeedConfigNode();

    testDir = Files.createTempDirectory("ConsensusManagerTest").toFile();
    conf.setSystemDir(new File(testDir, "system").getAbsolutePath());
    conf.setConsensusDir(new File(testDir, "consensus").getAbsolutePath());
    conf.setInternalAddress("127.0.0.1");
    conf.setInternalPort(10710);
    conf.setConsensusPort(10720);
    conf.setSeedConfigNode(new TEndPoint("127.0.0.1", 10710));
    SystemPropertiesUtils.reinitializeStatics();
  }

  @After
  public void tearDown() {
    conf.setSystemDir(originalSystemDir);
    conf.setConsensusDir(originalConsensusDir);
    conf.setInternalAddress(originalInternalAddress);
    conf.setInternalPort(originalInternalPort);
    conf.setConsensusPort(originalConsensusPort);
    conf.setSeedConfigNode(originalSeedConfigNode);
    SystemPropertiesUtils.reinitializeStatics();
    FileUtils.deleteFileOrDirectory(testDir, true);
  }

  @Test
  public void startShouldCreateSeedPeerOnFirstStart() throws Exception {
    IConsensus consensus = Mockito.mock(IConsensus.class);
    ConsensusManager consensusManager = newConsensusManager(consensus);

    consensusManager.start();

    Mockito.verify(consensus).start();
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Peer>> peerCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(consensus)
        .createLocalPeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), peerCaptor.capture());
    Assert.assertEquals(1, peerCaptor.getValue().size());
    Peer localPeer = peerCaptor.getValue().get(0);
    Assert.assertEquals(0, localPeer.getNodeId());
    Assert.assertEquals(new TEndPoint("127.0.0.1", 10720), localPeer.getEndpoint());
    Assert.assertTrue(consensusManager.isInitialized());
  }

  @Test
  public void startShouldFailWhenSeedPeerCreationFails() throws Exception {
    IConsensus consensus = Mockito.mock(IConsensus.class);
    Mockito.doThrow(new ConsensusException("create local peer failed"))
        .when(consensus)
        .createLocalPeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.anyList());
    ConsensusManager consensusManager = newConsensusManager(consensus);

    IOException exception = Assert.assertThrows(IOException.class, consensusManager::start);

    Assert.assertTrue(exception.getMessage().contains("Failed to create local"));
    Mockito.verify(consensus).start();
    Mockito.verify(consensus)
        .createLocalPeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.anyList());
    Assert.assertFalse(consensusManager.isInitialized());
  }

  @Test
  public void startShouldFailWhenSeedPeerAlreadyExistsOnFirstStart() throws Exception {
    IConsensus consensus = Mockito.mock(IConsensus.class);
    Mockito.doThrow(
            new ConsensusGroupAlreadyExistException(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID))
        .when(consensus)
        .createLocalPeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.anyList());
    ConsensusManager consensusManager = newConsensusManager(consensus);

    IOException exception = Assert.assertThrows(IOException.class, consensusManager::start);

    Assert.assertTrue(exception.getMessage().contains("Failed to create local"));
    Mockito.verify(consensus).start();
    Mockito.verify(consensus)
        .createLocalPeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.anyList());
    Assert.assertFalse(consensusManager.isInitialized());
  }

  @Test
  public void startShouldNotCreatePeerWhenRestarted() throws Exception {
    writeSystemProperties();
    createConsensusStateFile();
    IConsensus consensus = Mockito.mock(IConsensus.class);
    ConsensusManager consensusManager = newConsensusManager(consensus);

    consensusManager.start();

    Mockito.verify(consensus).start();
    Mockito.verify(consensus, Mockito.never())
        .createLocalPeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.anyList());
    Assert.assertTrue(consensusManager.isInitialized());
  }

  @Test
  public void createPeerForConsensusGroupShouldIgnoreAlreadyCreatedLocalPeer() throws Exception {
    final IConsensus consensus = Mockito.mock(IConsensus.class);
    Mockito.doThrow(
            new ConsensusGroupAlreadyExistException(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID))
        .when(consensus)
        .createLocalPeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.anyList());

    newConsensusManager(consensus)
        .createPeerForConsensusGroup(Collections.singletonList(newConfigNodeLocation(1)));

    Mockito.verify(consensus)
        .createLocalPeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.anyList());
  }

  @Test
  public void addConfigNodePeerShouldIgnoreAlreadyAddedPeer() throws Exception {
    final IConsensus consensus = Mockito.mock(IConsensus.class);
    Mockito.doThrow(
            new PeerAlreadyInConsensusGroupException(
                ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID,
                new Peer(
                    ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID,
                    1,
                    new TEndPoint("127.0.0.1", 10720))))
        .when(consensus)
        .addRemotePeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));

    newConsensusManager(consensus).addConfigNodePeer(newConfigNodeLocation(1));

    Mockito.verify(consensus)
        .addRemotePeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));
  }

  @Test
  public void addConfigNodePeerShouldKeepFailingForOtherConsensusErrors() throws Exception {
    final IConsensus consensus = Mockito.mock(IConsensus.class);
    Mockito.doThrow(new ConsensusException("reconfiguration failed"))
        .when(consensus)
        .addRemotePeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));

    Assert.assertThrows(
        AddPeerException.class,
        () -> newConsensusManager(consensus).addConfigNodePeer(newConfigNodeLocation(1)));
  }

  @Test
  public void removeConfigNodePeerShouldIgnoreAlreadyRemovedPeer() throws Exception {
    final IConsensus consensus = Mockito.mock(IConsensus.class);
    Mockito.doThrow(
            new PeerNotInConsensusGroupException(
                ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID, "127.0.0.1:10720"))
        .when(consensus)
        .removeRemotePeer(
            Mockito.eq(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID), Mockito.any(Peer.class));

    Assert.assertTrue(
        newConsensusManager(consensus).removeConfigNodePeer(newConfigNodeLocation(1)));
  }

  private ConsensusManager newConsensusManager(final IConsensus consensus) {
    return new ConsensusManager(Mockito.mock(IManager.class), consensus);
  }

  private static TConfigNodeLocation newConfigNodeLocation(final int configNodeId) {
    return new TConfigNodeLocation(
        configNodeId,
        new TEndPoint("127.0.0.1", 10710 + configNodeId),
        new TEndPoint("127.0.0.1", 10720 + configNodeId));
  }

  private void writeSystemProperties() throws IOException {
    File systemFile = new File(conf.getSystemDir(), ConfigNodeConstant.SYSTEM_FILE_NAME);
    Assert.assertTrue(systemFile.getParentFile().mkdirs());
    Properties properties = new Properties();
    properties.setProperty("config_node_id", "0");
    properties.setProperty("is_seed_config_node", "true");
    properties.setProperty("cn_internal_address", "127.0.0.1");
    properties.setProperty("cn_internal_port", "10710");
    properties.setProperty("cn_consensus_port", "10720");
    try (FileOutputStream fileOutputStream = new FileOutputStream(systemFile);
        Writer writer = new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8)) {
      properties.store(writer, "");
    }
  }

  private void createConsensusStateFile() throws IOException {
    File stateFile =
        new File(
            conf.getConsensusDir()
                + File.separator
                + "47474747-4747-4747-4747-000000000000"
                + File.separator
                + "current",
            "raft-meta");
    Assert.assertTrue(stateFile.getParentFile().mkdirs());
    Assert.assertTrue(stateFile.createNewFile());
  }
}
