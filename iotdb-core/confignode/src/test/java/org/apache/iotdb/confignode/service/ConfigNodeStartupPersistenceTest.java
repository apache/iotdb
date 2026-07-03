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

package org.apache.iotdb.confignode.service;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class ConfigNodeStartupPersistenceTest {

  private final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();

  private String originalSystemDir;
  private String originalConsensusDir;
  private String originalInternalAddress;
  private int originalInternalPort;
  private int originalConsensusPort;
  private int originalConfigNodeId;
  private TEndPoint originalSeedConfigNode;
  private File testDir;

  @Before
  public void setUp() throws IOException {
    originalSystemDir = conf.getSystemDir();
    originalConsensusDir = conf.getConsensusDir();
    originalInternalAddress = conf.getInternalAddress();
    originalInternalPort = conf.getInternalPort();
    originalConsensusPort = conf.getConsensusPort();
    originalConfigNodeId = conf.getConfigNodeId();
    originalSeedConfigNode = conf.getSeedConfigNode();

    testDir = Files.createTempDirectory("ConfigNodeStartupPersistenceTest").toFile();
    conf.setSystemDir(new File(testDir, "system").getAbsolutePath());
    conf.setConsensusDir(new File(testDir, "consensus").getAbsolutePath());
    conf.setInternalAddress("127.0.0.1");
    conf.setInternalPort(10710);
    conf.setConsensusPort(10720);
    conf.setConfigNodeId(0);
    conf.setSeedConfigNode(new TEndPoint("127.0.0.1", 10710));
    SystemPropertiesUtils.reinitializeStatics();
    Assert.assertTrue(new File(conf.getSystemDir()).mkdirs());
    createConsensusStateFile();
  }

  @After
  public void tearDown() {
    conf.setSystemDir(originalSystemDir);
    conf.setConsensusDir(originalConsensusDir);
    conf.setInternalAddress(originalInternalAddress);
    conf.setInternalPort(originalInternalPort);
    conf.setConsensusPort(originalConsensusPort);
    conf.setConfigNodeId(originalConfigNodeId);
    conf.setSeedConfigNode(originalSeedConfigNode);
    SystemPropertiesUtils.reinitializeStatics();
    FileUtils.deleteFileOrDirectory(testDir, true);
  }

  @Test
  public void applySeedConfigNodeShouldNotStoreSystemPropertiesWhenApplyFails() throws Exception {
    ConfigNode configNode = new ConfigNode();
    NodeManager nodeManager = Mockito.mock(NodeManager.class);
    Mockito.when(nodeManager.applyConfigNode(Mockito.any(), Mockito.any()))
        .thenReturn(
            new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
                .setMessage("apply failed"));
    configNode.setConfigManager(mockConfigManager(nodeManager));

    Assert.assertThrows(
        StartupException.class, configNode::applySeedConfigNodeAndStoreSystemParameters);

    Assert.assertFalse(getSystemPropertiesFile().exists());
    Assert.assertEquals(
        SystemPropertiesUtils.StartupState.PARTIAL_START_CONSENSUS_ONLY,
        SystemPropertiesUtils.getStartupState());
  }

  @Test
  public void applySeedConfigNodeShouldStoreSystemPropertiesAfterApplySucceeds() throws Exception {
    ConfigNode configNode = new ConfigNode();
    NodeManager nodeManager = Mockito.mock(NodeManager.class);
    Mockito.when(nodeManager.applyConfigNode(Mockito.any(), Mockito.any()))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    configNode.setConfigManager(mockConfigManager(nodeManager));

    configNode.applySeedConfigNodeAndStoreSystemParameters();

    Assert.assertTrue(getSystemPropertiesFile().exists());
    Assert.assertEquals(
        SystemPropertiesUtils.StartupState.RESTART, SystemPropertiesUtils.getStartupState());
  }

  private ConfigManager mockConfigManager(NodeManager nodeManager) {
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    Mockito.when(configManager.getNodeManager()).thenReturn(nodeManager);
    return configManager;
  }

  private File getSystemPropertiesFile() {
    return new File(conf.getSystemDir(), ConfigNodeConstant.SYSTEM_FILE_NAME);
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
