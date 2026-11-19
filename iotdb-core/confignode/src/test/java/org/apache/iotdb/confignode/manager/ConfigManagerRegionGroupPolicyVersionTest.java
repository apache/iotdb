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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.RegionGroupExtensionPolicy;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ConfigManagerRegionGroupPolicyVersionTest {

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final String DATA_POLICY_KEY = "data_region_group_extension_policy";
  private static final TNodeVersionInfo LOCAL_VERSION =
      new TNodeVersionInfo(IoTDBConstant.VERSION, IoTDBConstant.BUILD_INFO);

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ConfigManager manager;
  private NodeManager nodeManager;
  private ConsensusManager consensusManager;
  private LoadManager loadManager;
  private ClusterSchemaManager schemaManager;
  private File configurationFile;
  private byte[] originalFileContent;
  private String originalConfigurationPath;
  private int originalNodeId;
  private RegionGroupExtensionPolicy originalSchemaPolicy;
  private RegionGroupExtensionPolicy originalDataPolicy;
  private String originalReadConsistency;
  private String originalClusterName;
  private Map<String, String> originalAppliedProperties;

  @Before
  public void setUp() throws IOException {
    originalConfigurationPath = System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF);
    originalNodeId = CONF.getConfigNodeId();
    originalSchemaPolicy = CONF.getSchemaRegionGroupExtensionPolicy();
    originalDataPolicy = CONF.getDataRegionGroupExtensionPolicy();
    originalReadConsistency = CONF.getReadConsistencyLevel();
    originalClusterName = CONF.getClusterName();
    originalAppliedProperties = new HashMap<>(ConfigurationFileUtils.getAppliedProperties());
    CONF.setConfigNodeId(0);
    File configurationDirectory = temporaryFolder.newFolder();
    configurationFile = new File(configurationDirectory, CommonConfig.SYSTEM_CONFIG_NAME);
    Files.writeString(
        configurationFile.toPath(),
        "schema_region_group_extension_policy="
            + originalSchemaPolicy.getPolicy()
            + "\n"
            + DATA_POLICY_KEY
            + "="
            + originalDataPolicy.getPolicy()
            + "\nread_consistency_level="
            + originalReadConsistency
            + "\n",
        StandardCharsets.UTF_8);
    originalFileContent = Files.readAllBytes(configurationFile.toPath());
    System.setProperty(
        ConfigNodeConstant.CONFIGNODE_CONF, configurationDirectory.getAbsolutePath());

    // Run SET dispatch, policy validation and file updates without starting ConfigManager
    // services.
    manager = mock(ConfigManager.class);
    nodeManager = mock(NodeManager.class);
    consensusManager = mock(ConsensusManager.class);
    loadManager = mock(LoadManager.class);
    schemaManager = mock(ClusterSchemaManager.class);
    manager.nodeManager = nodeManager;
    when(manager.setConfiguration(any())).thenCallRealMethod();
    when(manager.getNodeManager()).thenReturn(nodeManager);
    when(manager.getConsensusManager()).thenReturn(consensusManager);
    when(manager.getLoadManager()).thenReturn(loadManager);
    when(manager.getClusterSchemaManager()).thenReturn(schemaManager);
    when(manager.confirmLeader())
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    when(consensusManager.isLeader()).thenReturn(true);
    when(nodeManager.isConfigNodeVersionConsistentWith(any())).thenReturn(false);
    when(nodeManager.setConfiguration(any())).thenReturn(Collections.emptyList());
  }

  @After
  public void tearDown() {
    CONF.setConfigNodeId(originalNodeId);
    CONF.setSchemaRegionGroupExtensionPolicy(originalSchemaPolicy);
    CONF.setDataRegionGroupExtensionPolicy(originalDataPolicy);
    CONF.setReadConsistencyLevel(originalReadConsistency);
    CONF.setClusterName(originalClusterName);
    ConfigurationFileUtils.getAppliedProperties().clear();
    ConfigurationFileUtils.getAppliedProperties().putAll(originalAppliedProperties);
    if (originalConfigurationPath == null) {
      System.clearProperty(ConfigNodeConstant.CONFIGNODE_CONF);
    } else {
      System.setProperty(ConfigNodeConstant.CONFIGNODE_CONF, originalConfigurationPath);
    }
  }

  @Test
  public void testMixedVersionsRejectEntireRequestBeforeAnyChanges() throws IOException {
    Map<String, String> configs = new HashMap<>();
    configs.put(DATA_POLICY_KEY, differentDataPolicy().getPolicy());
    configs.put(
        "read_consistency_level", "strong".equals(originalReadConsistency) ? "weak" : "strong");
    configs.put("cluster_name", "changed-cluster");

    TSStatus status = manager.setConfiguration(new TSetConfigurationReq(configs, -1));

    assertVersionRejection(status);
    assertConfigurationUnchanged();
    assertEquals(originalAppliedProperties, ConfigurationFileUtils.getAppliedProperties());
    verify(nodeManager).isConfigNodeVersionConsistentWith(LOCAL_VERSION);
    verify(nodeManager, never()).setConfiguration(any());
    verifyZeroInteractions(schemaManager, loadManager);
  }

  @Test
  public void testMixedVersionsRejectSchemaPolicyBeforeAnyChanges() throws IOException {
    RegionGroupExtensionPolicy nextPolicy =
        originalSchemaPolicy == RegionGroupExtensionPolicy.PROACTIVE
            ? RegionGroupExtensionPolicy.AUTO
            : RegionGroupExtensionPolicy.PROACTIVE;
    TSetConfigurationReq request =
        new TSetConfigurationReq(
            Collections.singletonMap(
                "schema_region_group_extension_policy", nextPolicy.getPolicy()),
            -1);

    TSStatus status = manager.setConfiguration(request);

    assertVersionRejection(status);
    assertConfigurationUnchanged();
    assertEquals(originalAppliedProperties, ConfigurationFileUtils.getAppliedProperties());
    verify(nodeManager).isConfigNodeVersionConsistentWith(LOCAL_VERSION);
    verify(nodeManager, never()).setConfiguration(any());
    verifyZeroInteractions(schemaManager, loadManager);
  }

  @Test
  public void testMixedVersionsRejectUnchangedPolicy() throws IOException {
    TSStatus status = manager.setConfiguration(policyRequest(originalDataPolicy, -1));

    assertVersionRejection(status);
    assertConfigurationUnchanged();
    assertEquals(originalAppliedProperties, ConfigurationFileUtils.getAppliedProperties());
    verify(nodeManager).isConfigNodeVersionConsistentWith(LOCAL_VERSION);
    verify(nodeManager, never()).setConfiguration(any());
    verifyZeroInteractions(schemaManager, loadManager);
  }

  @Test
  public void testInternalFollowerApplicationSkipsVersionCheck() throws IOException {
    when(consensusManager.isLeader()).thenReturn(false);
    RegionGroupExtensionPolicy nextPolicy = differentDataPolicy();

    TSStatus status =
        manager.setConfiguration(policyRequest(nextPolicy, NodeManager.APPLY_CONFIG_LOCALLY));

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    assertEquals(nextPolicy, CONF.getDataRegionGroupExtensionPolicy());
    Properties fileProperties = new Properties();
    try (Reader reader =
        Files.newBufferedReader(configurationFile.toPath(), StandardCharsets.UTF_8)) {
      fileProperties.load(reader);
    }
    assertEquals(nextPolicy.getPolicy(), fileProperties.getProperty(DATA_POLICY_KEY));
    verify(nodeManager, never()).isConfigNodeVersionConsistentWith(any());
    verify(nodeManager, never()).setConfiguration(any());
    verifyZeroInteractions(schemaManager, loadManager);
  }

  @Test
  public void testConsistentVersionsAllowUnchangedPolicyAndBroadcast() throws IOException {
    when(nodeManager.isConfigNodeVersionConsistentWith(any())).thenReturn(true);
    TSetConfigurationReq request = policyRequest(originalDataPolicy, -1);

    TSStatus status = manager.setConfiguration(request);

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    assertConfigurationUnchanged();
    verify(nodeManager).isConfigNodeVersionConsistentWith(LOCAL_VERSION);
    verify(nodeManager).setConfiguration(request);
    verifyZeroInteractions(schemaManager, loadManager);
  }

  @Test
  public void testOtherConfigurationSkipsVersionCheckAndBroadcasts() throws IOException {
    TSetConfigurationReq request =
        new TSetConfigurationReq(
            Collections.singletonMap("read_consistency_level", originalReadConsistency), -1);

    TSStatus status = manager.setConfiguration(request);

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    assertConfigurationUnchanged();
    verify(nodeManager, never()).isConfigNodeVersionConsistentWith(any());
    verify(nodeManager).setConfiguration(request);
    verifyZeroInteractions(schemaManager, loadManager);
  }

  private void assertVersionRejection(TSStatus status) {
    assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
    assertEquals(
        ConfigNodeMessages
            .EXCEPTION_REGIONGROUP_POLICY_HOT_RELOAD_REQUIRES_IDENTICAL_CONFIGNODE_VERSIONS_AND_BUILDS_FA3BBFEA,
        status.getMessage());
  }

  private void assertConfigurationUnchanged() throws IOException {
    assertEquals(originalSchemaPolicy, CONF.getSchemaRegionGroupExtensionPolicy());
    assertEquals(originalDataPolicy, CONF.getDataRegionGroupExtensionPolicy());
    assertEquals(originalReadConsistency, CONF.getReadConsistencyLevel());
    assertEquals(originalClusterName, CONF.getClusterName());
    assertArrayEquals(originalFileContent, Files.readAllBytes(configurationFile.toPath()));
  }

  private RegionGroupExtensionPolicy differentDataPolicy() {
    return originalDataPolicy == RegionGroupExtensionPolicy.PROACTIVE
        ? RegionGroupExtensionPolicy.AUTO
        : RegionGroupExtensionPolicy.PROACTIVE;
  }

  private TSetConfigurationReq policyRequest(RegionGroupExtensionPolicy policy, int nodeId) {
    return new TSetConfigurationReq(
        Collections.singletonMap(DATA_POLICY_KEY, policy.getPolicy()), nodeId);
  }
}
