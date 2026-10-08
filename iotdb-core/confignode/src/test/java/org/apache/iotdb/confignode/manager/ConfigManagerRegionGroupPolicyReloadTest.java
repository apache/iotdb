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

import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.partition.RegionGroupExtensionPolicy;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ConfigManagerRegionGroupPolicyReloadTest {
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private RegionGroupExtensionPolicy originalSchemaPolicy;
  private RegionGroupExtensionPolicy originalDataPolicy;
  private ConfigManager manager;
  private ConsensusManager consensusManager;
  private ClusterSchemaManager schemaManager;
  private LoadManager loadManager;

  @Before
  public void setUp() {
    originalSchemaPolicy = CONF.getSchemaRegionGroupExtensionPolicy();
    originalDataPolicy = CONF.getDataRegionGroupExtensionPolicy();
    CONF.setSchemaRegionGroupExtensionPolicy(RegionGroupExtensionPolicy.AUTO);
    CONF.setDataRegionGroupExtensionPolicy(RegionGroupExtensionPolicy.PROACTIVE);
    manager = mock(ConfigManager.class);
    consensusManager = mock(ConsensusManager.class);
    schemaManager = mock(ClusterSchemaManager.class);
    loadManager = mock(LoadManager.class);
    when(manager.getConsensusManager()).thenReturn(consensusManager);
    when(manager.getClusterSchemaManager()).thenReturn(schemaManager);
    when(manager.getLoadManager()).thenReturn(loadManager);
    when(consensusManager.isLeader()).thenReturn(true);
    when(schemaManager.getDatabaseNames(null)).thenReturn(Arrays.asList("root.one", "root.two"));
  }

  @After
  public void tearDown() {
    CONF.setSchemaRegionGroupExtensionPolicy(originalSchemaPolicy);
    CONF.setDataRegionGroupExtensionPolicy(originalDataPolicy);
  }

  @Test
  public void testEnteringProactiveRebalancesEveryDatabase() throws Exception {
    for (RegionGroupExtensionPolicy previous :
        Arrays.asList(RegionGroupExtensionPolicy.AUTO, RegionGroupExtensionPolicy.CUSTOM)) {
      clearInvocations(schemaManager, loadManager);
      reload(RegionGroupExtensionPolicy.AUTO, previous, CONF.getDataRegionPerDataNode());
      verify(schemaManager).adjustMaxRegionGroupNum();
      verify(loadManager).reBalanceDataPartitionPolicy("root.one");
      verify(loadManager).reBalanceDataPartitionPolicy("root.two");
    }
  }

  @Test
  public void testRepeatedPolicyDoesNotRebalance() throws Exception {
    reload(
        RegionGroupExtensionPolicy.AUTO,
        RegionGroupExtensionPolicy.PROACTIVE,
        CONF.getDataRegionPerDataNode());
    verifyZeroInteractions(loadManager, schemaManager);
  }

  @Test
  public void testSchemaAndQuotaChangesDoNotRebalanceData() throws Exception {
    reload(
        RegionGroupExtensionPolicy.CUSTOM,
        RegionGroupExtensionPolicy.PROACTIVE,
        CONF.getDataRegionPerDataNode());
    reload(
        RegionGroupExtensionPolicy.AUTO,
        RegionGroupExtensionPolicy.PROACTIVE,
        CONF.getDataRegionPerDataNode() + 1);
    verifyZeroInteractions(loadManager);
  }

  @Test
  public void testLeavingProactiveDoesNotRebalance() throws Exception {
    for (RegionGroupExtensionPolicy next :
        Arrays.asList(RegionGroupExtensionPolicy.AUTO, RegionGroupExtensionPolicy.CUSTOM)) {
      CONF.setDataRegionGroupExtensionPolicy(next);
      reload(
          RegionGroupExtensionPolicy.AUTO,
          RegionGroupExtensionPolicy.PROACTIVE,
          CONF.getDataRegionPerDataNode());
    }
    verifyZeroInteractions(loadManager);
  }

  @Test
  public void testFollowerDoesNotRebalance() throws Exception {
    when(consensusManager.isLeader()).thenReturn(false);
    reload(
        RegionGroupExtensionPolicy.AUTO,
        RegionGroupExtensionPolicy.CUSTOM,
        CONF.getDataRegionPerDataNode());
    verifyZeroInteractions(loadManager, schemaManager);
  }

  private void reload(
      RegionGroupExtensionPolicy previousSchema,
      RegionGroupExtensionPolicy previousData,
      int previousDataQuota)
      throws Exception {
    Method reload =
        ConfigManager.class.getDeclaredMethod(
            "handleRegionGroupConfigHotReload",
            int.class,
            int.class,
            RegionGroupExtensionPolicy.class,
            RegionGroupExtensionPolicy.class);
    reload.setAccessible(true);
    reload.invoke(
        manager,
        CONF.getSchemaRegionPerDataNode(),
        previousDataQuota,
        previousSchema,
        previousData);
  }
}
