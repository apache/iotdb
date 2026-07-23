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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.scheduler.LockQueue;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfigManagerTest {

  @Test
  public void testGetRegionLeaderLocationWhenLeaderIsUnavailable() {
    ConfigManager configManager = mock(ConfigManager.class, CALLS_REAL_METHODS);
    LoadManager loadManager = mock(LoadManager.class);
    LoadCache loadCache = mock(LoadCache.class);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(loadManager.getLoadCache()).thenReturn(loadCache);
    when(loadCache.getRegionLeaderMap()).thenReturn(Collections.emptyMap());

    Assert.assertFalse(
        configManager
            .getRegionLeaderLocation(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0))
            .isPresent());
  }

  @Test
  public void testGetRegionLeaderLocationWhenLeaderIsNotRegistered() {
    ConfigManager configManager = mock(ConfigManager.class, CALLS_REAL_METHODS);
    LoadManager loadManager = mock(LoadManager.class);
    LoadCache loadCache = mock(LoadCache.class);
    NodeManager nodeManager = mock(NodeManager.class);
    TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 0);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(loadManager.getLoadCache()).thenReturn(loadCache);
    when(loadCache.getRegionLeaderMap()).thenReturn(Collections.singletonMap(regionId, 1));
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(nodeManager.getRegisteredDataNodeLocations()).thenReturn(Collections.emptyMap());

    Assert.assertFalse(configManager.getRegionLeaderLocation(regionId).isPresent());
  }

  @Test
  public void testGetRegionLeaderLocationWhenLeaderIsRegistered() {
    ConfigManager configManager = mock(ConfigManager.class, CALLS_REAL_METHODS);
    LoadManager loadManager = mock(LoadManager.class);
    LoadCache loadCache = mock(LoadCache.class);
    NodeManager nodeManager = mock(NodeManager.class);
    TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 0);
    TDataNodeLocation regionLeader = new TDataNodeLocation().setDataNodeId(1);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(loadManager.getLoadCache()).thenReturn(loadCache);
    when(loadCache.getRegionLeaderMap()).thenReturn(Collections.singletonMap(regionId, 1));
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(nodeManager.getRegisteredDataNodeLocations())
        .thenReturn(Map.of(regionLeader.getDataNodeId(), regionLeader));

    Assert.assertSame(regionLeader, configManager.getRegionLeaderLocation(regionId).orElseThrow());
  }

  @Test
  public void testHandleRegionMigrationConcurrencyLimitHotReload() {
    ConfigManager configManager = mock(ConfigManager.class, CALLS_REAL_METHODS);
    ProcedureManager procedureManager = mock(ProcedureManager.class);
    ConfigNodeProcedureEnv procedureEnv = mock(ConfigNodeProcedureEnv.class);
    LockQueue regionMigrateSemaphore = new LockQueue(0);
    when(configManager.getProcedureManager()).thenReturn(procedureManager);
    when(procedureManager.getEnv()).thenReturn(procedureEnv);
    when(procedureEnv.getRegionMigrateSemaphore()).thenReturn(regionMigrateSemaphore);

    ConfigNodeConfig config = ConfigNodeDescriptor.getInstance().getConf();
    int originalConcurrencyLimit = config.getRegionMigrationConcurrencyLimit();
    try {
      config.setRegionMigrationConcurrencyLimit(1);
      configManager.handleRegionMigrationConcurrencyLimitHotReload(0);
      Assert.assertEquals(1, regionMigrateSemaphore.getMaxPermits());
    } finally {
      config.setRegionMigrationConcurrencyLimit(originalConcurrencyLimit);
    }
  }
}
