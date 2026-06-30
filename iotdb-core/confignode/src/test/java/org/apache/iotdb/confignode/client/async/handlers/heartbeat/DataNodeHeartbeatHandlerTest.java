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

package org.apache.iotdb.confignode.client.async.handlers.heartbeat;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link DataNodeHeartbeatHandler}, focused on how the heartbeat reported region
 * disk usage is mapped into the LOAD BALANCE disk metric.
 *
 * <p>The compression-ratio fix split the DataNode reported region disk into two heartbeat maps:
 * {@code regionDisk} (TsFile bytes only) and {@code dataRegionObjectFileSize} (object-storage file
 * bytes only). The disk size consumed by the LOAD BALANCE migrators ({@link
 * RegionGroupStatistics#getDiskUsage()}) must be the sum of both, otherwise object-storage
 * DataRegions are under-counted and balancing becomes blind to their real footprint.
 */
public class DataNodeHeartbeatHandlerTest {

  private static final TConsensusGroupId GROUP_ID =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
  private static final int DATA_NODE_ID = 0;

  /**
   * Build a heartbeat handler bound to {@link #DATA_NODE_ID} that writes into the given {@link
   * LoadCache}.
   */
  private DataNodeHeartbeatHandler buildHandler(LoadCache loadCache) {
    LoadManager loadManager = Mockito.mock(LoadManager.class);
    Mockito.when(loadManager.getLoadCache()).thenReturn(loadCache);
    return new DataNodeHeartbeatHandler(
        DATA_NODE_ID,
        loadManager,
        new HashMap<>(),
        new HashMap<>(),
        new HashMap<>(),
        seriesUsage -> {},
        deviceUsage -> {},
        null);
  }

  /** Build a minimal valid DataRegion heartbeat response carrying the given disk maps. */
  private TDataNodeHeartbeatResp buildResp(
      Map<Integer, Long> regionDisk, Map<Integer, Long> dataRegionObjectFileSize) {
    TDataNodeHeartbeatResp resp = new TDataNodeHeartbeatResp();
    resp.setHeartbeatTimestamp(System.nanoTime());
    resp.setStatus(RegionStatus.Running.getStatus());
    // A single DataRegion replica on DATA_NODE_ID; not the leader so no consensus sample is cached.
    resp.setJudgedLeaders(Collections.singletonMap(GROUP_ID, false));
    if (regionDisk != null) {
      resp.setRegionDisk(regionDisk);
    }
    if (dataRegionObjectFileSize != null) {
      resp.setDataRegionObjectFileSize(dataRegionObjectFileSize);
    }
    return resp;
  }

  /** Register an empty RegionGroup with a single replica cache on DATA_NODE_ID. */
  private LoadCache newLoadCacheWithRegion() {
    LoadCache loadCache = new LoadCache();
    loadCache.createRegionGroupHeartbeatCache(
        "root.db", GROUP_ID, Collections.singleton(DATA_NODE_ID));
    loadCache.createRegionCache(GROUP_ID, DATA_NODE_ID);
    return loadCache;
  }

  private long diskUsageOf(LoadCache loadCache) {
    loadCache.updateRegionGroupStatistics();
    return loadCache.getCurrentRegionGroupStatisticsMap().get(GROUP_ID).getDiskUsage();
  }

  @Test
  public void diskUsageIncludesObjectFileSizeTest() {
    // regionDisk (TsFile) + dataRegionObjectFileSize (object) must be summed into the balancing
    // disk metric.
    LoadCache loadCache = newLoadCacheWithRegion();
    buildHandler(loadCache)
        .onComplete(
            buildResp(
                Collections.singletonMap(GROUP_ID.getId(), 100L),
                Collections.singletonMap(GROUP_ID.getId(), 25L)));
    Assert.assertEquals(125L, diskUsageOf(loadCache));
  }

  @Test
  public void diskUsageFallsBackToTsFileOnlyWhenObjectUnsetTest() {
    // Back-compat: an older/mixed-version DataNode may not set dataRegionObjectFileSize at all. The
    // handler must not NPE and must fall back to the TsFile-only size.
    LoadCache loadCache = newLoadCacheWithRegion();
    buildHandler(loadCache)
        .onComplete(buildResp(Collections.singletonMap(GROUP_ID.getId(), 100L), null));
    Assert.assertEquals(100L, diskUsageOf(loadCache));
  }

  @Test
  public void diskUsageFallsBackToTsFileOnlyWhenObjectMissesRegionTest() {
    // The object map is present but does not contain this region id -> contribute 0 object bytes.
    LoadCache loadCache = newLoadCacheWithRegion();
    buildHandler(loadCache)
        .onComplete(
            buildResp(
                Collections.singletonMap(GROUP_ID.getId(), 100L),
                Collections.singletonMap(GROUP_ID.getId() + 1, 25L)));
    Assert.assertEquals(100L, diskUsageOf(loadCache));
  }
}
