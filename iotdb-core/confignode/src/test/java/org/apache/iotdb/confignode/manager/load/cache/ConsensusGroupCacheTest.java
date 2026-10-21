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
package org.apache.iotdb.confignode.manager.load.cache;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupCache;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupHeartbeatSample;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class ConsensusGroupCacheTest {

  @Test
  public void regionLeaderLookupTest() {
    LoadCache loadCache = new LoadCache();
    TConsensusGroupId dataRegion = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    TConsensusGroupId schemaRegion = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1);
    Assert.assertEquals(-1, loadCache.getRegionLeaderId(dataRegion));
    Assert.assertEquals(-1, loadCache.getRegionLeaderId(schemaRegion));

    loadCache.createRegionGroupHeartbeatCache("root.test", dataRegion, Collections.singleton(2));
    loadCache.createRegionGroupHeartbeatCache("root.test", schemaRegion, Collections.singleton(3));
    Assert.assertEquals(-1, loadCache.getRegionLeaderId(dataRegion));
    Assert.assertEquals(-1, loadCache.getRegionLeaderId(schemaRegion));

    loadCache.cacheConsensusSample(dataRegion, new ConsensusGroupHeartbeatSample(1L, 2));
    loadCache.cacheConsensusSample(schemaRegion, new ConsensusGroupHeartbeatSample(1L, 3));
    loadCache.updateConsensusGroupStatistics();
    Assert.assertEquals(2, loadCache.getRegionLeaderId(dataRegion));
    Assert.assertEquals(3, loadCache.getRegionLeaderId(schemaRegion));

    loadCache.removeRegionGroupCache(dataRegion);
    Assert.assertEquals(-1, loadCache.getRegionLeaderId(dataRegion));
    Assert.assertEquals(3, loadCache.getRegionLeaderId(schemaRegion));
  }

  @Test
  public void periodicUpdateTest() {
    ConsensusGroupCache consensusGroupCache = new ConsensusGroupCache();
    ConsensusGroupHeartbeatSample sample = new ConsensusGroupHeartbeatSample(1L, 1);
    consensusGroupCache.cacheHeartbeatSample(sample);
    consensusGroupCache.updateCurrentStatistics();
    Assert.assertEquals(1, consensusGroupCache.getLeaderId());
  }
}
