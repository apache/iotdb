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

import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupCache;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupHeartbeatSample;

import org.junit.Assert;
import org.junit.Test;

public class ConsensusGroupCacheTest {

  @Test
  public void periodicUpdateTest() {
    ConsensusGroupCache consensusGroupCache = new ConsensusGroupCache();
    ConsensusGroupHeartbeatSample sample = new ConsensusGroupHeartbeatSample(1L, 1);
    consensusGroupCache.cacheHeartbeatSample(sample);
    consensusGroupCache.updateCurrentStatistics(false);
    Assert.assertEquals(1, consensusGroupCache.getLeaderId());
  }
}
