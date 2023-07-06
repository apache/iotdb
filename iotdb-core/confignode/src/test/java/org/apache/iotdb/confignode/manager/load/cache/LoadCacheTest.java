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
import org.apache.iotdb.confignode.manager.load.cache.route.RegionRouteCache;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.Assert;
import org.junit.Test;

public class LoadCacheTest {

  @Test
  public void periodicUpdateTest() {
    RegionRouteCache regionRouteCache =
        new RegionRouteCache(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1));
    long currentTime = System.currentTimeMillis();
    Pair<Long, Integer> leaderSample = new Pair<>(currentTime, 1);
    regionRouteCache.cacheLeaderSample(leaderSample);
    Assert.assertTrue(regionRouteCache.periodicUpdate());
    Assert.assertEquals(1, regionRouteCache.getLeaderId());
  }
}
