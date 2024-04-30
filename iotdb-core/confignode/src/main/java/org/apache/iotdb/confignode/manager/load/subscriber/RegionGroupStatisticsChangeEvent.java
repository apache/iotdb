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

package org.apache.iotdb.confignode.manager.load.subscriber;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;

import org.apache.tsfile.utils.Pair;

import java.util.Map;

/** RegionGroupStatisticsChangeEvent represents the change of RegionGroup statistics. */
public class RegionGroupStatisticsChangeEvent {

  // Map<RegionGroupId, Pair<old RegionGroupStatistics, new RegionGroupStatistics>>
  private final Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
      differentRegionGroupStatisticsMap;

  public RegionGroupStatisticsChangeEvent(
      Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
          differentRegionGroupStatisticsMap) {
    this.differentRegionGroupStatisticsMap = differentRegionGroupStatisticsMap;
  }

  public Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
      getDifferentRegionGroupStatisticsMap() {
    return differentRegionGroupStatisticsMap;
  }
}
