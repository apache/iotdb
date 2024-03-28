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

package org.apache.iotdb.confignode.it.regionmigration;

import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.state.RegionTransitionState;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class IoTDBRegionMigrateConfigNodeCrashIT
    extends IoTDBRegionMigrateReliabilityTestFramework {
  // region ConfigNode crash tests
  @Test
  @Ignore
  public void cnCrashDuringPreCheck() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(RegionTransitionState.REGION_MIGRATE_PREPARE.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringCreatePeer() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(AddRegionPeerState.CREATE_NEW_REGION_PEER.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringDoAddPeer() throws Exception {
    generalTest(1, 1, 1, 2, buildSet(AddRegionPeerState.DO_ADD_REGION_PEER.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringUpdateCache() throws Exception {
    generalTest(
        1,
        1,
        1,
        2,
        buildSet(AddRegionPeerState.UPDATE_REGION_LOCATION_CACHE.toString()),
        buildSet());
  }

  @Test
  public void cnCrashDuringChangeRegionLeader() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(RegionTransitionState.CHANGE_REGION_LEADER.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringRemoveRegionPeer() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(RemoveRegionPeerState.REMOVE_REGION_PEER.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringDeleteOldRegionPeer() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(RemoveRegionPeerState.DELETE_OLD_REGION_PEER.toString()), buildSet());
  }

  @Test
  public void cnCrashDuringRemoveRegionLocationCache() throws Exception {
    generalTest(
        1,
        1,
        1,
        2,
        buildSet(RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE.toString()),
        buildSet());
  }

  @Test
  public void cnCrashTest() throws Exception {
    ConcurrentHashMap.KeySetView<String, Boolean> killConfigNodeKeywords = buildSet();
    killConfigNodeKeywords.addAll(
        Arrays.stream(AddRegionPeerState.values())
            .map(Enum::toString)
            .collect(Collectors.toList()));
    killConfigNodeKeywords.addAll(
        Arrays.stream(RemoveRegionPeerState.values())
            .map(Enum::toString)
            .collect(Collectors.toList()));
    generalTest(1, 1, 1, 2, killConfigNodeKeywords, buildSet());
  }
}
