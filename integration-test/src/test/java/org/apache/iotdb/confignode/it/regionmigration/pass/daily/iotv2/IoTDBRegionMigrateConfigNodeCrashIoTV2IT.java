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

package org.apache.iotdb.confignode.it.regionmigration.pass.daily.iotv2;

import org.apache.iotdb.commons.utils.KillPoint.KillNode;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionMigrateReliabilityITFramework;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.state.RegionTransitionState;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.DailyIT;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Category({DailyIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionMigrateConfigNodeCrashIoTV2IT
    extends IoTDBRegionMigrateReliabilityITFramework {
  @Test
  @Ignore
  public void cnCrashDuringPreCheckTest() throws Exception {
    successTest(
        1,
        1,
        1,
        2,
        buildSet(RegionTransitionState.REGION_MIGRATE_PREPARE),
        noKillPoints(),
        KillNode.CONFIG_NODE);
  }

  @Test
  public void cnCrashDuringCreatePeerTest() throws Exception {
    successTest(
        1,
        1,
        1,
        2,
        buildSet(AddRegionPeerState.CREATE_NEW_REGION_PEER),
        noKillPoints(),
        KillNode.CONFIG_NODE);
  }

  @Test
  public void testCnCrashDuringDoAddPeer() throws Exception {
    successTest(
        1,
        1,
        1,
        2,
        buildSet(AddRegionPeerState.DO_ADD_REGION_PEER),
        noKillPoints(),
        KillNode.CONFIG_NODE);
  }

  @Test
  public void cnCrashDuringUpdateCacheTest() throws Exception {
    successTest(
        1,
        1,
        1,
        2,
        buildSet(AddRegionPeerState.UPDATE_REGION_LOCATION_CACHE),
        noKillPoints(),
        KillNode.CONFIG_NODE);
  }

  @Test
  public void cnCrashDuringChangeRegionLeaderTest() throws Exception {
    successTest(
        1,
        1,
        1,
        2,
        buildSet(RemoveRegionPeerState.TRANSFER_REGION_LEADER),
        noKillPoints(),
        KillNode.CONFIG_NODE);
  }

  @Test
  public void cnCrashDuringRemoveRegionPeerTest() throws Exception {
    successTest(
        1,
        1,
        1,
        2,
        buildSet(RemoveRegionPeerState.REMOVE_REGION_PEER),
        noKillPoints(),
        KillNode.CONFIG_NODE);
  }

  @Test
  public void cnCrashDuringDeleteOldRegionPeerTest() throws Exception {
    successTest(
        1,
        1,
        1,
        2,
        buildSet(RemoveRegionPeerState.DELETE_OLD_REGION_PEER),
        noKillPoints(),
        KillNode.CONFIG_NODE);
  }

  @Test
  public void cnCrashDuringRemoveRegionLocationCacheTest() throws Exception {
    successTest(
        1,
        1,
        1,
        2,
        buildSet(RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE),
        noKillPoints(),
        KillNode.CONFIG_NODE);
  }

  @Test
  public void cnCrashTest() throws Exception {
    ConcurrentHashMap.KeySetView<String, Boolean> killConfigNodeKeywords = noKillPoints();
    killConfigNodeKeywords.addAll(
        Arrays.stream(AddRegionPeerState.values())
            .map(KillPoint::enumToString)
            .collect(Collectors.toList()));
    killConfigNodeKeywords.addAll(
        Arrays.stream(RemoveRegionPeerState.values())
            .map(KillPoint::enumToString)
            .collect(Collectors.toList()));
    successTest(1, 1, 1, 2, killConfigNodeKeywords, noKillPoints(), KillNode.CONFIG_NODE);
  }
}
