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

package org.apache.iotdb.confignode.it.regionmigration.pass.daily.ratis;

import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionMigrateITFrameworkForRatis;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.DailyIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category({DailyIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionMigrateClusterCrashForRatisIT
    extends IoTDBRegionMigrateITFrameworkForRatis {

  @Test
  public void clusterCrashDuringCreateNewRegionPeer() throws Exception {
    killClusterTest(buildSet(AddRegionPeerState.CREATE_NEW_REGION_PEER), true);
  }

  @Test
  public void clusterCrashDuringCreateConsensusPipes() throws Exception {
    killClusterTest(buildSet(AddRegionPeerState.CREATE_CONSENSUS_PIPES), true);
  }

  @Test
  public void clusterCrashDuringDoAddRegionPeer() throws Exception {
    killClusterTest(buildSet(AddRegionPeerState.DO_ADD_REGION_PEER), false);
  }

  @Test
  public void clusterCrashDuringUpdateRegionLocationCache() throws Exception {
    killClusterTest(buildSet(AddRegionPeerState.UPDATE_REGION_LOCATION_CACHE), true);
  }

  @Test
  public void clusterCrashDuringTransferRegionLeader() throws Exception {
    killClusterTest(buildSet(RemoveRegionPeerState.TRANSFER_REGION_LEADER), true);
  }

  @Test
  public void clusterCrashDuringRemoveRegionPeer() throws Exception {
    killClusterTest(buildSet(RemoveRegionPeerState.REMOVE_REGION_PEER), true);
  }

  @Test
  public void clusterCrashDuringDropConsensusPipes() throws Exception {
    killClusterTest(buildSet(RemoveRegionPeerState.DROP_CONSENSUS_PIPES), true);
  }

  @Test
  public void clusterCrashDuringRemoveRegionLocationCache() throws Exception {
    killClusterTest(buildSet(RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE), true);
  }
}
