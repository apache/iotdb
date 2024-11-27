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

package org.apache.iotdb.confignode.it.regionmigration.pass.datanodecrash;

import org.apache.iotdb.commons.utils.KillPoint.IoTConsensusRemovePeerCoordinatorKillPoints;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionMigrateDataNodeCrashITFramework;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.DailyIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category({DailyIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionMigrateCoordinatorCrashWhenRemoveRemotePeerIT
    extends IoTDBRegionMigrateDataNodeCrashITFramework {

  @Test
  public void initCrash() throws Exception {
    success(IoTConsensusRemovePeerCoordinatorKillPoints.INIT);
  }

  @Test
  public void crashAfterNotifyPeersToRemoveSyncLogChannel() throws Exception {
    if (isIoTV1) {
      success(
          IoTConsensusRemovePeerCoordinatorKillPoints
              .AFTER_NOTIFY_PEERS_TO_REMOVE_REPLICATE_CHANNEL);
    }
  }

  @Test
  public void crashAfterInactivePeer() throws Exception {
    success(IoTConsensusRemovePeerCoordinatorKillPoints.AFTER_INACTIVE_PEER);
  }

  @Test
  public void crashAfterFinish() throws Exception {
    success(IoTConsensusRemovePeerCoordinatorKillPoints.FINISH);
  }
}
