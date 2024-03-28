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

package org.apache.iotdb.confignode.it.regionmigration.notpass.datanodecrash;

import org.apache.iotdb.commons.utils.KillPoints.IoTConsensusRemovePeerKillPoints;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionMigrateReliabilityTestFramework;

import org.junit.Test;

public class CoordinatorRemoveRemotePeerCrashIT extends IoTDBRegionMigrateReliabilityTestFramework {
  private <T extends Enum<T>> void base(T... dataNodeKillPoints) throws Exception {
    generalTest(1, 1, 1, 2, noKillPoints(), buildSet(dataNodeKillPoints));
  }

  @Test
  public void initCrash() throws Exception {
    base(IoTConsensusRemovePeerKillPoints.INIT);
  }

  @Test
  public void crashAfterNotifyPeersToRemoveSyncLogChannel() throws Exception {
    base(IoTConsensusRemovePeerKillPoints.AFTER_NOTIFY_PEERS_TO_REMOVE_SYNC_LOG_CHANNEL);
  }

  @Test
  public void crashAfterInactivePeer() throws Exception {
    base(IoTConsensusRemovePeerKillPoints.AFTER_INACTIVE_PEER);
  }

  @Test
  public void crashAfterFinish() throws Exception {
    base(IoTConsensusRemovePeerKillPoints.FINISH);
  }
}
