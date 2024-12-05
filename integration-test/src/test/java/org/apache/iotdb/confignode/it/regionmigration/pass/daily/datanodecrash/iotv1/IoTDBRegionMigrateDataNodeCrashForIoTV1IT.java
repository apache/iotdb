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

package org.apache.iotdb.confignode.it.regionmigration.pass.daily.datanodecrash.iotv1;

import org.apache.iotdb.commons.utils.KillPoint.DataNodeKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.KillNode;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionMigrateReliabilityITFramework;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.DailyIT;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category({DailyIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionMigrateDataNodeCrashForIoTV1IT
    extends IoTDBRegionMigrateReliabilityITFramework {
  // region Coordinator DataNode crash tests

  private final int dataReplicateFactor = 2;
  private final int schemaReplicationFactor = 2;
  private final int configNodeNum = 1;
  private final int dataNodeNum = 3;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
  }

  @Test
  public void coordinatorCrashDuringAddPeerTransition() throws Exception {
    failTest(
        2,
        2,
        1,
        3,
        noKillPoints(),
        buildSet(DataNodeKillPoints.COORDINATOR_ADD_PEER_TRANSITION),
        KillNode.COORDINATOR_DATANODE);
  }

  @Test
  public void coordinatorCrashDuringAddPeerDone() throws Exception {
    failTest(
        2,
        2,
        1,
        3,
        noKillPoints(),
        buildSet(DataNodeKillPoints.COORDINATOR_ADD_PEER_DONE),
        KillNode.COORDINATOR_DATANODE);
  }

  // endregion ----------------------------------------------

  // region Original DataNode crash tests

  @Test
  public void originalCrashDuringAddPeerDone() throws Exception {
    failTest(
        2,
        2,
        1,
        3,
        noKillPoints(),
        buildSet(DataNodeKillPoints.ORIGINAL_ADD_PEER_DONE),
        KillNode.ORIGINAL_DATANODE);
  }

  // endregion ----------------------------------------------

  // region Destination DataNode crash tests

  @Test
  public void destinationCrashDuringCreateLocalPeer() throws Exception {
    failTest(
        2,
        2,
        1,
        3,
        noKillPoints(),
        buildSet(DataNodeKillPoints.DESTINATION_CREATE_LOCAL_PEER),
        KillNode.DESTINATION_DATANODE);
  }

  @Test
  public void destinationCrashDuringAddPeerTransition() throws Exception {
    failTest(
        2,
        2,
        1,
        3,
        noKillPoints(),
        buildSet(DataNodeKillPoints.DESTINATION_ADD_PEER_TRANSITION),
        KillNode.DESTINATION_DATANODE);
  }

  @Test
  public void destinationCrashDuringAddPeerDone() throws Exception {
    failTest(
        2,
        2,
        1,
        3,
        noKillPoints(),
        buildSet(DataNodeKillPoints.DESTINATION_ADD_PEER_DONE),
        KillNode.DESTINATION_DATANODE);
  }

  // endregion ----------------------------------------------
}
