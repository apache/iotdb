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

package org.apache.iotdb.confignode.it;

import org.apache.iotdb.commons.utils.DataNodeKillPoints;

import org.junit.Ignore;
import org.junit.Test;

public class IoTDBRegionMigrateDataNodeCrashTest
    extends IoTDBRegionMigrateReliabilityTestFramework {
  // region Coordinator DataNode crash tests

  @Ignore
  @Test
  public void coordinatorCrashDuringRemovePeer() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.COORDINATOR_REMOVE_PEER.name()));
  }

  @Test
  public void coordinatorCrashDuringAddPeerTransition() throws Exception {
    generalTest(
        1,
        1,
        1,
        2,
        buildSet(),
        buildSet(DataNodeKillPoints.COORDINATOR_ADD_PEER_TRANSITION.name()));
  }

  @Test
  public void coordinatorCrashDuringAddPeerDone() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.COORDINATOR_ADD_PEER_DONE.name()));
  }

  // endregion

  // region Original DataNode crash tests

  @Ignore
  @Test
  public void originalCrashDuringRemovePeer() throws Exception {
    generalTest(1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.ORIGINAL_REMOVE_PEER.name()));
  }

  @Ignore
  @Test
  public void originalCrashDuringDeleteLocalPeer() throws Exception {
    generalTest(
        1,
        1,
        1,
        2,
        buildSet(),
        buildSet(DataNodeKillPoints.ORIGINAL_DELETE_OLD_REGION_PEER.name()));
  }

  @Test
  public void originalCrashDuringAddPeerDone() throws Exception {
    generalTest(2, 2, 1, 3, buildSet(), buildSet(DataNodeKillPoints.ORIGINAL_ADD_PEER_DONE.name()));
  }

  // endregion

  // region Destination DataNode crash tests

  @Test
  public void destinationCrashDuringCreateLocalPeer() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.DESTINATION_CREATE_LOCAL_PEER.name()));
  }

  @Test
  public void destinationCrashDuringAddPeerTransition() throws Exception {
    generalTest(
        1,
        1,
        1,
        2,
        buildSet(),
        buildSet(DataNodeKillPoints.DESTINATION_ADD_PEER_TRANSITION.name()));
  }

  @Test
  public void destinationCrashDuringAddPeerDone() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.DESTINATION_ADD_PEER_DONE.name()));
  }
}
