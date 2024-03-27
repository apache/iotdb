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

import org.junit.Test;

public class IoTDBRegionMigrateDataNodeCrashTest
    extends IoTDBRegionMigrateReliabilityTestFramework {
  // region Coordinator DataNode crash tests

  @Test
  public void coordinatorCrashDuringRemovePeer() throws Exception {
    generalTest(1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.COORDINATOR_REMOVE_PEER.name()));
  }

  // endregion

  // region Original DataNode crash tests

  @Test
  public void originalCrashDuringRemovePeer() throws Exception {
    generalTest(1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.ORIGINAL_REMOVE_PEER.name()));
  }

  @Test
  public void originalCrashDuringDeleteLocalPeer() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.ORIGINAL_DELETE_OLD_REGION_PEER.name()));
  }

  // endregion

  // region Destination DataNode crash tests

  @Test
  public void destinationCrashDuringCreateLocalPeer() throws Exception {
    generalTest(
        1, 1, 1, 2, buildSet(), buildSet(DataNodeKillPoints.DESTINATION_CREATE_LOCAL_PEER.name()));
  }
}
