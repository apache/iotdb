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

package org.apache.iotdb.commons.consensus.iotv2.consistency.repair;

import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DataPointLocator;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class RepairConflictResolverTest {

  @Test
  public void leaderHasWithoutNewerFollowerDeletionShouldSendToFollower() {
    RepairConflictResolver resolver =
        new RepairConflictResolver(Collections.emptyList(), Collections.emptyList());

    Assert.assertEquals(
        RepairAction.SEND_TO_FOLLOWER,
        resolver.resolveLeaderHas(new DataPointLocator("root.sg.d1", "s1", 100L), 10L));
  }

  @Test
  public void leaderHasWithNewerFollowerDeletionShouldDeleteOnLeader() {
    RepairConflictResolver resolver =
        new RepairConflictResolver(
            Collections.emptyList(),
            Collections.singletonList(
                new ModEntrySummary("root.sg.d1", "s1", 0L, 200L, 20L)));

    Assert.assertEquals(
        RepairAction.DELETE_ON_LEADER,
        resolver.resolveLeaderHas(new DataPointLocator("root.sg.d1", "s1", 100L), 10L));
  }

  @Test
  public void followerHasWithoutNewerLeaderDeletionShouldSendToLeader() {
    RepairConflictResolver resolver =
        new RepairConflictResolver(Collections.emptyList(), Collections.emptyList());

    Assert.assertEquals(
        RepairAction.SEND_TO_LEADER,
        resolver.resolveFollowerHas(new DataPointLocator("root.sg.d1", "s1", 100L), 10L));
  }

  @Test
  public void followerHasWithNewerLeaderDeletionShouldDeleteOnFollower() {
    RepairConflictResolver resolver =
        new RepairConflictResolver(
            Collections.singletonList(
                new ModEntrySummary("root.sg.d1", "s1", 0L, 200L, 20L)),
            Collections.emptyList());

    Assert.assertEquals(
        RepairAction.DELETE_ON_FOLLOWER,
        resolver.resolveFollowerHas(new DataPointLocator("root.sg.d1", "s1", 100L), 10L));
  }
}
