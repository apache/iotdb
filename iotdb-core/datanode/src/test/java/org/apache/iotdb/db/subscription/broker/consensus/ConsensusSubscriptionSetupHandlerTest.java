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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusSubscriptionSetupHandlerTest {

  @Test
  public void testResolveFallbackCommittedRegionProgressUsesRecoveredState() {
    final ConsensusSubscriptionCommitManager commitManager =
        mock(ConsensusSubscriptionCommitManager.class);
    final DataRegionId regionId = new DataRegionId(11);
    final Map<WriterId, WriterProgress> writerPositions = new LinkedHashMap<>();
    writerPositions.put(new WriterId(regionId.toString(), 3, 7L), new WriterProgress(100L, 9L));
    final RegionProgress committedRegionProgress = new RegionProgress(writerPositions);
    when(commitManager.getCommittedRegionProgress("cg", "topic", regionId))
        .thenReturn(committedRegionProgress);

    final RegionProgress resolved =
        ConsensusSubscriptionSetupHandler.resolveFallbackCommittedRegionProgress(
            commitManager, "cg", "topic", regionId);

    assertSame(committedRegionProgress, resolved);
    verify(commitManager).getOrCreateState("cg", "topic", regionId);
  }

  @Test
  public void testResolveFallbackCommittedRegionProgressReturnsNullForEmptyState() {
    final ConsensusSubscriptionCommitManager commitManager =
        mock(ConsensusSubscriptionCommitManager.class);
    final DataRegionId regionId = new DataRegionId(12);
    when(commitManager.getCommittedRegionProgress("cg", "topic", regionId))
        .thenReturn(new RegionProgress(Collections.emptyMap()));

    final RegionProgress resolved =
        ConsensusSubscriptionSetupHandler.resolveFallbackCommittedRegionProgress(
            commitManager, "cg", "topic", regionId);

    assertNull(resolved);
    verify(commitManager).getOrCreateState("cg", "topic", regionId);
  }
}
