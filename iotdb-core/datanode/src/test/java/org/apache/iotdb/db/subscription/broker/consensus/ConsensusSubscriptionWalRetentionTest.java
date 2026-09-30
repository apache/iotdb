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
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.Collections;
import java.util.function.LongSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusSubscriptionWalRetentionTest {

  private static final WriterId WRITER_ID = new WriterId("region", 7);

  @Test
  public void testProgressRegressionInvalidatesCoveredWalVersion() {
    final RegionProgress previous = progress(100L, 2L);

    assertTrue(ConsensusSubscriptionWalRetention.isProgressMonotonic(null, previous));
    assertTrue(ConsensusSubscriptionWalRetention.isProgressMonotonic(previous, previous));
    assertTrue(ConsensusSubscriptionWalRetention.isProgressMonotonic(previous, progress(100L, 3L)));
    assertTrue(ConsensusSubscriptionWalRetention.isProgressMonotonic(previous, progress(101L, 0L)));
    assertFalse(
        ConsensusSubscriptionWalRetention.isProgressMonotonic(previous, progress(100L, 1L)));
    assertFalse(ConsensusSubscriptionWalRetention.isProgressMonotonic(previous, progress(99L, 3L)));
    assertFalse(
        ConsensusSubscriptionWalRetention.isProgressMonotonic(
            previous, new RegionProgress(Collections.emptyMap())));
    assertFalse(ConsensusSubscriptionWalRetention.isProgressMonotonic(previous, null));
  }

  @Test
  public void testDetachedRetentionUsesSortedWalSnapshot() {
    final DataRegionId regionId = new DataRegionId(1);
    final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
    final WALNode walNode = mock(WALNode.class);
    final ConsensusSubscriptionCommitManager commitManager =
        mock(ConsensusSubscriptionCommitManager.class);
    final SubscriptionWalRetentionPolicy retentionPolicy =
        new SubscriptionWalRetentionPolicy(
            "topic",
            SubscriptionWalRetentionPolicy.UNBOUNDED,
            SubscriptionWalRetentionPolicy.UNBOUNDED);
    when(serverImpl.getConsensusReqReader()).thenReturn(walNode);
    when(walNode.getCurrentWALFileVersion()).thenReturn(7L);
    when(walNode.getSortedWalFilesSnapshot()).thenReturn(new File[0]);
    when(commitManager.getCommittedRegionProgress("group", "topic", regionId))
        .thenReturn(new RegionProgress(Collections.emptyMap()));

    ConsensusSubscriptionWalRetention.registerDetached(
        "group", "topic", regionId, serverImpl, retentionPolicy, commitManager);

    final ArgumentCaptor<LongSupplier> supplierCaptor = ArgumentCaptor.forClass(LongSupplier.class);
    verify(serverImpl)
        .registerDetachedSubscriptionRetention(
            eq(ConsensusSubscriptionWalRetention.generateRetentionId("group", "topic", regionId)),
            same(retentionPolicy),
            supplierCaptor.capture());
    assertEquals(7L, supplierCaptor.getValue().getAsLong());
    verify(walNode).getSortedWalFilesSnapshot();
  }

  private static RegionProgress progress(final long physicalTime, final long localSeq) {
    return new RegionProgress(
        Collections.singletonMap(WRITER_ID, new WriterProgress(physicalTime, localSeq)));
  }
}
