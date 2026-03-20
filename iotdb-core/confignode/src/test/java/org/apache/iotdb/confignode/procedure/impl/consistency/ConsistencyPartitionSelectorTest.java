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

package org.apache.iotdb.confignode.procedure.impl.consistency;

import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable;
import org.apache.iotdb.mpp.rpc.thrift.TPartitionConsistencyEligibility;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConsistencyPartitionSelectorTest {

  @Test
  public void verifiedPartitionWithSameSnapshotShouldNotBeRequeued() {
    RepairProgressTable progressTable = new RepairProgressTable("DataRegion-1");
    progressTable.markVerified(1L, 100L, 200L, 300L, 300L, RepairProgressTable.SnapshotState.READY);

    Map<Long, TPartitionConsistencyEligibility> eligibilityByPartition = new LinkedHashMap<>();
    eligibilityByPartition.put(1L, eligibility(1L, 300L, 300L, "READY"));
    eligibilityByPartition.put(2L, eligibility(2L, 301L, 301L, "READY"));

    List<Long> selected =
        ConsistencyPartitionSelector.selectCandidatePartitions(
            eligibilityByPartition, Collections.emptySet(), false, null, progressTable);

    Assert.assertEquals(Collections.singletonList(2L), selected);
  }

  @Test
  public void dirtyAndBuildingPartitionsShouldBeRechecked() {
    RepairProgressTable progressTable = new RepairProgressTable("DataRegion-1");
    progressTable.markVerified(1L, 100L, 200L, 300L, 300L, RepairProgressTable.SnapshotState.READY);
    progressTable.markVerified(2L, 100L, 200L, 400L, 400L, RepairProgressTable.SnapshotState.READY);

    Map<Long, TPartitionConsistencyEligibility> eligibilityByPartition = new LinkedHashMap<>();
    eligibilityByPartition.put(1L, eligibility(1L, 301L, 301L, "READY"));
    eligibilityByPartition.put(2L, eligibility(2L, 400L, 400L, "BUILDING"));

    List<Long> selected =
        ConsistencyPartitionSelector.selectCandidatePartitions(
            eligibilityByPartition, Collections.emptySet(), false, null, progressTable);

    Assert.assertEquals(Arrays.asList(1L, 2L), selected);
  }

  @Test
  public void verifiedPartitionShouldBeRequeuedWhenReplicaObservationChanges() {
    RepairProgressTable progressTable = new RepairProgressTable("DataRegion-1");
    progressTable.markVerified(
        1L,
        100L,
        200L,
        300L,
        300L,
        RepairProgressTable.SnapshotState.READY,
        "4:300:300:READY|3:300:300:READY");

    Map<Long, TPartitionConsistencyEligibility> eligibilityByPartition = new LinkedHashMap<>();
    eligibilityByPartition.put(1L, eligibility(1L, 300L, 300L, "READY"));

    Map<Long, String> replicaObservationTokens = new LinkedHashMap<>();
    replicaObservationTokens.put(1L, "4:300:300:READY|3:0:0:READY");

    List<Long> selected =
        ConsistencyPartitionSelector.selectCandidatePartitions(
            eligibilityByPartition,
            Collections.emptySet(),
            false,
            null,
            progressTable,
            replicaObservationTokens);

    Assert.assertEquals(Collections.singletonList(1L), selected);
  }

  @Test
  public void repairModeShouldPreferLatestMismatchScope() {
    RepairProgressTable progressTable = new RepairProgressTable("DataRegion-1");
    progressTable.markMismatch(
        1L,
        100L,
        200L,
        300L,
        300L,
        RepairProgressTable.SnapshotState.READY,
        "LIVE@leaf:1:0",
        1,
        "leader:1:200:300:300");
    progressTable.markVerified(2L, 100L, 200L, 400L, 400L, RepairProgressTable.SnapshotState.READY);

    Map<Long, TPartitionConsistencyEligibility> eligibilityByPartition = new LinkedHashMap<>();
    eligibilityByPartition.put(1L, eligibility(1L, 300L, 300L, "READY"));
    eligibilityByPartition.put(2L, eligibility(2L, 401L, 401L, "READY"));

    List<Long> selected =
        ConsistencyPartitionSelector.selectCandidatePartitions(
            eligibilityByPartition,
            Collections.emptySet(),
            true,
            "leader:1:200:300:300",
            progressTable);

    Assert.assertEquals(Collections.singletonList(1L), selected);
  }

  private static TPartitionConsistencyEligibility eligibility(
      long partitionId, long partitionMutationEpoch, long snapshotEpoch, String snapshotState) {
    return new TPartitionConsistencyEligibility(
        partitionId,
        partitionMutationEpoch,
        snapshotEpoch,
        snapshotState,
        partitionId,
        partitionId,
        partitionId,
        partitionId);
  }
}
