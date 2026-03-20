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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class ConsistencyPartitionSelector {

  private ConsistencyPartitionSelector() {}

  static List<Long> selectCandidatePartitions(
      Map<Long, TPartitionConsistencyEligibility> leaderEligibilityByPartition,
      Set<Long> requestedPartitions,
      boolean repairMode,
      String requestedRepairEpoch,
      RepairProgressTable repairProgressTable) {
    return selectCandidatePartitions(
        leaderEligibilityByPartition,
        requestedPartitions,
        repairMode,
        requestedRepairEpoch,
        repairProgressTable,
        Collections.emptyMap());
  }

  static List<Long> selectCandidatePartitions(
      Map<Long, TPartitionConsistencyEligibility> leaderEligibilityByPartition,
      Set<Long> requestedPartitions,
      boolean repairMode,
      String requestedRepairEpoch,
      RepairProgressTable repairProgressTable,
      Map<Long, String> replicaObservationTokens) {
    List<Long> eligiblePartitions =
        leaderEligibilityByPartition.values().stream()
            .map(TPartitionConsistencyEligibility::getTimePartitionId)
            .filter(
                partitionId ->
                    requestedPartitions == null
                        || requestedPartitions.isEmpty()
                        || requestedPartitions.contains(partitionId))
            .sorted()
            .collect(Collectors.toList());
    if (eligiblePartitions.isEmpty()) {
      return Collections.emptyList();
    }

    if (repairMode && (requestedPartitions == null || requestedPartitions.isEmpty())) {
      List<Long> mismatchPartitions =
          eligiblePartitions.stream()
              .filter(
                  partitionId -> {
                    RepairProgressTable.PartitionProgress progress =
                        repairProgressTable.getPartition(partitionId);
                    return progress != null
                        && progress.getCheckState() == RepairProgressTable.CheckState.MISMATCH
                        && (requestedRepairEpoch == null
                            || requestedRepairEpoch.equals(progress.getRepairEpoch()));
                  })
              .collect(Collectors.toList());
      if (!mismatchPartitions.isEmpty()) {
        return mismatchPartitions;
      }
    }

    return eligiblePartitions.stream()
        .filter(
            partitionId ->
                shouldInspectPartition(
                    leaderEligibilityByPartition.get(partitionId),
                    repairProgressTable.getPartition(partitionId),
                    replicaObservationTokens.get(partitionId)))
        .collect(Collectors.toList());
  }

  static boolean shouldInspectPartition(
      TPartitionConsistencyEligibility eligibility,
      RepairProgressTable.PartitionProgress progress) {
    return shouldInspectPartition(eligibility, progress, null);
  }

  static boolean shouldInspectPartition(
      TPartitionConsistencyEligibility eligibility,
      RepairProgressTable.PartitionProgress progress,
      String replicaObservationToken) {
    if (eligibility == null) {
      return false;
    }
    if (progress == null) {
      return true;
    }
    RepairProgressTable.SnapshotState snapshotState =
        RepairProgressTable.SnapshotState.valueOf(eligibility.getSnapshotState());
    if (snapshotState != RepairProgressTable.SnapshotState.READY) {
      return true;
    }
    switch (progress.getCheckState()) {
      case VERIFIED:
        return progress.shouldCheck(
            eligibility.getPartitionMutationEpoch(),
            eligibility.getSnapshotEpoch(),
            snapshotState,
            replicaObservationToken);
      case PENDING:
      case DIRTY:
      case MISMATCH:
      case FAILED:
      default:
        return true;
    }
  }
}
