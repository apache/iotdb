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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * ProgressIndex-based conflict resolver for all repair decisions. Replaces the unsafe "Leader wins"
 * heuristic with causal ordering via ProgressIndex comparison. This is critical for preventing
 * deletion resurrection and handling leadership transfers correctly.
 *
 * <p>Diff Interpretation Matrix:
 *
 * <table>
 *   <tr><th>Diff Type</th><th>Leader State</th><th>Follower State</th><th>Resolution</th></tr>
 *   <tr><td>LEADER_HAS</td><td>Point exists, PI=X</td><td>Missing</td>
 *       <td>If no Follower deletion covers it: SEND_TO_FOLLOWER. If Follower deletion PI_del > X:
 *       DELETE_ON_LEADER</td></tr>
 *   <tr><td>FOLLOWER_HAS</td><td>Missing</td><td>Point exists, PI=Y</td>
 *       <td>If Leader deletion PI_del > Y: DELETE_ON_FOLLOWER. Otherwise: SEND_TO_LEADER</td></tr>
 *   <tr><td>VALUE_DIFF</td><td>PI=X, V=Vl</td><td>PI=Y, V=Vf</td>
 *       <td>Higher ProgressIndex wins and repairs the stale replica</td></tr>
 * </table>
 */
public class RepairConflictResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(RepairConflictResolver.class);

  private final List<ModEntrySummary> leaderDeletions;
  private final List<ModEntrySummary> followerDeletions;

  public RepairConflictResolver(
      List<ModEntrySummary> leaderDeletions, List<ModEntrySummary> followerDeletions) {
    this.leaderDeletions = leaderDeletions != null ? leaderDeletions : Collections.emptyList();
    this.followerDeletions =
        followerDeletions != null ? followerDeletions : Collections.emptyList();
  }

  /**
   * Resolve a LEADER_HAS diff: Leader has the point but Follower doesn't.
   *
   * @param loc the data point location
   * @param pointProgressIndex the ProgressIndex of the TsFile containing this point on the Leader
   * @return the repair action to take
   */
  public RepairAction resolveLeaderHas(DataPointLocator loc, long pointProgressIndex) {
    Optional<ModEntrySummary> followerDel = findCoveringDeletion(followerDeletions, loc);
    if (followerDel.isPresent() && followerDel.get().getProgressIndex() > pointProgressIndex) {
      // Follower's deletion is more recent than the Leader's write -- repair the stale Leader.
      LOGGER.debug(
          "Deleting stale Leader point for {}: Follower deletion PI={} > point PI={}",
          loc,
          followerDel.get().getProgressIndex(),
          pointProgressIndex);
      return RepairAction.DELETE_ON_LEADER;
    }
    return RepairAction.SEND_TO_FOLLOWER;
  }

  /**
   * Resolve a FOLLOWER_HAS diff: Follower has the point but Leader doesn't.
   *
   * @param loc the data point location
   * @param pointProgressIndex the ProgressIndex of the data on the Follower
   * @return the repair action to take
   */
  public RepairAction resolveFollowerHas(DataPointLocator loc, long pointProgressIndex) {
    Optional<ModEntrySummary> leaderDel = findCoveringDeletion(leaderDeletions, loc);
    if (leaderDel.isPresent() && leaderDel.get().getProgressIndex() > pointProgressIndex) {
      // Leader's deletion is more recent than the Follower's write -- delete on Follower
      return RepairAction.DELETE_ON_FOLLOWER;
    }

    // The point can legitimately originate from a previous leadership epoch on the Follower.
    return RepairAction.SEND_TO_LEADER;
  }

  /**
   * Resolve a VALUE_DIFF: both sides have the point but with different values.
   *
   * @param loc the data point location
   * @param leaderProgressIndex the Leader's ProgressIndex for this point
   * @param followerProgressIndex the Follower's ProgressIndex for this point
   * @return the repair action to take
   */
  public RepairAction resolveValueDiff(
      DataPointLocator loc, long leaderProgressIndex, long followerProgressIndex) {
    if (leaderProgressIndex > followerProgressIndex) {
      return RepairAction.SEND_TO_FOLLOWER;
    } else if (followerProgressIndex > leaderProgressIndex) {
      return RepairAction.SEND_TO_LEADER;
    } else {
      // Concurrent writes (same ProgressIndex) -- Leader wins as tiebreaker
      LOGGER.debug(
          "Concurrent writes for {} with equal PI={}, using Leader as tiebreaker",
          loc,
          leaderProgressIndex);
      return RepairAction.SEND_TO_FOLLOWER;
    }
  }

  /**
   * Find a deletion entry that covers the given data point location.
   *
   * @param deletions the list of deletion summaries to search
   * @param loc the data point to check
   * @return the covering deletion with the highest ProgressIndex, if any
   */
  static Optional<ModEntrySummary> findCoveringDeletion(
      List<ModEntrySummary> deletions, DataPointLocator loc) {
    ModEntrySummary bestMatch = null;
    for (ModEntrySummary del : deletions) {
      if (del.covers(loc.getDeviceId(), loc.getMeasurement(), loc.getTimestamp())) {
        if (bestMatch == null || del.getProgressIndex() > bestMatch.getProgressIndex()) {
          bestMatch = del;
        }
      }
    }
    return Optional.ofNullable(bestMatch);
  }
}
