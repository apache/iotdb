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

import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DiffEntry;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.RowRefIndex;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileContent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Orchestrates the adaptive repair execution phase, combining direct TsFile transfers (for dense
 * diffs or small TsFiles) with point-level streaming (for sparse diffs in large TsFiles).
 *
 * <p>The executor follows these steps:
 *
 * <ol>
 *   <li>Attribute decoded diffs to source TsFiles via DiffAttribution
 *   <li>For each TsFile, use RepairCostModel to select the optimal strategy
 *   <li>Execute TsFile transfers (zero packaging cost, idempotent loading)
 *   <li>Execute point streaming via RepairSession with atomic promote
 * </ol>
 */
public class AdaptiveRepairExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveRepairExecutor.class);

  private final RepairCostModel costModel;
  private final DiffAttribution attribution;

  /** Callback interface for executing actual repair operations. */
  public interface RepairOperationCallback {
    /** Transfer an existing TsFile from Leader to Follower. */
    void transferTsFile(String tsFilePath) throws Exception;

    /** Get the size of a TsFile. */
    long getTsFileSize(String tsFilePath);

    /** Get the total point count from a TsFile's .merkle content. */
    int getTotalPointCount(String tsFilePath);

    /** Build a concrete repair record for point streaming. */
    default RepairRecord buildRepairRecord(DiffEntry diffEntry, RowRefIndex rowRefIndex) {
      return null;
    }
  }

  public AdaptiveRepairExecutor() {
    this(new RepairCostModel(), new DiffAttribution());
  }

  public AdaptiveRepairExecutor(RepairCostModel costModel, DiffAttribution attribution) {
    this.costModel = costModel;
    this.attribution = attribution;
  }

  /**
   * Build repair plans for all attributed TsFiles.
   *
   * @param decodedDiffs the decoded diff entries
   * @param rowRefIndex the composite key resolver
   * @param leaderMerkleFiles Leader's .merkle files for the checked range
   * @param callback for querying TsFile metadata
   * @return map of TsFile path to RepairPlan
   */
  public Map<String, RepairPlan> buildRepairPlans(
      List<DiffEntry> decodedDiffs,
      RowRefIndex rowRefIndex,
      List<MerkleFileContent> leaderMerkleFiles,
      RepairOperationCallback callback) {

    // Step 1: Attribute diffs to source TsFiles
    Map<String, List<DiffEntry>> attributedDiffs =
        attribution.attributeToSourceTsFiles(decodedDiffs, rowRefIndex, leaderMerkleFiles);

    // Step 2: Select strategy per TsFile
    Map<String, RepairPlan> plans = new HashMap<>();
    for (Map.Entry<String, List<DiffEntry>> entry : attributedDiffs.entrySet()) {
      String tsFilePath = entry.getKey();
      List<DiffEntry> diffs = entry.getValue();

      long tsFileSize = callback.getTsFileSize(tsFilePath);
      int totalPoints = callback.getTotalPointCount(tsFilePath);

      RepairStrategy strategy = costModel.selectStrategy(tsFileSize, totalPoints, diffs.size());

      if (strategy == RepairStrategy.DIRECT_TSFILE_TRANSFER) {
        plans.put(tsFilePath, RepairPlan.directTransfer(tsFilePath, tsFileSize));
      } else {
        plans.put(tsFilePath, RepairPlan.pointStreaming(tsFilePath, tsFileSize, diffs));
      }
    }

    return plans;
  }

  /**
   * Execute all repair plans.
   *
   * @param plans the repair plans (per TsFile)
   * @param rowRefIndex for resolving composite keys during point streaming
   * @param session the RepairSession for staging point-level repairs
   * @param callback for executing actual transfer operations
   * @return true if all operations succeeded
   */
  public boolean executeRepairPlans(
      Map<String, RepairPlan> plans,
      RowRefIndex rowRefIndex,
      RepairSession session,
      RepairOperationCallback callback) {
    boolean allSucceeded = true;

    // Phase 1: Direct TsFile transfers (zero packaging cost)
    List<RepairPlan> transferPlans = new ArrayList<>();
    List<RepairPlan> streamingPlans = new ArrayList<>();

    for (RepairPlan plan : plans.values()) {
      if (plan.getStrategy() == RepairStrategy.DIRECT_TSFILE_TRANSFER) {
        transferPlans.add(plan);
      } else {
        streamingPlans.add(plan);
      }
    }

    LOGGER.info(
        "Executing repair: {} TsFile transfers, {} point streaming plans",
        transferPlans.size(),
        streamingPlans.size());

    for (RepairPlan plan : transferPlans) {
      try {
        callback.transferTsFile(plan.getTsFilePath());
        LOGGER.debug("Transferred TsFile: {}", plan.getTsFilePath());
      } catch (Exception e) {
        LOGGER.error("Failed to transfer TsFile {}: {}", plan.getTsFilePath(), e.getMessage(), e);
        allSucceeded = false;
      }
    }

    // Phase 2: Point-level streaming with atomic promote
    if (!streamingPlans.isEmpty()) {
      for (RepairPlan plan : streamingPlans) {
        for (DiffEntry diff : plan.getDiffs()) {
          RepairRecord record = callback.buildRepairRecord(diff, rowRefIndex);
          if (record != null) {
            session.stage(record);
            LOGGER.debug("Staged diff entry for streaming: {}", diff);
          }
        }
      }

      if (session.getStagedCount() > 0) {
        boolean promoted = session.promoteAtomically();
        if (!promoted) {
          LOGGER.error("Failed to atomically promote repair session");
          allSucceeded = false;
        }
      }
    }

    return allSucceeded;
  }

  /**
   * Build repair plans for full-range fallback (when IBF decode fails). Transfers all overlapping
   * Leader TsFiles for the affected range.
   *
   * @param overlappingTsFilePaths paths of Leader TsFiles overlapping the affected range
   * @param callback for querying TsFile sizes
   * @return list of direct transfer plans
   */
  public List<RepairPlan> buildFullRangeFallbackPlans(
      List<String> overlappingTsFilePaths, RepairOperationCallback callback) {
    List<RepairPlan> plans = new ArrayList<>();
    for (String tsFilePath : overlappingTsFilePaths) {
      long tsFileSize = callback.getTsFileSize(tsFilePath);
      plans.add(RepairPlan.directTransfer(tsFilePath, tsFileSize));
    }
    return plans;
  }
}
