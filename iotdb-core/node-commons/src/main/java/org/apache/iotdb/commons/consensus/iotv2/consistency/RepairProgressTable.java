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

package org.apache.iotdb.commons.consensus.iotv2.consistency;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory representation of the partition-level repair progress tracking for a single consensus
 * group. This serves as the runtime cache backed by the __system.repair_progress system table.
 *
 * <p>Schema:
 *
 * <ul>
 *   <li>Region-level row: (consensus_group_id, global_repaired_watermark, last_repair_time,
 *       repair_status)
 *   <li>Partition-level rows: (consensus_group_id, partition_id, repaired_to, status,
 *       last_failure_reason)
 * </ul>
 */
public class RepairProgressTable {

  /** Status of repair for a single partition. */
  public enum PartitionRepairStatus {
    PENDING,
    VERIFIED,
    FAILED,
    DIRTY
  }

  /** Status of the overall repair cycle for a region. */
  public enum RegionRepairStatus {
    IDLE,
    RUNNING,
    FAILED
  }

  /** Progress entry for a single partition. */
  public static class PartitionProgress {
    private final long partitionId;
    private volatile long repairedTo;
    private volatile PartitionRepairStatus status;
    private volatile String lastFailureReason;

    public PartitionProgress(long partitionId) {
      this.partitionId = partitionId;
      this.repairedTo = 0L;
      this.status = PartitionRepairStatus.PENDING;
      this.lastFailureReason = null;
    }

    public long getPartitionId() {
      return partitionId;
    }

    public long getRepairedTo() {
      return repairedTo;
    }

    public void setRepairedTo(long repairedTo) {
      this.repairedTo = repairedTo;
    }

    public PartitionRepairStatus getStatus() {
      return status;
    }

    public void setStatus(PartitionRepairStatus status) {
      this.status = status;
    }

    public String getLastFailureReason() {
      return lastFailureReason;
    }

    public void markVerified(long newRepairedTo) {
      this.repairedTo = newRepairedTo;
      this.status = PartitionRepairStatus.VERIFIED;
      this.lastFailureReason = null;
    }

    public void markFailed(String reason) {
      this.status = PartitionRepairStatus.FAILED;
      this.lastFailureReason = reason;
    }

    public void markDirty() {
      this.status = PartitionRepairStatus.DIRTY;
    }
  }

  private final String consensusGroupId;
  private volatile long globalRepairedWatermark;
  private volatile long lastRepairTime;
  private volatile RegionRepairStatus regionStatus;
  private final ConcurrentHashMap<Long, PartitionProgress> partitionProgress;

  public RepairProgressTable(String consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
    this.globalRepairedWatermark = 0L;
    this.lastRepairTime = 0L;
    this.regionStatus = RegionRepairStatus.IDLE;
    this.partitionProgress = new ConcurrentHashMap<>();
  }

  /** Get or create progress entry for a partition. */
  public PartitionProgress getOrCreatePartition(long partitionId) {
    return partitionProgress.computeIfAbsent(partitionId, PartitionProgress::new);
  }

  public PartitionProgress getPartition(long partitionId) {
    return partitionProgress.get(partitionId);
  }

  /**
   * Commit a partition as verified with a new repaired_to watermark.
   *
   * @param partitionId the partition to commit
   * @param repairedTo the new watermark for this partition
   */
  public void commitPartition(long partitionId, long repairedTo) {
    PartitionProgress progress = getOrCreatePartition(partitionId);
    progress.markVerified(repairedTo);
  }

  /**
   * Mark a partition as failed.
   *
   * @param partitionId the partition that failed
   * @param reason the failure reason
   */
  public void failPartition(long partitionId, String reason) {
    PartitionProgress progress = getOrCreatePartition(partitionId);
    progress.markFailed(reason);
  }

  /**
   * Mark a partition as dirty (modified after verification).
   *
   * @param partitionId the partition to mark dirty
   */
  public void dirtyPartition(long partitionId) {
    PartitionProgress progress = getOrCreatePartition(partitionId);
    progress.markDirty();
  }

  /**
   * Advance the global watermark. Rule: global_repaired_watermark = MIN(p.repaired_to for ALL
   * partitions p in effective range). Only advances when every partition has been successfully
   * verified.
   *
   * @return the new global watermark
   */
  public long advanceGlobalWatermark() {
    long minRepairedTo = Long.MAX_VALUE;
    boolean hasPartitions = false;

    for (PartitionProgress progress : partitionProgress.values()) {
      if (progress.getStatus() == PartitionRepairStatus.VERIFIED) {
        hasPartitions = true;
        minRepairedTo = Math.min(minRepairedTo, progress.getRepairedTo());
      } else if (progress.getStatus() == PartitionRepairStatus.FAILED
          || progress.getStatus() == PartitionRepairStatus.DIRTY) {
        // Failed/dirty partitions block watermark advancement
        hasPartitions = true;
        minRepairedTo = Math.min(minRepairedTo, progress.getRepairedTo());
      }
    }

    if (hasPartitions && minRepairedTo != Long.MAX_VALUE) {
      this.globalRepairedWatermark = minRepairedTo;
    }
    this.lastRepairTime = System.currentTimeMillis();
    return this.globalRepairedWatermark;
  }

  /** Get all partitions that need repair (PENDING, FAILED, or DIRTY). */
  public Collection<PartitionProgress> getPartitionsNeedingRepair() {
    return partitionProgress.values().stream()
        .filter(
            p ->
                p.getStatus() == PartitionRepairStatus.PENDING
                    || p.getStatus() == PartitionRepairStatus.FAILED
                    || p.getStatus() == PartitionRepairStatus.DIRTY)
        .collect(java.util.stream.Collectors.toList());
  }

  public String getConsensusGroupId() {
    return consensusGroupId;
  }

  public long getGlobalRepairedWatermark() {
    return globalRepairedWatermark;
  }

  public long getLastRepairTime() {
    return lastRepairTime;
  }

  public RegionRepairStatus getRegionStatus() {
    return regionStatus;
  }

  public void setRegionStatus(RegionRepairStatus status) {
    this.regionStatus = status;
  }

  public Map<Long, PartitionProgress> getAllPartitionProgress() {
    return java.util.Collections.unmodifiableMap(partitionProgress);
  }
}
