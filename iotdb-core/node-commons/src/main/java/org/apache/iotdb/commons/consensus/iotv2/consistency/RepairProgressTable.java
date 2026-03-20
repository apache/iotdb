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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory representation of the partition-scoped consistency check / repair progress for a single
 * consensus group.
 */
public class RepairProgressTable {

  public enum CheckState {
    PENDING,
    DIRTY,
    VERIFIED,
    MISMATCH,
    FAILED
  }

  public enum RepairState {
    IDLE,
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED
  }

  public enum SnapshotState {
    PENDING,
    BUILDING,
    READY,
    DIRTY,
    FAILED
  }

  public static class PartitionProgress {
    private final long partitionId;
    private volatile CheckState checkState;
    private volatile RepairState repairState;
    private volatile long lastCheckedAt;
    private volatile long lastSafeWatermark;
    private volatile long partitionMutationEpoch;
    private volatile long snapshotEpoch;
    private volatile SnapshotState snapshotState;
    private volatile long lastMismatchAt;
    private volatile String mismatchScopeRef;
    private volatile int mismatchLeafCount;
    private volatile String repairEpoch;
    private volatile String replicaObservationToken;
    private volatile String lastErrorCode;
    private volatile String lastErrorMessage;

    public PartitionProgress(long partitionId) {
      this.partitionId = partitionId;
      this.checkState = CheckState.PENDING;
      this.repairState = RepairState.IDLE;
      this.lastCheckedAt = 0L;
      this.lastSafeWatermark = Long.MIN_VALUE;
      this.partitionMutationEpoch = Long.MIN_VALUE;
      this.snapshotEpoch = Long.MIN_VALUE;
      this.snapshotState = SnapshotState.PENDING;
      this.lastMismatchAt = 0L;
      this.mismatchScopeRef = null;
      this.mismatchLeafCount = 0;
      this.repairEpoch = null;
      this.replicaObservationToken = null;
      this.lastErrorCode = null;
      this.lastErrorMessage = null;
    }

    public long getPartitionId() {
      return partitionId;
    }

    public CheckState getCheckState() {
      return checkState;
    }

    public RepairState getRepairState() {
      return repairState;
    }

    public long getLastCheckedAt() {
      return lastCheckedAt;
    }

    public long getLastSafeWatermark() {
      return lastSafeWatermark;
    }

    public long getPartitionMutationEpoch() {
      return partitionMutationEpoch;
    }

    public long getSnapshotEpoch() {
      return snapshotEpoch;
    }

    public SnapshotState getSnapshotState() {
      return snapshotState;
    }

    public long getLastMismatchAt() {
      return lastMismatchAt;
    }

    public String getMismatchScopeRef() {
      return mismatchScopeRef;
    }

    public int getMismatchLeafCount() {
      return mismatchLeafCount;
    }

    public String getRepairEpoch() {
      return repairEpoch;
    }

    public String getReplicaObservationToken() {
      return replicaObservationToken;
    }

    public String getLastErrorCode() {
      return lastErrorCode;
    }

    public String getLastErrorMessage() {
      return lastErrorMessage;
    }

    public boolean shouldCheck(
        long candidatePartitionMutationEpoch,
        long candidateSnapshotEpoch,
        SnapshotState candidateSnapshotState) {
      return shouldCheck(
          candidatePartitionMutationEpoch, candidateSnapshotEpoch, candidateSnapshotState, null);
    }

    public boolean shouldCheck(
        long candidatePartitionMutationEpoch,
        long candidateSnapshotEpoch,
        SnapshotState candidateSnapshotState,
        String candidateReplicaObservationToken) {
      return checkState != CheckState.VERIFIED
          || partitionMutationEpoch != candidatePartitionMutationEpoch
          || snapshotEpoch != candidateSnapshotEpoch
          || snapshotState != candidateSnapshotState
          || !Objects.equals(replicaObservationToken, candidateReplicaObservationToken);
    }

    public void markVerified(
        long checkedAt,
        long safeWatermark,
        long newPartitionMutationEpoch,
        long newSnapshotEpoch,
        SnapshotState newSnapshotState) {
      markVerified(
          checkedAt,
          safeWatermark,
          newPartitionMutationEpoch,
          newSnapshotEpoch,
          newSnapshotState,
          null);
    }

    public void markVerified(
        long checkedAt,
        long safeWatermark,
        long newPartitionMutationEpoch,
        long newSnapshotEpoch,
        SnapshotState newSnapshotState,
        String newReplicaObservationToken) {
      checkState = CheckState.VERIFIED;
      lastCheckedAt = checkedAt;
      lastSafeWatermark = safeWatermark;
      partitionMutationEpoch = newPartitionMutationEpoch;
      snapshotEpoch = newSnapshotEpoch;
      snapshotState = newSnapshotState;
      replicaObservationToken = newReplicaObservationToken;
      mismatchScopeRef = null;
      mismatchLeafCount = 0;
      clearError();
      if (repairState == RepairState.RUNNING
          || repairState == RepairState.PENDING
          || repairState == RepairState.FAILED) {
        repairState = RepairState.SUCCEEDED;
      }
    }

    public void markMismatch(
        long checkedAt,
        long safeWatermark,
        long newPartitionMutationEpoch,
        long newSnapshotEpoch,
        SnapshotState newSnapshotState,
        String newMismatchScopeRef,
        int newMismatchLeafCount,
        String newRepairEpoch) {
      markMismatch(
          checkedAt,
          safeWatermark,
          newPartitionMutationEpoch,
          newSnapshotEpoch,
          newSnapshotState,
          newMismatchScopeRef,
          newMismatchLeafCount,
          newRepairEpoch,
          null);
    }

    public void markMismatch(
        long checkedAt,
        long safeWatermark,
        long newPartitionMutationEpoch,
        long newSnapshotEpoch,
        SnapshotState newSnapshotState,
        String newMismatchScopeRef,
        int newMismatchLeafCount,
        String newRepairEpoch,
        String newReplicaObservationToken) {
      checkState = CheckState.MISMATCH;
      repairState = RepairState.PENDING;
      lastCheckedAt = checkedAt;
      lastSafeWatermark = safeWatermark;
      partitionMutationEpoch = newPartitionMutationEpoch;
      snapshotEpoch = newSnapshotEpoch;
      snapshotState = newSnapshotState;
      lastMismatchAt = checkedAt;
      mismatchScopeRef = newMismatchScopeRef;
      mismatchLeafCount = newMismatchLeafCount;
      repairEpoch = newRepairEpoch;
      replicaObservationToken = newReplicaObservationToken;
      clearError();
    }

    public void markCheckFailed(
        long checkedAt,
        long safeWatermark,
        long newPartitionMutationEpoch,
        long newSnapshotEpoch,
        SnapshotState newSnapshotState,
        String errorCode,
        String errorMessage) {
      markCheckFailed(
          checkedAt,
          safeWatermark,
          newPartitionMutationEpoch,
          newSnapshotEpoch,
          newSnapshotState,
          errorCode,
          errorMessage,
          null);
    }

    public void markCheckFailed(
        long checkedAt,
        long safeWatermark,
        long newPartitionMutationEpoch,
        long newSnapshotEpoch,
        SnapshotState newSnapshotState,
        String errorCode,
        String errorMessage,
        String newReplicaObservationToken) {
      checkState = CheckState.FAILED;
      lastCheckedAt = checkedAt;
      lastSafeWatermark = safeWatermark;
      partitionMutationEpoch = newPartitionMutationEpoch;
      snapshotEpoch = newSnapshotEpoch;
      snapshotState = newSnapshotState;
      replicaObservationToken = newReplicaObservationToken;
      lastErrorCode = errorCode;
      lastErrorMessage = errorMessage;
    }

    public void markDirty() {
      checkState = CheckState.DIRTY;
      snapshotState = SnapshotState.DIRTY;
      mismatchScopeRef = null;
      mismatchLeafCount = 0;
      if (repairState == RepairState.SUCCEEDED) {
        repairState = RepairState.IDLE;
      }
    }

    public void markRepairRunning(String newRepairEpoch) {
      repairState = RepairState.RUNNING;
      repairEpoch = newRepairEpoch;
      clearError();
    }

    public void markRepairSucceeded(
        long checkedAt,
        long safeWatermark,
        long newPartitionMutationEpoch,
        long newSnapshotEpoch,
        SnapshotState newSnapshotState,
        String newRepairEpoch) {
      markRepairSucceeded(
          checkedAt,
          safeWatermark,
          newPartitionMutationEpoch,
          newSnapshotEpoch,
          newSnapshotState,
          newRepairEpoch,
          null);
    }

    public void markRepairSucceeded(
        long checkedAt,
        long safeWatermark,
        long newPartitionMutationEpoch,
        long newSnapshotEpoch,
        SnapshotState newSnapshotState,
        String newRepairEpoch,
        String newReplicaObservationToken) {
      repairState = RepairState.SUCCEEDED;
      repairEpoch = newRepairEpoch;
      markVerified(
          checkedAt,
          safeWatermark,
          newPartitionMutationEpoch,
          newSnapshotEpoch,
          newSnapshotState,
          newReplicaObservationToken);
      repairState = RepairState.SUCCEEDED;
    }

    public void markRepairFailed(String newRepairEpoch, String errorCode, String errorMessage) {
      repairState = RepairState.FAILED;
      repairEpoch = newRepairEpoch;
      lastErrorCode = errorCode;
      lastErrorMessage = errorMessage;
    }

    public PartitionProgress copy() {
      PartitionProgress copy = new PartitionProgress(partitionId);
      copy.checkState = checkState;
      copy.repairState = repairState;
      copy.lastCheckedAt = lastCheckedAt;
      copy.lastSafeWatermark = lastSafeWatermark;
      copy.partitionMutationEpoch = partitionMutationEpoch;
      copy.snapshotEpoch = snapshotEpoch;
      copy.snapshotState = snapshotState;
      copy.lastMismatchAt = lastMismatchAt;
      copy.mismatchScopeRef = mismatchScopeRef;
      copy.mismatchLeafCount = mismatchLeafCount;
      copy.repairEpoch = repairEpoch;
      copy.replicaObservationToken = replicaObservationToken;
      copy.lastErrorCode = lastErrorCode;
      copy.lastErrorMessage = lastErrorMessage;
      return copy;
    }

    private void clearError() {
      lastErrorCode = null;
      lastErrorMessage = null;
    }

    private void serialize(OutputStream outputStream) throws IOException {
      ReadWriteIOUtils.write(partitionId, outputStream);
      ReadWriteIOUtils.write(checkState.ordinal(), outputStream);
      ReadWriteIOUtils.write(repairState.ordinal(), outputStream);
      ReadWriteIOUtils.write(lastCheckedAt, outputStream);
      ReadWriteIOUtils.write(lastSafeWatermark, outputStream);
      ReadWriteIOUtils.write(partitionMutationEpoch, outputStream);
      ReadWriteIOUtils.write(snapshotEpoch, outputStream);
      ReadWriteIOUtils.write(snapshotState.ordinal(), outputStream);
      ReadWriteIOUtils.write(lastMismatchAt, outputStream);
      ReadWriteIOUtils.write(mismatchScopeRef, outputStream);
      ReadWriteIOUtils.write(mismatchLeafCount, outputStream);
      ReadWriteIOUtils.write(repairEpoch, outputStream);
      ReadWriteIOUtils.write(replicaObservationToken, outputStream);
      ReadWriteIOUtils.write(lastErrorCode, outputStream);
      ReadWriteIOUtils.write(lastErrorMessage, outputStream);
    }

    private static PartitionProgress deserialize(InputStream inputStream) throws IOException {
      PartitionProgress progress = new PartitionProgress(ReadWriteIOUtils.readLong(inputStream));
      progress.checkState = CheckState.values()[ReadWriteIOUtils.readInt(inputStream)];
      progress.repairState = RepairState.values()[ReadWriteIOUtils.readInt(inputStream)];
      progress.lastCheckedAt = ReadWriteIOUtils.readLong(inputStream);
      progress.lastSafeWatermark = ReadWriteIOUtils.readLong(inputStream);
      progress.partitionMutationEpoch = ReadWriteIOUtils.readLong(inputStream);
      progress.snapshotEpoch = ReadWriteIOUtils.readLong(inputStream);
      progress.snapshotState = SnapshotState.values()[ReadWriteIOUtils.readInt(inputStream)];
      progress.lastMismatchAt = ReadWriteIOUtils.readLong(inputStream);
      progress.mismatchScopeRef = ReadWriteIOUtils.readString(inputStream);
      progress.mismatchLeafCount = ReadWriteIOUtils.readInt(inputStream);
      progress.repairEpoch = ReadWriteIOUtils.readString(inputStream);
      progress.replicaObservationToken = ReadWriteIOUtils.readString(inputStream);
      progress.lastErrorCode = ReadWriteIOUtils.readString(inputStream);
      progress.lastErrorMessage = ReadWriteIOUtils.readString(inputStream);
      return progress;
    }

    private static PartitionProgress deserialize(ByteBuffer byteBuffer) {
      PartitionProgress progress = new PartitionProgress(ReadWriteIOUtils.readLong(byteBuffer));
      progress.checkState = CheckState.values()[ReadWriteIOUtils.readInt(byteBuffer)];
      progress.repairState = RepairState.values()[ReadWriteIOUtils.readInt(byteBuffer)];
      progress.lastCheckedAt = ReadWriteIOUtils.readLong(byteBuffer);
      progress.lastSafeWatermark = ReadWriteIOUtils.readLong(byteBuffer);
      progress.partitionMutationEpoch = ReadWriteIOUtils.readLong(byteBuffer);
      progress.snapshotEpoch = ReadWriteIOUtils.readLong(byteBuffer);
      progress.snapshotState = SnapshotState.values()[ReadWriteIOUtils.readInt(byteBuffer)];
      progress.lastMismatchAt = ReadWriteIOUtils.readLong(byteBuffer);
      progress.mismatchScopeRef = ReadWriteIOUtils.readString(byteBuffer);
      progress.mismatchLeafCount = ReadWriteIOUtils.readInt(byteBuffer);
      progress.repairEpoch = ReadWriteIOUtils.readString(byteBuffer);
      progress.replicaObservationToken = ReadWriteIOUtils.readString(byteBuffer);
      progress.lastErrorCode = ReadWriteIOUtils.readString(byteBuffer);
      progress.lastErrorMessage = ReadWriteIOUtils.readString(byteBuffer);
      return progress;
    }

    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof PartitionProgress)) {
        return false;
      }
      PartitionProgress that = (PartitionProgress) object;
      return partitionId == that.partitionId
          && lastCheckedAt == that.lastCheckedAt
          && lastSafeWatermark == that.lastSafeWatermark
          && partitionMutationEpoch == that.partitionMutationEpoch
          && snapshotEpoch == that.snapshotEpoch
          && lastMismatchAt == that.lastMismatchAt
          && mismatchLeafCount == that.mismatchLeafCount
          && checkState == that.checkState
          && repairState == that.repairState
          && snapshotState == that.snapshotState
          && Objects.equals(mismatchScopeRef, that.mismatchScopeRef)
          && Objects.equals(repairEpoch, that.repairEpoch)
          && Objects.equals(replicaObservationToken, that.replicaObservationToken)
          && Objects.equals(lastErrorCode, that.lastErrorCode)
          && Objects.equals(lastErrorMessage, that.lastErrorMessage);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          partitionId,
          checkState,
          repairState,
          lastCheckedAt,
          lastSafeWatermark,
          partitionMutationEpoch,
          snapshotEpoch,
          snapshotState,
          lastMismatchAt,
          mismatchScopeRef,
          mismatchLeafCount,
          repairEpoch,
          replicaObservationToken,
          lastErrorCode,
          lastErrorMessage);
    }
  }

  private final String consensusGroupId;
  private final ConcurrentHashMap<Long, PartitionProgress> partitionProgress;

  public RepairProgressTable(String consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
    this.partitionProgress = new ConcurrentHashMap<>();
  }

  public String getConsensusGroupId() {
    return consensusGroupId;
  }

  public PartitionProgress getOrCreatePartition(long partitionId) {
    return partitionProgress.computeIfAbsent(partitionId, PartitionProgress::new);
  }

  public PartitionProgress getPartition(long partitionId) {
    return partitionProgress.get(partitionId);
  }

  public void markVerified(
      long partitionId,
      long checkedAt,
      long safeWatermark,
      long partitionMutationEpoch,
      long snapshotEpoch,
      SnapshotState snapshotState) {
    markVerified(
        partitionId,
        checkedAt,
        safeWatermark,
        partitionMutationEpoch,
        snapshotEpoch,
        snapshotState,
        null);
  }

  public void markVerified(
      long partitionId,
      long checkedAt,
      long safeWatermark,
      long partitionMutationEpoch,
      long snapshotEpoch,
      SnapshotState snapshotState,
      String replicaObservationToken) {
    getOrCreatePartition(partitionId)
        .markVerified(
            checkedAt,
            safeWatermark,
            partitionMutationEpoch,
            snapshotEpoch,
            snapshotState,
            replicaObservationToken);
  }

  public void markMismatch(
      long partitionId,
      long checkedAt,
      long safeWatermark,
      long partitionMutationEpoch,
      long snapshotEpoch,
      SnapshotState snapshotState,
      String mismatchScopeRef,
      int mismatchLeafCount,
      String repairEpoch) {
    markMismatch(
        partitionId,
        checkedAt,
        safeWatermark,
        partitionMutationEpoch,
        snapshotEpoch,
        snapshotState,
        mismatchScopeRef,
        mismatchLeafCount,
        repairEpoch,
        null);
  }

  public void markMismatch(
      long partitionId,
      long checkedAt,
      long safeWatermark,
      long partitionMutationEpoch,
      long snapshotEpoch,
      SnapshotState snapshotState,
      String mismatchScopeRef,
      int mismatchLeafCount,
      String repairEpoch,
      String replicaObservationToken) {
    getOrCreatePartition(partitionId)
        .markMismatch(
            checkedAt,
            safeWatermark,
            partitionMutationEpoch,
            snapshotEpoch,
            snapshotState,
            mismatchScopeRef,
            mismatchLeafCount,
            repairEpoch,
            replicaObservationToken);
  }

  public void markCheckFailed(
      long partitionId,
      long checkedAt,
      long safeWatermark,
      long partitionMutationEpoch,
      long snapshotEpoch,
      SnapshotState snapshotState,
      String errorCode,
      String errorMessage) {
    markCheckFailed(
        partitionId,
        checkedAt,
        safeWatermark,
        partitionMutationEpoch,
        snapshotEpoch,
        snapshotState,
        errorCode,
        errorMessage,
        null);
  }

  public void markCheckFailed(
      long partitionId,
      long checkedAt,
      long safeWatermark,
      long partitionMutationEpoch,
      long snapshotEpoch,
      SnapshotState snapshotState,
      String errorCode,
      String errorMessage,
      String replicaObservationToken) {
    getOrCreatePartition(partitionId)
        .markCheckFailed(
            checkedAt,
            safeWatermark,
            partitionMutationEpoch,
            snapshotEpoch,
            snapshotState,
            errorCode,
            errorMessage,
            replicaObservationToken);
  }

  public void markDirty(long partitionId) {
    getOrCreatePartition(partitionId).markDirty();
  }

  public void markRepairRunning(long partitionId, String repairEpoch) {
    getOrCreatePartition(partitionId).markRepairRunning(repairEpoch);
  }

  public void markRepairSucceeded(
      long partitionId,
      long checkedAt,
      long safeWatermark,
      long partitionMutationEpoch,
      long snapshotEpoch,
      SnapshotState snapshotState,
      String repairEpoch) {
    markRepairSucceeded(
        partitionId,
        checkedAt,
        safeWatermark,
        partitionMutationEpoch,
        snapshotEpoch,
        snapshotState,
        repairEpoch,
        null);
  }

  public void markRepairSucceeded(
      long partitionId,
      long checkedAt,
      long safeWatermark,
      long partitionMutationEpoch,
      long snapshotEpoch,
      SnapshotState snapshotState,
      String repairEpoch,
      String replicaObservationToken) {
    getOrCreatePartition(partitionId)
        .markRepairSucceeded(
            checkedAt,
            safeWatermark,
            partitionMutationEpoch,
            snapshotEpoch,
            snapshotState,
            repairEpoch,
            replicaObservationToken);
  }

  public void markRepairFailed(
      long partitionId, String repairEpoch, String errorCode, String errorMessage) {
    getOrCreatePartition(partitionId).markRepairFailed(repairEpoch, errorCode, errorMessage);
  }

  public List<PartitionProgress> getAllPartitions() {
    List<PartitionProgress> result = new ArrayList<>();
    for (PartitionProgress progress : partitionProgress.values()) {
      result.add(progress.copy());
    }
    result.sort((left, right) -> Long.compare(left.getPartitionId(), right.getPartitionId()));
    return Collections.unmodifiableList(result);
  }

  public Map<Long, PartitionProgress> getAllPartitionProgress() {
    return Collections.unmodifiableMap(partitionProgress);
  }

  public RepairProgressTable copy() {
    RepairProgressTable copy = new RepairProgressTable(consensusGroupId);
    partitionProgress.forEach(
        (partitionId, progress) -> copy.partitionProgress.put(partitionId, progress.copy()));
    return copy;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(consensusGroupId, outputStream);
    ReadWriteIOUtils.write(partitionProgress.size(), outputStream);
    for (PartitionProgress progress : getAllPartitions()) {
      progress.serialize(outputStream);
    }
  }

  public static RepairProgressTable deserialize(InputStream inputStream) throws IOException {
    RepairProgressTable table = new RepairProgressTable(ReadWriteIOUtils.readString(inputStream));
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      PartitionProgress progress = PartitionProgress.deserialize(inputStream);
      table.partitionProgress.put(progress.getPartitionId(), progress);
    }
    return table;
  }

  public static RepairProgressTable deserialize(ByteBuffer byteBuffer) {
    RepairProgressTable table = new RepairProgressTable(ReadWriteIOUtils.readString(byteBuffer));
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      PartitionProgress progress = PartitionProgress.deserialize(byteBuffer);
      table.partitionProgress.put(progress.getPartitionId(), progress);
    }
    return table;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof RepairProgressTable)) {
      return false;
    }
    RepairProgressTable that = (RepairProgressTable) object;
    return Objects.equals(consensusGroupId, that.consensusGroupId)
        && Objects.equals(getAllPartitions(), that.getAllPartitions());
  }

  @Override
  public int hashCode() {
    return Objects.hash(consensusGroupId, getAllPartitions());
  }
}
