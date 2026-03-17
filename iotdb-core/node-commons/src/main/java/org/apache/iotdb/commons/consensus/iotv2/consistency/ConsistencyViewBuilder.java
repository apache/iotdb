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

import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileCache;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds a consistency view for a partition using the snapshot-then-release pattern: acquires a
 * short read lock to snapshot the TsFile list, then releases the lock before performing heavy
 * .merkle file I/O. Includes staleness detection via version counter.
 */
public class ConsistencyViewBuilder {

  /**
   * Interface to decouple from TsFileManager; allows the builder to be used without direct
   * dependency on the storage engine module.
   */
  public interface TsFileListProvider {

    /** Acquire a read lock on the TsFile resource list. */
    void readLock();

    /** Release the read lock. */
    void readUnlock();

    /**
     * Get all TsFile paths for the given partition.
     *
     * @param partitionId the time partition to query
     * @return list of TsFile paths (both sequence and unsequence)
     */
    List<TsFileSnapshot> getTsFileSnapshots(long partitionId);
  }

  /** Lightweight snapshot of a TsFile reference for off-lock processing. */
  public static class TsFileSnapshot {
    private final String tsFilePath;
    private final boolean deleted;

    public TsFileSnapshot(String tsFilePath, boolean deleted) {
      this.tsFilePath = tsFilePath;
      this.deleted = deleted;
    }

    public String getTsFilePath() {
      return tsFilePath;
    }

    public boolean isDeleted() {
      return deleted;
    }
  }

  private final MerkleFileCache merkleFileCache;

  public ConsistencyViewBuilder(MerkleFileCache merkleFileCache) {
    this.merkleFileCache = merkleFileCache;
  }

  /**
   * Build a consistency view for a specific partition.
   *
   * @param provider the TsFile list provider (wraps TsFileManager)
   * @param partitionNode the partition's Merkle node for staleness checks
   * @return list of loaded MerkleFileContent for all active TsFiles in the partition
   * @throws StaleSnapshotException if the partition was modified while building the view
   * @throws IOException on .merkle file read failure
   */
  public List<MerkleFileContent> buildView(
      TsFileListProvider provider, TimePartitionMerkleNode partitionNode)
      throws IOException, StaleSnapshotException {
    long expectedVersion = partitionNode.getVersion();

    // Step 1: Short read lock -- snapshot the TsFile list (microseconds)
    List<TsFileSnapshot> snapshot;
    provider.readLock();
    try {
      snapshot = new ArrayList<>(provider.getTsFileSnapshots(partitionNode.getPartitionId()));
    } finally {
      provider.readUnlock();
    }

    // Step 2: Off-lock -- heavy I/O to load .merkle files (milliseconds-seconds)
    List<MerkleFileContent> contents = new ArrayList<>();
    for (TsFileSnapshot tsFile : snapshot) {
      if (tsFile.isDeleted()) {
        continue;
      }
      try {
        MerkleFileContent content = merkleFileCache.get(tsFile.getTsFilePath());
        contents.add(content);
      } catch (IOException e) {
        // .merkle file may have been cleaned up by compaction; skip this file
        if (!tsFile.isDeleted()) {
          throw e;
        }
      }
    }

    // Step 3: Staleness check -- verify snapshot is still valid
    long currentVersion = partitionNode.getVersion();
    if (currentVersion != expectedVersion) {
      throw new StaleSnapshotException(
          String.format(
              "Partition %d modified during view build (expected version=%d, current=%d), retry",
              partitionNode.getPartitionId(), expectedVersion, currentVersion));
    }

    return contents;
  }
}
