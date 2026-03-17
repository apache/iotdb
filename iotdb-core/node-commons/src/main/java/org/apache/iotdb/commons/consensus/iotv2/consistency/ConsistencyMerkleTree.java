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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory incremental Merkle tree that holds Level 0 (region digest) and Level 1 (per-partition
 * digest) as resident data. Level 2+ (device/measurement/timeBucket) are loaded on-demand from
 * .merkle files. Supports dual-digest aggregation and lazy invalidation for deletions.
 */
public class ConsistencyMerkleTree {

  private volatile DualDigest regionDigest;
  private final TreeMap<Long, TimePartitionMerkleNode> partitions;
  private final ReadWriteLock treeLock;

  public ConsistencyMerkleTree() {
    this.regionDigest = DualDigest.ZERO;
    this.partitions = new TreeMap<>();
    this.treeLock = new ReentrantReadWriteLock();
  }

  /**
   * Get or create the node for a given partition. Lazily initializes partition nodes on first
   * encounter.
   */
  public TimePartitionMerkleNode getOrCreatePartitionNode(long partitionId) {
    treeLock.readLock().lock();
    try {
      TimePartitionMerkleNode node = partitions.get(partitionId);
      if (node != null) {
        return node;
      }
    } finally {
      treeLock.readLock().unlock();
    }

    treeLock.writeLock().lock();
    try {
      return partitions.computeIfAbsent(partitionId, TimePartitionMerkleNode::new);
    } finally {
      treeLock.writeLock().unlock();
    }
  }

  public TimePartitionMerkleNode getPartitionNode(long partitionId) {
    treeLock.readLock().lock();
    try {
      return partitions.get(partitionId);
    } finally {
      treeLock.readLock().unlock();
    }
  }

  /**
   * Called after a TsFile flush to incorporate the new file's hash into the appropriate partition's
   * dual-digest.
   */
  public void onTsFileFlushed(long partitionId, long fileRootHash) {
    onTsFileFlushed(partitionId, DualDigest.fromSingleHash(fileRootHash));
  }

  public void onTsFileFlushed(long partitionId, DualDigest fileDigest) {
    TimePartitionMerkleNode node = getOrCreatePartitionNode(partitionId);
    node.addDigest(fileDigest);
    recomputeRegionDigest();
  }

  /**
   * Called during compaction when source TsFiles are removed and a target TsFile is created.
   *
   * @param partitionId the partition being compacted
   * @param sourceFileRootHashes root hashes of the source TsFiles being removed
   * @param targetFileRootHash root hash of the newly created target TsFile
   */
  public void onCompaction(
      long partitionId, List<Long> sourceFileRootHashes, long targetFileRootHash) {
    onCompaction(
        partitionId,
        sourceFileRootHashes.stream().map(DualDigest::fromSingleHash).collect(java.util.stream.Collectors.toList()),
        Collections.singletonList(DualDigest.fromSingleHash(targetFileRootHash)));
  }

  public void onCompaction(
      long partitionId, List<DualDigest> sourceDigests, List<DualDigest> targetDigests) {
    TimePartitionMerkleNode node = getOrCreatePartitionNode(partitionId);
    for (DualDigest sourceDigest : sourceDigests) {
      node.removeDigest(sourceDigest);
    }
    for (DualDigest targetDigest : targetDigests) {
      node.addDigest(targetDigest);
    }
    recomputeRegionDigest();
  }

  /**
   * Overloaded compaction hook accepting pre-collected source hashes and a combined target hash.
   *
   * @param sourceHashes root hashes of source TsFiles being removed
   * @param targetCombinedHash XOR of all target TsFile root hashes
   * @param partitionId the partition being compacted
   */
  public void onCompaction(List<Long> sourceHashes, long targetCombinedHash, long partitionId) {
    onCompaction(
        partitionId,
        sourceHashes.stream().map(DualDigest::fromSingleHash).collect(java.util.stream.Collectors.toList()),
        Collections.singletonList(DualDigest.fromSingleHash(targetCombinedHash)));
  }

  public void onCompaction(
      List<DualDigest> sourceDigests, List<DualDigest> targetDigests, long partitionId) {
    TimePartitionMerkleNode node = getOrCreatePartitionNode(partitionId);
    for (DualDigest sourceDigest : sourceDigests) {
      node.removeDigest(sourceDigest);
    }
    for (DualDigest targetDigest : targetDigests) {
      node.addDigest(targetDigest);
    }
    recomputeRegionDigest();
  }

  /**
   * Mark a partition as dirty when deletions affect it. The next consistency check cycle will
   * trigger a full rescan and rebuild for this partition.
   */
  public void markPartitionDirty(long partitionId) {
    TimePartitionMerkleNode node = getOrCreatePartitionNode(partitionId);
    node.markDirty();
  }

  /**
   * Prune a verified cold partition from the in-memory tree. Called after successful verification
   * and commit to reduce memory footprint.
   */
  public void prunePartition(long partitionId) {
    treeLock.writeLock().lock();
    try {
      partitions.remove(partitionId);
      recomputeRegionDigestLocked();
    } finally {
      treeLock.writeLock().unlock();
    }
  }

  /** Get the current region-level dual-digest (Level 0). */
  public DualDigest getRegionDigest() {
    return regionDigest;
  }

  /** Return all partition IDs within the given time range [startInclusive, endExclusive). */
  public List<Long> getPartitionIds(long startInclusive, long endExclusive) {
    treeLock.readLock().lock();
    try {
      return new ArrayList<>(partitions.subMap(startInclusive, endExclusive).keySet());
    } finally {
      treeLock.readLock().unlock();
    }
  }

  /** Return all partition IDs in the tree. */
  public List<Long> getAllPartitionIds() {
    treeLock.readLock().lock();
    try {
      return new ArrayList<>(partitions.keySet());
    } finally {
      treeLock.readLock().unlock();
    }
  }

  /** Return an unmodifiable view of dirty partitions. */
  public List<Long> getDirtyPartitionIds() {
    treeLock.readLock().lock();
    try {
      List<Long> dirtyIds = new ArrayList<>();
      for (Map.Entry<Long, TimePartitionMerkleNode> entry : partitions.entrySet()) {
        if (entry.getValue().isDirty()) {
          dirtyIds.add(entry.getKey());
        }
      }
      return Collections.unmodifiableList(dirtyIds);
    } finally {
      treeLock.readLock().unlock();
    }
  }

  /** Recompute Level-0 region digest from all partition digests. */
  private void recomputeRegionDigest() {
    treeLock.readLock().lock();
    try {
      recomputeRegionDigestLocked();
    } finally {
      treeLock.readLock().unlock();
    }
  }

  /** Must be called while holding at least a read lock on treeLock. */
  private void recomputeRegionDigestLocked() {
    DualDigest digest = DualDigest.ZERO;
    for (TimePartitionMerkleNode node : partitions.values()) {
      digest = digest.merge(node.getPartitionDigest());
    }
    this.regionDigest = digest;
  }

  /** Return the number of tracked partitions. */
  public int getPartitionCount() {
    treeLock.readLock().lock();
    try {
      return partitions.size();
    } finally {
      treeLock.readLock().unlock();
    }
  }
}
