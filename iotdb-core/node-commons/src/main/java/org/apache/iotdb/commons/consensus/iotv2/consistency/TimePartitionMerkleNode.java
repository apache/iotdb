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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a Level-1 node in the consistency Merkle tree, holding the dual-digest for a single
 * time partition. Uses fine-grained locking to support concurrent flush/compaction/check threads.
 */
public class TimePartitionMerkleNode {

  private final long partitionId;
  private volatile DualDigest partitionDigest;
  private volatile boolean dirty;
  private volatile long lastVerifiedTime;
  private final AtomicLong version;
  private final ReadWriteLock lock;

  public TimePartitionMerkleNode(long partitionId) {
    this.partitionId = partitionId;
    this.partitionDigest = DualDigest.ZERO;
    this.dirty = false;
    this.lastVerifiedTime = 0L;
    this.version = new AtomicLong(0);
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * XOR a file's root hash into this partition's dual-digest. Called when a new TsFile is flushed
   * or a compaction target is created.
   */
  public void xorIn(long fileRootHash) {
    addDigest(DualDigest.fromSingleHash(fileRootHash));
  }

  public void addDigest(DualDigest digest) {
    lock.writeLock().lock();
    try {
      partitionDigest = partitionDigest.merge(digest);
      version.incrementAndGet();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * XOR a file's root hash out of this partition's dual-digest. Called when a source TsFile is
   * removed during compaction.
   */
  public void xorOut(long fileRootHash) {
    removeDigest(DualDigest.fromSingleHash(fileRootHash));
  }

  public void removeDigest(DualDigest digest) {
    lock.writeLock().lock();
    try {
      partitionDigest = partitionDigest.subtract(digest);
      version.incrementAndGet();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public DualDigest getPartitionDigest() {
    lock.readLock().lock();
    try {
      return partitionDigest;
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Reset the digest from scratch, typically after rebuilding a dirty partition. */
  public void resetDigest(DualDigest newDigest) {
    lock.writeLock().lock();
    try {
      this.partitionDigest = newDigest;
      this.dirty = false;
      this.version.incrementAndGet();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void markDirty() {
    this.dirty = true;
    this.version.incrementAndGet();
  }

  public boolean isDirty() {
    return dirty;
  }

  public long getPartitionId() {
    return partitionId;
  }

  public long getVersion() {
    return version.get();
  }

  public long getLastVerifiedTime() {
    return lastVerifiedTime;
  }

  public void setLastVerifiedTime(long lastVerifiedTime) {
    this.lastVerifiedTime = lastVerifiedTime;
  }
}
