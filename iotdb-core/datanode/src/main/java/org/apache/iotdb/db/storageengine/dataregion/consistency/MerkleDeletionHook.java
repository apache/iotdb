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

package org.apache.iotdb.db.storageengine.dataregion.consistency;

import org.apache.iotdb.commons.consensus.iotv2.consistency.ConsistencyMerkleTree;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Hook into DataRegion's deletion path to mark affected time partitions as dirty in the
 * ConsistencyMerkleTree. Rather than attempting to XOR-out exact deleted hashes (which requires
 * expensive disk reads and has crash-consistency risks), we use lazy invalidation: mark the
 * partition dirty and trigger a full rescan during the next consistency check cycle.
 */
public class MerkleDeletionHook {

  private static final Logger LOGGER = LoggerFactory.getLogger(MerkleDeletionHook.class);

  private final ConsistencyMerkleTree merkleTree;

  public MerkleDeletionHook(ConsistencyMerkleTree merkleTree) {
    this.merkleTree = merkleTree;
  }

  /**
   * Called when a deletion (mod entry) affects one or more TsFiles. Marks the time partitions of
   * all affected TsFiles as dirty.
   *
   * @param affectedTsFiles list of TsFileResource objects that overlap with the deletion
   */
  public void onDeletion(List<TsFileResource> affectedTsFiles) {
    Set<Long> affectedPartitions = new TreeSet<>();
    for (TsFileResource resource : affectedTsFiles) {
      long partitionId = resource.getTimePartition();
      affectedPartitions.add(partitionId);
    }

    for (long partitionId : affectedPartitions) {
      merkleTree.markPartitionDirty(partitionId);
    }

    LOGGER.debug(
        "Deletion affected {} partitions, marked dirty: {}",
        affectedPartitions.size(),
        affectedPartitions);
  }

  /**
   * Called when a deletion affects a specific time range. Marks all tracked partitions that overlap
   * with the range as dirty.
   *
   * @param startTime inclusive start timestamp of deletion
   * @param endTime inclusive end timestamp of deletion
   * @param timePartitionInterval the time partition interval in ms
   */
  public void onDeletion(long startTime, long endTime, long timePartitionInterval) {
    if (timePartitionInterval <= 0) {
      return;
    }
    long startPartition = startTime / timePartitionInterval;
    long endPartition = endTime / timePartitionInterval;

    for (long partitionId = startPartition; partitionId <= endPartition; partitionId++) {
      merkleTree.markPartitionDirty(partitionId);
    }

    LOGGER.debug(
        "Deletion [{}, {}] marked partitions [{}, {}] as dirty",
        startTime,
        endTime,
        startPartition,
        endPartition);
  }
}
