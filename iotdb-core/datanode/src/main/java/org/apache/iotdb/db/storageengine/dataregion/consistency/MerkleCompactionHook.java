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
import org.apache.iotdb.commons.consensus.iotv2.consistency.DualDigest;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleEntry;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileCache;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileContent;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hook into TsFileManager.replace() for compaction-aware Merkle tree updates. When source TsFiles
 * are merged into target TsFiles during compaction, this hook:
 *
 * <ol>
 *   <li>XOR-out each source file's root hash from the partition digest
 *   <li>Scan target TsFiles and generate .merkle files
 *   <li>XOR-in each target file's root hash
 *   <li>Delete source .merkle files
 *   <li>Invalidate cache entries for source files
 * </ol>
 */
public class MerkleCompactionHook {

  private static final Logger LOGGER = LoggerFactory.getLogger(MerkleCompactionHook.class);

  private final ConsistencyMerkleTree merkleTree;
  private final MerkleFileCache merkleFileCache;

  public MerkleCompactionHook(ConsistencyMerkleTree merkleTree, MerkleFileCache merkleFileCache) {
    this.merkleTree = merkleTree;
    this.merkleFileCache = merkleFileCache;
  }

  /**
   * Called after compaction replaces source TsFiles with target TsFiles.
   *
   * @param seqSourceFiles source sequence TsFiles being removed
   * @param unseqSourceFiles source unsequence TsFiles being removed
   * @param targetFiles newly created target TsFiles
   * @param timePartition the time partition being compacted
   */
  public void onCompaction(
      List<TsFileResource> seqSourceFiles,
      List<TsFileResource> unseqSourceFiles,
      List<TsFileResource> targetFiles,
      long timePartition) {
    try {
      List<DualDigest> sourceDigests = new ArrayList<>();
      boolean digestUpdatePossible = true;

      // Collect and XOR-out source file hashes
      for (TsFileResource source : seqSourceFiles) {
        DualDigest digest = getFileDigest(source);
        if (digest == null) {
          digestUpdatePossible = false;
        } else {
          sourceDigests.add(digest);
        }
      }
      for (TsFileResource source : unseqSourceFiles) {
        DualDigest digest = getFileDigest(source);
        if (digest == null) {
          digestUpdatePossible = false;
        } else {
          sourceDigests.add(digest);
        }
      }

      // Compute target file hashes and generate .merkle files
      List<DualDigest> targetDigests = new ArrayList<>();
      for (TsFileResource target : targetFiles) {
        if (target.isDeleted()) {
          continue;
        }
        String tsFilePath = target.getTsFilePath();
        List<MerkleEntry> entries = MerkleHashComputer.computeEntries(tsFilePath);
        if (entries.isEmpty()) {
          continue;
        }

        long fileXorHash = MerkleFileWriter.computeFileXorHash(entries);
        long fileAddHash = MerkleFileWriter.computeFileAddHash(entries);
        MerkleFileWriter.write(tsFilePath + ".merkle", entries, fileXorHash, fileAddHash);
        targetDigests.add(new DualDigest(fileXorHash, fileAddHash));
      }

      if (digestUpdatePossible) {
        merkleTree.onCompaction(sourceDigests, targetDigests, timePartition);
      } else {
        merkleTree.markPartitionDirty(timePartition);
      }

      // Cleanup: delete source .merkle files and invalidate cache
      cleanupSourceMerkleFiles(seqSourceFiles);
      cleanupSourceMerkleFiles(unseqSourceFiles);

      LOGGER.debug(
          "Compaction hook: updated partition {} Merkle tree, removed {} source digests",
          timePartition,
          sourceDigests.size());

    } catch (IOException e) {
      LOGGER.warn(
          "Failed to update Merkle tree during compaction for partition {}: {}",
          timePartition,
          e.getMessage(),
          e);
      merkleTree.markPartitionDirty(timePartition);
    }
  }

  private DualDigest getFileDigest(TsFileResource source) {
    String tsFilePath = source.getTsFilePath();
    try {
      MerkleFileContent content = merkleFileCache.get(tsFilePath);
      return content.getFileDigest();
    } catch (IOException e) {
      // .merkle file might not exist (e.g., created before consistency module was enabled)
      LOGGER.debug("No .merkle file for source TsFile {}: {}", tsFilePath, e.getMessage());
      return null;
    }
  }

  private void cleanupSourceMerkleFiles(List<TsFileResource> sourceFiles) {
    for (TsFileResource source : sourceFiles) {
      String tsFilePath = source.getTsFilePath();
      merkleFileCache.invalidate(tsFilePath);
      File merkleFile = new File(tsFilePath + ".merkle");
      if (merkleFile.exists() && !merkleFile.delete()) {
        LOGGER.warn("Failed to delete .merkle file: {}", merkleFile.getAbsolutePath());
      }
    }
  }
}
