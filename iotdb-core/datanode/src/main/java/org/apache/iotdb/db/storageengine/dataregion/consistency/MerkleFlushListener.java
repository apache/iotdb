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
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushListener;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * FlushListener that computes hash digests for a newly flushed TsFile and generates the
 * corresponding .merkle sidecar file. Also updates the in-memory ConsistencyMerkleTree with the
 * file's root hash.
 */
public class MerkleFlushListener implements FlushListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(MerkleFlushListener.class);

  private final TsFileResource tsFileResource;
  private final ConsistencyMerkleTree merkleTree;

  public MerkleFlushListener(TsFileResource tsFileResource, ConsistencyMerkleTree merkleTree) {
    this.tsFileResource = tsFileResource;
    this.merkleTree = merkleTree;
  }

  @Override
  public void onMemTableFlushStarted(IMemTable memTable) {
    // No action needed at flush start
  }

  @Override
  public void onMemTableFlushed(IMemTable memTable) {
    try {
      String tsFilePath = tsFileResource.getTsFilePath();
      long partitionId = tsFileResource.getTimePartition();

      // Scan the flushed TsFile and compute per-(device, measurement, timeBucket) hashes
      List<MerkleEntry> entries = MerkleHashComputer.computeEntries(tsFilePath);

      if (entries.isEmpty()) {
        return;
      }

      long fileXorHash = MerkleFileWriter.computeFileXorHash(entries);
      long fileAddHash = MerkleFileWriter.computeFileAddHash(entries);

      // Write .merkle sidecar file
      MerkleFileWriter.write(tsFilePath + ".merkle", entries, fileXorHash, fileAddHash);

      // Update in-memory Merkle tree
      merkleTree.onTsFileFlushed(partitionId, new DualDigest(fileXorHash, fileAddHash));

      LOGGER.debug(
          "Generated .merkle file for {} with {} entries, xorHash=0x{}, addHash=0x{}, partitionId={}",
          tsFilePath,
          entries.size(),
          Long.toHexString(fileXorHash),
          Long.toHexString(fileAddHash),
          partitionId);
    } catch (IOException e) {
      LOGGER.warn(
          "Failed to generate .merkle file for {}: {}",
          tsFileResource.getTsFilePath(),
          e.getMessage(),
          e);
    }
  }
}
