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

package org.apache.iotdb.commons.consensus.iotv2.consistency.merkle;

import org.apache.iotdb.commons.consensus.iotv2.consistency.DualDigest;

import java.util.Collections;
import java.util.List;

/**
 * In-memory representation of a parsed .merkle file. Holds the file-level dual-digest and all
 * per-(device, measurement, timeBucket) entries.
 */
public class MerkleFileContent {

  private final long fileXorHash;
  private final long fileAddHash;
  private final List<MerkleEntry> entries;
  private final String sourceTsFilePath;

  public MerkleFileContent(
      long fileXorHash, long fileAddHash, List<MerkleEntry> entries, String sourceTsFilePath) {
    this.fileXorHash = fileXorHash;
    this.fileAddHash = fileAddHash;
    this.entries = Collections.unmodifiableList(entries);
    this.sourceTsFilePath = sourceTsFilePath;
  }

  public long getFileXorHash() {
    return fileXorHash;
  }

  public long getFileAddHash() {
    return fileAddHash;
  }

  public DualDigest getFileDigest() {
    return new DualDigest(fileXorHash, fileAddHash);
  }

  public List<MerkleEntry> getEntries() {
    return entries;
  }

  public String getSourceTsFilePath() {
    return sourceTsFilePath;
  }

  public int getTotalPointCount() {
    int total = 0;
    for (MerkleEntry entry : entries) {
      total += entry.getPointCount();
    }
    return total;
  }

  /** Estimate heap memory consumed by this content for cache weighing. */
  public int estimatedMemoryBytes() {
    int base = 64;
    for (MerkleEntry entry : entries) {
      base += 80 + entry.getDeviceId().length() * 2 + entry.getMeasurement().length() * 2;
    }
    return base;
  }
}
