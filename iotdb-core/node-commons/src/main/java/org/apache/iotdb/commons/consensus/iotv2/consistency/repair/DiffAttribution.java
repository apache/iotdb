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

import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DataPointLocator;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DiffEntry;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.RowRefIndex;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleEntry;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileContent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps decoded IBF diff entries back to their source TsFiles on the Leader. Each diff point is
 * attributed to the TsFile whose .merkle file contains a matching (device, measurement, timeBucket)
 * entry. This attribution enables per-TsFile repair strategy selection.
 */
public class DiffAttribution {

  /**
   * Attribute decoded diffs to their source TsFiles on the Leader.
   *
   * @param decodedDiffs the decoded diff entries from IBF
   * @param rowRefIndex the index for resolving composite keys to data point locators
   * @param leaderMerkleFiles .merkle file contents loaded from the Leader
   * @return map from TsFile path to list of attributed diff entries
   */
  public Map<String, List<DiffEntry>> attributeToSourceTsFiles(
      List<DiffEntry> decodedDiffs,
      RowRefIndex rowRefIndex,
      List<MerkleFileContent> leaderMerkleFiles) {
    Map<String, Map<String, List<BucketOwner>>> bucketIndex = new HashMap<>();
    for (MerkleFileContent content : leaderMerkleFiles) {
      for (MerkleEntry entry : content.getEntries()) {
        bucketIndex
            .computeIfAbsent(entry.getDeviceId(), ignored -> new HashMap<>())
            .computeIfAbsent(entry.getMeasurement(), ignored -> new ArrayList<>())
            .add(
                new BucketOwner(
                    entry.getTimeBucketStart(),
                    entry.getTimeBucketEnd(),
                    content.getSourceTsFilePath()));
      }
    }

    Map<String, List<DiffEntry>> attribution = new HashMap<>();
    for (DiffEntry diff : decodedDiffs) {
      DataPointLocator loc = rowRefIndex.resolve(diff.getCompositeKey());

      String sourceTsFile = findSourceTsFile(loc, bucketIndex);
      if (sourceTsFile != null) {
        attribution.computeIfAbsent(sourceTsFile, k -> new ArrayList<>()).add(diff);
      }
    }

    return attribution;
  }

  private String findSourceTsFile(
      DataPointLocator loc, Map<String, Map<String, List<BucketOwner>>> bucketIndex) {
    List<BucketOwner> owners =
        bucketIndex
            .getOrDefault(loc.getDeviceId(), java.util.Collections.emptyMap())
            .get(loc.getMeasurement());
    if (owners == null) {
      return null;
    }
    for (BucketOwner owner : owners) {
      if (owner.contains(loc.getTimestamp())) {
        return owner.sourceTsFilePath;
      }
    }
    return null;
  }

  private static final class BucketOwner {
    private final long bucketStart;
    private final long bucketEnd;
    private final String sourceTsFilePath;

    private BucketOwner(long bucketStart, long bucketEnd, String sourceTsFilePath) {
      this.bucketStart = bucketStart;
      this.bucketEnd = bucketEnd;
      this.sourceTsFilePath = sourceTsFilePath;
    }

    private boolean contains(long timestamp) {
      return timestamp >= bucketStart && timestamp < bucketEnd;
    }
  }
}
