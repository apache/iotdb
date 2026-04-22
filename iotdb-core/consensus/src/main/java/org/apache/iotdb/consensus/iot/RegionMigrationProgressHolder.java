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

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.utils.FileUtils;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds the current snapshot transmission progress per consensus group, so that SHOW MIGRATIONS can
 * display it. Tracks both file count and byte size. Updated by IoTConsensusServerImpl during
 * transmitSnapshot(); only IoTConsensus reports this progress.
 */
public final class RegionMigrationProgressHolder {

  /** [0]=migratedFileCount, [1]=totalFileCount, [2]=migratedSizeBytes, [3]=totalSizeBytes. */
  private static final ConcurrentHashMap<ConsensusGroupId, long[]> PROGRESS_MAP =
      new ConcurrentHashMap<>();

  public static void setTotal(ConsensusGroupId groupId, int totalFileCount, long totalSizeBytes) {
    PROGRESS_MAP.put(groupId, new long[] {0, totalFileCount, 0, totalSizeBytes});
  }

  public static void setMigrated(
      ConsensusGroupId groupId, int migratedFileCount, long migratedSizeBytes) {
    long[] arr = PROGRESS_MAP.get(groupId);
    if (arr != null) {
      arr[0] = migratedFileCount;
      arr[2] = migratedSizeBytes;
    }
  }

  public static void clear(ConsensusGroupId groupId) {
    PROGRESS_MAP.remove(groupId);
  }

  /**
   * Returns progress string like "files 3/19, size 1.2MB/23.4MB", or empty if no progress for this
   * group.
   */
  public static Optional<String> getProgress(ConsensusGroupId groupId) {
    long[] arr = PROGRESS_MAP.get(groupId);
    if (arr == null || arr[1] == 0) {
      return Optional.empty();
    }
    return Optional.of(
        String.format(
            "files %d/%d, size %s/%s",
            arr[0],
            arr[1],
            FileUtils.humanReadableByteCountSI(arr[2]),
            FileUtils.humanReadableByteCountSI(arr[3])));
  }

  private RegionMigrationProgressHolder() {}
}
