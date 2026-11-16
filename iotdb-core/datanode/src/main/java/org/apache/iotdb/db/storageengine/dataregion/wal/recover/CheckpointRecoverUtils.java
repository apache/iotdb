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

package org.apache.iotdb.db.storageengine.dataregion.wal.recover;

import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.Checkpoint;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.CheckpointType;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.CheckpointReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.CheckpointFileUtils;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CheckpointRecoverUtils {
  private CheckpointRecoverUtils() {}

  /** Recover memTable information from checkpoint folder. */
  public static CheckpointInfo recoverMemTableInfo(File logDirectory) {
    // find all .checkpoint file
    File[] checkpointFiles = CheckpointFileUtils.listAllCheckpointFiles(logDirectory);
    if (checkpointFiles == null) {
      return new CheckpointInfo(0, Collections.emptyMap());
    }
    // desc sort by version id
    CheckpointFileUtils.descSortByVersionId(checkpointFiles);
    // Read back to a complete snapshot within the newest contiguous run of readable checkpoints.
    // A missing version may have removed mappings that are still present in an older snapshot.
    Deque<CheckpointReader> readers = new ArrayDeque<>();
    int nextVersion = -1;
    for (File checkpointFile : checkpointFiles) {
      int version = CheckpointFileUtils.parseVersionId(checkpointFile.getName());
      if (!readers.isEmpty() && version != nextVersion) {
        break;
      }
      CheckpointReader reader = new CheckpointReader(checkpointFile);
      List<Checkpoint> checkpoints = reader.getCheckpoints();
      if (checkpoints.isEmpty()) {
        if (!readers.isEmpty()) {
          break;
        }
        continue;
      }
      readers.addFirst(reader);
      if (checkpoints.get(0).getType() == CheckpointType.GLOBAL_MEMORY_TABLE_INFO) {
        break;
      }
      nextVersion = version - 1;
    }

    // Apply the snapshot and the readable incremental checkpoints in version order.
    long maxMemTableId = 0;
    Map<Long, MemTableInfo> memTableId2Info = new HashMap<>();
    for (CheckpointReader reader : readers) {
      maxMemTableId = Math.max(maxMemTableId, reader.getMaxMemTableId());
      for (Checkpoint checkpoint : reader.getCheckpoints()) {
        for (MemTableInfo memTableInfo : checkpoint.getMemTableInfos()) {
          if (checkpoint.getType() == CheckpointType.FLUSH_MEMORY_TABLE) {
            memTableId2Info.remove(memTableInfo.getMemTableId());
          } else {
            maxMemTableId = Math.max(maxMemTableId, memTableInfo.getMemTableId());
            memTableId2Info.put(memTableInfo.getMemTableId(), memTableInfo);
          }
        }
      }
    }
    return new CheckpointInfo(maxMemTableId, memTableId2Info);
  }

  public static class CheckpointInfo {
    private final long maxMemTableId;
    private final Map<Long, MemTableInfo> memTableId2Info;

    public CheckpointInfo(long maxMemTableId, Map<Long, MemTableInfo> memTableId2Info) {
      this.maxMemTableId = maxMemTableId;
      this.memTableId2Info = memTableId2Info;
    }

    public long getMaxMemTableId() {
      return maxMemTableId;
    }

    public Map<Long, MemTableInfo> getMemTableId2Info() {
      return memTableId2Info;
    }
  }
}
