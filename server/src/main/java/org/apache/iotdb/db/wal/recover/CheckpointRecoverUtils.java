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
package org.apache.iotdb.db.wal.recover;

import org.apache.iotdb.db.wal.checkpoint.Checkpoint;
import org.apache.iotdb.db.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.wal.io.CheckpointReader;
import org.apache.iotdb.db.wal.utils.CheckpointFileUtils;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CheckpointRecoverUtils {
  private CheckpointRecoverUtils() {}

  /** Recover memTable information from checkpoint folder */
  public static CheckpointInfo recoverMemTableInfo(File logDirectory) {
    // find all .checkpoint file
    File[] checkpointFiles = CheckpointFileUtils.listAllCheckpointFiles(logDirectory);
    if (checkpointFiles == null) {
      return new CheckpointInfo(0, Collections.emptyMap());
    }
    // desc sort by version id
    CheckpointFileUtils.descSortByVersionId(checkpointFiles);
    // find last valid .checkpoint file and load checkpoints from it
    long maxMemTableId = 0;
    List<Checkpoint> checkpoints = null;
    for (File checkpointFile : checkpointFiles) {
      CheckpointReader reader = new CheckpointReader(checkpointFile);
      maxMemTableId = reader.getMaxMemTableId();
      checkpoints = reader.getCheckpoints();
      if (!checkpoints.isEmpty()) {
        break;
      }
    }
    if (checkpoints == null || checkpoints.isEmpty()) {
      return new CheckpointInfo(0, Collections.emptyMap());
    }
    // recover memTables information by checkpoints
    Map<Long, MemTableInfo> memTableId2Info = new HashMap<>();
    for (Checkpoint checkpoint : checkpoints) {
      switch (checkpoint.getType()) {
        case GLOBAL_MEMORY_TABLE_INFO:
        case CREATE_MEMORY_TABLE:
          for (MemTableInfo memTableInfo : checkpoint.getMemTableInfos()) {
            maxMemTableId = Math.max(maxMemTableId, memTableInfo.getMemTableId());
            memTableId2Info.put(memTableInfo.getMemTableId(), memTableInfo);
          }
          break;
        case FLUSH_MEMORY_TABLE:
          for (MemTableInfo memTableInfo : checkpoint.getMemTableInfos()) {
            memTableId2Info.remove(memTableInfo.getMemTableId());
          }
          break;
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
