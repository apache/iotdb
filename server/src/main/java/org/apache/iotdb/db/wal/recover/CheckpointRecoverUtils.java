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
import org.apache.iotdb.db.wal.io.CheckpointWriter;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CheckpointRecoverUtils {
  private CheckpointRecoverUtils() {}

  /** Recover memTable information from checkpoint folder */
  public static Map<Integer, MemTableInfo> recoverMemTableInfo(File logDirectory) {
    // find all .checkpoint file
    File[] checkpointFiles = logDirectory.listFiles(CheckpointWriter::checkpointFilenameFilter);
    if (checkpointFiles == null) {
      return Collections.emptyMap();
    }
    Arrays.sort(
        checkpointFiles,
        Comparator.comparingInt(file -> CheckpointWriter.parseVersionId(((File) file).getName()))
            .reversed());
    // find last valid .checkpoint file and load checkpoints from it
    List<Checkpoint> checkpoints = null;
    for (File checkpointFile : checkpointFiles) {
      checkpoints = new CheckpointReader(checkpointFile).readAll();
      if (!checkpoints.isEmpty()) {
        break;
      }
    }
    if (checkpoints == null || checkpoints.isEmpty()) {
      return Collections.emptyMap();
    }
    // recover memTables information by checkpoints
    Map<Integer, MemTableInfo> memTableId2Info = new HashMap<>();
    for (Checkpoint checkpoint : checkpoints) {
      switch (checkpoint.getType()) {
        case GLOBAL_MEMORY_TABLE_INFO:
        case CREATE_MEMORY_TABLE:
          for (MemTableInfo memTableInfo : checkpoint.getMemTableInfos()) {
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
    return memTableId2Info;
  }
}
