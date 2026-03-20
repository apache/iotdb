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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ConsistencyProgressInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyProgressInfo.class);
  private static final String SNAPSHOT_FILENAME = "consistency_progress.bin";

  private final ConcurrentHashMap<String, RepairProgressTable> progressTables =
      new ConcurrentHashMap<>();

  public RepairProgressTable getOrCreateTable(String consensusGroupKey) {
    return progressTables.computeIfAbsent(consensusGroupKey, RepairProgressTable::new);
  }

  public RepairProgressTable getTable(String consensusGroupKey) {
    RepairProgressTable table = progressTables.get(consensusGroupKey);
    return table == null ? null : table.copy();
  }

  public void updateTable(RepairProgressTable repairProgressTable) {
    if (repairProgressTable == null) {
      return;
    }
    progressTables.put(repairProgressTable.getConsensusGroupId(), repairProgressTable.copy());
  }

  public List<RepairProgressTable> getAllTables() {
    List<RepairProgressTable> tables = new ArrayList<>();
    for (RepairProgressTable table : progressTables.values()) {
      tables.add(table.copy());
    }
    tables.sort((left, right) -> left.getConsensusGroupId().compareTo(right.getConsensusGroupId()));
    return tables;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take consistency progress snapshot because [{}] already exists",
          snapshotFile.getAbsolutePath());
      return false;
    }

    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());
    try (FileOutputStream outputStream = new FileOutputStream(tmpFile)) {
      List<RepairProgressTable> tables = getAllTables();
      ReadWriteIOUtils.write(tables.size(), outputStream);
      for (RepairProgressTable table : tables) {
        table.serialize(outputStream);
      }
      outputStream.getFD().sync();
    }
    return tmpFile.renameTo(snapshotFile);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.info(
          "Consistency progress snapshot [{}] does not exist, skip loading",
          snapshotFile.getAbsolutePath());
      return;
    }

    ConcurrentHashMap<String, RepairProgressTable> recovered = new ConcurrentHashMap<>();
    try (FileInputStream inputStream = new FileInputStream(snapshotFile)) {
      int size = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < size; i++) {
        RepairProgressTable table = RepairProgressTable.deserialize(inputStream);
        recovered.put(table.getConsensusGroupId(), table);
      }
    }
    progressTables.clear();
    progressTables.putAll(recovered);
  }

  public int size() {
    return progressTables.size();
  }

  public Map<String, RepairProgressTable> view() {
    return progressTables;
  }
}
