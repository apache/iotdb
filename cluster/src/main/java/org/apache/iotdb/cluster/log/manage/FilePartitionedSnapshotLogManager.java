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

package org.apache.iotdb.cluster.log.manage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Different from PartitionedSnapshotLogManager, FilePartitionedSnapshotLogManager does not store
 * the committed in memory after snapshots, it considers the logs are contained in the TsFiles so it will record
 * every TsFiles in the slot instead.
 * TODO IOTDB-439 but the log is stored in memory already by appendLog method.
 */
public class FilePartitionedSnapshotLogManager extends PartitionedSnapshotLogManager<FileSnapshot> {

  private static final Logger logger = LoggerFactory.getLogger(FilePartitionedSnapshotLogManager.class);

  public FilePartitionedSnapshotLogManager(LogApplier logApplier, PartitionTable partitionTable,
      Node header) {
    super(logApplier, partitionTable, header, FileSnapshot::new);
  }

  @Override
  public void takeSnapshot() throws IOException {

    logger.info("Taking snapshots, flushing IoTDB");
    StorageEngine.getInstance().syncCloseAllProcessor();
    logger.info("Taking snapshots, IoTDB is flushed");
    synchronized (slotSnapshots) {
      collectTimeseriesSchemas();


      int i = 0;
      for (; i < logBuffer.size(); i++) {
        if (logBuffer.get(i).getCurrLogIndex() > commitLogIndex) {
          break;
        }
        snapshotLastLogId = logBuffer.get(i).getCurrLogIndex();
        snapshotLastLogTerm = logBuffer.get(i).getCurrLogTerm();
      }
      removeFromHead(i);

      collectTsFiles();

      logger.info("Snapshot is taken");
    }
  }

  private void collectTsFiles() throws IOException {
    // TODO-Cluster#349: the collection is re-collected each time to prevent inconsistency when
    //  some of them are removed during two snapshots. Incremental addition or removal may be
    //  used to optimize

    startCollect:
    while (true) {
      slotSnapshots.clear();
      Map<String, Map<Long, List<TsFileResource>>> allClosedStorageGroupTsFile = StorageEngine
          .getInstance().getAllClosedStorageGroupTsFile();
      List<TsFileResource> createdHardlinks = new ArrayList<>();
      // group the TsFiles by their slots
      for (Entry<String, Map<Long, List<TsFileResource>>> entry :
          allClosedStorageGroupTsFile.entrySet()) {
        String storageGroupName = entry.getKey();
        Map<Long, List<TsFileResource>> storageGroupsFiles = entry.getValue();
        for (Entry<Long, List<TsFileResource>> storageGroupFiles : storageGroupsFiles.entrySet()) {
          Long partitionNum = storageGroupFiles.getKey();
          int slotNum = PartitionUtils.calculateStorageGroupSlotByPartition(storageGroupName,
              partitionNum, partitionTable.getTotalSlotNumbers());
          FileSnapshot snapshot = slotSnapshots.computeIfAbsent(slotNum,
              s -> new FileSnapshot());
          if (snapshot.getTimeseriesSchemas().isEmpty()) {
            snapshot.setTimeseriesSchemas(slotTimeseries.getOrDefault(slotNum,
                Collections.emptySet()));
          }

          for (TsFileResource tsFileResource : storageGroupFiles.getValue()) {
            TsFileResource hardlink = tsFileResource.createHardlink();
            if (hardlink == null) {
              // some file is deleted during the collecting, clean created hardlinks and restart
              // from the beginning
              for (TsFileResource createdHardlink : createdHardlinks) {
                createdHardlink.remove();
              }
              continue startCollect;
            }
            createdHardlinks.add(hardlink);
            snapshot.addFile(hardlink, header);
          }
        }
      }
      break;
    }
  }
}
