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

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.memtable.AbstractMemTable;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.db.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.wal.io.WALReader;
import org.apache.iotdb.db.wal.recover.file.UnsealedTsFileRecoverPerformer;
import org.apache.iotdb.db.wal.utils.CheckpointFileUtils;
import org.apache.iotdb.db.wal.utils.WALFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/** This task is responsible for the recovery of one wal node. */
public class WALNodeRecoverTask implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(WALNodeRecoverTask.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final WALRecoverManager walRecoverManger = WALRecoverManager.getInstance();

  /** this directory store one wal node's .wal and .checkpoint files */
  private final File logDirectory;
  /** latch to collect all nodes' recovery end information */
  private final CountDownLatch allNodesRecoveredLatch;
  /** version id of first valid .wal file */
  private int firstValidVersionId = Integer.MAX_VALUE;
  /** version id of last .wal file */
  private int lastVersionId = -1;

  private Map<Integer, MemTableInfo> memTableId2Info;
  private Map<Integer, UnsealedTsFileRecoverPerformer> memTableId2RecoverPerformer;

  public WALNodeRecoverTask(File logDirectory, CountDownLatch allNodesRecoveredLatch) {
    this.logDirectory = logDirectory;
    this.allNodesRecoveredLatch = allNodesRecoveredLatch;
  }

  @Override
  public void run() {
    logger.info("Start recovering WAL node in the directory {}", logDirectory);
    try {
      recoverInfoFromCheckpoints();
      recoverTsFiles();
    } catch (Exception e) {
      for (UnsealedTsFileRecoverPerformer recoverPerformer : memTableId2RecoverPerformer.values()) {
        recoverPerformer.getRecoverListener().fail(e);
      }
    } finally {
      allNodesRecoveredLatch.countDown();
      for (UnsealedTsFileRecoverPerformer recoverPerformer : memTableId2RecoverPerformer.values()) {
        try {
          if (!recoverPerformer.canWrite()) {
            recoverPerformer.close();
          }
        } catch (Exception e) {
          // continue
        }
      }
    }

    if (!config
        .getDataRegionConsensusProtocolClass()
        .equals(ConsensusFactory.MultiLeaderConsensus)) {
      // delete this wal node folder
      FileUtils.deleteDirectory(logDirectory);
      logger.info(
          "Successfully recover WAL node in the directory {}, so delete these wal files.",
          logDirectory);
    } else {
      File[] checkpointFiles = CheckpointFileUtils.listAllCheckpointFiles(logDirectory);
      for (File checkpointFile : checkpointFiles) {
        checkpointFile.delete();
      }
      WALManager.getInstance()
          .registerWALNode(
              logDirectory.getName(), logDirectory.getAbsolutePath(), lastVersionId + 1);
      logger.info(
          "Successfully recover WAL node in the directory {}, add this node to WALManger.",
          logDirectory);
    }
  }

  private void recoverInfoFromCheckpoints() {
    // parse memTables information
    CheckpointRecoverUtils.CheckpointInfo info =
        CheckpointRecoverUtils.recoverMemTableInfo(logDirectory);
    memTableId2Info = info.getMemTableId2Info();
    memTableId2RecoverPerformer = new HashMap<>();
    // update init memTable id
    int maxMemTableId = info.getMaxMemTableId();
    AtomicInteger memTableIdCounter = AbstractMemTable.memTableIdCounter;
    int oldVal = memTableIdCounter.get();
    while (maxMemTableId > oldVal) {
      if (!memTableIdCounter.compareAndSet(oldVal, maxMemTableId)) {
        oldVal = memTableIdCounter.get();
      }
    }
    // update firstValidVersionId and get recover performer from WALRecoverManager
    for (MemTableInfo memTableInfo : memTableId2Info.values()) {
      firstValidVersionId = Math.min(firstValidVersionId, memTableInfo.getFirstFileVersionId());

      File tsFile = new File(memTableInfo.getTsFilePath());
      UnsealedTsFileRecoverPerformer recoverPerformer =
          walRecoverManger.removeRecoverPerformer(tsFile.getAbsolutePath());
      if (recoverPerformer != null) {
        memTableId2RecoverPerformer.put(memTableInfo.getMemTableId(), recoverPerformer);
      }
    }
  }

  private void recoverTsFiles() {
    if (memTableId2RecoverPerformer.isEmpty()) {
      return;
    }
    // make preparation for recovery
    for (UnsealedTsFileRecoverPerformer recoverPerformer : memTableId2RecoverPerformer.values()) {
      try {
        recoverPerformer.startRecovery();
      } catch (Exception e) {
        recoverPerformer.getRecoverListener().fail(e);
      }
    }
    // find all valid .wal files
    File[] walFiles =
        logDirectory.listFiles(
            (dir, name) ->
                WALFileUtils.walFilenameFilter(dir, name)
                    && WALFileUtils.parseVersionId(name) >= firstValidVersionId);
    if (walFiles == null) {
      return;
    }
    // asc sort by version id
    WALFileUtils.ascSortByVersionId(walFiles);
    // update last version id
    if (walFiles.length != 0) {
      lastVersionId = WALFileUtils.parseVersionId(walFiles[walFiles.length - 1].getName());
    }
    // read .wal files and redo logs
    for (File walFile : walFiles) {
      try (WALReader walReader = new WALReader(walFile)) {
        while (walReader.hasNext()) {
          WALEntry walEntry = walReader.next();
          if (!memTableId2Info.containsKey(walEntry.getMemTableId())) {
            continue;
          }

          UnsealedTsFileRecoverPerformer recoverPerformer =
              memTableId2RecoverPerformer.get(walEntry.getMemTableId());
          if (recoverPerformer != null) {
            recoverPerformer.redoLog(walEntry);
          } else {
            logger.warn(
                "Fail to find TsFile recover performer for wal entry in TsFile {}", walFile);
          }
        }
      } catch (Exception e) {
        logger.warn("Fail to read wal logs from {}, skip them", walFile, e);
      }
    }
    // end recovering all recover performers
    for (UnsealedTsFileRecoverPerformer recoverPerformer : memTableId2RecoverPerformer.values()) {
      try {
        recoverPerformer.endRecovery();
        recoverPerformer.getRecoverListener().succeed();
      } catch (Exception e) {
        recoverPerformer.getRecoverListener().fail(e);
      }
    }
  }
}
