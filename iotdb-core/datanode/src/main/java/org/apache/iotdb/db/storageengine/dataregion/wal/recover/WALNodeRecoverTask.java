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

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AbstractMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.BrokenWALFileException;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.UnsealedTsFileRecoverPerformer;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.CheckpointFileUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.consensus.iot.log.ConsensusReqReader.DEFAULT_SEARCH_INDEX;

/** This task is responsible for the recovery of one wal node. */
public class WALNodeRecoverTask implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(WALNodeRecoverTask.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final WALRecoverManager walRecoverManger = WALRecoverManager.getInstance();

  // this directory store one wal node's .wal and .checkpoint files
  private final File logDirectory;
  // latch to collect all nodes' recovery end information
  private final CountDownLatch allNodesRecoveredLatch;
  // version id of first valid .wal file
  private long firstValidVersionId = Long.MAX_VALUE;
  private Map<Long, MemTableInfo> memTableId2Info;
  private Map<Long, UnsealedTsFileRecoverPerformer> memTableId2RecoverPerformer;

  public WALNodeRecoverTask(File logDirectory, CountDownLatch allNodesRecoveredLatch) {
    this.logDirectory = logDirectory;
    this.allNodesRecoveredLatch = allNodesRecoveredLatch;
  }

  @Override
  public void run() {
    logger.info("Start recovering WAL node in the directory {}", logDirectory);

    // recover version id and search index
    long[] indexInfo = readLastFileInfoAndRepairIt();
    long lastVersionId = indexInfo[0];
    long lastSearchIndex = indexInfo[1];

    try {
      recoverInfoFromCheckpoints();
      recoverTsFiles();
    } catch (Exception e) {
      for (UnsealedTsFileRecoverPerformer recoverPerformer : memTableId2RecoverPerformer.values()) {
        recoverPerformer.getRecoverListener().fail(e);
      }
    } finally {
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

    try {
      if (config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
        // delete checkpoint info to avoid repeated recover
        File[] checkpointFiles = CheckpointFileUtils.listAllCheckpointFiles(logDirectory);
        for (File checkpointFile : checkpointFiles) {
          try {
            Files.delete(checkpointFile.toPath());
          } catch (IOException e) {
            logger.error("error when delete checkpoint file. {}", checkpointFile, e);
          }
        }
        // register wal node
        WALManager.getInstance()
            .registerWALNode(
                logDirectory.getName(),
                logDirectory.getAbsolutePath(),
                lastVersionId + 1,
                lastSearchIndex);
        logger.info(
            "Successfully recover WAL node in the directory {}, add this node to WALManger.",
            logDirectory);
      } else {
        // delete this wal node folder
        FileUtils.deleteFileOrDirectory(logDirectory);
        logger.info(
            "Successfully recover WAL node in the directory {}, so delete these wal files.",
            logDirectory);
      }

      // PipeConsensus will not only delete WAL node folder, but also register WAL node.
      if (config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.FAST_IOT_CONSENSUS)
          || config
              .getDataRegionConsensusProtocolClass()
              .equals(ConsensusFactory.IOT_CONSENSUS_V2)) {
        // register wal node
        WALManager.getInstance()
            .registerWALNode(
                logDirectory.getName(),
                logDirectory.getAbsolutePath(),
                lastVersionId + 1,
                lastSearchIndex);
        logger.info(
            "Successfully recover WAL node in the directory {}, add this node to WALManger.",
            logDirectory);
      }
    } finally {
      allNodesRecoveredLatch.countDown();
    }
  }

  private long[] readLastFileInfoAndRepairIt() {
    File[] walFiles = WALFileUtils.listAllWALFiles(logDirectory);
    if (walFiles == null || walFiles.length == 0) {
      return new long[] {0L, 0L};
    }
    // get last search index from last wal file
    WALFileUtils.ascSortByVersionId(walFiles);
    File lastWALFile = walFiles[walFiles.length - 1];
    long lastVersionId = WALFileUtils.parseVersionId(lastWALFile.getName());
    long lastSearchIndex = WALFileUtils.parseStartSearchIndex(lastWALFile.getName());
    WALMetaData metaData = new WALMetaData(lastSearchIndex, new ArrayList<>(), new HashSet<>());
    WALFileStatus fileStatus = WALFileStatus.CONTAINS_NONE_SEARCH_INDEX;
    try (WALReader walReader = new WALReader(lastWALFile, true)) {
      while (walReader.hasNext()) {
        WALEntry walEntry = walReader.next();
        long searchIndex = DEFAULT_SEARCH_INDEX;
        if (walEntry.getType().needSearch()) {
          SearchNode searchNode = (SearchNode) walEntry.getValue();
          if (searchNode.getSearchIndex() != SearchNode.NO_CONSENSUS_INDEX) {
            searchIndex = searchNode.getSearchIndex();
            lastSearchIndex = Math.max(lastSearchIndex, searchNode.getSearchIndex());
            fileStatus = WALFileStatus.CONTAINS_SEARCH_INDEX;
          }
        }
        metaData.setTruncateOffSet(walReader.getWALCurrentReadOffset());
        metaData.add(walEntry.serializedSize(), searchIndex, walEntry.getMemTableId());
      }
    } catch (Exception e) {
      logger.warn("Fail to read wal logs from {}, skip them", lastWALFile, e);
    }
    // make sure last wal file is correct
    repairWalFileIfBroken(lastWALFile, metaData);
    // rename last wal file when file status are inconsistent
    if (WALFileUtils.parseStatusCode(lastWALFile.getName()) != fileStatus) {
      String targetName =
          WALFileUtils.getLogFileName(
              WALFileUtils.parseVersionId(lastWALFile.getName()),
              WALFileUtils.parseStartSearchIndex(lastWALFile.getName()),
              fileStatus);
      if (!lastWALFile.renameTo(SystemFileFactory.INSTANCE.getFile(logDirectory, targetName))) {
        logger.error("Fail to rename file {} to {}", lastWALFile, targetName);
      }
    }
    return new long[] {lastVersionId, lastSearchIndex};
  }

  private static void repairWalFileIfBroken(File walFile, WALMetaData metaData) {
    WALRepairWriter walRepairWriter = new WALRepairWriter(walFile);
    try {
      walRepairWriter.repair(metaData);
    } catch (IOException e) {
      logger.error("Fail to recover metadata of wal file {}", walFile, e);
    }
  }

  private void recoverInfoFromCheckpoints() {
    // parse memTables information
    CheckpointRecoverUtils.CheckpointInfo info =
        CheckpointRecoverUtils.recoverMemTableInfo(logDirectory);
    memTableId2Info = info.getMemTableId2Info();
    // update init memTable id
    long maxMemTableId = info.getMaxMemTableId();
    AtomicLong memTableIdCounter = AbstractMemTable.memTableIdCounter;
    long oldVal = memTableIdCounter.get();
    while (maxMemTableId > oldVal) {
      if (!memTableIdCounter.compareAndSet(oldVal, maxMemTableId)) {
        oldVal = memTableIdCounter.get();
      }
    }
    // update firstValidVersionId and get recover performer from WALRecoverManager
    memTableId2RecoverPerformer = new HashMap<>();
    for (MemTableInfo memTableInfo : memTableId2Info.values()) {
      firstValidVersionId = Math.min(firstValidVersionId, memTableInfo.getFirstFileVersionId());

      UnsealedTsFileRecoverPerformer recoverPerformer =
          walRecoverManger.removeRecoverPerformer(new File(memTableInfo.getTsFilePath()));
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
      endRecovery();
      return;
    }
    // asc sort by version id
    WALFileUtils.ascSortByVersionId(walFiles);
    // read .wal files and redo logs
    for (int i = 0; i < walFiles.length; ++i) {
      File walFile = walFiles[i];
      try (WALByteBufReader reader = new WALByteBufReader(walFile)) {
        if (Collections.disjoint(memTableId2Info.keySet(), reader.getMetaData().getMemTablesId())) {
          continue;
        }
        while (reader.hasNext()) {
          ByteBuffer buffer = reader.next();
          // see WALInfoEntry#serialize, entry type
          buffer.position(Byte.BYTES);
          long memTableId = buffer.getLong();
          if (!memTableId2Info.containsKey(memTableId)) {
            continue;
          }
          buffer.clear();
          WALEntry walEntry =
              WALEntry.deserialize(new DataInputStream(new ByteArrayInputStream(buffer.array())));
          UnsealedTsFileRecoverPerformer recoverPerformer =
              memTableId2RecoverPerformer.get(walEntry.getMemTableId());
          if (recoverPerformer != null) {
            recoverPerformer.redoLog(walEntry);
          } else {
            logger.debug(
                "Fail to find TsFile recover performer for wal entry in TsFile {}", walFile);
          }
        }
      } catch (BrokenWALFileException e) {
        logger.warn(
            "Fail to read memTable ids from the wal file {} of wal node: {}",
            walFile.getAbsoluteFile(),
            e.getMessage());
      } catch (IOException e) {
        logger.warn(
            "Fail to read memTable ids from the wal file {} of wal node.",
            walFile.getAbsoluteFile(),
            e);
      } catch (Exception e) {
        logger.warn("Fail to read wal logs from {}, skip them", walFile, e);
      }
    }
    endRecovery();
  }

  private void endRecovery() {
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
