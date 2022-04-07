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
package org.apache.iotdb.db.wal.node;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.wal.buffer.IWALBuffer;
import org.apache.iotdb.db.wal.buffer.WALBuffer;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.db.wal.checkpoint.CheckpointManager;
import org.apache.iotdb.db.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.wal.io.WALWriter;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.TsFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This class encapsulates {@link IWALBuffer} and {@link CheckpointManager}. */
public class WALNode implements IWALNode {
  public static final Pattern WAL_NODE_FOLDER_PATTERN = Pattern.compile("(?<nodeIdentifier>\\d+)");

  private static final Logger logger = LoggerFactory.getLogger(WALNode.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long WAL_FILE_TTL_IN_MS = config.getWalFileTTLInMs();
  private static final long MEM_TABLE_SNAPSHOT_THRESHOLD_IN_BYTE =
      config.getWalMemTableSnapshotThreshold();

  /** unique identifier of this WALNode */
  private final String identifier;
  /** directory to store this node's files */
  private final String logDirectory;
  /** wal buffer */
  private final IWALBuffer buffer;
  /** manage checkpoints */
  private final CheckpointManager checkpointManager;

  public WALNode(String identifier, String logDirectory) throws FileNotFoundException {
    this.identifier = identifier;
    this.logDirectory = logDirectory;
    File logDirFile = SystemFileFactory.INSTANCE.getFile(logDirectory);
    if (!logDirFile.exists() && logDirFile.mkdirs()) {
      logger.info("create folder {} for wal node-{}.", logDirectory, identifier);
    }
    this.buffer = new WALBuffer(identifier, logDirectory);
    this.checkpointManager = new CheckpointManager(identifier, logDirectory);
  }

  /** Return true when this folder wal node folder */
  public static boolean walNodeFolderNameFilter(File dir, String name) {
    return WAL_NODE_FOLDER_PATTERN.matcher(name).find();
  }

  @Override
  public WALFlushListener log(int memTableId, InsertRowPlan insertRowPlan) {
    WALEntry walEntry = new WALEntry(memTableId, insertRowPlan);
    return log(walEntry);
  }

  @Override
  public WALFlushListener log(
      int memTableId, InsertTabletPlan insertTabletPlan, int start, int end) {
    WALEntry walEntry = new WALEntry(memTableId, insertTabletPlan, start, end);
    return log(walEntry);
  }

  @Override
  public WALFlushListener log(int memTableId, DeletePlan deletePlan) {
    WALEntry walEntry = new WALEntry(memTableId, deletePlan);
    return log(walEntry);
  }

  private WALFlushListener log(WALEntry walEntry) {
    buffer.write(walEntry);
    return walEntry.getWalFlushListener();
  }

  @Override
  public void onMemTableFlushStarted(IMemTable memTable) {
    // do nothing
  }

  @Override
  public void onMemTableFlushed(IMemTable memTable) {
    if (memTable.isSignalMemTable()) {
      return;
    }
    checkpointManager.makeFlushMemTableCP(memTable.getMemTableId());
  }

  @Override
  public void onMemTableCreated(IMemTable memTable, String targetTsFile) {
    if (memTable.isSignalMemTable()) {
      return;
    }
    // use current log version id as first file version id
    int firstFileVersionId = buffer.getCurrentWALFileVersion();
    MemTableInfo memTableInfo = new MemTableInfo(memTable, targetTsFile, firstFileVersionId);
    checkpointManager.makeCreateMemTableCP(memTableInfo);
  }

  // region Task to delete outdated .wal files
  /** Delete outdated .wal files */
  public void deleteOutdatedFiles() {
    new DeleteOutdatedFileTask().run();
  }

  private class DeleteOutdatedFileTask implements Runnable {
    /** .wal files whose version ids are less than first valid version id should be deleted */
    private int firstValidVersionId;

    @Override
    public void run() {
      // init firstValidVersionId
      firstValidVersionId = checkpointManager.getFirstValidWALVersionId();
      if (firstValidVersionId == Integer.MIN_VALUE) {
        firstValidVersionId = buffer.getCurrentWALFileVersion();
      }
      // delete outdated files
      File[] filesToDelete = deleteOutdatedFiles();
      // exceed time limit, update first valid version id by snapshotting or flushing memTable,
      // then delete old .wal files again
      if (filesToDelete != null && filesToDelete.length == 0) {
        File firstWALFile =
            SystemFileFactory.INSTANCE.getFile(
                logDirectory, WALWriter.getLogFileName(firstValidVersionId));
        if (firstWALFile.exists()) {
          long fileCreatedTime = Long.MAX_VALUE;
          try {
            fileCreatedTime =
                Files.readAttributes(firstWALFile.toPath(), BasicFileAttributes.class)
                    .creationTime()
                    .toMillis();
          } catch (IOException e) {
            logger.warn("Fail to get creation time of wal file {}", firstWALFile, e);
          }
          long currentTime = System.currentTimeMillis();
          if (fileCreatedTime + WAL_FILE_TTL_IN_MS < currentTime) {
            snapshotOrFlushMemTable();
            run();
          }
        }
      }
    }

    private File[] deleteOutdatedFiles() {
      File directory = SystemFileFactory.INSTANCE.getFile(logDirectory);
      File[] filesToDelete = directory.listFiles(this::filterFilesToDelete);
      if (filesToDelete != null) {
        for (File file : filesToDelete) {
          if (!file.delete()) {
            logger.info("Fail to delete outdated wal file {} of wal node-{}.", file, identifier);
          }
        }
      }
      return filesToDelete;
    }

    private boolean filterFilesToDelete(File dir, String name) {
      Pattern pattern = WALWriter.WAL_FILE_NAME_PATTERN;
      Matcher matcher = pattern.matcher(name);
      boolean toDelete = false;
      if (matcher.find()) {
        int versionId = Integer.parseInt(matcher.group("versionId"));
        toDelete = versionId < firstValidVersionId;
      }
      return toDelete;
    }

    private void snapshotOrFlushMemTable() {
      // find oldest memTable
      MemTableInfo oldestMemTableInfo = checkpointManager.getOldestMemTableInfo();
      if (oldestMemTableInfo == null) {
        return;
      }

      // get memTable's virtual storage group processor
      File oldestTsFile =
          FSFactoryProducer.getFSFactory().getFile(oldestMemTableInfo.getTsFilePath());
      VirtualStorageGroupProcessor vsgProcessor;
      try {
        vsgProcessor =
            StorageEngine.getInstance()
                .getProcessorByVSGId(
                    new PartialPath(TsFileUtils.getStorageGroup(oldestTsFile)),
                    TsFileUtils.getVirtualStorageGroupId(oldestTsFile));
      } catch (IllegalPathException | StorageEngineException e) {
        logger.error("Fail to get virtual storage group processor for {}", oldestTsFile, e);
        return;
      }

      // snapshot or flush memTable
      if (oldestMemTableInfo.getMemTable().getTVListsRamCost()
          > MEM_TABLE_SNAPSHOT_THRESHOLD_IN_BYTE) {
        vsgProcessor.submitAFlushTask(
            TsFileUtils.getTimePartition(oldestTsFile), TsFileUtils.isSequence(oldestTsFile));
      } else {
        // get vsg write lock to make sure no more writes to the memTable
        vsgProcessor.writeLock("CheckpointManager#snapshotOrFlushOldestMemTable");
        try {
          // update first version id first to make sure snapshot is in the files ≥ current log
          // version
          oldestMemTableInfo.setFirstFileVersionId(buffer.getCurrentWALFileVersion());
          // log snapshot in .wal file
          WALEntry walEntry =
              new WALEntry(
                  oldestMemTableInfo.getMemTableId(), oldestMemTableInfo.getMemTable(), true);
          WALFlushListener flushListener = log(walEntry);
          // wait until getting the result
          // it's low-risk to block writes awhile because this memTable accumulates slowly
          if (flushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
            logger.error("Fail to snapshot memTable of {}", oldestTsFile, flushListener.getCause());
          }
        } finally {
          vsgProcessor.writeUnlock();
        }
      }
    }
  }
  // endregion

  @Override
  public void close() {
    buffer.close();
    checkpointManager.close();
  }

  @TestOnly
  boolean isAllWALEntriesConsumed() {
    return buffer.isAllWALEntriesConsumed();
  }

  @TestOnly
  int getCurrentLogVersion() {
    return buffer.getCurrentWALFileVersion();
  }
}
