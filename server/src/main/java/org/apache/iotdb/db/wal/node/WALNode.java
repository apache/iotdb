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

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.wal.buffer.IWALBuffer;
import org.apache.iotdb.db.wal.buffer.WALBuffer;
import org.apache.iotdb.db.wal.buffer.WALEdit;
import org.apache.iotdb.db.wal.checkpoint.CheckpointManager;
import org.apache.iotdb.db.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.wal.io.WALWriter;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This class encapsulates {@link IWALBuffer} and {@link CheckpointManager}. */
public class WALNode implements IWALNode {
  private static final Logger logger = LoggerFactory.getLogger(WALNode.class);
  public static final Pattern WAL_NODE_FOLDER_PATTERN = Pattern.compile("(?<nodeIdentifier>\\d+)");

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

  @Override
  public WALFlushListener log(int memTableId, InsertPlan insertPlan) {
    WALEdit walEdit = new WALEdit(memTableId, insertPlan);
    return log(walEdit);
  }

  @Override
  public WALFlushListener log(int memTableId, DeletePlan deletePlan) {
    WALEdit walEdit = new WALEdit(memTableId, deletePlan);
    return log(walEdit);
  }

  @Override
  public WALFlushListener log(int memTableId, IMemTable memTable) {
    WALEdit walEdit = new WALEdit(memTableId, memTable);
    return log(walEdit);
  }

  private WALFlushListener log(WALEdit walEdit) {
    buffer.write(walEdit);
    return walEdit.getWalFlushListener();
  }

  @Override
  public void onFlushStart(IMemTable memTable) {
    // do nothing
  }

  @Override
  public void onFlushEnd(IMemTable memTable) {
    checkpointManager.makeFlushMemTableCP(memTable.getMemTableId());
  }

  @Override
  public void onMemTableCreated(IMemTable memTable, String targetTsFile) {
    // use current log version id as first file version id
    int firstFileVersionId = buffer.getCurrentLogVersion();
    MemTableInfo memTableInfo =
        new MemTableInfo(memTable.getMemTableId(), targetTsFile, firstFileVersionId);
    checkpointManager.makeCreateMemTableCP(memTableInfo);
  }

  /** Fsync checkpoints to the disk */
  public void fsyncCheckpointFile() {
    checkpointManager.fsyncCheckpointFile();
  }

  // region Task to delete outdated .wal files
  /** Delete outdated .wal files */
  public void deleteOutdatedFiles() {
    int firstValidVersionId = checkpointManager.getFirstValidVersionId();
    if (firstValidVersionId == Integer.MIN_VALUE) {
      firstValidVersionId = buffer.getCurrentLogVersion();
    }
    new DeleteOutdatedFileTask(firstValidVersionId).run();
  }

  private class DeleteOutdatedFileTask implements Runnable {
    /** .wal files whose version ids are less than first valid version id should be deleted */
    private final int firstValidVersionId;

    public DeleteOutdatedFileTask(int firstValidVersionId) {
      this.firstValidVersionId = firstValidVersionId;
    }

    @Override
    public void run() {
      File directory = SystemFileFactory.INSTANCE.getFile(logDirectory);
      File[] filesToDelete = directory.listFiles(this::filterFilesToDelete);
      if (filesToDelete != null) {
        for (File file : filesToDelete) {
          if (!file.delete()) {
            logger.info("Fail to delete outdated wal file {} of wal node-{}.", file, identifier);
          }
        }
      }
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
  }
  // endregion

  @Override
  public void close() {
    buffer.close();
    checkpointManager.close();
  }

  @TestOnly
  boolean isAllWALEditConsumed() {
    return buffer.isAllWALEditConsumed();
  }

  @TestOnly
  int getCurrentLogVersion() {
    return buffer.getCurrentLogVersion();
  }
}
