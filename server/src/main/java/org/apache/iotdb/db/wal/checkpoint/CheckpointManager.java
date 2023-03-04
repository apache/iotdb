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
package org.apache.iotdb.db.wal.checkpoint;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.recorder.WritingMetricsManager;
import org.apache.iotdb.db.wal.io.CheckpointWriter;
import org.apache.iotdb.db.wal.io.ILogWriter;
import org.apache.iotdb.db.wal.utils.CheckpointFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** This class is used to manage checkpoints of one wal node */
public class CheckpointManager implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(CheckpointManager.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final WritingMetricsManager WRITING_METRICS = WritingMetricsManager.getInstance();

  /** WALNode identifier of this checkpoint manager */
  protected final String identifier;
  /** directory to store .checkpoint file */
  protected final String logDirectory;
  /**
   * protect concurrent safety of checkpoint info, including memTableId2Info, cachedByteBuffer,
   * currentLogVersion and currentLogWriter
   */
  private final Lock infoLock = new ReentrantLock();
  // region these variables should be protected by infoLock
  /** memTable id -> memTable info */
  private final Map<Long, MemTableInfo> memTableId2Info = new HashMap<>();
  /** cache the biggest byte buffer to serialize checkpoint */
  private volatile ByteBuffer cachedByteBuffer;
  /** max memTable id */
  private long maxMemTableId = 0;
  /** current checkpoint file version id, only updated by fsyncAndDeleteThread */
  private int currentCheckPointFileVersion = 0;
  /** current checkpoint file log writer, only updated by fsyncAndDeleteThread */
  private ILogWriter currentLogWriter;
  // endregion

  public CheckpointManager(String identifier, String logDirectory) throws FileNotFoundException {
    this.identifier = identifier;
    this.logDirectory = logDirectory;
    File logDirFile = SystemFileFactory.INSTANCE.getFile(logDirectory);
    if (!logDirFile.exists() && logDirFile.mkdirs()) {
      logger.info("create folder {} for wal buffer-{}.", logDirectory, identifier);
    }
    currentLogWriter =
        new CheckpointWriter(
            SystemFileFactory.INSTANCE.getFile(
                logDirectory, CheckpointFileUtils.getLogFileName(currentCheckPointFileVersion)));
    logHeader();
  }

  private void logHeader() {
    infoLock.lock();
    try {
      // log max memTable id
      ByteBuffer tmpBuffer = ByteBuffer.allocate(Long.BYTES);
      tmpBuffer.putLong(maxMemTableId);
      try {
        currentLogWriter.write(tmpBuffer);
      } catch (IOException e) {
        logger.error("Fail to log max memTable id: {}", maxMemTableId, e);
      }
      // log global memTables' info
      makeGlobalInfoCP();
    } finally {
      infoLock.unlock();
    }
  }

  /**
   * make checkpoint for global memTables' info, this checkpoint only exists in the beginning of
   * each checkpoint file
   */
  private void makeGlobalInfoCP() {
    long start = System.nanoTime();
    Checkpoint checkpoint =
        new Checkpoint(
            CheckpointType.GLOBAL_MEMORY_TABLE_INFO, new ArrayList<>(memTableId2Info.values()));
    logByCachedByteBuffer(checkpoint);
    WRITING_METRICS.recordMakeCheckpointCost(checkpoint.getType(), System.nanoTime() - start);
  }

  /** make checkpoint for create memTable info */
  public void makeCreateMemTableCP(MemTableInfo memTableInfo) {
    infoLock.lock();
    long start = System.nanoTime();
    try {
      maxMemTableId = Math.max(maxMemTableId, memTableInfo.getMemTableId());
      memTableId2Info.put(memTableInfo.getMemTableId(), memTableInfo);
      Checkpoint checkpoint =
          new Checkpoint(
              CheckpointType.CREATE_MEMORY_TABLE, Collections.singletonList(memTableInfo));
      logByCachedByteBuffer(checkpoint);
    } finally {
      WRITING_METRICS.recordMakeCheckpointCost(
          CheckpointType.CREATE_MEMORY_TABLE, System.nanoTime() - start);
      infoLock.unlock();
    }
  }

  /** make checkpoint for flush memTable info */
  public void makeFlushMemTableCP(long memTableId) {
    infoLock.lock();
    long start = System.nanoTime();
    try {
      MemTableInfo memTableInfo = memTableId2Info.remove(memTableId);
      if (memTableInfo == null) {
        return;
      }
      Checkpoint checkpoint =
          new Checkpoint(
              CheckpointType.FLUSH_MEMORY_TABLE, Collections.singletonList(memTableInfo));
      logByCachedByteBuffer(checkpoint);
    } finally {
      WRITING_METRICS.recordMakeCheckpointCost(
          CheckpointType.FLUSH_MEMORY_TABLE, System.nanoTime() - start);
      infoLock.unlock();
    }
  }

  private void logByCachedByteBuffer(Checkpoint checkpoint) {
    // make sure cached ByteBuffer has enough capacity
    int estimateSize = checkpoint.serializedSize();
    if (cachedByteBuffer == null || estimateSize > cachedByteBuffer.capacity()) {
      cachedByteBuffer = ByteBuffer.allocate(estimateSize);
    }
    checkpoint.serialize(cachedByteBuffer);

    try {
      currentLogWriter.write(cachedByteBuffer);
    } catch (IOException e) {
      logger.error("Fail to make checkpoint: {}", checkpoint, e);
    } finally {
      cachedByteBuffer.clear();
    }

    fsyncCheckpointFile();
  }

  // region Task to fsync checkpoint file
  /** Fsync checkpoints to the disk */
  private void fsyncCheckpointFile() {
    infoLock.lock();
    try {
      try {
        currentLogWriter.force();
      } catch (IOException e) {
        logger.error(
            "Fail to fsync wal node-{}'s checkpoint writer, change system mode to error.",
            identifier,
            e);
        CommonDescriptor.getInstance().getConfig().handleUnrecoverableError();
      }

      try {
        if (tryRollingLogWriter()) {
          // first log max memTable id and global memTables' info, then delete old checkpoint file
          logHeader();
          currentLogWriter.force();
          File oldFile =
              SystemFileFactory.INSTANCE.getFile(
                  logDirectory,
                  CheckpointFileUtils.getLogFileName(currentCheckPointFileVersion - 1));
          oldFile.delete();
        }
      } catch (IOException e) {
        logger.error(
            "Fail to roll wal node-{}'s checkpoint writer, change system mode to error.",
            identifier,
            e);
        CommonDescriptor.getInstance().getConfig().handleUnrecoverableError();
      }
    } finally {
      infoLock.unlock();
    }
  }

  private boolean tryRollingLogWriter() throws IOException {
    if (currentLogWriter.size() < config.getCheckpointFileSizeThresholdInByte()) {
      return false;
    }
    currentLogWriter.close();
    currentCheckPointFileVersion++;
    File nextLogFile =
        SystemFileFactory.INSTANCE.getFile(
            logDirectory, CheckpointFileUtils.getLogFileName(currentCheckPointFileVersion));
    currentLogWriter = new CheckpointWriter(nextLogFile);
    return true;
  }
  // endregion

  /** Get MemTableInfo of oldest MemTable, whose first version id is smallest */
  public MemTableInfo getOldestMemTableInfo() {
    // find oldest memTable
    List<MemTableInfo> memTableInfos;
    infoLock.lock();
    try {
      memTableInfos = new ArrayList<>(memTableId2Info.values());
    } finally {
      infoLock.unlock();
    }
    if (memTableInfos.isEmpty()) {
      return null;
    }
    MemTableInfo oldestMemTableInfo = memTableInfos.get(0);
    for (MemTableInfo memTableInfo : memTableInfos) {
      if (oldestMemTableInfo.getFirstFileVersionId() > memTableInfo.getFirstFileVersionId()) {
        oldestMemTableInfo = memTableInfo;
      }
    }
    return oldestMemTableInfo;
  }

  /**
   * Get version id of first valid .wal file
   *
   * @return Return {@link Long#MIN_VALUE} if no file is valid
   */
  public long getFirstValidWALVersionId() {
    List<MemTableInfo> memTableInfos;
    infoLock.lock();
    try {
      memTableInfos = new ArrayList<>(memTableId2Info.values());
    } finally {
      infoLock.unlock();
    }
    long firstValidVersionId = memTableInfos.isEmpty() ? Long.MIN_VALUE : Long.MAX_VALUE;
    for (MemTableInfo memTableInfo : memTableInfos) {
      firstValidVersionId = Math.min(firstValidVersionId, memTableInfo.getFirstFileVersionId());
    }
    return firstValidVersionId;
  }

  /** Get total cost of active memTables */
  public long getTotalCostOfActiveMemTables() {
    long totalCost = 0;

    if (!config.isEnableMemControl()) {
      infoLock.lock();
      try {
        totalCost = memTableId2Info.size();
      } finally {
        infoLock.unlock();
      }
    } else {
      List<MemTableInfo> memTableInfos;
      infoLock.lock();
      try {
        memTableInfos = new ArrayList<>(memTableId2Info.values());
      } finally {
        infoLock.unlock();
      }
      for (MemTableInfo memTableInfo : memTableInfos) {
        totalCost += memTableInfo.getMemTable().getTVListsRamCost();
      }
    }

    return totalCost;
  }

  @Override
  public void close() {
    infoLock.lock();
    try {
      if (currentLogWriter != null) {
        try {
          currentLogWriter.close();
        } catch (IOException e) {
          logger.error("Fail to close wal node-{}'s checkpoint writer.", identifier, e);
        }
      }
    } finally {
      infoLock.unlock();
    }
  }
}
