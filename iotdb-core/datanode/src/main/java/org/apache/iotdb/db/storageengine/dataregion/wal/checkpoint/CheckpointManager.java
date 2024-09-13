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

package org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.MemTablePinException;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.CheckpointWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.ILogWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.CheckpointFileUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALInsertNodeCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** This class is used to manage checkpoints of one wal node. */
public class CheckpointManager implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(CheckpointManager.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final WritingMetrics WRITING_METRICS = WritingMetrics.getInstance();

  // WALNode identifier of this checkpoint manager
  protected final String identifier;
  // directory to store .checkpoint file
  protected final String logDirectory;
  // protect concurrent safety of checkpoint info
  // including memTableId2Info, cachedByteBuffer, currentLogVersion and currentLogWriter
  private final Lock infoLock = new ReentrantLock();
  // region these variables should be protected by infoLock
  // memTable id -> memTable info
  private final Map<Long, MemTableInfo> memTableId2Info = new ConcurrentHashMap<>();

  // cache the biggest byte buffer to serialize checkpoint
  // it's safe to use volatile here to make this reference thread-safe.
  @SuppressWarnings("squid:S3077")
  private volatile ByteBuffer cachedByteBuffer;

  // max memTable id
  private long maxMemTableId = 0;
  // current checkpoint file version id, only updated by fsyncAndDeleteThread
  private long currentCheckPointFileVersion = 0;
  // current checkpoint file log writer, only updated by fsyncAndDeleteThread
  private ILogWriter currentLogWriter;

  // endregion

  public CheckpointManager(String identifier, String logDirectory) throws IOException {
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

  public List<MemTableInfo> activeOrPinnedMemTables() {
    infoLock.lock();
    try {
      return new ArrayList<>(memTableId2Info.values());
    } finally {
      infoLock.unlock();
    }
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
   * Make checkpoint for global memTables info, this checkpoint only exists in the beginning of each
   * checkpoint file.
   */
  private void makeGlobalInfoCP() {
    long start = System.nanoTime();
    List<MemTableInfo> memTableInfos = activeOrPinnedMemTables();
    memTableInfos.removeIf(MemTableInfo::isFlushed);
    Checkpoint checkpoint = new Checkpoint(CheckpointType.GLOBAL_MEMORY_TABLE_INFO, memTableInfos);
    logByCachedByteBuffer(checkpoint);
    fsyncCheckpointFile();
    WRITING_METRICS.recordMakeCheckpointCost(checkpoint.getType(), System.nanoTime() - start);
  }

  /** Make checkpoint for create memTable info in memory. */
  public void makeCreateMemTableCPInMemory(MemTableInfo memTableInfo) {
    infoLock.lock();
    long start = System.nanoTime();
    try {
      maxMemTableId = Math.max(maxMemTableId, memTableInfo.getMemTableId());
      memTableId2Info.put(memTableInfo.getMemTableId(), memTableInfo);
    } finally {
      WRITING_METRICS.recordMakeCheckpointCost(
          CheckpointType.CREATE_MEMORY_TABLE, System.nanoTime() - start);
      infoLock.unlock();
    }
  }

  /** Make checkpoint for create memTable info on disk. */
  public void makeCreateMemTableCPOnDisk(long memTableId) {
    infoLock.lock();
    long start = System.nanoTime();
    try {
      MemTableInfo memTableInfo = memTableId2Info.get(memTableId);
      if (memTableInfo == null) {
        return;
      }
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

  /** Make checkpoint for flush memTable info. */
  public void makeFlushMemTableCP(long memTableId) {
    infoLock.lock();
    long start = System.nanoTime();
    try {
      MemTableInfo memTableInfo = memTableId2Info.get(memTableId);
      if (memTableInfo == null) {
        return;
      }
      memTableInfo.setFlushed();
      if (!memTableInfo.isPinned()) {
        memTableId2Info.remove(memTableId);
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
  }

  // region Task to fsync checkpoint file
  /** Fsync checkpoints to the disk. */
  public void fsyncCheckpointFile() {
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
          Files.delete(oldFile.toPath());
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

  // region methods for pipe
  /**
   * Pin the wal files of the given memory table. Notice: cannot pin one memTable too long,
   * otherwise the wal disk usage may too large.
   *
   * @throws MemTablePinException If the memTable has been flushed
   */
  public void pinMemTable(long memTableId) throws MemTablePinException {
    infoLock.lock();
    try {
      if (!memTableId2Info.containsKey(memTableId)) {
        throw new MemTablePinException(
            String.format(
                "Fail to pin memTable-%d because this memTable doesn't exist in the wal.",
                memTableId));
      }
      MemTableInfo memTableInfo = memTableId2Info.get(memTableId);
      if (!memTableInfo.isPinned()) {
        WALInsertNodeCache.getInstance(memTableInfo.getDataRegionId()).addMemTable(memTableId);
      }
      memTableInfo.pin();
    } finally {
      infoLock.unlock();
    }
  }

  /**
   * Unpin the wal files of the given memory table.
   *
   * @throws MemTablePinException If there aren't corresponding pin operations
   */
  public void unpinMemTable(long memTableId) throws MemTablePinException {
    infoLock.lock();
    try {
      if (!memTableId2Info.containsKey(memTableId)) {
        throw new MemTablePinException(
            String.format(
                "Fail to unpin memTable-%d because this memTable doesn't exist in the wal.",
                memTableId));
      }
      if (!memTableId2Info.get(memTableId).isPinned()) {
        throw new MemTablePinException(
            String.format(
                "Fail to unpin memTable-%d because this memTable hasn't been pinned.", memTableId));
      }
      MemTableInfo memTableInfo = memTableId2Info.get(memTableId);
      memTableInfo.unpin();
      if (!memTableInfo.isPinned()) {
        WALInsertNodeCache.getInstance(memTableInfo.getDataRegionId()).removeMemTable(memTableId);
        if (memTableInfo.isFlushed()) {
          memTableId2Info.remove(memTableId);
        }
      }
    } finally {
      infoLock.unlock();
    }
  }

  // endregion

  /** Get MemTableInfo of oldest unpinned MemTable, whose first version id is smallest. */
  public MemTableInfo getOldestUnpinnedMemTableInfo() {
    // find oldest memTable
    return activeOrPinnedMemTables().stream()
        .filter(memTableInfo -> !memTableInfo.isPinned())
        .min(Comparator.comparingLong(MemTableInfo::getMemTableId))
        .orElse(null);
  }

  /**
   * Get version id of first valid .wal file
   *
   * @return Return {@link Long#MIN_VALUE} if no file is valid
   */
  public long getFirstValidWALVersionId() {
    List<MemTableInfo> memTableInfos = activeOrPinnedMemTables();
    long firstValidVersionId = memTableInfos.isEmpty() ? Long.MIN_VALUE : Long.MAX_VALUE;
    for (MemTableInfo memTableInfo : memTableInfos) {
      firstValidVersionId = Math.min(firstValidVersionId, memTableInfo.getFirstFileVersionId());
    }
    return firstValidVersionId;
  }

  /** Update wal disk cost of active memTables. */
  public void updateCostOfActiveMemTables(
      Map<Long, Long> memTableId2WalDiskUsage, double compressionRate) {
    for (Map.Entry<Long, Long> memTableWalUsage : memTableId2WalDiskUsage.entrySet()) {
      memTableId2Info.computeIfPresent(
          memTableWalUsage.getKey(),
          (k, v) -> {
            v.addWalDiskUsage((long) (memTableWalUsage.getValue() * compressionRate));
            return v;
          });
    }
  }

  /** Get total cost of active memTables. */
  public long getTotalCostOfActiveMemTables() {
    List<MemTableInfo> memTableInfos = activeOrPinnedMemTables();
    long totalCost = 0;
    for (MemTableInfo memTableInfo : memTableInfos) {
      // flushed memTables are not active
      if (memTableInfo.isFlushed()) {
        continue;
      }
      totalCost += memTableInfo.getWalDiskUsage();
    }

    return totalCost;
  }

  public int getRegionId(long memtableId) {
    final MemTableInfo info = memTableId2Info.get(memtableId);
    if (info == null) {
      if (memtableId != Long.MIN_VALUE && memtableId != TsFileProcessor.MEMTABLE_NOT_EXIST) {
        logger.warn("memtableId {} not found in MemTableId2Info", memtableId);
      }
      return -1;
    }
    return Integer.parseInt(info.getMemTable().getDataRegionId());
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
