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
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.service.metrics.DataNodeExceptionMetrics;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.CheckpointWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.ILogWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.CheckpointFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
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
  // size of the file before any incremental checkpoints are appended
  private long initialCheckpointSize;
  // Both names may survive after a failed publication and rollback. Stop appending until recovery.
  private volatile IOException checkpointPublicationFailure;

  // endregion

  public CheckpointManager(String identifier, String logDirectory) throws IOException {
    this.identifier = identifier;
    this.logDirectory = logDirectory;
    File logDirFile = SystemFileFactory.INSTANCE.getFile(logDirectory);
    if (!logDirFile.exists() && logDirFile.mkdirs()) {
      logger.info(StorageEngineMessages.CREATE_FOLDER_FOR_WAL_BUFFER, logDirectory, identifier);
    }
    File initialFile =
        SystemFileFactory.INSTANCE.getFile(
            logDirectory, CheckpointFileUtils.getLogFileName(currentCheckPointFileVersion));
    currentLogWriter =
        initializeLogWriter(
            initialFile, Files.exists(initialFile.toPath(), LinkOption.NOFOLLOW_LINKS));
    initialCheckpointSize = currentLogWriter.size();
  }

  public List<MemTableInfo> activeOrPinnedMemTables() {
    infoLock.lock();
    try {
      return new ArrayList<>(memTableId2Info.values());
    } finally {
      infoLock.unlock();
    }
  }

  /** Writes the recovery header and global snapshot while the checkpoint state is stable. */
  private void writeInitialCheckpoint(ILogWriter writer) throws IOException {
    writer.write(ByteBuffer.allocate(Long.BYTES).putLong(maxMemTableId));
    long start = System.nanoTime();
    List<MemTableInfo> memTableInfos = activeOrPinnedMemTables();
    memTableInfos.removeIf(MemTableInfo::isFlushed);
    Checkpoint checkpoint = new Checkpoint(CheckpointType.GLOBAL_MEMORY_TABLE_INFO, memTableInfos);
    writeCheckpoint(writer, checkpoint);
    writer.force();
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
      if (memTableInfo == null || checkpointPublicationFailure != null) {
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
      if (memTableInfo == null || checkpointPublicationFailure != null) {
        return;
      }
      memTableInfo.setFlushed();
      memTableId2Info.remove(memTableId);
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
    try {
      writeCheckpoint(currentLogWriter, checkpoint);
    } catch (IOException e) {
      logger.error(StorageEngineMessages.FAIL_TO_MAKE_CHECKPOINT, checkpoint, e);
      DataNodeExceptionMetrics.getInstance().recordSuspiciousDiskException(e);
    }
  }

  private void writeCheckpoint(ILogWriter writer, Checkpoint checkpoint) throws IOException {
    // make sure cached ByteBuffer has enough capacity
    int estimateSize = checkpoint.serializedSize();
    if (cachedByteBuffer == null || estimateSize > cachedByteBuffer.capacity()) {
      cachedByteBuffer = ByteBuffer.allocate(estimateSize);
    }
    try {
      checkpoint.serialize(cachedByteBuffer);
      writer.write(cachedByteBuffer);
    } finally {
      cachedByteBuffer.clear();
    }
  }

  public void checkCheckpointPublication() throws IOException {
    IOException failure = checkpointPublicationFailure;
    if (failure != null) {
      throw failure;
    }
  }

  // region Task to fsync checkpoint file
  /** Fsync checkpoints to the disk. */
  public void fsyncCheckpointFile() {
    infoLock.lock();
    try {
      if (checkpointPublicationFailure != null) {
        return;
      }
      try {
        currentLogWriter.force();
      } catch (IOException e) {
        logger.error(
            StorageEngineMessages
                .STORAGE_LOG_FAIL_TO_FSYNC_WAL_NODE_S_CHECKPOINT_WRITER_CHANGE_SYSTEM_6E1EE226,
            identifier,
            e);
        DataNodeExceptionMetrics.getInstance().recordSuspiciousDiskException(e);
        CommonDescriptor.getInstance().getConfig().handleUnrecoverableError(e);
        return;
      }

      try {
        rollLogWriterIfNeeded();
      } catch (IOException e) {
        logger.error(
            StorageEngineMessages
                .STORAGE_LOG_FAIL_TO_ROLL_WAL_NODE_S_CHECKPOINT_WRITER_CHANGE_SYSTEM_791DDAB7,
            identifier,
            e);
        DataNodeExceptionMetrics.getInstance().recordSuspiciousDiskException(e);
        CommonDescriptor.getInstance().getConfig().handleUnrecoverableError(e);
      }
    } finally {
      infoLock.unlock();
    }
  }

  private void rollLogWriterIfNeeded() throws IOException {
    long size = currentLogWriter.size();
    // A global snapshot can exceed the threshold by itself. Wait for an incremental checkpoint.
    if (size <= initialCheckpointSize || size < config.getCheckpointFileSizeThresholdInByte()) {
      return;
    }
    long nextVersion = currentCheckPointFileVersion + 1;
    File nextLogFile =
        SystemFileFactory.INSTANCE.getFile(
            logDirectory, CheckpointFileUtils.getLogFileName(nextVersion));
    ILogWriter oldWriter = currentLogWriter;
    currentLogWriter = initializeLogWriter(nextLogFile, false);
    currentCheckPointFileVersion = nextVersion;
    initialCheckpointSize = currentLogWriter.size();
    oldWriter.close();
    Files.deleteIfExists(oldWriter.getLogFile().toPath());
  }

  private CheckpointWriter initializeLogWriter(File file, boolean append) throws IOException {
    File candidate = append ? null : CheckpointFileUtils.createCheckpointCandidate(file);
    CheckpointWriter writer = null;
    try {
      writer = openCheckpointWriter(candidate == null ? file : candidate);
      writeInitialCheckpoint(writer);
      if (candidate != null) {
        publishCheckpoint(writer, file, candidate);
      }
      return writer;
    } catch (IOException | RuntimeException | Error e) {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException | RuntimeException | Error closeException) {
          if (e != closeException) {
            e.addSuppressed(closeException);
          }
        }
      }
      if (candidate != null) {
        try {
          Files.deleteIfExists(candidate.toPath());
        } catch (IOException | RuntimeException | Error deleteException) {
          e.addSuppressed(deleteException);
        }
      }
      throw e;
    }
  }

  private void publishCheckpoint(CheckpointWriter writer, File file, File candidate)
      throws IOException {
    try {
      writer.publish(file);
    } catch (IOException | RuntimeException | Error failure) {
      if (file.equals(writer.getLogFile())) {
        // The final name became visible before synchronization failed. Make its removal durable
        // before accepting any more writes through the old checkpoint.
        try {
          writer.publish(candidate);
        } catch (IOException | RuntimeException | Error rollbackFailure) {
          if (rollbackFailure != failure) {
            failure.addSuppressed(rollbackFailure);
          }
          checkpointPublicationFailure =
              failure instanceof IOException ioFailure ? ioFailure : new IOException(failure);
        }
      }
      throw failure;
    }
  }

  CheckpointWriter openCheckpointWriter(File file) throws IOException {
    return new CheckpointWriter(file);
  }

  // endregion

  public MemTableInfo getOldestMemTableInfo() {
    // find oldest memTable
    return activeOrPinnedMemTables().stream()
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
        logger.warn(StorageEngineMessages.MEMTABLE_ID_NOT_FOUND_IN_MAP, memtableId);
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
          logger.error(StorageEngineMessages.FAIL_TO_CLOSE_WAL_CHECKPOINT_WRITER, identifier, e);
          DataNodeExceptionMetrics.getInstance().recordSuspiciousDiskException(e);
        }
      }
    } finally {
      infoLock.unlock();
    }
  }
}
