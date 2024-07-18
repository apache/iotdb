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

package org.apache.iotdb.db.storageengine.dataregion.wal.node;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ContinuousSameSearchIndexSeparatorNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushStatus;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALBuffer;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALBuffer;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALSignalEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.Checkpoint;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.CheckpointManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.CheckpointType;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.MemTablePinException;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.AbstractResultListener;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.AbstractResultListener.Status;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;

import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.utils.TsFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * This class encapsulates {@link IWALBuffer} and {@link CheckpointManager}. If search is enabled,
 * the order of search index should be protected by the upper layer, and the value should start from
 * 1.
 */
public class WALNode implements IWALNode {
  private static final Logger logger = LoggerFactory.getLogger(WALNode.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  // no iot consensus, all insert nodes can be safely deleted
  public static final long DEFAULT_SAFELY_DELETED_SEARCH_INDEX = Long.MAX_VALUE;
  // timeout threshold when waiting for next wal entry
  private static final long WAIT_FOR_NEXT_WAL_ENTRY_TIMEOUT_IN_SEC = 30;
  private static final WritingMetrics WRITING_METRICS = WritingMetrics.getInstance();

  // unique identifier of this WALNode
  private final String identifier;
  // directory to store this node's files
  private final File logDirectory;
  // wal buffer
  private final WALBuffer buffer;
  // manage checkpoints
  private final CheckpointManager checkpointManager;
  // memTable id -> memTable snapshot count
  // used to avoid write amplification caused by frequent snapshot
  private final Map<Long, Integer> memTableSnapshotCount = new ConcurrentHashMap<>();
  // insert nodes whose search index are before this value can be deleted safely
  private volatile long safelyDeletedSearchIndex = DEFAULT_SAFELY_DELETED_SEARCH_INDEX;

  public WALNode(String identifier, String logDirectory) throws IOException {
    this(identifier, logDirectory, 0, 0L);
  }

  public WALNode(
      String identifier, String logDirectory, long startFileVersion, long startSearchIndex)
      throws IOException {
    this.identifier = identifier;
    this.logDirectory = SystemFileFactory.INSTANCE.getFile(logDirectory);
    if (!this.logDirectory.exists() && this.logDirectory.mkdirs()) {
      logger.info("create folder {} for wal node-{}.", logDirectory, identifier);
    }
    this.checkpointManager = new CheckpointManager(identifier, logDirectory);
    this.buffer =
        new WALBuffer(
            identifier, logDirectory, checkpointManager, startFileVersion, startSearchIndex);
  }

  @Override
  public WALFlushListener log(long memTableId, InsertRowNode insertRowNode) {
    logger.debug(
        "WAL node-{} logs insertRowNode, the search index is {}.",
        identifier,
        insertRowNode.getSearchIndex());
    WALEntry walEntry = new WALInfoEntry(memTableId, insertRowNode);
    return log(walEntry);
  }

  @Override
  public WALFlushListener log(long memTableId, InsertRowsNode insertRowsNode) {
    logger.debug(
        "WAL node-{} logs insertRowsNode, the search index is {}.",
        identifier,
        insertRowsNode.getSearchIndex());
    WALEntry walEntry = new WALInfoEntry(memTableId, insertRowsNode);
    return log(walEntry);
  }

  @Override
  public WALFlushListener log(
      long memTableId, InsertTabletNode insertTabletNode, int start, int end) {
    logger.debug(
        "WAL node-{} logs insertTabletNode, the search index is {}.",
        identifier,
        insertTabletNode.getSearchIndex());
    WALEntry walEntry = new WALInfoEntry(memTableId, insertTabletNode, start, end);
    return log(walEntry);
  }

  @Override
  public WALFlushListener log(long memTableId, DeleteDataNode deleteDataNode) {
    logger.debug(
        "WAL node-{} logs deleteDataNode, the search index is {}.",
        identifier,
        deleteDataNode.getSearchIndex());
    WALEntry walEntry = new WALInfoEntry(memTableId, deleteDataNode);
    return log(walEntry);
  }

  @Override
  public WALFlushListener log(
      long memTableId, ContinuousSameSearchIndexSeparatorNode separatorNode) {
    WALEntry walEntry = new WALInfoEntry(memTableId, separatorNode);
    return log(walEntry);
  }

  private WALFlushListener log(WALEntry walEntry) {

    buffer.write(walEntry);
    // set handler for pipe
    walEntry.getWalFlushListener().getWalEntryHandler().setWalNode(this, walEntry.getMemTableId());
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

    MemTableInfo memTableInfo = new MemTableInfo(memTable, null, -1);
    Checkpoint checkpoint =
        new Checkpoint(CheckpointType.FLUSH_MEMORY_TABLE, Collections.singletonList(memTableInfo));
    buffer.write(new WALInfoEntry(memTable.getMemTableId(), checkpoint));

    // remove snapshot info
    memTableSnapshotCount.remove(memTable.getMemTableId());
  }

  @Override
  public void onMemTableCreated(IMemTable memTable, String targetTsFile) {
    if (memTable.isSignalMemTable()) {
      return;
    }
    // use current log version id as first file version id
    long firstFileVersionId = buffer.getCurrentWALFileVersion();
    MemTableInfo memTableInfo = new MemTableInfo(memTable, targetTsFile, firstFileVersionId);
    checkpointManager.makeCreateMemTableCPInMemory(memTableInfo);

    Checkpoint checkpoint =
        new Checkpoint(CheckpointType.CREATE_MEMORY_TABLE, Collections.singletonList(memTableInfo));
    buffer.write(new WALInfoEntry(memTable.getMemTableId(), checkpoint));
  }

  // region methods for pipe

  /**
   * Pin the wal files of the given memory table. Notice: cannot pin one memTable too long,
   * otherwise the wal disk usage may too large.
   *
   * @throws MemTablePinException If the memTable has been flushed
   */
  public void pinMemTable(long memTableId) throws MemTablePinException {
    checkpointManager.pinMemTable(memTableId);
  }

  /**
   * Unpin the wal files of the given memory table.
   *
   * @throws MemTablePinException If there aren't corresponding pin operations
   */
  public void unpinMemTable(long memTableId) throws MemTablePinException {
    checkpointManager.unpinMemTable(memTableId);
  }

  // endregion

  // region Task to delete outdated .wal files

  /** Delete outdated .wal files. */
  public void deleteOutdatedFiles() {
    try {
      new DeleteOutdatedFileTask().run();
    } catch (Exception e) {
      logger.error("Fail to delete wal node-{}'s outdated files.", identifier, e);
    }
  }

  private class DeleteOutdatedFileTask implements Runnable {
    private File[] sortedWalFilesExcludingLast;

    private List<MemTableInfo> activeOrPinnedMemTables;

    private static final int MAX_RECURSION_TIME = 5;

    // the effective information ratio
    private double effectiveInfoRatio = 1.0d;

    private List<Long> pinnedMemTableIds;

    private int fileIndexAfterFilterSafelyDeleteIndex = Integer.MAX_VALUE;
    private List<Long> successfullyDeleted;
    private long deleteFileSize;

    private int recursionTime = 0;

    public DeleteOutdatedFileTask() {
      // Do nothing
    }

    private boolean initAndCheckIfNeedContinue() {
      rollWalFileIfHaveNoActiveMemTable();
      File[] allWalFilesOfOneNode = WALFileUtils.listAllWALFiles(logDirectory);
      if (allWalFilesOfOneNode == null || allWalFilesOfOneNode.length <= 1) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "wal node-{}:no wal file or wal file number less than or equal to one was found",
              identifier);
        }
        return false;
      }
      WALFileUtils.ascSortByVersionId(allWalFilesOfOneNode);
      this.sortedWalFilesExcludingLast =
          Arrays.copyOfRange(allWalFilesOfOneNode, 0, allWalFilesOfOneNode.length - 1);
      this.activeOrPinnedMemTables = checkpointManager.activeOrPinnedMemTables();
      this.pinnedMemTableIds = initPinnedMemTableIds();
      this.fileIndexAfterFilterSafelyDeleteIndex = initFileIndexAfterFilterSafelyDeleteIndex();
      this.successfullyDeleted = new ArrayList<>();
      this.deleteFileSize = 0;
      return true;
    }

    /**
     * This means that the relevant memTable in the file has been successfully flushed, so we should
     * scroll through a new wal file so that the current file can be deleted
     */
    public void rollWalFileIfHaveNoActiveMemTable() {
      long firstVersionId = checkpointManager.getFirstValidWALVersionId();
      if (firstVersionId == Long.MIN_VALUE) {
        // roll wal log writer to delete current wal file
        if (buffer.getCurrentWALFileSize() > 0) {
          rollWALFile();
        }
      }
    }

    private List<Long> initPinnedMemTableIds() {
      List<MemTableInfo> memTableInfos = checkpointManager.activeOrPinnedMemTables();
      if (memTableInfos.isEmpty()) {
        return new ArrayList<>();
      }
      List<Long> pinnedIds = new ArrayList<>();
      for (MemTableInfo memTableInfo : memTableInfos) {
        if (memTableInfo.isFlushed() && memTableInfo.isPinned()) {
          pinnedIds.add(memTableInfo.getMemTableId());
        }
      }
      return pinnedIds;
    }

    @Override
    public void run() {
      // The intent of the loop execution here is to try to get as many memTable flush or snapshot
      // as possible when the valid information ratio is less than the configured value.
      while (recursionTime < MAX_RECURSION_TIME) {
        // init delete outdated file task fields, if the number of wal files is less than one, the
        // subsequent logic is not executed
        if (!initAndCheckIfNeedContinue()) {
          break;
        }

        // delete outdated WAL files and record which delete successfully and which delete failed.
        deleteOutdatedFilesAndUpdateMetric();

        // summary the execution result and output a log
        summarizeExecuteResult();

        // update current effective info ration
        updateEffectiveInfoRationAndUpdateMetric();

        // decide whether to snapshot or flush based on the effective info ration and throttle
        // threshold
        if (trySnapshotOrFlushMemTable()
            && safelyDeletedSearchIndex != DEFAULT_SAFELY_DELETED_SEARCH_INDEX) {
          return;
        }
        recursionTime++;
      }
    }

    private void updateEffectiveInfoRationAndUpdateMetric() {
      // calculate effective information ratio
      long costOfActiveMemTables = checkpointManager.getTotalCostOfActiveMemTables();
      MemTableInfo oldestUnpinnedMemTableInfo = checkpointManager.getOldestUnpinnedMemTableInfo();
      long avgFileSize =
          getFileNum() != 0
              ? getDiskUsage() / getFileNum()
              : config.getWalFileSizeThresholdInByte();
      long totalCost =
          oldestUnpinnedMemTableInfo == null
              ? costOfActiveMemTables
              : (getCurrentWALFileVersion() - oldestUnpinnedMemTableInfo.getFirstFileVersionId())
                  * avgFileSize;
      if (costOfActiveMemTables == 0 || totalCost == 0) {
        effectiveInfoRatio = 1.0d;
        return;
      }
      effectiveInfoRatio = (double) costOfActiveMemTables / totalCost;
      logger.debug(
          "Effective information ratio is {}, active memTables cost is {}, total cost is {}",
          effectiveInfoRatio,
          costOfActiveMemTables,
          totalCost);
      WRITING_METRICS.recordWALNodeEffectiveInfoRatio(identifier, effectiveInfoRatio);
    }

    private void summarizeExecuteResult() {
      if (!pinnedMemTableIds.isEmpty()
          || fileIndexAfterFilterSafelyDeleteIndex < sortedWalFilesExcludingLast.length) {
        if (logger.isDebugEnabled()) {
          StringBuilder summary =
              new StringBuilder(
                  String.format(
                      "wal node-%s delete outdated files summary:the range is: [%d,%d], delete successful is [%s], safely delete file index is: [%s].The following reasons influenced the result: %s",
                      identifier,
                      WALFileUtils.parseVersionId(sortedWalFilesExcludingLast[0].getName()),
                      WALFileUtils.parseVersionId(
                          sortedWalFilesExcludingLast[sortedWalFilesExcludingLast.length - 1]
                              .getName()),
                      StringUtils.join(successfullyDeleted, ","),
                      fileIndexAfterFilterSafelyDeleteIndex,
                      System.getProperty("line.separator")));

          if (!pinnedMemTableIds.isEmpty()) {
            summary
                .append("- MemTable has been flushed but pinned by PIPE, the MemTableId list is : ")
                .append(StringUtils.join(pinnedMemTableIds, ","))
                .append(".")
                .append(System.getProperty("line.separator"));
          }
          if (fileIndexAfterFilterSafelyDeleteIndex < sortedWalFilesExcludingLast.length) {
            summary.append(
                String.format(
                    "- The data in the wal file was not consumed by the consensus group,current search index is %d, safely delete index is %d",
                    getCurrentSearchIndex(), safelyDeletedSearchIndex));
          }
          String summaryLog = summary.toString();
          logger.debug(summaryLog);
        }

      } else {
        logger.debug(
            "Successfully delete {} outdated wal files for wal node-{}",
            successfullyDeleted.size(),
            identifier);
      }
    }

    /** Delete obsolete wal files while recording which succeeded or failed */
    private void deleteOutdatedFilesAndUpdateMetric() {
      for (int fileArrIdx = 0; fileArrIdx < sortedWalFilesExcludingLast.length; ++fileArrIdx) {
        File currentWal = sortedWalFilesExcludingLast[fileArrIdx];
        WALFileStatus walFileStatus = WALFileUtils.parseStatusCode(currentWal.getName());
        long versionId = WALFileUtils.parseVersionId(currentWal.getName());
        if (canDeleteFile(fileArrIdx, walFileStatus, versionId)) {
          long fileSize = currentWal.length();
          if (currentWal.delete()) {
            deleteFileSize += fileSize;
            buffer.removeMemTableIdsOfWal(versionId);
            successfullyDeleted.add(versionId);
          } else {
            logger.info(
                "Fail to delete outdated wal file {} of wal node-{}.", currentWal, identifier);
          }
        }
      }
      buffer.subtractDiskUsage(deleteFileSize);
      buffer.subtractFileNum(successfullyDeleted.size());
    }

    private int initFileIndexAfterFilterSafelyDeleteIndex() {
      return safelyDeletedSearchIndex == DEFAULT_SAFELY_DELETED_SEARCH_INDEX
          ? sortedWalFilesExcludingLast.length
          : WALFileUtils.binarySearchFileBySearchIndex(
              sortedWalFilesExcludingLast, safelyDeletedSearchIndex + 1);
    }

    /** Return true iff effective information ratio is too small or disk usage is too large. */
    private boolean shouldSnapshotOrFlush() {
      return effectiveInfoRatio < config.getWalMinEffectiveInfoRatio()
          || WALManager.getInstance().shouldThrottle();
    }

    /**
     * Snapshot or flush one memTable.
     *
     * @return true if snapshot or flush is executed successfully
     */
    private boolean trySnapshotOrFlushMemTable() {
      if (!shouldSnapshotOrFlush()) {
        return false;
      }
      // find oldest memTable
      MemTableInfo oldestMemTableInfo = checkpointManager.getOldestUnpinnedMemTableInfo();
      if (oldestMemTableInfo == null) {
        return false;
      }
      if (oldestMemTableInfo.isPinned()) {
        logger.warn(
            "Pipe: Effective information ratio {} of wal node-{} is below wal min effective info ratio {}. But fail to delete memTable-{}'s wal files because they are pinned by the Pipe module. Pin count: {}.",
            effectiveInfoRatio,
            identifier,
            config.getWalMinEffectiveInfoRatio(),
            oldestMemTableInfo.getMemTableId(),
            oldestMemTableInfo.getPinCount());
        return false;
      }
      IMemTable oldestMemTable = oldestMemTableInfo.getMemTable();
      if (oldestMemTable == null) {
        return false;
      }
      // get memTable's virtual database processor
      File oldestTsFile =
          FSFactoryProducer.getFSFactory().getFile(oldestMemTableInfo.getTsFilePath());
      DataRegion dataRegion;
      try {
        dataRegion =
            StorageEngine.getInstance()
                .getDataRegion(new DataRegionId(TsFileUtils.getDataRegionId(oldestTsFile)));
      } catch (Exception e) {
        logger.error("Fail to get data region processor for {}", oldestTsFile, e);
        return false;
      }
      if (dataRegion == null) {
        return false;
      }

      // snapshot or flush memTable, flush memTable when it belongs to an old time partition, or
      // it's snapshot count or size reach threshold.
      int snapshotCount = memTableSnapshotCount.getOrDefault(oldestMemTable.getMemTableId(), 0);
      long oldestMemTableTVListsRamCost = oldestMemTable.getTVListsRamCost();
      if (TsFileUtils.getTimePartition(new File(oldestMemTableInfo.getTsFilePath()))
              < dataRegion.getLatestTimePartition()
          || snapshotCount >= config.getMaxWalMemTableSnapshotNum()
          || oldestMemTableTVListsRamCost > config.getWalMemTableSnapshotThreshold()) {
        flushMemTable(dataRegion, oldestTsFile, oldestMemTable);
        WRITING_METRICS.recordWalFlushMemTableCount(1);
        WRITING_METRICS.recordMemTableRamWhenCauseFlush(identifier, oldestMemTableTVListsRamCost);
      } else {
        snapshotMemTable(dataRegion, oldestTsFile, oldestMemTableInfo);
      }
      return true;
    }

    private void flushMemTable(DataRegion dataRegion, File tsFile, IMemTable memTable) {
      boolean submitted = true;
      if (memTable.getFlushStatus() == FlushStatus.WORKING) {
        submitted =
            dataRegion.submitAFlushTask(
                TsFileUtils.getTimePartition(tsFile), TsFileUtils.isSequence(tsFile), memTable);
        logger.info(
            "WAL node-{} flushes memTable-{} to TsFile {} because Effective information ratio {} is below wal min effective info ratio {}, memTable size is {}.",
            identifier,
            memTable.getMemTableId(),
            tsFile,
            String.format("%.4f", effectiveInfoRatio),
            config.getWalMinEffectiveInfoRatio(),
            memTable.getTVListsRamCost());
      }

      // it's fine to wait until memTable has been flushed, because deleting files is not urgent.
      if (submitted || memTable.getFlushStatus() == FlushStatus.FLUSHING) {
        long sleepTime = 0;
        while (memTable.getFlushStatus() != FlushStatus.FLUSHED) {
          try {
            Thread.sleep(1_000);
            sleepTime += 1_000;
            if (sleepTime > 10_000) {
              logger.warn("Waiting too long for memTable flush to be done.");
              break;
            }
          } catch (InterruptedException e) {
            logger.warn("Interrupted when waiting for memTable flush to be done.");
            Thread.currentThread().interrupt();
          }
        }
      }
    }

    // synchronize memTable to make sure snapshot is made before memTable flush operation
    private void snapshotMemTable(DataRegion dataRegion, File tsFile, MemTableInfo memTableInfo) {
      IMemTable memTable = memTableInfo.getMemTable();

      // get dataRegion write lock to make sure no more writes to the memTable
      dataRegion.writeLock(
          "CheckpointManager$DeleteOutdatedFileTask.snapshotOrFlushOldestMemTable");
      try {
        // make sure snapshot is made before memTable flush operation
        synchronized (memTableInfo) {
          if (memTable == null || memTable.getFlushStatus() != FlushStatus.WORKING) {
            return;
          }

          // update snapshot count
          memTableSnapshotCount.compute(memTable.getMemTableId(), (k, v) -> v == null ? 1 : v + 1);
          // roll wal log writer to make sure first version id will be updated
          WALEntry rollWALFileSignal =
              new WALSignalEntry(WALEntryType.ROLL_WAL_LOG_WRITER_SIGNAL, true);
          WALFlushListener fileRolledListener = log(rollWALFileSignal);
          if (fileRolledListener.waitForResult() == Status.FAILURE) {
            logger.error("Fail to roll wal log writer.", fileRolledListener.getCause());
            return;
          }

          // update first version id first to make sure snapshot is in the files ≥ current log
          // version
          memTableInfo.setFirstFileVersionId(buffer.getCurrentWALFileVersion());

          // log snapshot in a new .wal file
          WALEntry walEntry = new WALInfoEntry(memTable.getMemTableId(), memTable, true);
          WALFlushListener flushListener = log(walEntry);

          // wait until getting the result
          // it's low-risk to block writes awhile because this memTable accumulates slowly
          if (flushListener.waitForResult() == Status.FAILURE) {
            logger.error("Fail to snapshot memTable of {}", tsFile, flushListener.getCause());
            return;
          }
          logger.info(
              "WAL node-{} snapshots memTable-{} to wal files because Effective information ratio {} is below wal min effective info ratio {}, memTable size is {}.",
              identifier,
              memTable.getMemTableId(),
              String.format("%.4f", effectiveInfoRatio),
              config.getWalMinEffectiveInfoRatio(),
              memTable.getTVListsRamCost());
          WRITING_METRICS.recordMemTableRamWhenCauseSnapshot(
              identifier, memTable.getTVListsRamCost());
        }
      } finally {
        dataRegion.writeUnlock();
      }
    }

    public boolean isContainsActiveOrPinnedMemTable(Long versionId) {
      Set<Long> memTableIdsOfCurrentWal = buffer.getMemTableIds(versionId);
      // If this set is empty, there is a case where WalEntry has been logged but not persisted,
      // because WalEntry is persisted asynchronously. In this case, the file cannot be deleted
      // directly, so it is considered active
      if (memTableIdsOfCurrentWal == null) {
        return true;
      }
      return !Collections.disjoint(
          activeOrPinnedMemTables.stream()
              .map(MemTableInfo::getMemTableId)
              .collect(Collectors.toSet()),
          memTableIdsOfCurrentWal);
    }

    private boolean canDeleteFile(long fileArrIdx, WALFileStatus walFileStatus, long versionId) {
      return (fileArrIdx < fileIndexAfterFilterSafelyDeleteIndex
              || walFileStatus == WALFileStatus.CONTAINS_NONE_SEARCH_INDEX)
          && !isContainsActiveOrPinnedMemTable(versionId);
    }
  }

  // endregion

  // region Search interfaces for consensus group
  @Override
  public void setSafelyDeletedSearchIndex(long safelyDeletedSearchIndex) {
    this.safelyDeletedSearchIndex = safelyDeletedSearchIndex;
  }

  /** This iterator is not concurrency-safe, cannot read the current-writing wal file. */
  @Override
  public ReqIterator getReqIterator(long startIndex) {
    return new PlanNodeIterator(startIndex);
  }

  private class PlanNodeIterator implements ReqIterator {
    /** search index of next element */
    private long nextSearchIndex;

    /** files to search */
    private File[] filesToSearch = null;

    /** index of current searching file in the filesToSearch */
    private int currentFileIndex = -1;

    /** true means filesToSearch and currentFileIndex are outdated, call updateFilesToSearch */
    private boolean needUpdatingFilesToSearch = true;

    /** batch store insert nodes */
    private final LinkedList<IndexedConsensusRequest> insertNodes = new LinkedList<>();

    /** iterator of insertNodes */
    private ListIterator<IndexedConsensusRequest> itr = null;

    /** last broken wal file's version id */
    private long brokenFileId = -1;

    public PlanNodeIterator(long startIndex) {
      this.nextSearchIndex = startIndex;
    }

    @Override
    public boolean hasNext() {
      if (itr != null && itr.hasNext()) {
        return true;
      }

      // clear outdated iterator
      insertNodes.clear();
      itr = null;
      if (filesToSearch == null || currentFileIndex >= filesToSearch.length - 1) {
        needUpdatingFilesToSearch = true;
      }

      // update files to search
      if (needUpdatingFilesToSearch) {
        updateFilesToSearch();
        if (needUpdatingFilesToSearch) {
          logger.debug(
              "update file to search failed, the next search index is {}", nextSearchIndex);
          return false;
        }
      }

      // find file contains search index
      while (WALFileUtils.parseStatusCode(filesToSearch[currentFileIndex].getName())
          == WALFileStatus.CONTAINS_NONE_SEARCH_INDEX) {
        currentFileIndex++;
        if (currentFileIndex >= filesToSearch.length - 1) {
          needUpdatingFilesToSearch = true;
          return false;
        }
      }

      // find all nodes of current wal file
      List<IConsensusRequest> tmpNodes = new ArrayList<>();
      long targetIndex = nextSearchIndex;
      try (WALByteBufReader walByteBufReader =
          new WALByteBufReader(filesToSearch[currentFileIndex])) {
        while (walByteBufReader.hasNext()) {
          ByteBuffer buffer = walByteBufReader.next();
          WALEntryType type = WALEntryType.valueOf(buffer.get());
          if (type.needSearch()) {
            // see WALInfoEntry#serialize, entry type + memtable id + plan node type
            buffer.position(WALInfoEntry.FIXED_SERIALIZED_SIZE + PlanNodeType.BYTES);
            long currentIndex = buffer.getLong();
            buffer.clear();
            if (currentIndex == targetIndex) {
              tmpNodes.add(new IoTConsensusRequest(buffer));
            } else {
              // different search index, all slices found
              if (!tmpNodes.isEmpty()) {
                insertNodes.add(new IndexedConsensusRequest(targetIndex, tmpNodes));
                tmpNodes = new ArrayList<>();
              }
              // remember to add current plan node
              if (currentIndex > targetIndex) {
                tmpNodes.add(new IoTConsensusRequest(buffer));
                targetIndex = currentIndex;
              }
            }
          } else if (!tmpNodes.isEmpty()) {
            // next entry doesn't need to be searched, all slices found
            insertNodes.add(new IndexedConsensusRequest(targetIndex, tmpNodes));
            targetIndex++;
            tmpNodes = new ArrayList<>();
          }
        }
      } catch (FileNotFoundException e) {
        logger.debug(
            "WAL file {} has been deleted, try to find next {} again.",
            identifier,
            nextSearchIndex);
        reset();
        return hasNext();
      } catch (Exception e) {
        brokenFileId = WALFileUtils.parseVersionId(filesToSearch[currentFileIndex].getName());
        logger.error(
            "Fail to read wal from wal file {}, skip this file.",
            filesToSearch[currentFileIndex],
            e);
        // skip this file when it's broken from the beginning
        if (insertNodes.isEmpty() && tmpNodes.isEmpty()) {
          currentFileIndex++;
          return hasNext();
        }
      }

      // find remaining slices of last plan node of targetIndex
      if (tmpNodes.isEmpty()) { // all plan nodes scanned
        currentFileIndex++;
      } else {
        int fileIndex = currentFileIndex + 1;
        while (!tmpNodes.isEmpty() && fileIndex < filesToSearch.length - 1) {
          // cannot find any in this file, so all slices of last plan node are found
          if (WALFileUtils.parseStatusCode(filesToSearch[fileIndex].getName())
              == WALFileStatus.CONTAINS_NONE_SEARCH_INDEX) {
            insertNodes.add(new IndexedConsensusRequest(targetIndex, tmpNodes));
            tmpNodes = Collections.emptyList();
            break;
          }
          // read until find all plan nodes whose search index equals target index
          try (WALByteBufReader walByteBufReader = new WALByteBufReader(filesToSearch[fileIndex])) {
            // first search index are different, so all slices of last plan node are found
            if (walByteBufReader.getFirstSearchIndex() != targetIndex) {
              insertNodes.add(new IndexedConsensusRequest(targetIndex, tmpNodes));
              tmpNodes = Collections.emptyList();
              break;
            } else {
              // read until one node has different search index
              while (walByteBufReader.hasNext()) {
                ByteBuffer buffer = walByteBufReader.next();
                WALEntryType type = WALEntryType.valueOf(buffer.get());
                if (type.needSearch()) {
                  // see WALInfoEntry#serialize, entry type + memtable id + plan node type
                  buffer.position(WALInfoEntry.FIXED_SERIALIZED_SIZE + PlanNodeType.BYTES);
                  long currentIndex = buffer.getLong();
                  buffer.clear();
                  if (currentIndex == targetIndex) {
                    tmpNodes.add(new IoTConsensusRequest(buffer));
                  } else { // find all slices of plan node
                    insertNodes.add(new IndexedConsensusRequest(targetIndex, tmpNodes));
                    tmpNodes = Collections.emptyList();
                    break;
                  }
                } else { // find all slices of plan node
                  insertNodes.add(new IndexedConsensusRequest(targetIndex, tmpNodes));
                  tmpNodes = Collections.emptyList();
                  break;
                }
              }
            }
          } catch (FileNotFoundException e) {
            logger.debug(
                "WAL file {} has been deleted, try to find next {} again.",
                identifier,
                nextSearchIndex);
            reset();
            return hasNext();
          } catch (Exception e) {
            brokenFileId = WALFileUtils.parseVersionId(filesToSearch[fileIndex].getName());
            logger.error(
                "Fail to read wal from wal file {}, skip this file.", filesToSearch[fileIndex], e);
          }
          if (!tmpNodes.isEmpty()) {
            fileIndex++;
          }
        }

        if (tmpNodes.isEmpty()) { // all insert plans scanned
          currentFileIndex = fileIndex;
        } else {
          needUpdatingFilesToSearch = true;
        }
      }

      // update file index and version id
      if (currentFileIndex >= filesToSearch.length - 1) {
        needUpdatingFilesToSearch = true;
      }

      // update iterator
      if (!insertNodes.isEmpty()) {
        itr = insertNodes.listIterator();
        return true;
      }
      return false;
    }

    @Override
    public IndexedConsensusRequest next() {
      if (itr == null && !hasNext()) {
        throw new NoSuchElementException();
      }

      IndexedConsensusRequest request = itr.next();
      nextSearchIndex = request.getSearchIndex() + 1;
      return request;
    }

    @Override
    public void waitForNextReady() throws InterruptedException {
      boolean walFileRolled = false;
      while (!hasNext()) {
        if (!walFileRolled) {
          boolean timeout =
              !buffer.waitForFlush(WAIT_FOR_NEXT_WAL_ENTRY_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
          if (timeout) {
            logger.info(
                "timeout when waiting for next WAL entry ready, execute rollWALFile. Current search index in wal buffer is {}, and next target index is {}",
                buffer.getCurrentSearchIndex(),
                nextSearchIndex);
            rollWALFile();
            walFileRolled = true;
          }
        } else {
          buffer.waitForFlush();
        }
      }
    }

    @Override
    public void waitForNextReady(long time, TimeUnit unit)
        throws InterruptedException, TimeoutException {
      if (!hasNext()) {
        boolean timeout = !buffer.waitForFlush(time, unit);
        if (timeout || !hasNext()) {
          throw new TimeoutException();
        }
      }
    }

    @Override
    public void skipTo(long targetIndex) {
      if (targetIndex < nextSearchIndex) {
        logger.warn(
            "Skip from {} to {}, it's a dangerous operation because insert plan {} may have been lost.",
            nextSearchIndex,
            targetIndex,
            targetIndex);
      }

      if (itr != null
          && itr.hasNext()
          && insertNodes.get(itr.nextIndex()).getSearchIndex() <= targetIndex
          && targetIndex <= insertNodes.getLast().getSearchIndex()) {
        while (itr.hasNext()) {
          IndexedConsensusRequest request = itr.next();
          if (targetIndex == request.getSearchIndex()) {
            itr.previous();
            nextSearchIndex = targetIndex;
            return;
          }
        }
      }

      reset();
      nextSearchIndex = targetIndex;
    }

    /** Reset all params except nextSearchIndex */
    private void reset() {
      insertNodes.clear();
      itr = null;
      filesToSearch = null;
      currentFileIndex = -1;
      brokenFileId = -1;
      needUpdatingFilesToSearch = true;
    }

    private void updateFilesToSearch() {
      File[] filesToSearch = WALFileUtils.listAllWALFiles(logDirectory);
      WALFileUtils.ascSortByVersionId(filesToSearch);
      int fileIndex = WALFileUtils.binarySearchFileBySearchIndex(filesToSearch, nextSearchIndex);
      logger.debug(
          "searchIndex: {}, result: {}, files: {}, ", nextSearchIndex, fileIndex, filesToSearch);
      // (xingtanzjr) When the target entry does not exist, the reader will return minimum one whose
      // searchIndex is larger than target searchIndex
      if (fileIndex == -1) {
        fileIndex = 0;
      }
      // skip broken files
      while (fileIndex < filesToSearch.length - 1
          && WALFileUtils.parseVersionId(filesToSearch[fileIndex].getName()) <= brokenFileId) {
        fileIndex++;
      }
      if (filesToSearch != null
          && (fileIndex >= 0 && fileIndex < filesToSearch.length - 1)) { // possible to find next
        this.filesToSearch = filesToSearch;
        this.currentFileIndex = fileIndex;
        this.needUpdatingFilesToSearch = false;
      } else { // impossible to find next
        this.filesToSearch = null;
        this.currentFileIndex = -1;
        this.needUpdatingFilesToSearch = true;
      }
    }
  }

  @Override
  public long getCurrentSearchIndex() {
    return buffer.getCurrentSearchIndex();
  }

  @Override
  public long getCurrentWALFileVersion() {
    return buffer.getCurrentWALFileVersion();
  }

  @Override
  public long getTotalSize() {
    return WALManager.getInstance().getTotalDiskUsage();
  }

  // endregion

  @Override
  public void close() {
    buffer.close();
  }

  public String getIdentifier() {
    return identifier;
  }

  public File getLogDirectory() {
    return logDirectory;
  }

  /** Get the .wal file starts with the specified version id */
  public File getWALFile(long versionId) throws FileNotFoundException {
    return WALFileUtils.getWALFile(logDirectory, versionId);
  }

  /** Return true when all wal entries all consumed and flushed */
  public boolean isAllWALEntriesConsumed() {
    return buffer.isAllWALEntriesConsumed();
  }

  /** Roll wal file */
  public void rollWALFile() {
    WALEntry rollWALFileSignal = new WALSignalEntry(WALEntryType.ROLL_WAL_LOG_WRITER_SIGNAL, true);
    WALFlushListener walFlushListener = log(rollWALFileSignal);
    if (walFlushListener.waitForResult() == AbstractResultListener.Status.FAILURE) {
      logger.error(
          "Fail to trigger rolling wal node-{}'s wal file log writer.",
          identifier,
          walFlushListener.getCause());
    }
  }

  public long getDiskUsage() {
    return buffer.getDiskUsage();
  }

  public long getFileNum() {
    return buffer.getFileNum();
  }

  public int getRegionId(long memtableId) {
    return checkpointManager.getRegionId(memtableId);
  }

  @TestOnly
  long getCurrentLogVersion() {
    return buffer.getCurrentWALFileVersion();
  }

  @TestOnly
  CheckpointManager getCheckpointManager() {
    return checkpointManager;
  }

  @TestOnly
  public void setBufferSize(int size) {
    buffer.setBufferSize(size);
  }

  @TestOnly
  public WALBuffer getWALBuffer() {
    return buffer;
  }
}
