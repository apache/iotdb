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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.flush.FlushStatus;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.service.metrics.recorder.WritingMetricsManager;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.buffer.IWALBuffer;
import org.apache.iotdb.db.wal.buffer.WALBuffer;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.db.wal.buffer.WALEntryType;
import org.apache.iotdb.db.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.wal.buffer.WALSignalEntry;
import org.apache.iotdb.db.wal.checkpoint.CheckpointManager;
import org.apache.iotdb.db.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.wal.io.WALByteBufReader;
import org.apache.iotdb.db.wal.utils.WALFileStatus;
import org.apache.iotdb.db.wal.utils.WALFileUtils;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.TsFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class encapsulates {@link IWALBuffer} and {@link CheckpointManager}. If search is enabled,
 * the order of search index should be protected by the upper layer, and the value should start from
 * 1.
 */
public class WALNode implements IWALNode {
  private static final Logger logger = LoggerFactory.getLogger(WALNode.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  /** no iot consensus, all insert nodes can be safely deleted */
  public static final long DEFAULT_SAFELY_DELETED_SEARCH_INDEX = Long.MAX_VALUE;

  /** timeout threshold when waiting for next wal entry */
  private static final long WAIT_FOR_NEXT_WAL_ENTRY_TIMEOUT_IN_SEC = 30;

  private static final WritingMetricsManager WRITING_METRICS = WritingMetricsManager.getInstance();

  /** unique identifier of this WALNode */
  private final String identifier;
  /** directory to store this node's files */
  private final File logDirectory;
  /** wal buffer */
  private final IWALBuffer buffer;
  /** manage checkpoints */
  private final CheckpointManager checkpointManager;
  /**
   * memTable id -> memTable snapshot count, used to avoid write amplification caused by frequent
   * snapshot
   */
  private final Map<Long, Integer> memTableSnapshotCount = new ConcurrentHashMap<>();
  /**
   * total cost of flushedMemTables. when memControl enabled, cost is memTable ram cost, otherwise
   * cost is memTable count
   */
  private final AtomicLong totalCostOfFlushedMemTables = new AtomicLong();
  /** version id -> cost sum of memTables flushed at this file version */
  private final Map<Long, Long> walFileVersionId2MemTablesTotalCost = new ConcurrentHashMap<>();
  /** insert nodes whose search index are before this value can be deleted safely */
  private volatile long safelyDeletedSearchIndex = DEFAULT_SAFELY_DELETED_SEARCH_INDEX;

  public WALNode(String identifier, String logDirectory) throws FileNotFoundException {
    this(identifier, logDirectory, 0, 0L);
  }

  public WALNode(
      String identifier, String logDirectory, long startFileVersion, long startSearchIndex)
      throws FileNotFoundException {
    this.identifier = identifier;
    this.logDirectory = SystemFileFactory.INSTANCE.getFile(logDirectory);
    if (!this.logDirectory.exists() && this.logDirectory.mkdirs()) {
      logger.info("create folder {} for wal node-{}.", logDirectory, identifier);
    }
    this.buffer = new WALBuffer(identifier, logDirectory, startFileVersion, startSearchIndex);
    this.checkpointManager = new CheckpointManager(identifier, logDirectory);
  }

  @Override
  public WALFlushListener log(long memTableId, InsertRowNode insertRowNode) {
    WALEntry walEntry = new WALInfoEntry(memTableId, insertRowNode);
    return log(walEntry);
  }

  @Override
  public WALFlushListener log(
      long memTableId, InsertTabletNode insertTabletNode, int start, int end) {
    WALEntry walEntry = new WALInfoEntry(memTableId, insertTabletNode, start, end);
    return log(walEntry);
  }

  @Override
  public WALFlushListener log(long memTableId, DeleteDataNode deleteDataNode) {
    WALEntry walEntry = new WALInfoEntry(memTableId, deleteDataNode);
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
    // remove snapshot info
    memTableSnapshotCount.remove(memTable.getMemTableId());
    // update cost info
    long cost = config.isEnableMemControl() ? memTable.getTVListsRamCost() : 1;
    long currentWALFileVersion = buffer.getCurrentWALFileVersion();
    walFileVersionId2MemTablesTotalCost.compute(
        currentWALFileVersion, (k, v) -> v == null ? cost : v + cost);
    totalCostOfFlushedMemTables.addAndGet(cost);
  }

  @Override
  public void onMemTableCreated(IMemTable memTable, String targetTsFile) {
    if (memTable.isSignalMemTable()) {
      return;
    }
    // use current log version id as first file version id
    long firstFileVersionId = buffer.getCurrentWALFileVersion();
    MemTableInfo memTableInfo = new MemTableInfo(memTable, targetTsFile, firstFileVersionId);
    checkpointManager.makeCreateMemTableCP(memTableInfo);
  }

  // region Task to delete outdated .wal files
  /** Delete outdated .wal files */
  public void deleteOutdatedFiles() {
    try {
      new DeleteOutdatedFileTask().run();
    } catch (Exception e) {
      logger.error("Fail to delete wal node-{}'s outdated files.", identifier, e);
    }
  }

  private class DeleteOutdatedFileTask implements Runnable {
    private static final int MAX_RECURSION_TIME = 5;
    /** .wal files whose version ids are less than first valid version id should be deleted */
    private long firstValidVersionId;
    /** recursion time of calling deletion */
    private int recursionTime = 0;

    @Override
    public void run() {
      // init firstValidVersionId
      firstValidVersionId = checkpointManager.getFirstValidWALVersionId();
      if (firstValidVersionId == Long.MIN_VALUE) {
        // roll wal log writer to delete current wal file
        if (buffer.getCurrentWALFileSize() > 0) {
          rollWALFile();
        }
        // update firstValidVersionId
        firstValidVersionId = checkpointManager.getFirstValidWALVersionId();
        if (firstValidVersionId == Long.MIN_VALUE) {
          firstValidVersionId = buffer.getCurrentWALFileVersion();
        }
      }

      logger.debug(
          "Start deleting outdated wal files for wal node-{}, the first valid version id is {}, and the safely deleted search index is {}.",
          identifier,
          firstValidVersionId,
          safelyDeletedSearchIndex);

      // delete outdated files
      deleteOutdatedFiles();

      // calculate effective information ratio
      long costOfActiveMemTables = checkpointManager.getTotalCostOfActiveMemTables();
      long costOfFlushedMemTables = totalCostOfFlushedMemTables.get();
      long totalCost = costOfActiveMemTables + costOfFlushedMemTables;
      if (totalCost == 0) {
        return;
      }
      double effectiveInfoRatio = (double) costOfActiveMemTables / totalCost;
      WRITING_METRICS.recordWALNodeEffectiveInfoRatio(identifier, effectiveInfoRatio);
      logger.debug(
          "Effective information ratio is {}, active memTables cost is {}, flushed memTables cost is {}",
          effectiveInfoRatio,
          costOfActiveMemTables,
          costOfFlushedMemTables);
      // effective information ratio is too small
      // update first valid version id by snapshotting or flushing memTable,
      // then delete old .wal files again
      if (effectiveInfoRatio < config.getWalMinEffectiveInfoRatio()) {
        logger.info(
            "Effective information ratio {} (active memTables cost is {}, flushed memTables cost is {}) of wal node-{} is below wal min effective info ratio {}, some memTables will be snapshot or flushed.",
            effectiveInfoRatio,
            costOfActiveMemTables,
            costOfFlushedMemTables,
            identifier,
            config.getWalMinEffectiveInfoRatio());
        if (snapshotOrFlushMemTable() && recursionTime < MAX_RECURSION_TIME) {
          // wal is used to search, cannot optimize files deletion
          if (safelyDeletedSearchIndex != DEFAULT_SAFELY_DELETED_SEARCH_INDEX) {
            return;
          }
          recursionTime++;
          run();
        }
      }
    }

    private void deleteOutdatedFiles() {
      // find all files to delete
      // delete files whose version < firstValidVersionId
      File[] filesToDelete = logDirectory.listFiles(this::filterFilesToDelete);
      if (filesToDelete == null) {
        return;
      }
      // delete files whose content's search index are all <= safelyDeletedSearchIndex
      WALFileUtils.ascSortByVersionId(filesToDelete);
      // judge DEFAULT_SAFELY_DELETED_SEARCH_INDEX for standalone, Long.MIN_VALUE for iot
      int endFileIndex =
          safelyDeletedSearchIndex == DEFAULT_SAFELY_DELETED_SEARCH_INDEX
              ? filesToDelete.length
              : WALFileUtils.binarySearchFileBySearchIndex(
                  filesToDelete, safelyDeletedSearchIndex + 1);
      // delete files whose file status is CONTAINS_NONE_SEARCH_INDEX
      if (endFileIndex == -1) {
        endFileIndex++;
      }
      while (endFileIndex < filesToDelete.length) {
        if (WALFileUtils.parseStatusCode(filesToDelete[endFileIndex].getName())
            == WALFileStatus.CONTAINS_SEARCH_INDEX) {
          break;
        }
        endFileIndex++;
      }
      // delete files
      int deletedFilesNum = 0;
      long deletedFilesSize = 0;
      for (int i = 0; i < endFileIndex; ++i) {
        long fileSize = filesToDelete[i].length();
        if (filesToDelete[i].delete()) {
          deletedFilesNum++;
          deletedFilesSize += fileSize;
        } else {
          logger.info(
              "Fail to delete outdated wal file {} of wal node-{}.", filesToDelete[i], identifier);
        }
        // update totalRamCostOfFlushedMemTables
        long versionId = WALFileUtils.parseVersionId(filesToDelete[i].getName());
        Long memTableRamCostSum = walFileVersionId2MemTablesTotalCost.remove(versionId);
        if (memTableRamCostSum != null) {
          totalCostOfFlushedMemTables.addAndGet(-memTableRamCostSum);
        }
      }
      WALManager.getInstance().subtractTotalDiskUsage(deletedFilesSize);
      WALManager.getInstance().subtractTotalFileNum(deletedFilesNum);
      logger.debug(
          "Successfully delete {} outdated wal files for wal node-{}.",
          deletedFilesNum,
          identifier);
    }

    private boolean filterFilesToDelete(File dir, String name) {
      Pattern pattern = WALFileUtils.WAL_FILE_NAME_PATTERN;
      Matcher matcher = pattern.matcher(name);
      boolean toDelete = false;
      if (matcher.find()) {
        long versionId = Long.parseLong(matcher.group(IoTDBConstant.WAL_VERSION_ID));
        toDelete = versionId < firstValidVersionId;
      }
      return toDelete;
    }

    /**
     * Snapshot or flush one memTable,
     *
     * @return true if snapshot or flushed
     */
    private boolean snapshotOrFlushMemTable() {
      // find oldest memTable
      MemTableInfo oldestMemTableInfo = checkpointManager.getOldestMemTableInfo();
      if (oldestMemTableInfo == null) {
        return false;
      }
      IMemTable oldestMemTable = oldestMemTableInfo.getMemTable();

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

      // snapshot or flush memTable, flush memTable when it belongs to an old time partition, or
      // it's snapshot count or size reach threshold.
      int snapshotCount = memTableSnapshotCount.getOrDefault(oldestMemTable.getMemTableId(), 0);
      long oldestMemTableTVListsRamCost = oldestMemTable.getTVListsRamCost();
      if (TsFileUtils.getTimePartition(new File(oldestMemTableInfo.getTsFilePath()))
              < dataRegion.getLatestTimePartition()
          || snapshotCount >= config.getMaxWalMemTableSnapshotNum()
          || oldestMemTableTVListsRamCost > config.getWalMemTableSnapshotThreshold()) {
        flushMemTable(dataRegion, oldestTsFile, oldestMemTable);
        WRITING_METRICS.recordMemTableRamWhenCauseFlush(identifier, oldestMemTableTVListsRamCost);
      } else {
        snapshotMemTable(dataRegion, oldestTsFile, oldestMemTableInfo);
        WRITING_METRICS.recordMemTableRamWhenCauseSnapshot(
            identifier, oldestMemTableTVListsRamCost);
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
            "WAL node-{} flushes memTable-{} to TsFile {}, memTable size is {}.",
            identifier,
            memTable.getMemTableId(),
            tsFile,
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

    /**
     * synchronize memTable to make sure snapshot is made before memTable flush operation, {@link
     * org.apache.iotdb.db.engine.storagegroup.TsFileProcessor#flushOneMemTable}
     */
    private void snapshotMemTable(DataRegion dataRegion, File tsFile, MemTableInfo memTableInfo) {
      IMemTable memTable = memTableInfo.getMemTable();

      // get dataRegion write lock to make sure no more writes to the memTable
      dataRegion.writeLock(
          "CheckpointManager$DeleteOutdatedFileTask.snapshotOrFlushOldestMemTable");
      try {
        // make sure snapshot is made before memTable flush operation
        synchronized (memTable) {
          if (memTable.getFlushStatus() != FlushStatus.WORKING) {
            return;
          }

          // update snapshot count
          memTableSnapshotCount.compute(memTable.getMemTableId(), (k, v) -> v == null ? 1 : v + 1);
          // roll wal log writer to make sure first version id will be updated
          WALEntry rollWALFileSignal =
              new WALSignalEntry(WALEntryType.ROLL_WAL_LOG_WRITER_SIGNAL, true);
          WALFlushListener fileRolledListener = log(rollWALFileSignal);
          if (fileRolledListener.waitForResult() == WALFlushListener.Status.FAILURE) {
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
          if (flushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
            logger.error("Fail to snapshot memTable of {}", tsFile, flushListener.getCause());
          }
          logger.info(
              "WAL node-{} snapshots memTable-{} to wal files, memTable size is {}.",
              identifier,
              memTable.getMemTableId(),
              memTable.getTVListsRamCost());
        }
      } finally {
        dataRegion.writeUnlock();
      }
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
    private final List<IndexedConsensusRequest> insertNodes = new LinkedList<>();
    /** iterator of insertNodes */
    private Iterator<IndexedConsensusRequest> itr = null;

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

      // update files to search
      if (needUpdatingFilesToSearch || filesToSearch == null) {
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
            } else { // different search index, all slices found
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
          } else if (!tmpNodes
              .isEmpty()) { // next entry doesn't need to be searched, all slices found
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
        itr = insertNodes.iterator();
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
      reset();
      nextSearchIndex = targetIndex;
    }

    /** Reset all params except nextSearchIndex */
    private void reset() {
      insertNodes.clear();
      itr = null;
      filesToSearch = null;
      currentFileIndex = -1;
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
  public long getTotalSize() {
    return WALManager.getInstance().getTotalDiskUsage();
  }

  // endregion

  @Override
  public void close() {
    buffer.close();
    checkpointManager.close();
  }

  public File getLogDirectory() {
    return logDirectory;
  }

  /** Return true when all wal entries all consumed and flushed */
  public boolean isAllWALEntriesConsumed() {
    return buffer.isAllWALEntriesConsumed();
  }

  /** Roll wal file */
  public void rollWALFile() {
    WALEntry rollWALFileSignal = new WALSignalEntry(WALEntryType.ROLL_WAL_LOG_WRITER_SIGNAL, true);
    WALFlushListener walFlushListener = log(rollWALFileSignal);
    if (walFlushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
      logger.error(
          "Fail to trigger rolling wal node-{}'s wal file log writer.",
          identifier,
          walFlushListener.getCause());
    }
  }

  @TestOnly
  long getCurrentLogVersion() {
    return buffer.getCurrentWALFileVersion();
  }
}
