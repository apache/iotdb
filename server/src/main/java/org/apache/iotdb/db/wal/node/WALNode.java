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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.flush.FlushStatus;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.wal.buffer.IWALBuffer;
import org.apache.iotdb.db.wal.buffer.SignalWALEntry;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This class encapsulates {@link IWALBuffer} and {@link CheckpointManager}. */
public class WALNode implements IWALNode {
  public static final Pattern WAL_NODE_FOLDER_PATTERN = Pattern.compile("(?<nodeIdentifier>\\d+)");

  private static final Logger logger = LoggerFactory.getLogger(WALNode.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /** unique identifier of this WALNode */
  private final String identifier;
  /** directory to store this node's files */
  private final String logDirectory;
  /** wal buffer */
  private final IWALBuffer buffer;
  /** manage checkpoints */
  private final CheckpointManager checkpointManager;
  /**
   * memTable id -> memTable snapshot count, used to avoid write amplification caused by frequent
   * snapshot
   */
  private final Map<Integer, Integer> memTableSnapshotCount = new ConcurrentHashMap<>();
  /**
   * total cost of flushedMemTables. when memControl enabled, cost is memTable ram cost, otherwise
   * cost is memTable count
   */
  private final AtomicLong totalCostOfFlushedMemTables = new AtomicLong();
  /** version id -> cost sum of memTables flushed at this file version */
  private final Map<Integer, Long> walFileVersionId2MemTablesTotalCost = new ConcurrentHashMap<>();

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
  public WALFlushListener log(int memTableId, InsertRowNode insertRowNode) {
    WALEntry walEntry = new WALEntry(memTableId, insertRowNode);
    return log(walEntry);
  }

  @Override
  public WALFlushListener log(
      int memTableId, InsertTabletPlan insertTabletPlan, int start, int end) {
    WALEntry walEntry = new WALEntry(memTableId, insertTabletPlan, start, end);
    return log(walEntry);
  }

  @Override
  public WALFlushListener log(
      int memTableId, InsertTabletNode insertTabletNode, int start, int end) {
    WALEntry walEntry = new WALEntry(memTableId, insertTabletNode, start, end);
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
    // remove snapshot info
    memTableSnapshotCount.remove(memTable.getMemTableId());
    // update cost info
    long cost = config.isEnableMemControl() ? memTable.getTVListsRamCost() : 1;
    int currentWALFileVersion = buffer.getCurrentWALFileVersion();
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
    int firstFileVersionId = buffer.getCurrentWALFileVersion();
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
    /** .wal files whose version ids are less than first valid version id should be deleted */
    private int firstValidVersionId;

    @Override
    public void run() {
      // init firstValidVersionId
      firstValidVersionId = checkpointManager.getFirstValidWALVersionId();
      if (firstValidVersionId == Integer.MIN_VALUE) {
        // roll wal log writer to delete current wal file
        WALEntry rollWALFileSignal =
            new SignalWALEntry(SignalWALEntry.SignalType.ROLL_WAL_LOG_WRITER_SIGNAL, true);
        WALFlushListener walFlushListener = log(rollWALFileSignal);
        if (walFlushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
          logger.error(
              "Fail to trigger rolling wal node-{}'s wal file log writer.",
              identifier,
              walFlushListener.getCause());
        }
        // update firstValidVersionId
        firstValidVersionId = checkpointManager.getFirstValidWALVersionId();
        if (firstValidVersionId == Integer.MIN_VALUE) {
          firstValidVersionId = buffer.getCurrentWALFileVersion();
        }
      }

      // delete outdated files
      deleteOutdatedFiles();

      // calculate effective information ratio
      long costOfActiveMemTables = checkpointManager.getTotalCostOfActiveMemTables();
      long costOfFlushedMemTables = totalCostOfFlushedMemTables.get();
      double effectiveInfoRatio =
          (double) costOfActiveMemTables / (costOfActiveMemTables + costOfFlushedMemTables);
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
            "Effective information ratio {} of wal node-{} is below wal min effective info ratio {}, some mamTables will be snapshot or flushed.",
            effectiveInfoRatio,
            identifier,
            config.getWalMinEffectiveInfoRatio());
        snapshotOrFlushMemTable();
        run();
      }
    }

    private void deleteOutdatedFiles() {
      File directory = SystemFileFactory.INSTANCE.getFile(logDirectory);
      File[] filesToDelete = directory.listFiles(this::filterFilesToDelete);
      if (filesToDelete != null) {
        for (File file : filesToDelete) {
          if (!file.delete()) {
            logger.info("Fail to delete outdated wal file {} of wal node-{}.", file, identifier);
          }
          // update totalRamCostOfFlushedMemTables
          int versionId = WALWriter.parseVersionId(file.getName());
          Long memTableRamCostSum = walFileVersionId2MemTablesTotalCost.remove(versionId);
          if (memTableRamCostSum != null) {
            totalCostOfFlushedMemTables.addAndGet(-memTableRamCostSum);
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

    private void snapshotOrFlushMemTable() {
      // find oldest memTable
      MemTableInfo oldestMemTableInfo = checkpointManager.getOldestMemTableInfo();
      if (oldestMemTableInfo == null) {
        return;
      }
      IMemTable oldestMemTable = oldestMemTableInfo.getMemTable();

      // get memTable's virtual storage group processor
      File oldestTsFile =
          FSFactoryProducer.getFSFactory().getFile(oldestMemTableInfo.getTsFilePath());
      DataRegion dataRegion;
      try {
        dataRegion =
            StorageEngine.getInstance()
                .getProcessorByDataRegionId(
                    new PartialPath(TsFileUtils.getStorageGroup(oldestTsFile)),
                    TsFileUtils.getDataRegionId(oldestTsFile));
      } catch (IllegalPathException | StorageEngineException e) {
        logger.error("Fail to get virtual storage group processor for {}", oldestTsFile, e);
        return;
      }

      // snapshot or flush memTable
      int snapshotCount = memTableSnapshotCount.getOrDefault(oldestMemTable.getMemTableId(), 0);
      if (snapshotCount >= config.getMaxWalMemTableSnapshotNum()
          || oldestMemTable.getTVListsRamCost() > config.getWalMemTableSnapshotThreshold()) {
        flushMemTable(dataRegion, oldestTsFile, oldestMemTable);
      } else {
        snapshotMemTable(dataRegion, oldestTsFile, oldestMemTableInfo);
      }
    }

    private void flushMemTable(DataRegion dataRegion, File tsFile, IMemTable memTable) {
      boolean shouldWait = true;
      if (memTable.getFlushStatus() == FlushStatus.WORKING) {
        shouldWait =
            dataRegion.submitAFlushTask(
                TsFileUtils.getTimePartition(tsFile), TsFileUtils.isSequence(tsFile));
        logger.info(
            "WAL node-{} flushes memTable-{} to TsFile {}, memTable size is {}.",
            identifier,
            memTable.getMemTableId(),
            tsFile,
            memTable.getTVListsRamCost());
      }

      // it's fine to wait until memTable has been flushed, because deleting files is not urgent.
      if (shouldWait) {
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

    private void snapshotMemTable(DataRegion dataRegion, File tsFile, MemTableInfo memTableInfo) {
      IMemTable memTable = memTableInfo.getMemTable();
      if (memTable.getFlushStatus() != FlushStatus.WORKING) {
        return;
      }

      // update snapshot count
      memTableSnapshotCount.compute(memTable.getMemTableId(), (k, v) -> v == null ? 1 : v + 1);
      // roll wal log writer to make sure first version id will be updated
      WALEntry rollWALFileSignal =
          new SignalWALEntry(SignalWALEntry.SignalType.ROLL_WAL_LOG_WRITER_SIGNAL, true);
      WALFlushListener fileRolledListener = log(rollWALFileSignal);
      if (fileRolledListener.waitForResult() == WALFlushListener.Status.FAILURE) {
        logger.error("Fail to roll wal log writer.", fileRolledListener.getCause());
        return;
      }

      // update first version id first to make sure snapshot is in the files ≥ current log
      // version
      memTableInfo.setFirstFileVersionId(buffer.getCurrentWALFileVersion());

      // get dataRegion write lock to make sure no more writes to the memTable
      dataRegion.writeLock(
          "CheckpointManager$DeleteOutdatedFileTask.snapshotOrFlushOldestMemTable");
      try {
        // log snapshot in a new .wal file
        WALEntry walEntry = new WALEntry(memTable.getMemTableId(), memTable, true);
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
      } finally {
        dataRegion.writeUnlock();
      }
    }
  }
  // endregion

  // region Search interfaces for consensus group
  @Override
  public IConsensusRequest getReq(long index) {
    return null;
  }

  @Override
  public List<IConsensusRequest> getReqs(long startIndex, int num) {
    return null;
  }

  @Override
  public ReqIterator getReqIterator(long startIndex) {
    return new PlanNodeIterator();
  }

  private class PlanNodeIterator implements ReqIterator {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public IConsensusRequest next() {
      return null;
    }

    @Override
    public IConsensusRequest waitForNext() throws InterruptedException {
      return null;
    }

    @Override
    public IConsensusRequest waitForNext(long timeout)
        throws InterruptedException, TimeoutException {
      return null;
    }

    @Override
    public void skipTo(long targetIndex) {}
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
