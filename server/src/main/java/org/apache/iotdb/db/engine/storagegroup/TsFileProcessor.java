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
package org.apache.iotdb.db.engine.storagegroup;

import static org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter.MEMTABLE_NUM_FOR_EACH_PARTITION;
import static org.apache.iotdb.db.engine.flush.VmLogger.VM_LOG_NAME;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.MERGED_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_UPGRADE;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.VM_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.ActiveTimeSeriesCounter;
import org.apache.iotdb.db.conf.adapter.CompressionRatio;
import org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.engine.flush.NotifyFlushMemTable;
import org.apache.iotdb.db.engine.flush.VmLogAnalyzer;
import org.apache.iotdb.db.engine.flush.VmLogger;
import org.apache.iotdb.db.engine.flush.VmMergeUtils;
import org.apache.iotdb.db.engine.flush.pool.VmMergeTaskPoolManager;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseTsFileCallBack;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.UpdateEndTimeCallBack;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.rescon.MemTablePool;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileProcessor {

  private static final Logger logger = LoggerFactory.getLogger(TsFileProcessor.class);
  private final String storageGroupName;

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /**
   * sync this object in query() and asyncTryToFlush()
   */
  private final ConcurrentLinkedDeque<IMemTable> flushingMemTables = new ConcurrentLinkedDeque<>();
  private RestorableTsFileIOWriter writer;
  private final TsFileResource tsFileResource;
  private final List<TsFileResource> vmTsFileResources;
  private final List<RestorableTsFileIOWriter> vmWriters;
  // time range index to indicate this processor belongs to which time range
  private long timeRangeId;
  /**
   * Whether the processor is in the queue of the FlushManager or being flushed by a flush thread.
   */
  private volatile boolean managedByFlushManager;
  private final ReadWriteLock flushQueryLock = new ReentrantReadWriteLock();
  /**
   * It is set by the StorageGroupProcessor and checked by flush threads. (If shouldClose == true
   * and its flushingMemTables are all flushed, then the flush thread will close this file.)
   */
  private volatile boolean shouldClose;
  private IMemTable workMemTable;

  private final VersionController versionController;
  /**
   * this callback is called after the corresponding TsFile is called endFile().
   */
  private final CloseTsFileCallBack closeTsFileCallback;
  /**
   * this callback is called before the workMemtable is added into the flushingMemTables.
   */
  private final UpdateEndTimeCallBack updateLatestFlushTimeCallback;
  private WriteLogNode logNode;
  private final boolean sequence;
  private long totalMemTableSize;

  private int flushVmTimes = 0;
  private static final String FLUSH_QUERY_WRITE_LOCKED = "{}: {} get flushQueryLock write lock";
  private static final String FLUSH_QUERY_WRITE_RELEASE = "{}: {} get flushQueryLock write lock released";

  private volatile boolean mergeWorking = false;

  TsFileProcessor(String storageGroupName, File tsfile, List<File> vmFiles,
      VersionController versionController,
      CloseTsFileCallBack closeTsFileCallback,
      UpdateEndTimeCallBack updateLatestFlushTimeCallback, boolean sequence)
      throws IOException {
    this.storageGroupName = storageGroupName;
    this.tsFileResource = new TsFileResource(tsfile, this);
    this.versionController = versionController;
    this.writer = new RestorableTsFileIOWriter(tsfile);
    this.vmTsFileResources = new CopyOnWriteArrayList<>();
    this.vmWriters = new CopyOnWriteArrayList<>();
    for (File file : vmFiles) {
      this.vmTsFileResources.add(new TsFileResource(file, this));
      this.vmWriters.add(new RestorableTsFileIOWriter(file));
    }
    this.closeTsFileCallback = closeTsFileCallback;
    this.updateLatestFlushTimeCallback = updateLatestFlushTimeCallback;
    this.sequence = sequence;
    logger.info("create a new tsfile processor {}", tsfile.getAbsolutePath());
    // a file generated by flush has only one historical version, which is itself
    this.tsFileResource
        .setHistoricalVersions(Collections.singleton(versionController.currVersion()));
  }

  public TsFileProcessor(String storageGroupName, TsFileResource tsFileResource,
      List<TsFileResource> vmTsFileResources,
      VersionController versionController, CloseTsFileCallBack closeUnsealedTsFileProcessor,
      UpdateEndTimeCallBack updateLatestFlushTimeCallback, boolean sequence,
      RestorableTsFileIOWriter writer, List<RestorableTsFileIOWriter> vmWriters) {
    this.storageGroupName = storageGroupName;
    this.tsFileResource = tsFileResource;
    this.vmTsFileResources = new CopyOnWriteArrayList<>(vmTsFileResources);
    this.versionController = versionController;
    this.writer = writer;
    this.vmWriters = new CopyOnWriteArrayList<>(vmWriters);
    this.closeTsFileCallback = closeUnsealedTsFileProcessor;
    this.updateLatestFlushTimeCallback = updateLatestFlushTimeCallback;
    this.sequence = sequence;
    logger.info("reopen a tsfile processor {}", tsFileResource.getTsFile());
  }

  /**
   * insert data in an InsertRowPlan into the workingMemtable.
   *
   * @param insertRowPlan physical plan of insertion
   */
  public void insert(InsertRowPlan insertRowPlan) throws WriteProcessException {

    if (workMemTable == null) {
      workMemTable = MemTablePool.getInstance().getAvailableMemTable(this);
    }

    // insert insertRowPlan to the work memtable
    workMemTable.insert(insertRowPlan);

    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      try {
        getLogNode().write(insertRowPlan);
      } catch (Exception e) {
        throw new WriteProcessException(String.format("%s: %s write WAL failed",
            storageGroupName, tsFileResource.getTsFile().getAbsolutePath()), e);
      }
    }

    // update start time of this memtable
    tsFileResource.updateStartTime(insertRowPlan.getDeviceId(), insertRowPlan.getTime());
    //for sequence tsfile, we update the endTime only when the file is prepared to be closed.
    //for unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource.updateEndTime(insertRowPlan.getDeviceId(), insertRowPlan.getTime());
    }
  }

  /**
   * insert batch data of insertTabletPlan into the workingMemtable The rows to be inserted are in
   * the range [start, end)
   *
   * @param insertTabletPlan insert a tablet of a device
   * @param start start index of rows to be inserted in insertTabletPlan
   * @param end end index of rows to be inserted in insertTabletPlan
   * @param results result array
   */
  public void insertTablet(InsertTabletPlan insertTabletPlan, int start, int end,
      TSStatus[] results) throws WriteProcessException {

    if (workMemTable == null) {
      workMemTable = MemTablePool.getInstance().getAvailableMemTable(this);
    }

    // insert insertRowPlan to the work memtable
    try {
      workMemTable.insertTablet(insertTabletPlan, start, end);
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        insertTabletPlan.setStart(start);
        insertTabletPlan.setEnd(end);
        getLogNode().write(insertTabletPlan);
      }
    } catch (Exception e) {
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
      }
      throw new WriteProcessException(e);
    }

    for (int i = start; i < end; i++) {
      results[i] = RpcUtils.SUCCESS_STATUS;
    }

    tsFileResource
        .updateStartTime(insertTabletPlan.getDeviceId(), insertTabletPlan.getTimes()[start]);

    //for sequence tsfile, we update the endTime only when the file is prepared to be closed.
    //for unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource
          .updateEndTime(
              insertTabletPlan.getDeviceId(), insertTabletPlan.getTimes()[end - 1]);
    }
  }

  /**
   * Delete data which belongs to the timeseries `deviceId.measurementId` and the timestamp of which
   * <= 'timestamp' in the deletion. <br/>
   * <p>
   * Delete data in both working MemTable and flushing MemTables.
   */
  public void deleteDataInMemory(Deletion deletion) {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      if (workMemTable != null) {
        workMemTable
            .delete(deletion.getDevice(), deletion.getMeasurement(), deletion.getStartTime(),
                deletion.getEndTime());
      }
      // flushing memTables are immutable, only record this deletion in these memTables for query
      for (IMemTable memTable : flushingMemTables) {
        memTable.delete(deletion);
      }
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(FLUSH_QUERY_WRITE_RELEASE, storageGroupName,
            tsFileResource.getTsFile().getName());
      }
    }
  }

  public TsFileResource getTsFileResource() {
    return tsFileResource;
  }


  boolean shouldFlush() {
    if (workMemTable == null) {
      return false;
    }

    if (workMemTable.memSize() >= getMemtableSizeThresholdBasedOnSeriesNum()) {
      logger.info("The memtable size {} of tsfile {} reaches the threshold",
          workMemTable.memSize(), tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }

    if (workMemTable.reachTotalPointNumThreshold()) {
      logger.info("The avg series points num {} of tsfile {} reaches the threshold",
          workMemTable.getTotalPointsNum() / workMemTable.getSeriesNumber(),
          tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }

    return false;
  }

  /**
   * <p>In the dynamic parameter adjustment module{@link IoTDBConfigDynamicAdapter}, it calculated
   * the average size of each metatable{@link IoTDBConfigDynamicAdapter#tryToAdaptParameters()}.
   * However, considering that the number of timeseries between storage groups may vary greatly,
   * it's inappropriate to judge whether to flush the memtable according to the average memtable
   * size. We need to adjust it according to the number of timeseries in a specific storage group.
   */
  private long getMemtableSizeThresholdBasedOnSeriesNum() {
    if (!config.isEnableParameterAdapter()) {
      return config.getMemtableSizeThreshold();
    }
    long memTableSize = (long) (config.getMemtableSizeThreshold() * config.getMaxMemtableNumber()
        / IoTDBDescriptor.getInstance().getConfig().getConcurrentWritingTimePartition()
        / MEMTABLE_NUM_FOR_EACH_PARTITION
        * ActiveTimeSeriesCounter.getInstance()
        .getActiveRatio(storageGroupName));
    return Math.max(memTableSize, config.getMemtableSizeThreshold());
  }

  public boolean shouldClose() {
    if (config.isEnableVm() && flushVmTimes >= config.getMaxMergeChunkNumInTsFile() - 1) {
      return true;
    }
    long fileSize = tsFileResource.getTsFileSize();
    for (TsFileResource vmFile : vmTsFileResources) {
      fileSize += vmFile.getTsFileSize();
    }
    long fileSizeThreshold = IoTDBDescriptor.getInstance().getConfig()
        .getTsFileSizeThreshold();
    if (fileSize >= fileSizeThreshold) {
      logger.info("{} fileSize {} >= fileSizeThreshold {}", tsFileResource.getTsFilePath(),
          fileSize, fileSizeThreshold);
    }
    return fileSize >= fileSizeThreshold;
  }

  void syncClose() {
    logger.info("Sync close file: {}, will firstly async close it",
        tsFileResource.getTsFile().getAbsolutePath());
    if (shouldClose) {
      return;
    }
    synchronized (flushingMemTables) {
      try {
        asyncClose();
        long startTime = System.currentTimeMillis();
        while (!flushingMemTables.isEmpty()) {
          flushingMemTables.wait(60_000);
          if (System.currentTimeMillis() - startTime > 60_000) {
            logger.warn(
                "{} has spent {}s for waiting flushing one memtable; {} left (first: {}). FlushingManager info: {}",
                this.tsFileResource.getTsFile().getAbsolutePath(),
                (System.currentTimeMillis() - startTime) / 1000,
                flushingMemTables.size(),
                flushingMemTables.getFirst(),
                FlushManager.getInstance()
            );
          }

        }
      } catch (InterruptedException e) {
        logger.error("{}: {} wait close interrupted", storageGroupName,
            tsFileResource.getTsFile().getName(), e);
        Thread.currentThread().interrupt();
      }
    }
    logger.info("File {} is closed synchronously", tsFileResource.getTsFile().getAbsolutePath());
  }


  void asyncClose() {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {

      if (logger.isInfoEnabled()) {
        if (workMemTable != null) {
          logger.info(
              "{}: flush a working memtable in async close tsfile {}, memtable size: {}, tsfile size: {}",
              storageGroupName, tsFileResource.getTsFile().getAbsolutePath(), workMemTable.memSize(),
              tsFileResource.getTsFileSize());
        } else {
          logger.info("{}: flush a NotifyFlushMemTable in async close tsfile {}, tsfile size: {}",
              storageGroupName, tsFileResource.getTsFile().getAbsolutePath(),
              tsFileResource.getTsFileSize());
        }
      }

      if (shouldClose) {
        return;
      }
      // when a flush thread serves this TsFileProcessor (because the processor is submitted by
      // registerTsFileProcessor()), the thread will seal the corresponding TsFile and
      // execute other cleanup works if (shouldClose == true and flushingMemTables is empty).

      // To ensure there must be a flush thread serving this processor after the field `shouldClose`
      // is set true, we need to generate a NotifyFlushMemTable as a signal task and submit it to
      // the FlushManager.

      //we have to add the memtable into flushingList first and then set the shouldClose tag.
      // see https://issues.apache.org/jira/browse/IOTDB-510
      IMemTable tmpMemTable = workMemTable == null || workMemTable.memSize() == 0
          ? new NotifyFlushMemTable()
          : workMemTable;

      try {
        addAMemtableIntoFlushingList(tmpMemTable);
        shouldClose = true;
        tsFileResource.setCloseFlag();
      } catch (Exception e) {
        logger.error("{}: {} async close failed, because", storageGroupName,
            tsFileResource.getTsFile().getName(), e);
      }
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(FLUSH_QUERY_WRITE_RELEASE, storageGroupName,
            tsFileResource.getTsFile().getName());
      }
    }
  }

  /**
   * TODO if the flushing thread is too fast, the tmpMemTable.wait() may never wakeup Tips: I am
   * trying to solve this issue by checking whether the table exist before wait()
   */
  public void syncFlush() throws IOException {
    IMemTable tmpMemTable;
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      tmpMemTable = workMemTable == null ? new NotifyFlushMemTable() : workMemTable;
      if (logger.isDebugEnabled() && tmpMemTable.isSignalMemTable()) {
        logger.debug("{}: {} add a signal memtable into flushing memtable list when sync flush",
            storageGroupName, tsFileResource.getTsFile().getName());
      }
      addAMemtableIntoFlushingList(tmpMemTable);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.error(FLUSH_QUERY_WRITE_RELEASE, storageGroupName,
            tsFileResource.getTsFile().getName());
      }
    }

    synchronized (tmpMemTable) {
      try {
        long startWait = System.currentTimeMillis();
        while (flushingMemTables.contains(tmpMemTable)) {
          tmpMemTable.wait(1000);

          if ((System.currentTimeMillis() - startWait) > 60_000) {
            logger.warn("has waited for synced flushing a memtable in {} for 60 seconds.",
                this.tsFileResource.getTsFile().getAbsolutePath());
            startWait = System.currentTimeMillis();
          }
        }
      } catch (InterruptedException e) {
        logger.error("{}: {} wait flush finished meets error", storageGroupName,
            tsFileResource.getTsFile().getName(), e);
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * put the working memtable into flushing list and set the working memtable to null
   */
  public void asyncFlush() {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      if (workMemTable == null) {
        return;
      }
      addAMemtableIntoFlushingList(workMemTable);
    } catch (Exception e) {
      logger.error("{}: {} add a memtable into flushing listfailed", storageGroupName,
          tsFileResource.getTsFile().getName(), e);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(FLUSH_QUERY_WRITE_RELEASE, storageGroupName,
            tsFileResource.getTsFile().getName());
      }
    }
  }

  /**
   * this method calls updateLatestFlushTimeCallback and move the given memtable into the flushing
   * queue, set the current working memtable as null and then register the tsfileProcessor into the
   * flushManager again.
   */
  private void addAMemtableIntoFlushingList(IMemTable tobeFlushed) throws IOException {
    if (!tobeFlushed.isSignalMemTable() &&
        (!updateLatestFlushTimeCallback.call(this) || tobeFlushed.memSize() == 0)) {
      logger.warn("This normal memtable is empty, skip it in flush. {}: {} Memetable info: {}",
          storageGroupName, tsFileResource.getTsFile().getName(), tobeFlushed.getMemTableMap());
      return;
    }
    flushingMemTables.addLast(tobeFlushed);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {} Memtable (signal = {}) is added into the flushing Memtable, queue size = {}",
          storageGroupName, tsFileResource.getTsFile().getName(),
          tobeFlushed.isSignalMemTable(), flushingMemTables.size());
    }
    long cur = versionController.nextVersion();
    tobeFlushed.setVersion(cur);
    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      getLogNode().notifyStartFlush();
    }
    if (!tobeFlushed.isSignalMemTable()) {
      totalMemTableSize += tobeFlushed.memSize();
    }
    workMemTable = null;
    FlushManager.getInstance().registerTsFileProcessor(this);
  }


  /**
   * put back the memtable to MemTablePool and make metadata in writer visible
   */
  private void releaseFlushedMemTable(IMemTable memTable) {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName,
          tsFileResource.getTsFile().getName());
    }
    try {
      writer.makeMetadataVisible();
      for (RestorableTsFileIOWriter vmWriter : vmWriters) {
        vmWriter.makeMetadataVisible();
      }
      if (!flushingMemTables.remove(memTable)) {
        logger.warn(
            "{}: {} put the memtable (signal={}) out of flushingMemtables but it is not in the queue.",
            storageGroupName, tsFileResource.getTsFile().getName(), memTable.isSignalMemTable());
      } else if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} memtable (signal={}) is removed from the queue. {} left.", storageGroupName,
            tsFileResource.getTsFile().getName(),
            memTable.isSignalMemTable(), flushingMemTables.size());
      }
      memTable.release();
      MemTablePool.getInstance().putBack(memTable, storageGroupName);
      if (logger.isDebugEnabled()) {
        logger.debug("{}: {} flush finished, remove a memtable from flushing list, "
                + "flushing memtable list size: {}", storageGroupName,
            tsFileResource.getTsFile().getName(), flushingMemTables.size());
      }
    } catch (Exception e) {
      logger.error("{}: {}", storageGroupName, tsFileResource.getTsFile().getName(), e);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(FLUSH_QUERY_WRITE_RELEASE, storageGroupName,
            tsFileResource.getTsFile().getName());
      }
    }
  }

  public static File createNewVMFile(TsFileResource tsFileResource) {
    File parent = tsFileResource.getTsFile().getParentFile();
    return FSFactoryProducer.getFSFactory().getFile(parent,
        tsFileResource.getTsFile().getName() + IoTDBConstant.FILE_NAME_SEPARATOR + System
            .currentTimeMillis()
            + VM_SUFFIX);
  }

  public static void deleteVmFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      ChunkMetadataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setDeleted(true);
      if (seqFile.getTsFile().exists()) {
        Files.delete(seqFile.getTsFile().toPath());
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }

  /**
   * recover vm processor and files
   */
  public void recover() {
    try {
      File logFile = FSFactoryProducer.getFSFactory()
          .getFile(tsFileResource.getTsFile().getParent(),
              tsFileResource.getTsFile().getName() + VM_LOG_NAME);
      Set<String> deviceSet = new HashSet<>();
      Pair<Set<String>, Long> result = new Pair<>(deviceSet, 0L);
      if (logFile.exists()) {
        VmLogAnalyzer logAnalyzer = new VmLogAnalyzer(logFile);
        result = logAnalyzer.analyze();
        deviceSet = result.left;
      }
      if (!deviceSet.isEmpty()) {
        writer.getIOWriterOut().truncate(result.right - 1);
        VmMergeUtils.fullMerge(writer, vmWriters,
            storageGroupName,
            new VmLogger(tsFileResource.getTsFile().getParent(), tsFileResource.getTsFile().getName()),
            deviceSet, sequence);
        for (TsFileResource vmTsFileResource : vmTsFileResources) {
          deleteVmFile(vmTsFileResource);
        }
        vmWriters.clear();
        vmTsFileResources.clear();
        if (logFile.exists()) {
          Files.delete(logFile.toPath());
        }
      } else {
        File[] tmpFiles = FSFactoryProducer.getFSFactory()
            .listFilesBySuffix(writer.getFile().getParent(), PATH_UPGRADE);
        if (tmpFiles.length > 0) {
          for (File file : tmpFiles) {
            Files.delete(file.toPath());
          }
        }
      }
    } catch (IOException e) {
      logger.error("recover vm error ", e);
    }
  }


  /**
   * Take the first MemTable from the flushingMemTables and flush it. Called by a flush thread of
   * the flush manager pool
   */
  public void flushOneMemTable() {
    IMemTable memTableToFlush;
    memTableToFlush = flushingMemTables.getFirst();
    if (logger.isInfoEnabled()) {
      logger.info("{}: {} starts to flush a memtable in a flush thread", storageGroupName,
          tsFileResource.getTsFile().getName());
    }
    // signal memtable only may appear when calling asyncClose()
    if (!memTableToFlush.isSignalMemTable()) {
      flushVmTimes++;
      try {
        MemTableFlushTask flushTask;
        RestorableTsFileIOWriter curWriter;
        if (config.isEnableVm()) {
          logger.info("[Flush] flush a vm");
          File newVmFile = createNewVMFile(tsFileResource);
          vmTsFileResources.add(new TsFileResource(newVmFile));
          curWriter = new RestorableTsFileIOWriter(newVmFile);
          vmWriters.add(curWriter);
        } else {
          curWriter = writer;
        }
        curWriter.mark();
        flushTask = new MemTableFlushTask(memTableToFlush, curWriter, storageGroupName);
        flushTask.syncFlushMemTable();
      } catch (Exception e) {
        logger.error("{}: {} meet error when flushing a memtable, change system mode to read-only",
            storageGroupName, tsFileResource.getTsFile().getName(), e);
        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
        try {
          logger.error("{}: {} IOTask meets error, truncate the corrupted data", storageGroupName,
              tsFileResource.getTsFile().getName(), e);
          writer.reset();
        } catch (IOException e1) {
          logger.error("{}: {} Truncate corrupted data meets error", storageGroupName,
              tsFileResource.getTsFile().getName(), e1);
        }
        Thread.currentThread().interrupt();
      }

      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        getLogNode().notifyEndFlush();
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: {} try get lock to release a memtable (signal={})", storageGroupName,
          tsFileResource.getTsFile().getName(), memTableToFlush.isSignalMemTable());
    }
    // for sync flush
    synchronized (memTableToFlush) {
      releaseFlushedMemTable(memTableToFlush);
      memTableToFlush.notifyAll();
      if (logger.isDebugEnabled()) {
        logger.debug("{}: {} released a memtable (signal={}), flushingMemtables size ={}",
            storageGroupName, tsFileResource.getTsFile().getName(),
            memTableToFlush.isSignalMemTable(), flushingMemTables.size());
      }
    }

    if (shouldClose && flushingMemTables.isEmpty()) {
      try {
        // merge vm to tsfile
        while (true) {
          if (!mergeWorking) {
            break;
          }
          try {
            TimeUnit.MILLISECONDS.sleep(10);
          } catch (@SuppressWarnings("squid:S2142") InterruptedException e) {
            logger.error("{}: {}, closing task is interrupted.",
                storageGroupName, tsFileResource.getTsFile().getName(), e);
            // generally it is because the thread pool is shutdown so the task should be aborted
            break;
          }
        }
        logger.info("[Flush] merge all {} vms to TsFile", vmTsFileResources.size() + 1);
        flushAllVmToTsFile(vmWriters, vmTsFileResources);
        writer.mark();
        try {
          double compressionRatio = ((double) totalMemTableSize) / writer.getPos();
          if (logger.isDebugEnabled()) {
            logger.debug(
                "The compression ratio of tsfile {} is {}, totalMemTableSize: {}, the file size: {}",
                writer.getFile().getAbsolutePath(), compressionRatio, totalMemTableSize,
                writer.getPos());
          }
          if (compressionRatio == 0 && !memTableToFlush.isSignalMemTable()) {
            logger.error(
                "{} The compression ratio of tsfile {} is 0, totalMemTableSize: {}, the file size: {}",
                storageGroupName, writer.getFile().getAbsolutePath(), totalMemTableSize,
                writer.getPos());
          }
          CompressionRatio.getInstance().updateRatio(compressionRatio);
        } catch (IOException e) {
          logger.error("{}: {} update compression ratio failed", storageGroupName,
              tsFileResource.getTsFile().getName(), e);
        }
        if (logger.isDebugEnabled()) {
          logger
              .debug("{}: {} flushingMemtables is empty and will close the file", storageGroupName,
                  tsFileResource.getTsFile().getName());
        }
        endFile();
        if (logger.isDebugEnabled()) {
          logger.debug("{} flushingMemtables is clear {} vm files", storageGroupName,
              vmTsFileResources.size());
        }
      } catch (Exception e) {
        logger.error("{} meet error when flush FileMetadata to {}, change system mode to read-only",
            storageGroupName, tsFileResource.getTsFile().getAbsolutePath(), e);
        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
        try {
          writer.reset();
        } catch (IOException e1) {
          logger.error("{}: {} truncate corrupted data meets error", storageGroupName,
              tsFileResource.getTsFile().getName(), e1);
        }
        logger.error("{}: {} marking or ending file meet error", storageGroupName,
            tsFileResource.getTsFile().getName(), e);
      }
      // for sync close
      if (logger.isDebugEnabled()) {
        logger.debug("{}: {} try to get flushingMemtables lock.", storageGroupName,
            tsFileResource.getTsFile().getName());
      }
      synchronized (flushingMemTables) {
        flushingMemTables.notifyAll();
      }
    } else {
      // on other merge task working now, it's safe to submit one.
      if (!mergeWorking) {
        mergeWorking = true;
        VmMergeTaskPoolManager.getInstance()
            .submit(new VmMergeTask(new ArrayList<>(vmTsFileResources), new ArrayList<>(vmWriters)));
      }
    }
  }

  private void endFile() throws IOException, TsFileProcessorException {
    long closeStartTime = System.currentTimeMillis();
    tsFileResource.serialize();
    writer.endFile();
    tsFileResource.cleanCloseFlag();

    // remove this processor from Closing list in StorageGroupProcessor,
    // mark the TsFileResource closed, no need writer anymore
    closeTsFileCallback.call(this);

    if (logger.isInfoEnabled()) {
      long closeEndTime = System.currentTimeMillis();
      logger.info("Storage group {} close the file {}, TsFile size is {}, "
              + "time consumption of flushing metadata is {}ms",
          storageGroupName, tsFileResource.getTsFile().getAbsoluteFile(),
          writer.getFile().length(),
          closeEndTime - closeStartTime);
    }

    writer = null;
  }


  public boolean isManagedByFlushManager() {
    return managedByFlushManager;
  }

  public void setManagedByFlushManager(boolean managedByFlushManager) {
    this.managedByFlushManager = managedByFlushManager;
  }

  WriteLogNode getLogNode() {
    if (logNode == null) {
      logNode = MultiFileLogNodeManager.getInstance()
          .getNode(storageGroupName + "-" + tsFileResource.getTsFile().getName());
    }
    return logNode;
  }

  public void close() throws TsFileProcessorException {
    try {
      //when closing resource file, its corresponding mod file is also closed.
      tsFileResource.close();
      MultiFileLogNodeManager.getInstance()
          .deleteNode(storageGroupName + "-" + tsFileResource.getTsFile().getName());
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  public int getFlushingMemTableSize() {
    return flushingMemTables.size();
  }

  public long getWorkMemTableMemory() {
    return workMemTable.memSize();
  }

  RestorableTsFileIOWriter getWriter() {
    return writer;
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  /**
   * get the chunk(s) in the memtable (one from work memtable and the other ones in flushing
   * memtables and then compact them into one TimeValuePairSorter). Then get the related
   * ChunkMetadata of data on disk.
   *
   * @param deviceId device id
   * @param measurementId measurements id
   * @param dataType data type
   * @param encoding encoding
   * @return left: the chunk data in memory; right: the chunkMetadatas of data on disk
   */
  public Pair<List<ReadOnlyMemChunk>, List<List<ChunkMetadata>>> query(String deviceId,
      String measurementId, TSDataType dataType, TSEncoding encoding, Map<String, String> props,
      QueryContext context) {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: {} get flushQueryLock read lock", storageGroupName,
          tsFileResource.getTsFile().getName());
    }
    flushQueryLock.readLock().lock();
    try {
      List<ReadOnlyMemChunk> readOnlyMemChunks = new ArrayList<>();
      for (IMemTable flushingMemTable : flushingMemTables) {
        if (flushingMemTable.isSignalMemTable()) {
          continue;
        }
        ReadOnlyMemChunk memChunk = flushingMemTable.query(deviceId, measurementId,
            dataType, encoding, props, context.getQueryTimeLowerBound());
        if (memChunk != null) {
          readOnlyMemChunks.add(memChunk);
        }
      }
      if (workMemTable != null) {
        ReadOnlyMemChunk memChunk = workMemTable.query(deviceId, measurementId, dataType, encoding,
            props, context.getQueryTimeLowerBound());
        if (memChunk != null) {
          readOnlyMemChunks.add(memChunk);
        }
      }

      ModificationFile modificationFile = tsFileResource.getModFile();
      List<Modification> modifications = context.getPathModifications(modificationFile,
          deviceId + IoTDBConstant.PATH_SEPARATOR + measurementId);

      List<List<ChunkMetadata>> rightResult = new ArrayList<>();

      // get unseal tsfile data and its read lock
      List<ChunkMetadata> chunkMetadataList = writer
          .getVisibleMetadataList(deviceId, measurementId, dataType);
      QueryUtils.modifyChunkMetaData(chunkMetadataList,
          modifications);
      chunkMetadataList.removeIf(context::chunkNotSatisfy);
      rightResult.add(chunkMetadataList);

      // get vm tsfile data
      for (int i = 0; i < vmWriters.size(); i++) {
        RestorableTsFileIOWriter vmWriter = vmWriters.get(i);
        for (Entry<String, Map<String, List<ChunkMetadata>>> entry : vmWriter.getMetadatasForQuery()
            .entrySet()) {
          String device = entry.getKey();
          for (List<ChunkMetadata> tmpChunkMetadataList : entry.getValue().values()) {
            for (ChunkMetadata chunkMetadata : tmpChunkMetadataList) {
              vmTsFileResources.get(i).updateStartTime(device, chunkMetadata.getStartTime());
              if (!sequence) {
                vmTsFileResources.get(i).updateStartTime(device, chunkMetadata.getEndTime());
              }
            }
          }
        }
        chunkMetadataList = vmWriter.getVisibleMetadataList(deviceId, measurementId, dataType);
        QueryUtils.modifyChunkMetaData(chunkMetadataList,
            modifications);
        chunkMetadataList.removeIf(context::chunkNotSatisfy);
        rightResult.add(chunkMetadataList);
      }

      return new Pair<>(readOnlyMemChunks, rightResult);
    } catch (Exception e) {
      logger.error("{}: {} get ReadOnlyMemChunk has error", storageGroupName,
          tsFileResource.getTsFile().getName(), e);
    } finally {
      flushQueryLock.readLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug("{}: {} release flushQueryLock read lock", storageGroupName,
            tsFileResource.getTsFile().getName());
      }
    }
    return null;
  }

  public long getTimeRangeId() {
    return timeRangeId;
  }

  public void setTimeRangeId(long timeRangeId) {
    this.timeRangeId = timeRangeId;
  }

  public void putMemTableBackAndClose() throws TsFileProcessorException {
    if (workMemTable != null) {
      workMemTable.release();
      MemTablePool.getInstance().putBack(workMemTable, storageGroupName);
    }
    try {
      writer.close();
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  public List<TsFileResource> getVmTsFileResources() {
    return vmTsFileResources;
  }

  private void flushAllVmToTsFile(List<RestorableTsFileIOWriter> currMergeVmWriters,
      List<TsFileResource> currMergeVmFiles) throws IOException {
    VmMergeUtils.fullMerge(writer, currMergeVmWriters,
        storageGroupName,
        new VmLogger(tsFileResource.getTsFile().getParent(),
            tsFileResource.getTsFile().getName()),
        new HashSet<>(), sequence);
    for (TsFileResource vmTsFileResource : currMergeVmFiles) {
      deleteVmFile(vmTsFileResource);
    }
    vmWriters.removeAll(currMergeVmWriters);
    vmTsFileResources.removeAll(currMergeVmFiles);
    File logFile = FSFactoryProducer.getFSFactory()
        .getFile(tsFileResource.getTsFile().getParent(),
            tsFileResource.getTsFile().getName() + VM_LOG_NAME);
    if (logFile.exists()) {
      Files.delete(logFile.toPath());
    }
  }

  class VmMergeTask implements Runnable {

    private final List<TsFileResource> vmMergeTsFiles;
    private final List<RestorableTsFileIOWriter> vmMergeWriters;

    public VmMergeTask(
        List<TsFileResource> vmMergeTsFiles,
        List<RestorableTsFileIOWriter> vmMergeWriters) {
      this.vmMergeTsFiles = vmMergeTsFiles;
      this.vmMergeWriters = vmMergeWriters;
    }

    @Override
    public void run() {
      try {
        long vmPointNum = 0;
        // all flush to target file
        Map<Path, MeasurementSchema> pathMeasurementSchemaMap = new HashMap<>();
        for (RestorableTsFileIOWriter vmWriter : vmMergeWriters) {
          Map<String, Map<String, List<ChunkMetadata>>> schemaMap = vmWriter
              .getMetadatasForQuery();
          for(Entry<String, Map<String, List<ChunkMetadata>>> schemaMapEntry : schemaMap
              .entrySet()) {
            String device = schemaMapEntry.getKey();
            for (Entry<String, List<ChunkMetadata>> entry : schemaMapEntry.getValue().entrySet()) {
              String measurement = entry.getKey();
              List<ChunkMetadata> chunkMetadataList = entry.getValue();
              for (ChunkMetadata chunkMetadata : chunkMetadataList) {
                vmPointNum += chunkMetadata.getNumOfPoints();
              }
              pathMeasurementSchemaMap.computeIfAbsent(new Path(device, measurement), k ->
                  new MeasurementSchema(measurement, chunkMetadataList.get(0).getDataType()));
            }
          }
        }
        if (pathMeasurementSchemaMap.size() > 0
            && vmPointNum / pathMeasurementSchemaMap.size() > config
            .getMergeChunkPointNumberThreshold() || flushVmTimes >= config
            .getMaxMergeChunkNumInTsFile()) {
          // merge vm to tsfile
          flushVmTimes = 0;
          logger.info("[Flush] merge {} vms to TsFile", vmMergeWriters.size() + 1);
          flushAllVmToTsFile(vmMergeWriters, vmMergeTsFiles);
        } else if (config.getMaxVmNum() <= vmMergeTsFiles.size()) {
          // merge vm files
          logger.info("[Flush] merge {} vms to vm", vmMergeTsFiles.size() + 1);
          // merge all vm files into a new vm file
          File tmpFile = createNewTmpFile();
          RestorableTsFileIOWriter tmpWriter = new RestorableTsFileIOWriter(tmpFile);
          VmMergeUtils.fullMerge(tmpWriter, vmMergeWriters,
              storageGroupName, null, new HashSet<>(), sequence);
          File newVmFile = createNewVMFile(tsFileResource);
          File mergedFile = FSFactoryProducer.getFSFactory().getFile(newVmFile.getPath()
              + MERGED_SUFFIX);
          if (!tmpFile.renameTo(mergedFile)) {
            logger.error("Failed to rename {} to {}", newVmFile, mergedFile);
          }
          for (TsFileResource vmTsFileResource : vmMergeTsFiles) {
            deleteVmFile(vmTsFileResource);
          }
          vmWriters.removeAll(vmMergeWriters);
          vmTsFileResources.removeAll(vmMergeTsFiles);
          if (!mergedFile.renameTo(newVmFile)) {
            logger.error("Failed to rename {} to {}", mergedFile, newVmFile);
          }
          vmTsFileResources.add(0, new TsFileResource(newVmFile));
          vmWriters.add(0, new RestorableTsFileIOWriter(newVmFile));
        }
      } catch (Exception e) {
        logger.error("Error occurred in Vm Merge thread", e);
      } finally {
        // reset the merge working state to false
        mergeWorking = false;
      }
    }

    public ReadWriteLock getFlushQueryLock() {
      return flushQueryLock;
    }

    private File createNewTmpFile() {
      File parent = writer.getFile().getParentFile();
      return FSFactoryProducer.getFSFactory().getFile(parent,
          writer.getFile().getName() + IoTDBConstant.FILE_NAME_SEPARATOR + System
              .currentTimeMillis()
              + VM_SUFFIX + IoTDBConstant.PATH_SEPARATOR
              + PATH_UPGRADE);
    }
  }
}
