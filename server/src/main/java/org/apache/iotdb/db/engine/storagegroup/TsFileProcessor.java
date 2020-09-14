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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.CompressionRatio;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.engine.flush.NotifyFlushMemTable;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseTsFileCallBack;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.UpdateEndTimeCallBack;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileProcessor {

  private static final Logger logger = LoggerFactory.getLogger(TsFileProcessor.class);
  private final String storageGroupName;

  private StorageGroupInfo storageGroupInfo;
  private TsFileProcessorInfo tsFileProcessorInfo;

  /**
   * sync this object in query() and asyncTryToFlush()
   */
  private final ConcurrentLinkedDeque<IMemTable> flushingMemTables = new ConcurrentLinkedDeque<>();
  private RestorableTsFileIOWriter writer;
  private final TsFileResource tsFileResource;
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

  private static final String FLUSH_QUERY_WRITE_LOCKED = "{}: {} get flushQueryLock write lock";
  private static final String FLUSH_QUERY_WRITE_RELEASE = "{}: {} get flushQueryLock write lock released";

  TsFileProcessor(String storageGroupName, StorageGroupInfo storageGroupInfo, File tsfile,
      VersionController versionController,
      CloseTsFileCallBack closeTsFileCallback,
      UpdateEndTimeCallBack updateLatestFlushTimeCallback, boolean sequence)
      throws IOException {
    this.storageGroupName = storageGroupName;
    this.storageGroupInfo = storageGroupInfo;
    this.tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
    this.storageGroupInfo.reportTsFileProcessorInfo(this);
    this.tsFileResource = new TsFileResource(tsfile, this);
    this.tsFileProcessorInfo.addUnsealedResourceMemCost(tsFileResource.calculateRamSize());
    SystemInfo.getInstance().reportStorageGroupStatus(storageGroupInfo);
    this.versionController = versionController;
    this.writer = new RestorableTsFileIOWriter(tsfile);
    this.closeTsFileCallback = closeTsFileCallback;
    this.updateLatestFlushTimeCallback = updateLatestFlushTimeCallback;
    this.sequence = sequence;
    logger.info("create a new tsfile processor {}", tsfile.getAbsolutePath());
    // a file generated by flush has only one historical version, which is itself
    this.tsFileResource
        .setHistoricalVersions(Collections.singleton(versionController.currVersion()));
  }

  public TsFileProcessor(String storageGroupName, StorageGroupInfo storageGroupInfo, TsFileResource tsFileResource,
      VersionController versionController, CloseTsFileCallBack closeUnsealedTsFileProcessor,
      UpdateEndTimeCallBack updateLatestFlushTimeCallback, boolean sequence,
      RestorableTsFileIOWriter writer) {
    this.storageGroupName = storageGroupName;
    this.storageGroupInfo = storageGroupInfo;
    this.tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
    this.storageGroupInfo.reportTsFileProcessorInfo(this);
    this.tsFileResource = tsFileResource;
    this.tsFileProcessorInfo.addUnsealedResourceMemCost(tsFileResource.calculateRamSize());
    SystemInfo.getInstance().reportStorageGroupStatus(storageGroupInfo); 
    this.versionController = versionController;
    this.writer = writer;
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

    while (SystemInfo.getInstance().isRejected()) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
        logger.debug("System rejected, waiting for memory releasing... "
            + "Current array pool cost{}, sg cost {}", SystemInfo.getInstance()
            .getArrayPoolMemCost(), SystemInfo.getInstance().getTotalSgMemCost());
      } catch (InterruptedException e) {
        logger.error("Failed when waiting for getting memory for insertion ", e);
        Thread.currentThread().interrupt();
      }
    }

    boolean needToReport = checkMemCostAndAddToTspInfo(insertRowPlan);
    if (workMemTable == null) {
      workMemTable = new PrimitiveMemTable();
    }
    // If there are enough size of arrays in memtable, insert insertRowPlan to
    // the work memtable directly
    if (workMemTable.checkIfArrayIsEnough(insertRowPlan)) {
      logger.debug("Array in work MemTable is enough");
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
      tsFileResource.updateStartTime(insertRowPlan.getDeviceId().getFullPath(), insertRowPlan.getTime());
      //for sequence tsfile, we update the endTime only when the file is prepared to be closed.
      //for unsequence tsfile, we have to update the endTime for each insertion.
      if (!sequence) {
        tsFileResource.updateEndTime(insertRowPlan.getDeviceId().getFullPath(), insertRowPlan.getTime());
      }
      if (needToReport) {
        SystemInfo.getInstance().reportStorageGroupStatus(storageGroupInfo);
      }
    }
    // If there aren't enough size of arrays in memtable, it may apply buffered array
    // from the array pool or get OOB array before inserting into the memtable
    else {
      try {
        logger.debug("Array in work MemTable isn't enough");
        workMemTable.insert(insertRowPlan);
        if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
          getLogNode().write(insertRowPlan);
        }
        // update start time of this memtable
        tsFileResource.updateStartTime(insertRowPlan.getDeviceId().getFullPath(), insertRowPlan.getTime());
        //for sequence tsfile, we update the endTime only when the file is prepared to be closed.
        //for unsequence tsfile, we have to update the endTime for each insertion.
        if (!sequence) {
          tsFileResource.updateEndTime(insertRowPlan.getDeviceId().getFullPath(), insertRowPlan.getTime());
        }
        if (needToReport) {
          SystemInfo.getInstance().reportStorageGroupStatus(storageGroupInfo);
        }
      } catch (Exception e) {
        throw new WriteProcessException(e);
      }
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

    while (SystemInfo.getInstance().isRejected()) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
        logger.debug("System rejected, waiting for memory releasing... "
            + "Current array pool cost{}, sg cost {}", SystemInfo.getInstance()
            .getArrayPoolMemCost(), SystemInfo.getInstance().getTotalSgMemCost());
      } catch (InterruptedException e) {
        logger.error("Failed when waiting for getting memory for insertion ", e);
        Thread.currentThread().interrupt();
      }
    }

    boolean needToReport = checkMemCostAndAddToTspInfo(insertTabletPlan);
    if (workMemTable == null) {
      workMemTable = new PrimitiveMemTable();
    }
    // If there are enough size of arrays in memtable, insert insertRowPlan to
    // the work memtable directly
    if (workMemTable.checkIfArrayIsEnough(insertTabletPlan)) {
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
          .updateStartTime(insertTabletPlan.getDeviceId().getFullPath(), insertTabletPlan.getTimes()[start]);

      //for sequence tsfile, we update the endTime only when the file is prepared to be closed.
      //for unsequence tsfile, we have to update the endTime for each insertion.
      if (!sequence) {
        tsFileResource
            .updateEndTime(
                insertTabletPlan.getDeviceId().getFullPath(), insertTabletPlan.getTimes()[end - 1]);
      }
      if (needToReport) {
        SystemInfo.getInstance().reportStorageGroupStatus(storageGroupInfo);
      }
    }
    // If there aren't enough size of arrays in memtable, it may apply buffered array
    // from the array pool or get OOB array before inserting into the memtable
    else {
      try {
        // if get buffered or OOB array successfully, estimate the memory cost
        for (int i = start; i < end; i++) {
          results[i] = RpcUtils.SUCCESS_STATUS;
        }
        
        workMemTable.insertTablet(insertTabletPlan, start, end);
        if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
          insertTabletPlan.setStart(start);
          insertTabletPlan.setEnd(end);
          getLogNode().write(insertTabletPlan);
        }
        tsFileResource
            .updateStartTime(insertTabletPlan.getDeviceId().getFullPath(), insertTabletPlan.getTimes()[start]);

        //for sequence tsfile, we update the endTime only when the file is prepared to be closed.
        //for unsequence tsfile, we have to update the endTime for each insertion.
        if (!sequence) {
          tsFileResource
              .updateEndTime(
                  insertTabletPlan.getDeviceId().getFullPath(), insertTabletPlan.getTimes()[end - 1]);
        }
      } catch (Exception e) {
        for (int i = start; i < end; i++) {
          results[i] = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        throw new WriteProcessException(e);
      }
      if (needToReport) {
        SystemInfo.getInstance().reportStorageGroupStatus(storageGroupInfo);
      }
    }
  }

  private boolean checkMemCostAndAddToTspInfo(InsertPlan insertPlan) {
    long bytesCost = 0L;
    long unsealedResourceCost = 0L;
    long chunkMetadataCost = 0L;
    unsealedResourceCost = tsFileResource.estimateRamIncrement(insertPlan.getDeviceId().getFullPath());
    for (int i = 0; i < insertPlan.getDataTypes().length; i++) {
      // skip failed Measurements
      if (insertPlan.getDataTypes()[i] == null) {
        continue;
      }
      // String array cost
      if (insertPlan.getDataTypes()[i] == TSDataType.TEXT) {
        if (insertPlan instanceof InsertRowPlan) {
          bytesCost += RamUsageEstimator.sizeOf((Binary) ((InsertRowPlan) insertPlan).getValues()[i]);
        }
        else {
          for (Binary bytes : (Binary[]) ((InsertTabletPlan) insertPlan).getColumns()[i]) {
            bytesCost += RamUsageEstimator.sizeOf(bytes);
          }
        }
      }
      if (workMemTable == null) {
        workMemTable = new PrimitiveMemTable();
      }
      // ChunkMetadataCost
      if (workMemTable.checkIfNeedStartNewChunk(insertPlan.getDeviceId().getFullPath(),
          insertPlan.getMeasurements()[i])) {
        chunkMetadataCost += ChunkMetadata.calculateRamSize(insertPlan.getMeasurements()[i],
            insertPlan.getDataTypes()[i]);
      }
    }
    tsFileProcessorInfo.addBytesMemCost(bytesCost);
    tsFileProcessorInfo.addUnsealedResourceMemCost(unsealedResourceCost);
    tsFileProcessorInfo.addChunkMetadataMemCost(chunkMetadataCost);
    if (bytesCost != 0 && storageGroupInfo.checkIfNeedToReportStatusToSystem()) {
      return true;
    }
    return false;
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
      logger
          .debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
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


  public boolean shouldFlush() {
    if (workMemTable == null) {
      return false;
    }

    if (workMemTable.reachTotalPointNumThreshold()) {
      logger.info("The avg series points num {} of tsfile {} reaches the threshold",
          workMemTable.getTotalPointsNum() / workMemTable.getSeriesNumber(),
          tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }

    return false;
  }

  public boolean shouldClose() {
    long fileSize = tsFileResource.getTsFileSize();
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
      logger
          .debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {

      if (logger.isInfoEnabled()) {
        if (workMemTable != null) {
          logger.info(
              "{}: flush a working memtable in async close tsfile {}, memtable size: {}, tsfile size: {}",
              storageGroupName, tsFileResource.getTsFile().getAbsolutePath(),
              workMemTable.memSize(),
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

      // we have to add the memtable into flushingList first and then set the shouldClose tag.
      // see https://issues.apache.org/jira/browse/IOTDB-510
      IMemTable tmpMemTable = workMemTable == null || workMemTable.memSize() == 0 
          ? new NotifyFlushMemTable() 
          : workMemTable;

      try {
        // When invoke closing TsFile after insert data to memTable, we shouldn't flush until invoke
        // flushing memTable in System module.
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
      logger
          .debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
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
      logger
          .debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      if (workMemTable == null || workMemTable.size() == 0) {
        return;
      }
      addAMemtableIntoFlushingList(workMemTable);
    } catch (Exception e) {
      logger.error("{}: {} add a memtable into flushing list failed", storageGroupName,
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
      logger.debug("before release a memtable, current Array cost {}, sg cost {}",
          SystemInfo.getInstance().getArrayPoolMemCost(), SystemInfo.getInstance().getTotalSgMemCost());
      memTable.release();
      logger.debug("after release a memtable, current Buffered Array cost {}, oob {}, sg cost {}",
          PrimitiveArrayManager.getBufferedArraysSize(), PrimitiveArrayManager.getOOBSize(), 
          SystemInfo.getInstance().getTotalSgMemCost());
      if (tsFileProcessorInfo.getBytesMemCost() > 0) {
        // For text type data, reset the mem cost in tsFileProcessorInfo
        tsFileProcessorInfo.clearBytesMemCost();
        // report to System
        SystemInfo.getInstance().resetStorageGroupInfoStatus(storageGroupInfo);
      }
      memTable = null;
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

  /**
   * Take the first MemTable from the flushingMemTables and flush it. Called by a flush thread of
   * the flush manager pool
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void flushOneMemTable() {
    IMemTable memTableToFlush;
    memTableToFlush = flushingMemTables.getFirst();
    // signal memtable only may appear when calling asyncClose()
    if (!memTableToFlush.isSignalMemTable()) {
      try {
        MemTableFlushTask flushTask;
        writer.mark();
        flushTask = new MemTableFlushTask(memTableToFlush, writer, storageGroupName);
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
          logger.debug("before end memtable, current Array cost {}, sg cost {}",
              SystemInfo.getInstance().getArrayPoolMemCost(), SystemInfo.getInstance().getTotalSgMemCost());
        }
        endFile();
        if (logger.isDebugEnabled()) {
          logger.debug("after end memtable, current Array cost {}, sg cost {}",
              SystemInfo.getInstance().getArrayPoolMemCost(), SystemInfo.getInstance().getTotalSgMemCost());
          logger.debug("{} flushingMemtables is clear", storageGroupName);
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
    tsFileProcessorInfo.clear();
    storageGroupInfo.closeTsFileProcessorAndReportToSystem(this);
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
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void query(String deviceId, String measurementId, TSDataType dataType, TSEncoding encoding,
      Map<String, String> props, QueryContext context,
      List<TsFileResource> tsfileResourcesForQuery) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: {} get flushQueryLock and vmMergeLock read lock", storageGroupName,
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

      List<ChunkMetadata> chunkMetadataList = writer
          .getVisibleMetadataList(deviceId, measurementId, dataType);
      QueryUtils.modifyChunkMetaData(chunkMetadataList,
          modifications);
      chunkMetadataList.removeIf(context::chunkNotSatisfy);

      // get in memory data
      if (!readOnlyMemChunks.isEmpty() || !chunkMetadataList.isEmpty()) {
        tsfileResourcesForQuery.add(new TsFileResource(tsFileResource.getTsFile(),
            tsFileResource.getDeviceToIndexMap(),
            tsFileResource.getStartTimes(), tsFileResource.getEndTimes(), readOnlyMemChunks,
            chunkMetadataList, tsFileResource));
      }
    } catch (QueryProcessException e) {
      logger.error("{}: {} get ReadOnlyMemChunk has error", storageGroupName,
          tsFileResource.getTsFile().getName(), e);
    } finally {
      flushQueryLock.readLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug("{}: {} release flushQueryLock", storageGroupName,
            tsFileResource.getTsFile().getName());
      }
    }
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
      workMemTable = null;
    }
    try {
      writer.close();
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  public TsFileProcessorInfo getTsFileProcessorInfo() {
    return tsFileProcessorInfo;
  }

  public long getWorkMemTableSize() {
    return workMemTable != null ? workMemTable.memSize() : 0;
  }

  public boolean isSequence() {
    return sequence;
  }

  public void startClose() {
    storageGroupInfo.getStorageGroupProcessor().asyncCloseOneTsFileProcessor(sequence, this);
    logger.info("Async close tsfile: {}",
        getTsFileResource().getTsFile().getAbsolutePath());
  }

  
}
