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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.ActiveTimeSeriesCounter;
import org.apache.iotdb.db.conf.adapter.CompressionRatio;
import org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.engine.flush.NotifyFlushMemTable;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseTsFileCallBack;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.UpdateEndTimeCallBack;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.rescon.MemTablePool;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileProcessor {

  private static final Logger logger = LoggerFactory.getLogger(TsFileProcessor.class);
  private final String storageGroupName;
  /**
   * sync this object in query() and asyncTryToFlush()
   */
  private final ConcurrentLinkedDeque<IMemTable> flushingMemTables = new ConcurrentLinkedDeque<>();
  private RestorableTsFileIOWriter writer;
  private Schema schema;
  private TsFileResource tsFileResource;
  // time range index to indicate this processor belongs to which time range
  private long timeRangeId;
  /**
   * Whether the processor is in the queue of the FlushManager or being flushed by a flush thread.
   */
  private volatile boolean managedByFlushManager;
  private ReadWriteLock flushQueryLock = new ReentrantReadWriteLock();
  /**
   * It is set by the StorageGroupProcessor and checked by flush threads. (If shouldClose == true
   * and its flushingMemTables are all flushed, then the flush thread will close this file.)
   */
  private volatile boolean shouldClose;
  private IMemTable workMemTable;
  private VersionController versionController;
  /**
   * this callback is called after the corresponding TsFile is called endFile().
   */
  private CloseTsFileCallBack closeTsFileCallback;
  /**
   * this callback is called before the workMemtable is added into the flushingMemTables.
   */
  private UpdateEndTimeCallBack updateLatestFlushTimeCallback;
  private WriteLogNode logNode;
  private boolean sequence;
  private long totalMemTableSize;

  private static final String FLUSH_QUERY_WRITE_LOCKED = "{}: {} get flushQueryLock write lock";
  private static final String FLUSH_QUERY_WRITE_RELEASE = "{}: {} get flushQueryLock write lock released";

  TsFileProcessor(String storageGroupName, File tsfile, Schema schema,
      VersionController versionController,
      CloseTsFileCallBack closeTsFileCallback,
      UpdateEndTimeCallBack updateLatestFlushTimeCallback, boolean sequence)
      throws IOException {
    this.storageGroupName = storageGroupName;
    this.schema = schema;
    this.tsFileResource = new TsFileResource(tsfile, this);
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

  public VersionController getVersionController() {
    return versionController;
  }

  public void setVersionController(VersionController versionController) {
    this.versionController = versionController;
  }

  public TsFileProcessor(String storageGroupName, TsFileResource tsFileResource, Schema schema,
      VersionController versionController, CloseTsFileCallBack closeUnsealedTsFileProcessor,
      UpdateEndTimeCallBack updateLatestFlushTimeCallback, boolean sequence,
      RestorableTsFileIOWriter writer) {
    this.storageGroupName = storageGroupName;
    this.tsFileResource = tsFileResource;
    this.schema = schema;
    this.versionController = versionController;
    this.writer = writer;
    this.closeTsFileCallback = closeUnsealedTsFileProcessor;
    this.updateLatestFlushTimeCallback = updateLatestFlushTimeCallback;
    this.sequence = sequence;
    logger.info("reopen a tsfile processor {}", tsFileResource.getFile());
  }

  /**
   * insert data in an InsertPlan into the workingMemtable.
   *
   * @param insertPlan physical plan of insertion
   * @return succeed or fail
   */
  public boolean insert(InsertPlan insertPlan) throws QueryProcessException {

    if (workMemTable == null) {
      workMemTable = MemTablePool.getInstance().getAvailableMemTable(this);
    }

    // insert insertPlan to the work memtable
    workMemTable.insert(insertPlan);

    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      try {
        getLogNode().write(insertPlan);
      } catch (IOException e) {
        logger.error("{}: {} write WAL failed", storageGroupName,
            tsFileResource.getFile().getName(), e);
        return false;
      }
    }

    // update start time of this memtable
    tsFileResource.updateStartTime(insertPlan.getDeviceId(), insertPlan.getTime());
    //for sequence tsfile, we update the endTime only when the file is prepared to be closed.
    //for unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource.updateEndTime(insertPlan.getDeviceId(), insertPlan.getTime());
    }

    return true;
  }

  public boolean insertBatch(BatchInsertPlan batchInsertPlan, int start, int end,
      Integer[] results) throws QueryProcessException {

    if (workMemTable == null) {
      workMemTable = MemTablePool.getInstance().getAvailableMemTable(this);
    }

    // insert insertPlan to the work memtable
    workMemTable.insertBatch(batchInsertPlan, start, end);

    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      try {
        batchInsertPlan.setStart(start);
        batchInsertPlan.setEnd(end);
        getLogNode().write(batchInsertPlan);
      } catch (IOException e) {
        logger.error("{}: {} write WAL failed", storageGroupName,
            tsFileResource.getFile().getName(), e);
        for (int i = start; i < end; i++) {
          results[i] = TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode();
        }
        return false;
      }
    }

    tsFileResource
        .updateStartTime(batchInsertPlan.getDeviceId(), batchInsertPlan.getTimes()[start]);

    //for sequence tsfile, we update the endTime only when the file is prepared to be closed.
    //for unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource
          .updateEndTime(batchInsertPlan.getDeviceId(), batchInsertPlan.getTimes()[end - 1]);
    }

    return true;
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
      logger.debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getFile().getName());
    }
    try {
      if (workMemTable != null) {
        workMemTable
            .delete(deletion.getDevice(), deletion.getMeasurement(), deletion.getTimestamp());
      }
      // flushing memTables are immutable, only record this deletion in these memTables for query
      for (IMemTable memTable : flushingMemTables) {
        memTable.delete(deletion);
      }
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(FLUSH_QUERY_WRITE_RELEASE, storageGroupName,
            tsFileResource.getFile().getName());
      }
    }
  }

  public TsFileResource getTsFileResource() {
    return tsFileResource;
  }


  boolean shouldFlush() {
    return workMemTable != null
        && workMemTable.memSize() > getMemtableSizeThresholdBasedOnSeriesNum();
  }

  /**
   * <p>In the dynamic parameter adjustment module{@link IoTDBConfigDynamicAdapter}, it calculated
   * the average size of each metatable{@link IoTDBConfigDynamicAdapter#tryToAdaptParameters()}.
   * However, considering that the number of timeseries between storage groups may vary greatly,
   * it's inappropriate to judge whether to flush the memtable according to the average memtable
   * size. We need to adjust it according to the number of timeseries in a specific storage group.
   */
  private long getMemtableSizeThresholdBasedOnSeriesNum() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    long memTableSize = (long) (config.getMemtableSizeThreshold() * config.getMaxMemtableNumber()
        / IoTDBDescriptor.getInstance().getConfig().getMemtableNumInEachStorageGroup()
        * ActiveTimeSeriesCounter.getInstance()
        .getActiveRatio(storageGroupName));
    return Math.max(memTableSize, config.getMemtableSizeThreshold());
  }


  public boolean shouldClose() {
    long fileSize = tsFileResource.getFileSize();
    long fileSizeThreshold = IoTDBDescriptor.getInstance().getConfig()
        .getTsFileSizeThreshold();
    return fileSize > fileSizeThreshold;
  }

  void syncClose() {
    logger.info("Sync close file: {}, will firstly async close it",
        tsFileResource.getFile().getAbsolutePath());
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
            logger.warn("{} has spent {}s for waiting flushing one memtable; {} left (first: {}). FlushingManager info: {}",
                this.tsFileResource.getFile().getAbsolutePath(),
                (System.currentTimeMillis() - startTime)/1000,
                flushingMemTables.size(),
                flushingMemTables.getFirst(),
                FlushManager.getInstance()
                );
          }

        }
      } catch (InterruptedException e) {
        logger.error("{}: {} wait close interrupted", storageGroupName,
            tsFileResource.getFile().getName(), e);
        Thread.currentThread().interrupt();
      }
    }
    logger.info("File {} is closed synchronously", tsFileResource.getFile().getAbsolutePath());
  }


  void asyncClose() {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getFile().getName());
    }
    try {
      logger.info("Async close the file: {}", tsFileResource.getFile().getAbsolutePath());
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
      IMemTable tmpMemTable = workMemTable == null ? new NotifyFlushMemTable() : workMemTable;
      if (logger.isDebugEnabled()) {
        logger
            .debug("{}: {} async flush a memtable (signal = {}) when async close",
                storageGroupName, tsFileResource.getFile().getName(), tmpMemTable.isSignalMemTable());
      }
      try {
        addAMemtableIntoFlushingList(tmpMemTable);
        shouldClose = true;
        tsFileResource.setCloseFlag();
      } catch (Exception e) {
        logger.error("{}: {} async close failed, because", storageGroupName,
            tsFileResource.getFile().getName(), e);
      }
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(FLUSH_QUERY_WRITE_RELEASE, storageGroupName,
            tsFileResource.getFile().getName());
      }
    }
  }

  /**
   * TODO if the flushing thread is too fast, the tmpMemTable.wait() may never wakeup
   * Tips: I am trying to solve this issue by checking whether the table exist before wait()
   */
  public void syncFlush() throws IOException {
    IMemTable tmpMemTable;
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getFile().getName());
    }
    try {
      tmpMemTable = workMemTable == null ? new NotifyFlushMemTable() : workMemTable;
      if (logger.isDebugEnabled() && tmpMemTable.isSignalMemTable()) {
        logger.debug("{}: {} add a signal memtable into flushing memtable list when sync flush",
            storageGroupName, tsFileResource.getFile().getName());
      }
      addAMemtableIntoFlushingList(tmpMemTable);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.error(FLUSH_QUERY_WRITE_RELEASE, storageGroupName,
            tsFileResource.getFile().getName());
      }
    }

    synchronized (tmpMemTable) {
      try {
        long startWait = System.currentTimeMillis();
        while (flushingMemTables.contains(tmpMemTable)) {
          tmpMemTable.wait(1000);

          if ((System.currentTimeMillis() - startWait) > 60_000) {
            logger.warn("has waited for synced flushing a memtable in {} for 60 seconds.",
                this.tsFileResource.getFile().getAbsolutePath());
            startWait = System.currentTimeMillis();
          }
        }
      } catch (InterruptedException e) {
        logger.error("{}: {} wait flush finished meets error", storageGroupName,
            tsFileResource.getFile().getName(), e);
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
      logger.debug(FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getFile().getName());
    }
    try {
      if (workMemTable == null) {
        return;
      }
      addAMemtableIntoFlushingList(workMemTable);
    } catch (Exception e) {
      logger.error("{}: {} add a memtable into flushing listfailed", storageGroupName,
          tsFileResource.getFile().getName(), e);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(FLUSH_QUERY_WRITE_RELEASE, storageGroupName,
            tsFileResource.getFile().getName());
      }
    }
  }

  /**
   * this method calls updateLatestFlushTimeCallback and move the given memtable into the flushing
   * queue, set the current working memtable as null and then register the tsfileProcessor into the
   * flushManager again.
   */
  private void addAMemtableIntoFlushingList(IMemTable tobeFlushed) throws IOException {
    if(!updateLatestFlushTimeCallback.call(this)){
      logger.error("{}: {} Memetable info: {}", storageGroupName,
          tsFileResource.getFile().getName(), tobeFlushed.getMemTableMap());
    }
    flushingMemTables.addLast(tobeFlushed);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {} Memtable (signal = {}) is added into the flushing Memtable, queue size = {}",
          storageGroupName, tsFileResource.getFile().getName(),
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
          tsFileResource.getFile().getName());
    }
    try {
      writer.makeMetadataVisible();
      if (!flushingMemTables.remove(memTable)) {
        logger.warn(
            "{}: {} put the memtable (signal={}) out of flushingMemtables but it is not in the queue.",
            storageGroupName, tsFileResource.getFile().getName(), memTable.isSignalMemTable());
      } else if (logger.isDebugEnabled()){
        logger.debug(
            "{}: {} memtable (signal={}) is removed from the queue. {} left.", storageGroupName,
            tsFileResource.getFile().getName(),
            memTable.isSignalMemTable(), flushingMemTables.size());
      }
      memTable.release();
      MemTablePool.getInstance().putBack(memTable, storageGroupName);
      if (logger.isDebugEnabled()) {
        logger.debug("{}: {} flush finished, remove a memtable from flushing list, "
            + "flushing memtable list size: {}", storageGroupName,
            tsFileResource.getFile().getName(), flushingMemTables.size());
      }
    } catch (Exception e) {
      logger.error("{}: {}", storageGroupName, tsFileResource.getFile().getName(), e);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(FLUSH_QUERY_WRITE_RELEASE, storageGroupName,
            tsFileResource.getFile().getName());
      }
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
              tsFileResource.getFile().getName());
    }
    // signal memtable only may appear when calling asyncClose()
    if (!memTableToFlush.isSignalMemTable()) {
      MemTableFlushTask flushTask = new MemTableFlushTask(memTableToFlush, schema, writer,
          storageGroupName);
      try {
        writer.mark();
        flushTask.syncFlushMemTable();
      } catch (Exception e) {
        logger.error("{}: {} meet error when flushing a memtable, change system mode to read-only",
            storageGroupName, tsFileResource.getFile().getName(), e);
        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
        try {
          logger.error("{}: {} IOTask meets error, truncate the corrupted data", storageGroupName,
              tsFileResource.getFile().getName(), e);
          writer.reset();
        } catch (IOException e1) {
          logger.error("{}: {} Truncate corrupted data meets error", storageGroupName,
              tsFileResource.getFile().getName(), e1);
        }
        Thread.currentThread().interrupt();
      }

      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        getLogNode().notifyEndFlush();
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: {} try get lock to release a memtable (signal={})", storageGroupName,
          tsFileResource.getFile().getName(), memTableToFlush.isSignalMemTable());
    }
    // for sync flush
    synchronized (memTableToFlush) {
      releaseFlushedMemTable(memTableToFlush);
      memTableToFlush.notifyAll();
      if (logger.isDebugEnabled()) {
        logger.debug("{}: {} released a memtable (signal={}), flushingMemtables size ={}",
            storageGroupName, tsFileResource.getFile().getName(),
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
          if (compressionRatio == 0) {
            logger.error(
                "{} The compression ratio of tsfile {} is 0, totalMemTableSize: {}, the file size: {}",
                storageGroupName, writer.getFile().getAbsolutePath(), totalMemTableSize, writer.getPos());
          }
          CompressionRatio.getInstance().updateRatio(compressionRatio);
        } catch (IOException e) {
          logger.error("{}: {} update compression ratio failed", storageGroupName,
              tsFileResource.getFile().getName(), e);
        }
        if (logger.isDebugEnabled()) {
          logger.debug("{}: {} flushingMemtables is empty and will close the file", storageGroupName,
              tsFileResource.getFile().getName());
        }
        endFile();
      } catch (Exception e) {
        logger.error("{} meet error when flush FileMetadata to {}, change system mode to read-only",
            storageGroupName, tsFileResource.getFile().getAbsolutePath());
        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
        try {
          writer.reset();
        } catch (IOException e1) {
          logger.error("{}: {} truncate corrupted data meets error", storageGroupName,
              tsFileResource.getFile().getName(), e1);
        }
        logger.error("{}: {} marking or ending file meet error", storageGroupName,
            tsFileResource.getFile().getName(), e);
      }
      // for sync close
      if (logger.isDebugEnabled()) {
        logger.debug("{}: {} try to get flushingMemtables lock.", storageGroupName,
            tsFileResource.getFile().getName());
      }
      synchronized (flushingMemTables) {
        flushingMemTables.notifyAll();
      }
    }
  }

  private void endFile() throws IOException, TsFileProcessorException {
    long closeStartTime = System.currentTimeMillis();
    tsFileResource.serialize();
    writer.endFile(schema);
    tsFileResource.cleanCloseFlag();

    // remove this processor from Closing list in StorageGroupProcessor,
    // mark the TsFileResource closed, no need writer anymore
    closeTsFileCallback.call(this);

    writer = null;

    if (logger.isInfoEnabled()) {
      long closeEndTime = System.currentTimeMillis();
      logger.info("Storage group {} close the file {}, start time is {}, end time is {}, "
              + "time consumption of flushing metadata is {}ms",
          storageGroupName, tsFileResource.getFile().getAbsoluteFile(),
          DatetimeUtils.convertMillsecondToZonedDateTime(closeStartTime),
          DatetimeUtils.convertMillsecondToZonedDateTime(closeEndTime),
          closeEndTime - closeStartTime);
    }
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
          .getNode(storageGroupName + "-" + tsFileResource.getFile().getName());
    }
    return logNode;
  }

  public void close() throws TsFileProcessorException {
    try {
      //when closing resource file, its corresponding mod file is also closed.
      tsFileResource.close();
      MultiFileLogNodeManager.getInstance()
          .deleteNode(storageGroupName + "-" + tsFileResource.getFile().getName());
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
   * @param measurementId sensor id
   * @param dataType data type
   * @param encoding encoding
   * @return left: the chunk data in memory; right: the chunkMetadatas of data on disk
   */
  public Pair<List<ReadOnlyMemChunk>, List<ChunkMetaData>> query(String deviceId,
      String measurementId, TSDataType dataType, TSEncoding encoding, Map<String, String> props,
      QueryContext context) {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: {} get flushQueryLock read lock", storageGroupName,
          tsFileResource.getFile().getName());
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

      List<ChunkMetaData> chunkMetaDataList = writer
          .getVisibleMetadataList(deviceId, measurementId, dataType);
      QueryUtils.modifyChunkMetaData(chunkMetaDataList,
          modifications);

      chunkMetaDataList.removeIf(context::chunkNotSatisfy);

      return new Pair<>(readOnlyMemChunks, chunkMetaDataList);
    } catch (Exception e) {
      logger.error("{}: {} get ReadOnlyMemChunk has error", storageGroupName,
          tsFileResource.getFile().getName(), e);
    } finally {
      flushQueryLock.readLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug("{}: {} release flushQueryLock read lock", storageGroupName,
            tsFileResource.getFile().getName());
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
}
