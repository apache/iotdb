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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
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
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseTsFileCallBack;
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
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileProcessor {

  private static final Logger logger = LoggerFactory.getLogger(TsFileProcessor.class);

  private RestorableTsFileIOWriter writer;

  private Schema schema;

  private final String storageGroupName;

  private TsFileResource tsFileResource;

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

  /**
   * sync this object in query() and asyncTryToFlush()
   */
  private final ConcurrentLinkedDeque<IMemTable> flushingMemTables = new ConcurrentLinkedDeque<>();


  private VersionController versionController;

  /**
   * this callback is called after the corresponding TsFile is called endFile().
   */
  private CloseTsFileCallBack closeTsFileCallback;

  /**
   * this callback is called before the workMemtable is added into the flushingMemTables.
   */
  private Supplier updateLatestFlushTimeCallback;

  private WriteLogNode logNode;

  private boolean sequence;

  private long totalMemTableSize;

  TsFileProcessor(String storageGroupName, File tsfile, Schema schema,
      VersionController versionController,
      CloseTsFileCallBack closeTsFileCallback,
      Supplier updateLatestFlushTimeCallback, boolean sequence)
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
    this.tsFileResource.setHistoricalVersions(Collections.singleton(versionController.currVersion()));
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
        logger.error("write WAL failed", e);
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

  public boolean insertBatch(BatchInsertPlan batchInsertPlan, List<Integer> indexes,
      Integer[] results) throws QueryProcessException {

    if (workMemTable == null) {
      workMemTable = MemTablePool.getInstance().getAvailableMemTable(this);
    }

    // insert insertPlan to the work memtable
    workMemTable.insertBatch(batchInsertPlan, indexes);

    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      try {
        batchInsertPlan.setIndex(new HashSet<>(indexes));
        getLogNode().write(batchInsertPlan);
      } catch (IOException e) {
        logger.error("write WAL failed", e);
        for (int index: indexes) {
          results[index] = TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode();
        }
        return false;
      }
    }

    tsFileResource.updateStartTime(batchInsertPlan.getDeviceId(), batchInsertPlan.getMinTime());

    //for sequence tsfile, we update the endTime only when the file is prepared to be closed.
    //for unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource.updateEndTime(batchInsertPlan.getDeviceId(), batchInsertPlan.getMaxTime());
    }

    return true;
  }

  /**
   * Delete data which belongs to the timeseries `deviceId.measurementId` and the timestamp of which
   * <= 'timestamp' in the deletion. <br/>
   *
   * Delete data in both working MemTable and flushing MemTables.
   */
  public void deleteDataInMemory(Deletion deletion) {
    flushQueryLock.writeLock().lock();
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
    }
  }

  TsFileResource getTsFileResource() {
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
   *
   */
  private long getMemtableSizeThresholdBasedOnSeriesNum() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    long memTableSize = (long) (config.getMemtableSizeThreshold() * config.getMaxMemtableNumber()
        / IoTDBConstant.MEMTABLE_NUM_IN_EACH_STORAGE_GROUP * ActiveTimeSeriesCounter.getInstance().getActiveRatio(storageGroupName));
    return Math.max(memTableSize, config.getMemtableSizeThreshold());
  }


  boolean shouldClose() {
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
        flushingMemTables.wait();
      } catch (InterruptedException e) {
        logger.error("wait close interrupted", e);
        Thread.currentThread().interrupt();
      }
    }
    logger.info("File {} is closed synchronously", tsFileResource.getFile().getAbsolutePath());
  }


  void asyncClose() {
    flushQueryLock.writeLock().lock();
    try {
      logger.info("Async close the file: {}", tsFileResource.getFile().getAbsolutePath());
      if (shouldClose) {
        return;
      }
      shouldClose = true;
      // when a flush thread serves this TsFileProcessor (because the processor is submitted by
      // registerTsFileProcessor()), the thread will seal the corresponding TsFile and
      // execute other cleanup works if (shouldClose == true and flushingMemTables is empty).

      // To ensure there must be a flush thread serving this processor after the field `shouldClose`
      // is set true, we need to generate a NotifyFlushMemTable as a signal task and submit it to
      // the FlushManager.
      IMemTable tmpMemTable = workMemTable == null ? new NotifyFlushMemTable() : workMemTable;
      if (logger.isDebugEnabled()) {
        if (tmpMemTable.isSignalMemTable()) {
          logger.debug(
              "storage group {} add a signal memtable into flushing memtable list when async close",
              storageGroupName);
        } else {
          logger
              .debug("storage group {} async flush a memtable when async close", storageGroupName);
        }
      }
      try {
        addAMemtableIntoFlushingList(tmpMemTable);
      } catch (IOException e) {
        logger.error("async close failed, because", e);
      }
    } finally {
      flushQueryLock.writeLock().unlock();
    }
  }

  /**
   * TODO if the flushing thread is too fast, the tmpMemTable.wait() may never wakeup
   */
  public void syncFlush() throws IOException {
    IMemTable tmpMemTable;
    flushQueryLock.writeLock().lock();
    try {
      tmpMemTable = workMemTable == null ? new NotifyFlushMemTable() : workMemTable;
      if (tmpMemTable.isSignalMemTable()) {
        logger.debug("add a signal memtable into flushing memtable list when sync flush");
      }
      addAMemtableIntoFlushingList(tmpMemTable);
    } finally {
      flushQueryLock.writeLock().unlock();
    }

    synchronized (tmpMemTable) {
      try {
        long startWait = System.currentTimeMillis();
        while (true) {
          tmpMemTable.wait(1000);

          flushQueryLock.readLock().lock();
          try {
            if (!flushingMemTables.contains(tmpMemTable)) {
              break;
            }
          } finally {
            flushQueryLock.readLock().unlock();
          }

          if ((System.currentTimeMillis() - startWait) > 60_000) {
            logger.warn("has waited for synced flushing a memtable in {} for 60 seconds.",
                this.tsFileResource.getFile().getAbsolutePath());
            startWait = System.currentTimeMillis();
          }
        }
      } catch (InterruptedException e) {
        logger.error("wait flush finished meets error", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * put the working memtable into flushing list and set the working memtable to null
   */
  public void asyncFlush() {
    flushQueryLock.writeLock().lock();
    try {
      if (workMemTable == null) {
        return;
      }

      addAMemtableIntoFlushingList(workMemTable);

    } catch (IOException e) {
      logger.error("WAL notify start flush failed", e);
    } finally {
      flushQueryLock.writeLock().unlock();
    }
  }

  /**
   * this method calls updateLatestFlushTimeCallback and move the given memtable into the flushing
   * queue, set the current working memtable as null and then register the tsfileProcessor into the
   * flushManager again.
   */
  private void addAMemtableIntoFlushingList(IMemTable tobeFlushed) throws IOException {
    updateLatestFlushTimeCallback.get();
    flushingMemTables.addLast(tobeFlushed);
    tobeFlushed.setVersion(versionController.nextVersion());
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
    try {
      writer.makeMetadataVisible();
      flushingMemTables.remove(memTable);
      memTable.release();
      MemTablePool.getInstance().putBack(memTable, storageGroupName);
      logger.debug("storage group {} flush finished, remove a memtable from flushing list, "
          + "flushing memtable list size: {}", storageGroupName, flushingMemTables.size());
    } finally {
      flushQueryLock.writeLock().unlock();
    }
  }

  /**
   * Take the first MemTable from the flushingMemTables and flush it. Called by a flush thread of
   * the flush manager pool
   */
  public void flushOneMemTable() {
    IMemTable memTableToFlush;
    memTableToFlush = flushingMemTables.getFirst();

    logger.info("storage group {} starts to flush a memtable in a flush thread", storageGroupName);

    // signal memtable only may appear when calling asyncClose()
    if (!memTableToFlush.isSignalMemTable()) {
      MemTableFlushTask flushTask = new MemTableFlushTask(memTableToFlush, schema, writer,
          storageGroupName);
      try {
        writer.mark();
        flushTask.syncFlushMemTable();
      } catch (ExecutionException | InterruptedException | IOException e) {
        logger.error("meet error when flushing a memtable, change system mode to read-only", e);
        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
        try {
          logger.error("IOTask meets error, truncate the corrupted data", e);
          writer.reset();
        } catch (IOException e1) {
          logger.error("Truncate corrupted data meets error", e1);
        }
        Thread.currentThread().interrupt();
      }

      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        getLogNode().notifyEndFlush();
      }
    }

    releaseFlushedMemTable(memTableToFlush);

    // for sync flush
    synchronized (memTableToFlush) {
      memTableToFlush.notifyAll();
    }

    if (shouldClose && flushingMemTables.isEmpty()) {
      try {
        writer.mark();
        try {
          double compressionRatio = ((double) totalMemTableSize) / writer.getPos();
          logger.debug("totalMemTableSize: {}, writer.getPos(): {}", totalMemTableSize,
              writer.getPos());
          if (compressionRatio == 0) {
            logger.error("compressionRatio = 0, please check the log.");
          }
          CompressionRatio.getInstance().updateRatio(compressionRatio);
        } catch (IOException e) {
          logger.error("update compression ratio failed", e);
        }
        endFile();
      } catch (IOException | TsFileProcessorException e) {
        logger.error("meet error when flush FileMetadata to {}, change system mode to read-only",
            tsFileResource.getFile().getAbsolutePath());
        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
        try {
          writer.reset();
        } catch (IOException e1) {
          logger.error("truncate corrupted data meets error", e1);
        }
        logger.error("marking or ending file meet error", e);
      }

      // for sync close
      synchronized (flushingMemTables) {
        flushingMemTables.notifyAll();
      }
    }
  }

  private void endFile() throws IOException, TsFileProcessorException {
    long closeStartTime = System.currentTimeMillis();

    tsFileResource.serialize();
    writer.endFile(schema);

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

  WriteLogNode getLogNode() {
    if (logNode == null) {
      logNode = MultiFileLogNodeManager.getInstance()
          .getNode(storageGroupName + "-" + tsFileResource.getFile().getName());
    }
    return logNode;
  }

  public void close() throws TsFileProcessorException {
    try {
      tsFileResource.close();
      MultiFileLogNodeManager.getInstance()
          .deleteNode(storageGroupName + "-" + tsFileResource.getFile().getName());
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  public void setManagedByFlushManager(boolean managedByFlushManager) {
    this.managedByFlushManager = managedByFlushManager;
  }

  public int getFlushingMemTableSize() {
    return flushingMemTables.size();
  }

  long getWorkMemTableMemory() {
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
   * @return left: the chunk data in memory; right: the chunkMetadatas of data on disk
   */
  public Pair<ReadOnlyMemChunk, List<ChunkMetaData>> query(String deviceId,
      String measurementId, TSDataType dataType, Map<String, String> props, QueryContext context) {
    flushQueryLock.readLock().lock();
    try {
      MemSeriesLazyMerger memSeriesLazyMerger = new MemSeriesLazyMerger();
      for (IMemTable flushingMemTable : flushingMemTables) {
        if (flushingMemTable.isSignalMemTable()) {
          continue;
        }
        ReadOnlyMemChunk memChunk = flushingMemTable
            .query(deviceId, measurementId, dataType, props, context.getQueryTimeLowerBound());
        if (memChunk != null) {
          memSeriesLazyMerger.addMemSeries(memChunk);
        }
      }
      if (workMemTable != null) {
        ReadOnlyMemChunk memChunk = workMemTable.query(deviceId, measurementId, dataType, props,
            context.getQueryTimeLowerBound());
        if (memChunk != null) {
          memSeriesLazyMerger.addMemSeries(memChunk);
        }
      }
      // memSeriesLazyMerger has handled the props,
      // so we do not need to handle it again in the following readOnlyMemChunk
      ReadOnlyMemChunk timeValuePairSorter = new ReadOnlyMemChunk(dataType, memSeriesLazyMerger,
          Collections.emptyMap());

      ModificationFile modificationFile = tsFileResource.getModFile();
      List<Modification> modifications = context.getPathModifications(modificationFile,
          deviceId + IoTDBConstant.PATH_SEPARATOR + measurementId);

      List<ChunkMetaData> chunkMetaDataList = writer
          .getVisibleMetadataList(deviceId, measurementId, dataType);
      QueryUtils.modifyChunkMetaData(chunkMetaDataList,
          modifications);

      chunkMetaDataList.removeIf(context::chunkNotSatisfy);

      return new Pair<>(timeValuePairSorter, chunkMetaDataList);
    } finally {
      flushQueryLock.readLock().unlock();
    }
  }

}
