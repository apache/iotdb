/**
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.memtable.EmptyMemTable;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.memtable.MemTableFlushTaskV2;
import org.apache.iotdb.db.engine.memtable.MemTablePool;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.writer.NativeRestorableIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnsealedTsFileProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnsealedTsFileProcessor.class);

  private NativeRestorableIOWriter writer;

  private FileSchema fileSchema;

  private final String storageGroupName;

  private TsFileResource tsFileResource;

  private volatile boolean managedByFlushManager;

  private ReadWriteLock flushQueryLock = new ReentrantReadWriteLock();

  /**
   * true: should be closed
   */
  private volatile boolean shouldClose;

  private IMemTable workMemTable;

  private VersionController versionController;

  private Consumer<UnsealedTsFileProcessor> closeUnsealedFileCallback;

  private Supplier flushUpdateLatestFlushTimeCallback;

  private WriteLogNode logNode;

  /**
   * sync this object in query() and asyncFlush()
   */
  private final ConcurrentLinkedDeque<IMemTable> flushingMemTables = new ConcurrentLinkedDeque<>();

  UnsealedTsFileProcessor(String storageGroupName, File tsfile, FileSchema fileSchema,
      VersionController versionController,
      Consumer<UnsealedTsFileProcessor> closeUnsealedFileCallback,
      Supplier flushUpdateLatestFlushTimeCallback)
      throws IOException {
    this.storageGroupName = storageGroupName;
    this.fileSchema = fileSchema;
    this.tsFileResource = new TsFileResource(tsfile, this);
    this.versionController = versionController;
    this.writer = new NativeRestorableIOWriter(tsfile);
    this.closeUnsealedFileCallback = closeUnsealedFileCallback;
    this.flushUpdateLatestFlushTimeCallback = flushUpdateLatestFlushTimeCallback;
    LOGGER.info("create a new tsfile processor {}", tsfile.getAbsolutePath());
  }

  /**
   * insert a TsRecord into the workMemtable. If the memory usage is beyond the memTableThreshold,
   * put it into flushing list.
   *
   * @param insertPlan physical plan of insertion
   * @return succeed or fail
   */
  public boolean insert(InsertPlan insertPlan, boolean sequence) {

    if (workMemTable == null) {
      // TODO change the impl of getAvailableMemTable to non-blocking
      workMemTable = MemTablePool.getInstance().getAvailableMemTable(this);

      // no empty memtable, return failure
      if (workMemTable == null) {
        return false;
      }
    }

    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      try {
        getLogNode().write(insertPlan);
      } catch (IOException e) {
        LOGGER.error("write WAL failed", e);
        return false;
      }
    }
    // update start time of this memtable
    tsFileResource.updateStartTime(insertPlan.getDeviceId(), insertPlan.getTime());
    if (!sequence) {
      tsFileResource.updateEndTime(insertPlan.getDeviceId(), insertPlan.getTime());
    }

    // insert tsRecord to work memtable
    workMemTable.insert(insertPlan);

    return true;
  }

  /**
   * Delete data whose timestamp <= 'timestamp' and belonging to timeseries deviceId.measurementId.
   * Delete data in both working MemTable and flushing MemTables.
   */
  public void delete(Deletion deletion) {
    flushQueryLock.writeLock().lock();
    try {
      if (workMemTable != null) {
        workMemTable
            .delete(deletion.getDevice(), deletion.getMeasurement(), deletion.getTimestamp());
      }
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
    return workMemTable.memSize() > TSFileDescriptor.getInstance().getConfig().groupSizeInByte;
  }


  boolean shouldClose() {
    long fileSize = tsFileResource.getFileSize();
    long fileSizeThreshold = IoTDBDescriptor.getInstance().getConfig()
        .getTsFileSizeThreshold();
    return fileSize > fileSizeThreshold;
  }

  public void setCloseMark() {
    shouldClose = true;
  }

  void syncClose() {
    LOGGER.info("Sync close file: {}, first async close it",
        tsFileResource.getFile().getAbsolutePath());
    asyncClose();
    synchronized (flushingMemTables) {
      try {
        flushingMemTables.wait();
      } catch (InterruptedException e) {
        LOGGER.error("wait close interrupted", e);
      }
    }
    LOGGER.info("File {} is closed synchronously", tsFileResource.getFile().getAbsolutePath());
  }

  /**
   * Ensure there must be a flush thread submitted after setCloseMark() is called, therefore the
   * close task will be executed by a flush thread.
   */
  void asyncClose() {
    flushQueryLock.writeLock().lock();
    LOGGER.info("Async close the file: {}", tsFileResource.getFile().getAbsolutePath());
    try {
      IMemTable tmpMemTable = workMemTable == null ? new EmptyMemTable() : workMemTable;
      if (!tmpMemTable.isManagedByMemPool()) {
        LOGGER.info(
            "storage group {} add an empty memtable into flushing memtable list when async close",
            storageGroupName);
      } else {
        LOGGER.info("storage group {} async flush a memtable when async close", storageGroupName);
      }
      flushingMemTables.add(tmpMemTable);
      shouldClose = true;
      workMemTable = null;
      tmpMemTable.setVersion(versionController.nextVersion());
      FlushManager.getInstance().registerUnsealedTsFileProcessor(this);
      flushUpdateLatestFlushTimeCallback.get();
    } finally {
      flushQueryLock.writeLock().unlock();
    }
  }

  /**
   * TODO if the flushing thread is too fast, the tmpMemTable.wait() may never wakeup
   */
  public void syncFlush() {
    IMemTable tmpMemTable;
    flushQueryLock.writeLock().lock();
    try {
      tmpMemTable = workMemTable == null ? new EmptyMemTable() : workMemTable;
      if (!tmpMemTable.isManagedByMemPool()) {
        LOGGER.debug("add an empty memtable into flushing memtable list when sync flush");
      }
      flushingMemTables.addLast(tmpMemTable);
      tmpMemTable.setVersion(versionController.nextVersion());
      FlushManager.getInstance().registerUnsealedTsFileProcessor(this);
      flushUpdateLatestFlushTimeCallback.get();
      workMemTable = null;
    } finally {
      flushQueryLock.writeLock().unlock();
    }

    synchronized (tmpMemTable) {
      try {
        tmpMemTable.wait();
      } catch (InterruptedException e) {
        LOGGER.error("wait flush finished meets error", e);
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
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        getLogNode().notifyStartFlush();
      }
      flushingMemTables.addLast(workMemTable);
      workMemTable.setVersion(versionController.nextVersion());
      FlushManager.getInstance().registerUnsealedTsFileProcessor(this);
      flushUpdateLatestFlushTimeCallback.get();
      workMemTable = null;
    } catch (IOException e) {
      LOGGER.error("WAL notify start flush failed", e);
    } finally {
      flushQueryLock.writeLock().unlock();
    }
  }


  /**
   * return the memtable to MemTablePool and make metadata in writer visible
   */
  private void releaseFlushedMemTable(IMemTable memTable) {
    flushQueryLock.writeLock().lock();
    try {
      writer.makeMetadataVisible();
      flushingMemTables.remove(memTable);
      memTable.release();
      MemTablePool.getInstance().putBack(memTable, storageGroupName);
      LOGGER.info("storage group {} flush finished, remove a memtable from flushing list, "
          + "flushing memtable list size: {}", storageGroupName, flushingMemTables.size());
    } finally {
      flushQueryLock.writeLock().unlock();
    }
  }

  /**
   * Take the first MemTable from the flushingMemTables and flush it. Called by a flush thread of
   * the flush manager pool
   */
  void flushOneMemTable() throws IOException {
    IMemTable memTableToFlush;
    memTableToFlush = flushingMemTables.getFirst();

    LOGGER.info("storage group {} start to flush a memtable in a flush thread", storageGroupName);

    // null memtable only appears when calling asyncClose()
    if (memTableToFlush.isManagedByMemPool()) {
      MemTableFlushTaskV2 flushTask = new MemTableFlushTaskV2(memTableToFlush, fileSchema, writer,
          storageGroupName);
      try {
        writer.mark();
        flushTask.flushMemTable();
      } catch (ExecutionException | InterruptedException e) {
        writer.reset();
        StorageEngine.getInstance().setReadOnly(true);
        LOGGER.error("IOTask meets error, truncate the corrupted data", e);
        return;
      }

      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        getLogNode().notifyEndFlush();
      }
    }

    releaseFlushedMemTable(memTableToFlush);

    // for sync flush
    synchronized (memTableToFlush) {
      memTableToFlush.notify();
    }

    if (shouldClose && flushingMemTables.isEmpty()) {
      endFile();

      // for sync close
      synchronized (flushingMemTables) {
        flushingMemTables.notify();
      }
    }

  }

  private void endFile() throws IOException {
    long closeStartTime = System.currentTimeMillis();

    tsFileResource.serialize();

    writer.endFile(fileSchema);

      // remove this processor from Closing list in StorageGroupProcessor, mark the TsFileResource closed, no need writer anymore
    closeUnsealedFileCallback.accept(this);

    writer = null;

    if (LOGGER.isInfoEnabled()) {

      long closeEndTime = System.currentTimeMillis();

      LOGGER.info("Storage group {} close the file {}, start time is {}, end time is {}, "
              + "time consumption of flushing metadata is {}ms",
          storageGroupName, tsFileResource.getFile().getAbsoluteFile(),
          DatetimeUtils.convertMillsecondToZonedDateTime(closeStartTime),
          DatetimeUtils.convertMillsecondToZonedDateTime(closeEndTime),
          closeEndTime - closeStartTime);
    }
  }


  boolean isManagedByFlushManager() {
    return managedByFlushManager;
  }

  WriteLogNode getLogNode() {
    if (logNode == null) {
      logNode = MultiFileLogNodeManager.getInstance()
          .getNode(storageGroupName + "-" + tsFileResource.getFile().getName());
    }
    return logNode;
  }

  public void close() throws IOException {
    tsFileResource.close();
    MultiFileLogNodeManager.getInstance()
        .deleteNode(storageGroupName + "-" + tsFileResource.getFile().getName());
  }

  void setManagedByFlushManager(boolean managedByFlushManager) {
    this.managedByFlushManager = managedByFlushManager;
  }

  int getFlushingMemTableSize() {
    return flushingMemTables.size();
  }

  long getWorkMemTableMemory() {
    return workMemTable.memSize();
  }

  public VersionController getVersionController() {
    return versionController;
  }

  String getStorageGroupName() {
    return storageGroupName;
  }

  /**
   * get the chunk(s) in the memtable ( one from work memtable and the other ones in flushing status
   * and then compact them into one TimeValuePairSorter). Then get the related ChunkMetadatas of
   * data in disk.
   *
   * @param deviceId device id
   * @param measurementId sensor id
   * @param dataType data type
   * @return corresponding chunk data and chunk metadata in memory
   */
  public Pair<ReadOnlyMemChunk, List<ChunkMetaData>> query(String deviceId,
      String measurementId, TSDataType dataType, Map<String, String> props) {
    flushQueryLock.readLock().lock();
    try {
      MemSeriesLazyMerger memSeriesLazyMerger = new MemSeriesLazyMerger();
      for (IMemTable flushingMemTable : flushingMemTables) {
        if (!flushingMemTable.isManagedByMemPool()) {
          continue;
        }
        ReadOnlyMemChunk memChunk = flushingMemTable.query(deviceId, measurementId, dataType, props);
        if (memChunk != null) {
          memSeriesLazyMerger.addMemSeries(memChunk);
        }
      }
      if (workMemTable != null) {
        ReadOnlyMemChunk memChunk = workMemTable.query(deviceId, measurementId, dataType, props);
        if (memChunk != null) {
          memSeriesLazyMerger.addMemSeries(memChunk);
        }
      }
      // memSeriesLazyMerger has handled the props,
      // so we do not need to handle it again in the following readOnlyMemChunk
      ReadOnlyMemChunk timeValuePairSorter = new ReadOnlyMemChunk(dataType, memSeriesLazyMerger,
          Collections.emptyMap());

      ModificationFile modificationFile = tsFileResource.getModFile();

      List<ChunkMetaData> chunkMetaDataList = writer
          .getVisibleMetadatas(deviceId, measurementId, dataType);
      QueryUtils.modifyChunkMetaData(chunkMetaDataList,
          (List<Modification>) modificationFile.getModifications());

      return new Pair<>(timeValuePairSorter, chunkMetaDataList);
    } finally {
      flushQueryLock.readLock().unlock();
    }
  }

}
