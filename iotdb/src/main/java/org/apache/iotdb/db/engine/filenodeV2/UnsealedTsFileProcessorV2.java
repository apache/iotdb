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
package org.apache.iotdb.db.engine.filenodeV2;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.memtable.EmptyMemTable;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.memtable.MemTableFlushTaskV2;
import org.apache.iotdb.db.engine.memtable.MemTablePool;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.writer.NativeRestorableIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnsealedTsFileProcessorV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnsealedTsFileProcessorV2.class);

  private NativeRestorableIOWriter writer;

  private FileSchema fileSchema;

  private final String storageGroupName;

  private TsFileResourceV2 tsFileResource;

  private volatile boolean managedByFlushManager;

  private ReadWriteLock flushQueryLock = new ReentrantReadWriteLock();

  /**
   * true: should be closed
   */
  private volatile boolean shouldClose;

  private IMemTable workMemTable;

  private VersionController versionController;

  private Consumer<UnsealedTsFileProcessorV2> closeUnsealedTsFileProcessor;

  /**
   * sync this object in query() and asyncFlush()
   */
  private final ConcurrentLinkedDeque<IMemTable> flushingMemTables = new ConcurrentLinkedDeque<>();

  public UnsealedTsFileProcessorV2(String storageGroupName, File tsfile, FileSchema fileSchema,
      VersionController versionController,
      Consumer<UnsealedTsFileProcessorV2> closeUnsealedTsFileProcessor)
      throws IOException {
    this.storageGroupName = storageGroupName;
    this.fileSchema = fileSchema;
    this.tsFileResource = new TsFileResourceV2(tsfile);
    this.versionController = versionController;
    this.writer = new NativeRestorableIOWriter(tsfile);
    this.closeUnsealedTsFileProcessor = closeUnsealedTsFileProcessor;
  }

  /**
   * write a TsRecord into the workMemtable. If the memory usage is beyond the memTableThreshold,
   * put it into flushing list.
   *
   * @param tsRecord data to be written
   * @return succeed or fail
   */
  public boolean write(TSRecord tsRecord) {

    if (workMemTable == null) {
      // TODO change the impl of getEmptyMemTable to non-blocking
      workMemTable = MemTablePool.getInstance().getEmptyMemTable(this);

      // no empty memtable, return failure
      if (workMemTable == null) {
        return false;
      }
    }

    // TODO write WAL

    // update start time of this memtable
    tsFileResource.updateStartTime(tsRecord.deviceId, tsRecord.time);

    // write tsRecord to work memtable
    workMemTable.insert(tsRecord);

    return true;
  }


  public TsFileResourceV2 getTsFileResource() {
    return tsFileResource;
  }

  /**
   * return the memtable to MemTablePool and make metadata in writer visible
   */
  private void releaseFlushedMemTable(IMemTable memTable) {
    flushQueryLock.writeLock().lock();
    try {
      writer.makeMetadataVisible();
      flushingMemTables.remove(memTable);
      MemTablePool.getInstance().putBack(memTable);
      LOGGER.info("Processor {} return back a memtable to MemTablePool", storageGroupName);
    } finally {
      flushQueryLock.writeLock().unlock();
    }
  }


  public boolean shouldFlush() {
    return workMemTable.memSize() > TSFileDescriptor.getInstance().getConfig().groupSizeInByte;
  }

  /**
   * put the workMemtable into flushing list and set the workMemtable to null
   */
  public void asyncFlush() {
    flushQueryLock.writeLock().lock();
    try {
      if (workMemTable == null) {
        return;
      }
      flushingMemTables.addLast(workMemTable);
      FlushManager.getInstance().registerUnsealedTsFileProcessor(this);
      workMemTable = null;
    } finally {
      flushQueryLock.writeLock().unlock();
    }
  }

  // only for test
  public void syncFlush() {
    IMemTable tmpMemTable;
    flushQueryLock.writeLock().lock();
    try {
      tmpMemTable = workMemTable == null ? new EmptyMemTable() : workMemTable;
      flushingMemTables.addLast(tmpMemTable);
      FlushManager.getInstance().registerUnsealedTsFileProcessor(this);
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

  public boolean shouldClose() {
    long fileSize = tsFileResource.getFileSize();
    long fileSizeThreshold = IoTDBDescriptor.getInstance().getConfig()
        .getBufferwriteFileSizeThreshold();
    return fileSize > fileSizeThreshold;
  }

  public void setCloseMark() {
    shouldClose = true;
  }

  public synchronized void asyncClose() {
    flushingMemTables.add(workMemTable == null ? new EmptyMemTable() : workMemTable);
    workMemTable = null;
    shouldClose = true;
    FlushManager.getInstance().registerUnsealedTsFileProcessor(this);
  }


  public void syncClose() {
    asyncClose();
    synchronized (flushingMemTables) {
      try {
        flushingMemTables.wait();
      } catch (InterruptedException e) {
        LOGGER.error("wait close interrupted", e);
      }
    }
  }


  /**
   * Take the first MemTable from the flushingMemTables and flush it. Called by a flush thread of
   * the flush manager pool
   */
  void flushOneMemTable() throws IOException {
    IMemTable memTableToFlush;
    memTableToFlush = flushingMemTables.getFirst();

    // null memtable only appears when calling asyncClose()
    if (memTableToFlush.isManagedByMemPool()) {
      MemTableFlushTaskV2 flushTask = new MemTableFlushTaskV2(writer, storageGroupName,
          this::releaseFlushedMemTable);
      flushTask.flushMemTable(fileSchema, memTableToFlush, versionController.nextVersion());
    }
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
    writer.endFile(fileSchema);
    //FIXME suppose the flushMetadata-thread-pool is 2.
    // then if a flushMetadata task and a endFile task are running in the same time
    // and the endFile task is faster, then writer == null, and the flushMetadata task will throw nullpointer
    // exception. Add "synchronized" keyword on both flushMetadata and endFile may solve the issue.
    writer = null;

    // remove this processor from Closing list in FileNodeProcessor
    closeUnsealedTsFileProcessor.accept(this);

    // delete the restore for this bufferwrite processor
    if (LOGGER.isInfoEnabled()) {

      long closeEndTime = System.currentTimeMillis();

      LOGGER.info(
          "Close bufferwrite processor {}, the file name is {}, start time is {}, end time is {}, "
              + "time consumption is {}ms",
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

  public int getFlushingMemTableSize() {
    return flushingMemTables.size();
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
        memSeriesLazyMerger
            .addMemSeries(flushingMemTable.query(deviceId, measurementId, dataType, props));
      }
      if (workMemTable != null) {
        memSeriesLazyMerger
            .addMemSeries(workMemTable.query(deviceId, measurementId, dataType, props));
      }
      // memSeriesLazyMerger has handled the props,
      // so we do not need to handle it again in the following readOnlyMemChunk
      ReadOnlyMemChunk timeValuePairSorter = new ReadOnlyMemChunk(dataType, memSeriesLazyMerger,
          Collections.emptyMap());
      return new Pair<>(timeValuePairSorter,
          writer.getVisibleMetadatas(deviceId, measurementId, dataType));
    } finally {
      flushQueryLock.readLock().unlock();
    }
  }
}
