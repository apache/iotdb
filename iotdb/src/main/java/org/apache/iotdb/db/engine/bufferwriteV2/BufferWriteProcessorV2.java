/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.bufferwriteV2;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.db.engine.memtable.Callback;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemTableFlushTaskV2;
import org.apache.iotdb.db.engine.memtable.MemTablePool;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.writer.NativeRestorableIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferWriteProcessorV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteProcessorV2.class);

  private NativeRestorableIOWriter writer;

  private FileSchema fileSchema;

  private final String storageGroupName;

  private TsFileResourceV2 tsFileResource;

  private volatile boolean managedByFlushManager;

  private ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * true: to be closed
   */
  private volatile boolean closing;

  private IMemTable workMemTable;

  private VersionController versionController;

  private Callback closeBufferWriteProcessor;

  // synch this object in query() and asyncFlush()
  private final ConcurrentLinkedDeque<IMemTable> flushingMemTables = new ConcurrentLinkedDeque<>();

  public BufferWriteProcessorV2(String storageGroupName, File file, FileSchema fileSchema, VersionController versionController, Callback closeBufferWriteProcessor) throws IOException {
    this.storageGroupName = storageGroupName;
    this.fileSchema = fileSchema;
    this.tsFileResource = new TsFileResourceV2(file);
    this.versionController = versionController;
    this.writer = new NativeRestorableIOWriter(file);
    this.closeBufferWriteProcessor = closeBufferWriteProcessor;
  }

  /**
   * write a TsRecord into the workMemtable.
   * If the memory usage is beyond the memTableThreshold, put it into flushing list.
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

    // write tsRecord to work memtable
    workMemTable.insert(tsRecord);

    return true;
  }


  public TsFileResourceV2 getTsFileResource() {
    return tsFileResource;
  }

  /**
   * return the memtable to MemTablePool and make metadata in writer visible
   * @param memTable
   */
  private void removeFlushedMemTable(Object memTable) {
    lock.writeLock().lock();
    writer.makeMetadataVisible();
    try {
      flushingMemTables.remove(memTable);
    } finally {
      lock.writeLock().unlock();
    }
  }


  public boolean shouldFlush() {
    return workMemTable.memSize() > TSFileDescriptor.getInstance().getConfig().groupSizeInByte;
  }

  /**
   * put the workMemtable into flushing list and set null
   */
  public void asyncFlush() {
    flushingMemTables.addLast(workMemTable);
    FlushManager.getInstance().registerBWP(this);
    workMemTable = null;
  }

  public void flushOneMemTable() {
    IMemTable memTableToFlush = flushingMemTables.pollFirst();
    MemTableFlushTaskV2 flushTask = new MemTableFlushTaskV2(writer, storageGroupName, this::removeFlushedMemTable);
    flushTask.flushMemTable(fileSchema, memTableToFlush, versionController.nextVersion());

    if (closing && flushingMemTables.isEmpty()) {

    }
  }

  public void close() throws IOException {
    long closeStartTime = System.currentTimeMillis();
    writer.endFile(fileSchema);
    //FIXME suppose the flushMetadata-thread-pool is 2.
    // then if a flushMetadata task and a close task are running in the same time
    // and the close task is faster, then writer == null, and the flushMetadata task will throw nullpointer
    // exception. Add "synchronized" keyword on both flushMetadata and close may solve the issue.
    writer = null;

    // remove this processor from Closing list in FileNodeProcessor
    closeBufferWriteProcessor.call(this);

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

  public boolean shouldClose() {
    long fileSize = tsFileResource.getFileSize();
    long fileSizeThreshold = IoTDBDescriptor.getInstance().getConfig().getBufferwriteFileSizeThreshold();
    return fileSize > fileSizeThreshold;
  }

  public void setClosing() {
    closing = true;
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
}
