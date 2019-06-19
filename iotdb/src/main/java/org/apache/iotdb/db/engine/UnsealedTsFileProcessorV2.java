package org.apache.iotdb.db.engine;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.bufferwriteV2.FlushManager;
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.db.engine.memtable.Callback;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.memtable.MemTableFlushTaskV2;
import org.apache.iotdb.db.engine.memtable.MemTablePool;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.querycontext.UnsealedTsFileV2;
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

  protected NativeRestorableIOWriter writer;

  protected FileSchema fileSchema;

  protected final String storageGroupName;

  protected TsFileResourceV2 tsFileResource;

  protected volatile boolean managedByFlushManager;

  protected ReadWriteLock flushQueryLock = new ReentrantReadWriteLock();

  /**
   * true: to be closed
   */
  protected volatile boolean shouldClose;

  protected IMemTable workMemTable;

  protected VersionController versionController;

  protected Callback closeBufferWriteProcessor;

  // synch this object in query() and asyncFlush()
  protected final LinkedList<IMemTable> flushingMemTables = new LinkedList<>();

  public UnsealedTsFileProcessorV2(String storageGroupName, File file, FileSchema fileSchema,
      VersionController versionController, Callback closeBufferWriteProcessor) throws IOException {
    this.storageGroupName = storageGroupName;
    this.fileSchema = fileSchema;
    this.tsFileResource = new UnsealedTsFileV2(file);
    this.versionController = versionController;
    this.writer = new NativeRestorableIOWriter(file);
    this.closeBufferWriteProcessor = closeBufferWriteProcessor;
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
  private void removeFlushedMemTable(Object memTable) {
    flushQueryLock.writeLock().lock();
    try {
      writer.makeMetadataVisible();
      flushingMemTables.remove(memTable);
    } finally {
      flushQueryLock.writeLock().unlock();
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
    FlushManager.getInstance().registerBWProcessor(this);
    workMemTable = null;
  }

  public void flushOneMemTable() throws IOException {
    IMemTable memTableToFlush = flushingMemTables.pollFirst();
    // null memtable only appears when calling forceClose()
    if (memTableToFlush != null) {
      MemTableFlushTaskV2 flushTask = new MemTableFlushTaskV2(writer, storageGroupName,
          this::removeFlushedMemTable);
      flushTask.flushMemTable(fileSchema, memTableToFlush, versionController.nextVersion());
    }

    if (shouldClose && flushingMemTables.isEmpty()) {
      endFile();
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

  public void forceClose() {
    flushingMemTables.add(workMemTable);
    workMemTable = null;
    shouldClose = true;
    FlushManager.getInstance().registerBWProcessor(this);
  }

  public boolean shouldClose() {
    long fileSize = tsFileResource.getFileSize();
    long fileSizeThreshold = IoTDBDescriptor.getInstance().getConfig()
        .getBufferwriteFileSizeThreshold();
    return fileSize > fileSizeThreshold;
  }

  public void close() {
    shouldClose = true;
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
   * and then compact them into one TimeValuePairSorter). Then get its (or their) ChunkMetadata(s).
   *
   * @param deviceId device id
   * @param measurementId sensor id
   * @param dataType data type
   * @return corresponding chunk data and chunk metadata in memory
   */
  public Pair<ReadOnlyMemChunk, List<ChunkMetaData>> queryUnsealedFile(String deviceId,
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
          writer.getMetadatas(deviceId, measurementId, dataType));
    } finally {
      flushQueryLock.readLock().unlock();
    }
  }
}
