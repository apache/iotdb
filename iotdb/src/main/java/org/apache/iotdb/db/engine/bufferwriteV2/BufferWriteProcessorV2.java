package org.apache.iotdb.db.engine.bufferwriteV2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemTablePool;
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

  /**
   * true: to be closed
   */
  private boolean closing;

  private IMemTable workMemTable;

  private final List<IMemTable> flushingMemTables = new ArrayList<>();

  public BufferWriteProcessorV2(String storageGroupName, File file, FileSchema fileSchema) throws IOException {
    this.storageGroupName = storageGroupName;
    this.fileSchema = fileSchema;
    tsFileResource = new TsFileResourceV2();
    // recover and get a writer
    writer = new NativeRestorableIOWriter(file);
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

  public boolean shouldFlush() {
    return workMemTable.memSize() > TSFileDescriptor.getInstance().getConfig().groupSizeInByte;
  }

  /**
   * put the workMemtable into flushing list and set null
   */
  public void flush() {
    synchronized (flushingMemTables) {
      flushingMemTables.add(workMemTable);
    }
    workMemTable = null;
  }

  public boolean shouldClose() {
    long fileSize = tsFileResource.getFileSize();
    long fileSizeThreshold = IoTDBDescriptor.getInstance().getConfig().getBufferwriteFileSizeThreshold();
    return fileSize > fileSizeThreshold;
  }

}
