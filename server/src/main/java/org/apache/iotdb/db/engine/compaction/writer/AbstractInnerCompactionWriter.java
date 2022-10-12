package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public abstract class AbstractInnerCompactionWriter extends AbstractCompactionWriter {
  protected TsFileIOWriter fileWriter;

  protected boolean isEmptyFile;

  protected TsFileResource targetResource;

  public AbstractInnerCompactionWriter(TsFileResource targetFileResource) throws IOException {
    this.fileWriter = new TsFileIOWriter(targetFileResource.getTsFile());
    this.targetResource = targetFileResource;
    isEmptyFile = true;
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    fileWriter.startChunkGroup(deviceId);
    this.isAlign = isAlign;
    this.deviceId = deviceId;
  }

  @Override
  public void endChunkGroup() throws IOException {
    fileWriter.endChunkGroup();
  }

  @Override
  public void endMeasurement(int subTaskId) throws IOException {
    flushChunkToFileWriter(fileWriter, chunkWriters[subTaskId]);
  }

  @Override
  public abstract void write(long timestamp, Object value, int subTaskId) throws IOException;

  @Override
  public abstract void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException;

  @Override
  public void endFile() throws IOException {
    fileWriter.endFile();
    if (isEmptyFile) {
      fileWriter.getFile().delete();
    }
  }

  @Override
  public void close() throws Exception {
    if (fileWriter != null && fileWriter.canWrite()) {
      fileWriter.close();
    }
    fileWriter = null;
  }

  @Override
  public List<TsFileIOWriter> getFileIOWriter() {
    return Collections.singletonList(fileWriter);
  }

  @Override
  public void updateStartTimeAndEndTime(String device, long time, int subTaskId) {
    // we need to synchronized here to avoid multi-thread competition in sub-task
    synchronized (this) {
      targetResource.updateStartTime(device, time);
      targetResource.updateEndTime(device, time);
    }
  }
}
