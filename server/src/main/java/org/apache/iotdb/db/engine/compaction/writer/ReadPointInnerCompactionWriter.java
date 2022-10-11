package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;

import java.io.IOException;


public class ReadPointInnerCompactionWriter extends AbstractInnerCompactionWriter {
  public ReadPointInnerCompactionWriter(TsFileResource targetFileResource) throws IOException {
    super(targetFileResource);
  }

  @Override
  public void write(long timestamp, Object value, int subTaskId) throws IOException {
    writeDataPoint(
        timestamp,
        value,
        chunkWriters[subTaskId],
        ++measurementPointCountArray[subTaskId] % checkPoint == 0 ? fileWriter : null,
        false);
    isEmptyFile = false;
  }

  @Override
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {
    AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) this.chunkWriters[subTaskId];
    chunkWriter.write(timestamps, columns, batchSize);
    synchronized (this) {
      // we need to synchronized here to avoid multi-thread competition in sub-task
      targetResource.updateStartTime(deviceId, timestamps.getStartTime());
      targetResource.updateEndTime(deviceId, timestamps.getEndTime());
    }
    checkChunkSizeAndMayOpenANewChunk(fileWriter, chunkWriter, false);
    isEmptyFile = false;
  }
}
