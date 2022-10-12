package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;

import java.io.IOException;
import java.util.List;

public class ReadPointCrossCompactionWriter extends AbstractCrossCompactionWriter {

  public ReadPointCrossCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqFileResources)
      throws IOException {
    super(targetResources, seqFileResources);
  }

  @Override
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {
    // Since a batch of data is written, the end time of the current aligned device may exceed the
    // end time of the device in the source file, but no error will be caused
    checkTimeAndMayFlushChunkToCurrentFile(timestamps.getStartTime(), subTaskId);
    AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) this.chunkWriters[subTaskId];
    chunkWriter.write(timestamps, columns, batchSize);
    synchronized (this) {
      // we need to synchronized here to avoid multi-thread competition in sub-task
      TsFileResource resource = targetResources.get(seqFileIndexArray[subTaskId]);
      resource.updateStartTime(deviceId, timestamps.getStartTime());
      resource.updateEndTime(deviceId, timestamps.getEndTime());
    }
    chunkPointNumArray[subTaskId] += timestamps.getTimes().length;
    checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(seqFileIndexArray[subTaskId]), chunkWriter, subTaskId, true);
    isDeviceExistedInTargetFiles[seqFileIndexArray[subTaskId]] = true;
    isEmptyFile[seqFileIndexArray[subTaskId]] = false;
  }
}
