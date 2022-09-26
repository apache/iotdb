package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;

import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.writer.CompactionWriterUtils.checkPoint;

public class ReadPointCrossCompactionWriter extends AbstractCrossCompactionWriter {

  public ReadPointCrossCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqFileResources)
      throws IOException {
    super(targetResources, seqFileResources);
  }

  @Override
  public void write(long timestamp, Object value, int subTaskId) throws IOException {
    checkTimeAndMayFlushChunkToCurrentFile(timestamp, subTaskId);
    int seqFileIndex = seqFileIndexArray[subTaskId];
    CompactionWriterUtils.writeDataPoint(
        timestamp,
        value,
        chunkWriters[subTaskId],
        ++measurementPointCountArray[subTaskId] % checkPoint == 0
            ? targetFileWriters.get(seqFileIndex)
            : null,
        true);
    isDeviceExistedInTargetFiles[seqFileIndex] = true;
    isEmptyFile[seqFileIndex] = false;
  }

  @Override
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {
    // todo control time range of target tsfile
    checkTimeAndMayFlushChunkToCurrentFile(timestamps.getStartTime(), subTaskId);
    AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) this.chunkWriters[subTaskId];
    chunkWriter.write(timestamps, columns, batchSize);
    synchronized (this) {
      // we need to synchronized here to avoid multi-thread competition in sub-task
      TsFileResource resource = targetResources.get(seqFileIndexArray[subTaskId]);
      resource.updateStartTime(deviceId, timestamps.getStartTime());
      resource.updateEndTime(deviceId, timestamps.getEndTime());
    }
    CompactionWriterUtils.checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(seqFileIndexArray[subTaskId]), chunkWriter, true);
    isDeviceExistedInTargetFiles[seqFileIndexArray[subTaskId]] = true;
    isEmptyFile[seqFileIndexArray[subTaskId]] = false;
  }
}
