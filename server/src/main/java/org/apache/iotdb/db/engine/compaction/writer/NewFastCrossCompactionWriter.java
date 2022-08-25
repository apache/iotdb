package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class NewFastCrossCompactionWriter extends AbstractCrossCompactionWriter {

  public NewFastCrossCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqSourceResources)
      throws IOException {
    super(targetResources, seqSourceResources);
  }



  @Override
  public void write(long timestamp, Object value, int subTaskId) throws IOException {}

  @Override
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {}

  public void writeTimeValue(TimeValuePair timeValuePair, int subTaskId) throws IOException {
    checkTimeAndMayFlushChunkToCurrentFile(timeValuePair.getTimestamp(), subTaskId);
    CompactionWriterUtils.writeTVPair(timeValuePair, chunkWriters[subTaskId], null, true);
    isDeviceExistedInTargetFiles[seqFileIndexArray[subTaskId]] = true;
    isEmptyFile[seqFileIndexArray[subTaskId]] = false;
  }

  public boolean flushChunkToFileWriter(ChunkMetadata chunkMetadata, int subTaskId)
      throws IOException {
    int fileIndex = seqFileIndexArray[subTaskId];
    while (chunkMetadata.getStartTime() > currentDeviceEndTime[fileIndex]
        && fileIndex != targetFileWriters.size() - 1) {
      // jump to next file
      seqFileIndexArray[subTaskId] = ++fileIndex;
    }
    if (chunkMetadata.getEndTime() > currentDeviceEndTime[fileIndex]
        && fileIndex != targetFileWriters.size() - 1) {
      // deserialize the chunk
      return false;
    }

    TsFileIOWriter tsFileIOWriter = targetFileWriters.get(fileIndex);

    synchronized (tsFileIOWriter) {
      // seal last chunk to file writer
      // Todo: may cause small chunk
      chunkWriters[subTaskId].writeToFileWriter(tsFileIOWriter);
      Chunk chunk = ChunkCache.getInstance().get(chunkMetadata);
      tsFileIOWriter.writeChunk(chunk, chunkMetadata);
    }
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    return true;
  }

  public boolean flushPageToChunkWriter(
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId)
      throws IOException, PageException {
    int fileIndex = seqFileIndexArray[subTaskId];
    while (pageHeader.getStartTime() > currentDeviceEndTime[fileIndex]
        && fileIndex != targetFileWriters.size() - 1) {
      // jump to next file
      seqFileIndexArray[subTaskId] = ++fileIndex;
    }
    if (pageHeader.getEndTime() > currentDeviceEndTime[fileIndex]
        && fileIndex != targetFileWriters.size() - 1) {
      // deserialize the page
      return false;
    }

    ChunkWriterImpl chunkWriter = (ChunkWriterImpl) chunkWriters[subTaskId];
    // seal current page
    chunkWriter.sealCurrentPage();
    // flush new page to chunk writer directly
    // Todo: may cause small page
    chunkWriter.writePageHeaderAndDataIntoBuff(compressedPageData, pageHeader);


    // check chunk size and may open a new chunk
    CompactionWriterUtils.checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(fileIndex), chunkWriter, true);
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    return true;
  }

  protected void checkTimeAndMayFlushChunkToCurrentFile(long timestamp, int subTaskId)
      throws IOException {
    int fileIndex = seqFileIndexArray[subTaskId];
    // if timestamp is later than the current source seq tsfile, than flush chunk writer
    while (timestamp > currentDeviceEndTime[fileIndex]) {
      if (fileIndex != targetFileWriters.size() - 1) {
        CompactionWriterUtils.flushChunkToFileWriter(
            targetFileWriters.get(fileIndex), chunkWriters[subTaskId]);
        seqFileIndexArray[subTaskId] = ++fileIndex;
      } else {
        // If the seq file is deleted for various reasons, the following two situations may occur
        // when selecting the source files: (1) unseq files may have some devices or measurements
        // which are not exist in seq files. (2) timestamp of one timeseries in unseq files may
        // later than any seq files. Then write these data into the last target file.
        return;
      }
    }
  }
}
