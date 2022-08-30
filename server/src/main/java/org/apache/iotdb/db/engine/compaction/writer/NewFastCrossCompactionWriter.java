package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
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

  public boolean flushChunkToFileWriter(IChunkMetadata iChunkMetadata, int subTaskId)
      throws IOException {
    checkTimeAndMayFlushChunkToCurrentFile(iChunkMetadata.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    if (iChunkMetadata.getEndTime() > currentDeviceEndTime[fileIndex]
        && fileIndex != targetFileWriters.size() - 1) {
      // chunk.endTime > file.endTime, then deserialize the chunk
      return false;
    }

    TsFileIOWriter tsFileIOWriter = targetFileWriters.get(fileIndex);

    synchronized (tsFileIOWriter) {
      // seal last chunk to file writer
      // Todo: may cause small chunk
      chunkWriters[subTaskId].writeToFileWriter(tsFileIOWriter);
      if (iChunkMetadata instanceof AlignedChunkMetadata) {
        AlignedChunkMetadata alignedChunkMetadata = (AlignedChunkMetadata) iChunkMetadata;
        // flush time chunk
        ChunkMetadata chunkMetadata = (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata();
        tsFileIOWriter.writeChunk(ChunkCache.getInstance().get(chunkMetadata), chunkMetadata);
        // flush value chunks
        for (IChunkMetadata valueChunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
          chunkMetadata = (ChunkMetadata) valueChunkMetadata;
          tsFileIOWriter.writeChunk(ChunkCache.getInstance().get(chunkMetadata), chunkMetadata);
        }
      } else {
        ChunkMetadata chunkMetadata = (ChunkMetadata) iChunkMetadata;
        tsFileIOWriter.writeChunk(ChunkCache.getInstance().get(chunkMetadata), chunkMetadata);
      }
    }
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    return true;
  }

  public boolean flushAlignedPageToChunkWriter(
      ByteBuffer compressedTimePageData,
      PageHeader timePageHeader,
      List<ByteBuffer> compressedValuePageDatas,
      List<PageHeader> valuePageHeaders,
      int subTaskId)
      throws IOException, PageException {
    checkTimeAndMayFlushChunkToCurrentFile(timePageHeader.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    if (timePageHeader.getEndTime() > currentDeviceEndTime[fileIndex]
        && fileIndex != targetFileWriters.size() - 1) {
      // page.endTime > file.endTime, then deserialize the page
      return false;
    }

    AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriters[subTaskId];
    // seal current page
    alignedChunkWriter.sealCurrentPage();
    // flush new time page to chunk writer directly
    // Todo: may cause small page
    alignedChunkWriter.writePageHeaderAndDataIntoTimeBuff(compressedTimePageData, timePageHeader);

    // flush new value pages to chunk writer directly
    for (int i = 0; i < valuePageHeaders.size(); i++) {
      alignedChunkWriter.writePageHeaderAndDataIntoValueBuff(
          compressedValuePageDatas.get(i), valuePageHeaders.get(i), i);
    }

    // check chunk size and may open a new chunk
    CompactionWriterUtils.checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(fileIndex), alignedChunkWriter, true);
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    return true;
  }

  public boolean flushPageToChunkWriter(
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId)
      throws IOException, PageException {
    checkTimeAndMayFlushChunkToCurrentFile(pageHeader.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    if (pageHeader.getEndTime() > currentDeviceEndTime[fileIndex]
        && fileIndex != targetFileWriters.size() - 1) {
      // page.endTime > file.endTime, then deserialize the page
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
}
