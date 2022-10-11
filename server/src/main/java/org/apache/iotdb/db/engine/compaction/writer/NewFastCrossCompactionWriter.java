package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;
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
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {}

  public boolean flushChunkToFileWriter(IChunkMetadata iChunkMetadata, int subTaskId)
      throws IOException {
    checkTimeAndMayFlushChunkToCurrentFile(iChunkMetadata.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    boolean isChunkLargeEnough =
        chunkWriters[subTaskId].getSerializedChunkSize() >= chunkSizeLowerBoundInCompaction
            || chunkWriters[subTaskId].getPointNum() >= chunkPointNumLowerBoundInCompaction;
    if (!isChunkLargeEnough
        || (iChunkMetadata.getEndTime() > currentDeviceEndTime[fileIndex]
            && fileIndex != targetFileWriters.size() - 1)) {
      // if unsealed chunk is not large enough or chunk.endTime > file.endTime, then deserialize the
      // chunk
      return false;
    }

    TsFileIOWriter tsFileIOWriter = targetFileWriters.get(fileIndex);

    synchronized (tsFileIOWriter) {
      // seal last chunk to file writer
      chunkWriters[subTaskId].writeToFileWriter(tsFileIOWriter);
      if (iChunkMetadata instanceof AlignedChunkMetadata) {
        AlignedChunkMetadata alignedChunkMetadata = (AlignedChunkMetadata) iChunkMetadata;
        // flush time chunk
        ChunkMetadata chunkMetadata = (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata();
        tsFileIOWriter.writeChunk(ChunkCache.getInstance().get(chunkMetadata), chunkMetadata);
        // flush value chunks
        for (int i = 0; i < alignedChunkMetadata.getValueChunkMetadataList().size(); i++) {
          IChunkMetadata valueChunkMetadata =
              alignedChunkMetadata.getValueChunkMetadataList().get(i);
          if (valueChunkMetadata == null) {
            // sub sensor does not exist in current file or value chunk has been deleted completely
            AlignedChunkWriterImpl alignedChunkWriter =
                (AlignedChunkWriterImpl) chunkWriters[subTaskId];
            ValueChunkWriter valueChunkWriter = alignedChunkWriter.getValueChunkWriterByIndex(i);
            tsFileIOWriter.writeEmptyValueChunk(
                valueChunkWriter.getMeasurementId(),
                valueChunkWriter.getCompressionType(),
                valueChunkWriter.getDataType(),
                valueChunkWriter.getEncodingType(),
                valueChunkWriter.getStatistics());
            continue;
          }
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
    boolean isPageLargeEnough = chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold();
    if (!isPageLargeEnough
        || (timePageHeader.getEndTime() > currentDeviceEndTime[fileIndex]
            && fileIndex != targetFileWriters.size() - 1)) {
      // page.endTime > file.endTime, then deserialize the page
      return false;
    }

    AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriters[subTaskId];
    // seal current page
    alignedChunkWriter.sealCurrentPage();
    // flush new time page to chunk writer directly
    alignedChunkWriter.writePageHeaderAndDataIntoTimeBuff(compressedTimePageData, timePageHeader);

    // flush new value pages to chunk writer directly
    for (int i = 0; i < valuePageHeaders.size(); i++) {
      if (valuePageHeaders.get(i) == null) {
        // sub sensor does not exist in current file or value page has been deleted completely
        alignedChunkWriter.getValueChunkWriterByIndex(i).writeEmptyPageToPageBuffer();
        continue;
      }
      alignedChunkWriter.writePageHeaderAndDataIntoValueBuff(
          compressedValuePageDatas.get(i), valuePageHeaders.get(i), i);
    }

    measurementPointCountArray[subTaskId] += timePageHeader.getStatistics().getCount();

    // check chunk size and may open a new chunk
    checkChunkSizeAndMayOpenANewChunk(targetFileWriters.get(fileIndex), alignedChunkWriter, true);
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    return true;
  }

  public boolean flushPageToChunkWriter(
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId)
      throws IOException, PageException {
    checkTimeAndMayFlushChunkToCurrentFile(pageHeader.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    boolean isPageLargeEnough = chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold();
    if (!isPageLargeEnough
        || (pageHeader.getEndTime() > currentDeviceEndTime[fileIndex]
            && fileIndex != targetFileWriters.size() - 1)) {
      // page.endTime > file.endTime, then deserialize the page
      return false;
    }

    ChunkWriterImpl chunkWriter = (ChunkWriterImpl) chunkWriters[subTaskId];
    // seal current page
    chunkWriter.sealCurrentPage();
    // flush new page to chunk writer directly
    chunkWriter.writePageHeaderAndDataIntoBuff(compressedPageData, pageHeader);

    measurementPointCountArray[subTaskId] += pageHeader.getStatistics().getCount();

    // check chunk size and may open a new chunk
    checkChunkSizeAndMayOpenANewChunk(targetFileWriters.get(fileIndex), chunkWriter, true);
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    return true;
  }
}
