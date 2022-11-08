package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class FastInnerCompactionWriter extends AbstractInnerCompactionWriter {

  public FastInnerCompactionWriter(TsFileResource targetFileResource) throws IOException {
    super(targetFileResource);
  }

  @Override
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {}

  /**
   * Flush chunk to tsfile directly. Return whether the chunk is flushed to tsfile successfully or
   * not. Return false if the unsealed chunk is too small or the end time of chunk exceeds the end
   * time of file, else return true. Notice: if sub-value measurement is null, then flush empty
   * value chunk.
   */
  public boolean flushChunk(
      IChunkMetadata iChunkMetadata, TsFileSequenceReader reader, int subTaskId)
      throws IOException {
    boolean isUnsealedChunkLargeEnough =
        chunkWriters[subTaskId].checkIsChunkSizeOverThreshold(targetChunkSize, targetChunkPointNum);
    if (!isUnsealedChunkLargeEnough) {
      // if unsealed chunk is not large enough , then deserialize the chunk
      return false;
    }

    flushChunkToFileWriter(fileWriter, chunkWriters[subTaskId], reader, iChunkMetadata, subTaskId);

    isEmptyFile = false;
    lastTime[subTaskId] = iChunkMetadata.getEndTime();
    return true;
  }

  /**
   * Flush aligned page to tsfile directly. Return whether the page is flushed to tsfile
   * successfully or not. Return false if the unsealed page is too small or the end time of page
   * exceeds the end time of file, else return true. Notice: if sub-value measurement is null, then
   * flush empty value page.
   */
  public boolean flushAlignedPage(
      ByteBuffer compressedTimePageData,
      PageHeader timePageHeader,
      List<ByteBuffer> compressedValuePageDatas,
      List<PageHeader> valuePageHeaders,
      int subTaskId)
      throws IOException, PageException {
    boolean isUnsealedPageLargeEnough =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            targetPageSize, targetPagePointNum);
    if (!isUnsealedPageLargeEnough) {
      // unsealed page is too small , then deserialize the page
      return false;
    }

    flushAlignedPageToChunkWriter(
        fileWriter,
        (AlignedChunkWriterImpl) chunkWriters[subTaskId],
        compressedTimePageData,
        timePageHeader,
        compressedValuePageDatas,
        valuePageHeaders,
        subTaskId);

    isEmptyFile = false;
    lastTime[subTaskId] = timePageHeader.getEndTime();
    return true;
  }

  /**
   * Flush nonAligned page to tsfile directly. Return whether the page is flushed to tsfile
   * successfully or not. Return false if the unsealed page is too small or the end time of page
   * exceeds the end time of file, else return true.
   */
  public boolean flushNonAlignedPage(
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId)
      throws IOException, PageException {
    boolean isUnsealedPageLargeEnough =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            targetPageSize, targetPagePointNum);
    if (!isUnsealedPageLargeEnough) {
      // unsealed page is too small , then deserialize the page
      return false;
    }

    flushNonAlignedPageToChunkWriter(
        fileWriter,
        (ChunkWriterImpl) chunkWriters[subTaskId],
        compressedPageData,
        pageHeader,
        subTaskId);

    isEmptyFile = false;
    lastTime[subTaskId] = pageHeader.getEndTime();
    return true;
  }
}
