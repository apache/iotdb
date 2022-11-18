package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Chunk;
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
      throws IOException {
    throw new RuntimeException("Does not support this method in FastInnerCompactionWriter");
  }

  @Override
  public void write(TimeValuePair timeValuePair, int subTaskId) throws IOException {
    // write points into page without checking page size
    if (isAlign) {
      ((AlignedChunkWriterImpl) chunkWriters[subTaskId])
          .write(timeValuePair.getTimestamp(), timeValuePair.getValue().getVector(), false);
    } else {
      ((ChunkWriterImpl) chunkWriters[subTaskId])
          .write(timeValuePair.getTimestamp(), timeValuePair.getValue());
    }

    chunkPointNumArray[subTaskId]++;
    isEmptyFile = false;
  }

  /**
   * Flush nonAligned chunk to tsfile directly. Return whether the chunk is flushed to tsfile
   * successfully or not. Return false if there is unsealed chunk or current chunk is not large
   * enough, else return true.
   */
  @Override
  public boolean flushNonAlignedChunk(Chunk chunk, ChunkMetadata chunkMetadata, int subTaskId)
      throws IOException {
    if (chunkPointNumArray[subTaskId] != 0
        && chunkWriters[subTaskId].checkIsChunkSizeOverThreshold(
            targetChunkSize, targetChunkPointNum, false)) {
      // if there is unsealed chunk which is large enough, then seal chunk
      sealChunk(fileWriter, chunkWriters[subTaskId], subTaskId);
    }
    if (chunkPointNumArray[subTaskId] != 0 || !checkIsChunkLargeEnough(chunk)) {
      // if there is unsealed chunk or current chunk is not large enough, then deserialize the chunk
      return false;
    }
    flushNonAlignedChunkToFileWriter(fileWriter, chunk, chunkMetadata, subTaskId);

    isEmptyFile = false;
    lastTime[subTaskId] = chunkMetadata.getEndTime();
    return true;
  }

  /**
   * Flush nonAligned chunk to tsfile directly. Return whether the chunk is flushed to tsfile
   * successfully or not. Return false if there is unsealed chunk or current chunk is not large
   * enough, else return true. Notice: if sub-value measurement is null, then flush empty value
   * chunk.
   */
  @Override
  public boolean flushAlignedChunk(
      Chunk timeChunk,
      IChunkMetadata timeChunkMetadata,
      List<Chunk> valueChunks,
      List<IChunkMetadata> valueChunkMetadatas,
      int subTaskId)
      throws IOException {
    if (chunkPointNumArray[subTaskId] != 0
        && chunkWriters[subTaskId].checkIsChunkSizeOverThreshold(
            targetChunkSize, targetChunkPointNum, false)) {
      // if there is unsealed chunk which is large enough, then seal chunk
      sealChunk(fileWriter, chunkWriters[subTaskId], subTaskId);
    }
    if (chunkPointNumArray[subTaskId] != 0
        || !checkIsAlignedChunkLargeEnough(timeChunk, valueChunks)) {
      // if there is unsealed chunk or current chunk is not large enough, then deserialize the chunk
      return false;
    }
    flushAlignedChunkToFileWriter(
        fileWriter, timeChunk, timeChunkMetadata, valueChunks, valueChunkMetadatas, subTaskId);

    isEmptyFile = false;
    lastTime[subTaskId] = timeChunkMetadata.getEndTime();
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
    boolean isUnsealedPageOverThreshold =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            targetPageSize, targetPagePointNum, true);
    if (isUnsealedPageOverThreshold) {
      // seal page
      chunkWriters[subTaskId].sealCurrentPage();
    }
    if (!isUnsealedPageOverThreshold
        || !checkIsAlignedPageLargeEnough(timePageHeader, valuePageHeaders)) {
      // there is unsealed page or current page is not large enough , then deserialize the page
      return false;
    }

    flushAlignedPageToChunkWriter(
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
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId) throws PageException {
    boolean isUnsealedPageOverThreshold =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            targetPageSize, targetPagePointNum, true);
    if (isUnsealedPageOverThreshold) {
      // seal page
      chunkWriters[subTaskId].sealCurrentPage();
    }
    if (!isUnsealedPageOverThreshold || !checkIsPageLargeEnough(pageHeader)) {
      // there is unsealed page or current page is not large enough , then deserialize the page
      return false;
    }

    flushNonAlignedPageToChunkWriter(
        (ChunkWriterImpl) chunkWriters[subTaskId], compressedPageData, pageHeader, subTaskId);

    isEmptyFile = false;
    lastTime[subTaskId] = pageHeader.getEndTime();
    return true;
  }

  private boolean checkIsAlignedChunkLargeEnough(Chunk timeChunk, List<Chunk> valueChunks) {
    if (checkIsChunkLargeEnough(timeChunk)) {
      return true;
    }
    for (Chunk valueChunk : valueChunks) {
      if (valueChunk == null) {
        continue;
      }
      if (checkIsChunkLargeEnough(valueChunk)) {
        return true;
      }
    }
    return false;
  }

  private boolean checkIsChunkLargeEnough(Chunk chunk) {
    return chunk.getChunkStatistic().getCount() >= targetChunkPointNum
        || getChunkSize(chunk) >= targetChunkSize;
  }

  private boolean checkIsAlignedPageLargeEnough(
      PageHeader timePageHeader, List<PageHeader> valuePageHeaders) {
    if (checkIsPageLargeEnough(timePageHeader)) {
      return true;
    }
    for (PageHeader valuePageHeader : valuePageHeaders) {
      if (valuePageHeader == null) {
        continue;
      }
      if (checkIsPageLargeEnough(valuePageHeader)) {
        return true;
      }
    }
    return false;
  }

  private boolean checkIsPageLargeEnough(PageHeader pageHeader) {
    return pageHeader.getStatistics().getCount() >= targetPagePointNum
        || pageHeader.getSerializedPageSize() >= targetPageSize;
  }
}
