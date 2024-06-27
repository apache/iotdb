/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.AlignedPageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.ChunkMetadataElement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;

public class FastInnerCompactionWriter extends AbstractInnerCompactionWriter {

  public FastInnerCompactionWriter(TsFileResource targetFileResource) throws IOException {
    super(targetFileResource);
  }

  @Override
  public void write(TsBlock tsBlock, int subTaskId) throws IOException {
    throw new RuntimeException("Does not support this method in FastInnerCompactionWriter");
  }

  /**
   * Flush nonAligned chunk to tsfile directly. Return whether the chunk is flushed to tsfile
   * successfully or not. Return false if there is unsealed chunk or current chunk is not large
   * enough, else return true.
   */
  @Override
  public boolean flushNonAlignedChunk(Chunk chunk, ChunkMetadata chunkMetadata, int subTaskId)
      throws IOException {
    checkPreviousTimestamp(chunkMetadata.getStartTime(), subTaskId);
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
   *
   * @throws IOException if io errors occurred
   */
  @Override
  public boolean flushAlignedChunk(
      ChunkMetadataElement chunkMetadataElement,
      int subTaskId,
      Supplier<Boolean> shouldDirectlyFlushChunkInBatchCompaction)
      throws IOException {
    AlignedChunkMetadata alignedChunkMetadata =
        (AlignedChunkMetadata) chunkMetadataElement.chunkMetadata;
    IChunkMetadata timeChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
    List<IChunkMetadata> valueChunkMetadatas = alignedChunkMetadata.getValueChunkMetadataList();
    Chunk timeChunk = chunkMetadataElement.chunk;
    List<Chunk> valueChunks = chunkMetadataElement.valueChunks;

    checkPreviousTimestamp(timeChunkMetadata.getStartTime(), subTaskId);
    if (chunkPointNumArray[subTaskId] != 0
        && chunkWriters[subTaskId].checkIsChunkSizeOverThreshold(
            targetChunkSize, targetChunkPointNum, false)) {
      // if there is unsealed chunk which is large enough, then seal chunk
      sealChunk(fileWriter, chunkWriters[subTaskId], subTaskId);
    }
    // if there is unsealed chunk or current chunk is not large enough, then deserialize the chunk
    if (chunkPointNumArray[subTaskId] != 0) {
      return false;
    }
    boolean isCompactingFollowedBatch = chunkMetadataElement.isFollowedBatch;
    if (!isCompactingFollowedBatch && !checkIsAlignedChunkLargeEnough(timeChunk, valueChunks)) {
      return false;
    }
    if (isCompactingFollowedBatch && !shouldDirectlyFlushChunkInBatchCompaction.get()) {
      return false;
    }
    flushAlignedChunkToFileWriter(
        fileWriter,
        isCompactingFollowedBatch ? null : timeChunk,
        timeChunkMetadata,
        valueChunks,
        valueChunkMetadatas,
        subTaskId);

    isEmptyFile = false;
    lastTime[subTaskId] = timeChunkMetadata.getEndTime();
    return true;
  }

  /**
   * Flush aligned page to tsfile directly. Return whether the page is flushed to tsfile
   * successfully or not. Return false if the unsealed page is too small or the end time of page
   * exceeds the end time of file, else return true. Notice: if sub-value measurement is null, then
   * flush empty value page.
   *
   * @throws IOException if io errors occurred
   * @throws PageException if errors occurred when write data page header
   */
  public boolean flushAlignedPage(AlignedPageElement alignedPageElement, int subTaskId)
      throws IOException, PageException {
    PageHeader timePageHeader = alignedPageElement.getTimePageHeader();
    List<PageHeader> valuePageHeaders = alignedPageElement.getValuePageHeaders();
    ByteBuffer compressedTimePageData = alignedPageElement.getTimePageData();
    List<ByteBuffer> compressedValuePageDatas = alignedPageElement.getValuePageDataList();

    checkPreviousTimestamp(timePageHeader.getStartTime(), subTaskId);
    boolean isUnsealedPageOverThreshold =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            pageSizeLowerBoundInCompaction, pagePointNumLowerBoundInCompaction, true);
    boolean isCompactingFollowedBatch = alignedPageElement.isFollowedBatch();
    // there is unsealed page or current page is not large enough , then deserialize the page
    if (!isUnsealedPageOverThreshold) {
      return false;
    }
    if (!isCompactingFollowedBatch
        && !checkIsAlignedPageLargeEnough(timePageHeader, valuePageHeaders)) {
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
   *
   * @throws PageException if errors occurred when write data page header
   */
  public boolean flushNonAlignedPage(
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId) throws PageException {
    checkPreviousTimestamp(pageHeader.getStartTime(), subTaskId);
    boolean isUnsealedPageOverThreshold =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            pageSizeLowerBoundInCompaction, pagePointNumLowerBoundInCompaction, true);
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
    return pageHeader.getStatistics().getCount() >= pagePointNumLowerBoundInCompaction
        || pageHeader.getSerializedPageSize() >= pageSizeLowerBoundInCompaction;
  }
}
