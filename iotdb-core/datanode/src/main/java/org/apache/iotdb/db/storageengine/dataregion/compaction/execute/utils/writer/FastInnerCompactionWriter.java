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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.LazyChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.LazyPageLoader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.access.Column;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
   *
   * @throws IOException if io errors occurred
   */
  @Override
  public boolean flushAlignedChunk(
      LazyChunkLoader timeChunkLoader,
      IChunkMetadata timeChunkMetadata,
      List<LazyChunkLoader> valueChunkLoaders,
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
        || !checkIsAlignedChunkLargeEnough(timeChunkLoader, valueChunkLoaders)) {
      // if there is unsealed chunk or current chunk is not large enough, then deserialize the chunk
      return false;
    }
    flushAlignedChunkToFileWriter(
        fileWriter,
        timeChunkLoader,
        timeChunkMetadata,
        valueChunkLoaders,
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
  public boolean flushAlignedPage(
      LazyPageLoader timePageLoader, List<LazyPageLoader> valuePageLoaders, int subTaskId)
      throws IOException, PageException {
    boolean isUnsealedPageOverThreshold =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            pageSizeLowerBoundInCompaction, pagePointNumLowerBoundInCompaction, true);
    if (isUnsealedPageOverThreshold) {
      // seal page
      chunkWriters[subTaskId].sealCurrentPage();
    }
    PageHeader timePageHeader = timePageLoader.getPageHeader();
    List<PageHeader> valuePageHeaders = new ArrayList<>(valuePageLoaders.size());
    for (LazyPageLoader valuePageLoader : valuePageLoaders) {
      valuePageHeaders.add(valuePageLoader.getPageHeader());
    }
    if (!isUnsealedPageOverThreshold
        || !checkIsAlignedPageLargeEnough(timePageHeader, valuePageHeaders)) {
      // there is unsealed page or current page is not large enough , then deserialize the page
      return false;
    }

    flushAlignedPageToChunkWriter(
        (AlignedChunkWriterImpl) chunkWriters[subTaskId],
        timePageLoader,
        valuePageLoaders,
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
    boolean isUnsealedPageOverThreshold =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            pageSizeLowerBoundInCompaction, pagePointNumLowerBoundInCompaction, true);
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

  private boolean checkIsAlignedChunkLargeEnough(
      LazyChunkLoader timeChunk, List<LazyChunkLoader> valueChunks) throws IOException {
    if (checkIsChunkLargeEnough(timeChunk.getChunkMetadata(), timeChunk.loadChunkHeader())) {
      return true;
    }
    for (LazyChunkLoader valueChunk : valueChunks) {
      if (valueChunk.isEmpty()) {
        continue;
      }
      if (checkIsChunkLargeEnough(valueChunk.getChunkMetadata(), valueChunk.loadChunkHeader())) {
        return true;
      }
    }
    return false;
  }

  private boolean checkIsChunkLargeEnough(Chunk chunk) {
    return chunk.getChunkStatistic().getCount() >= targetChunkPointNum
        || getChunkSize(chunk) >= targetChunkSize;
  }

  private boolean checkIsChunkLargeEnough(ChunkMetadata chunkMetadata, ChunkHeader chunkHeader) {
    return chunkMetadata.getStatistics().getCount() >= targetChunkPointNum
        || chunkHeader.getSerializedSize() + chunkHeader.getDataSize() >= targetChunkSize;
  }

  private boolean checkIsAlignedPageLargeEnough(
      PageHeader timePageHeader, List<PageHeader> valuePageHeaders) {
    if (checkIsPageLargeEnough(timePageHeader)) {
      return true;
    }
    for (PageHeader valuePageHeader : valuePageHeaders) {
      if (valuePageHeader == null || valuePageHeader.getStatistics() == null) {
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
