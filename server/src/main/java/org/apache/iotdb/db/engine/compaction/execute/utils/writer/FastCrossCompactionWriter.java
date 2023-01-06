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
package org.apache.iotdb.db.engine.compaction.execute.utils.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class FastCrossCompactionWriter extends AbstractCrossCompactionWriter {
  // Only used for fast compaction performer
  protected Map<TsFileResource, TsFileSequenceReader> readerMap;

  public FastCrossCompactionWriter(
      List<TsFileResource> targetResources,
      List<TsFileResource> seqSourceResources,
      Map<TsFileResource, TsFileSequenceReader> readerMap)
      throws IOException {
    super(targetResources, seqSourceResources);
    this.readerMap = readerMap;
  }

  @Override
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {
    throw new RuntimeException("Does not support this method in FastCrossCompactionWriter");
  }

  @Override
  protected TsFileSequenceReader getFileReader(TsFileResource resource) {
    return readerMap.get(resource);
  }

  /**
   * Flush nonAligned chunk to tsfile directly. Return whether the chunk is flushed to tsfile
   * successfully or not. Return false if the unsealed chunk is too small or the end time of chunk
   * exceeds the end time of file, else return true.
   */
  @Override
  public boolean flushNonAlignedChunk(Chunk chunk, ChunkMetadata chunkMetadata, int subTaskId)
      throws IOException {
    checkTimeAndMayFlushChunkToCurrentFile(chunkMetadata.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    if (!checkIsChunkSatisfied(chunkMetadata, fileIndex, subTaskId)) {
      // if unsealed chunk is not large enough or chunk.endTime > file.endTime, then deserialize the
      // chunk
      return false;
    }

    flushNonAlignedChunkToFileWriter(
        targetFileWriters.get(fileIndex), chunk, chunkMetadata, subTaskId);

    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    lastTime[subTaskId] = chunkMetadata.getEndTime();
    return true;
  }

  /**
   * Flush aligned chunk to tsfile directly. Return whether the chunk is flushed to tsfile
   * successfully or not. Return false if the unsealed chunk is too small or the end time of chunk
   * exceeds the end time of file, else return true. Notice: if sub-value measurement is null, then
   * flush empty value chunk.
   */
  @Override
  public boolean flushAlignedChunk(
      Chunk timeChunk,
      IChunkMetadata timeChunkMetadata,
      List<Chunk> valueChunks,
      List<IChunkMetadata> valueChunkMetadatas,
      int subTaskId)
      throws IOException {
    checkTimeAndMayFlushChunkToCurrentFile(timeChunkMetadata.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    if (!checkIsChunkSatisfied(timeChunkMetadata, fileIndex, subTaskId)) {
      // if unsealed chunk is not large enough or chunk.endTime > file.endTime, then deserialize the
      // chunk
      return false;
    }

    flushAlignedChunkToFileWriter(
        targetFileWriters.get(fileIndex),
        timeChunk,
        timeChunkMetadata,
        valueChunks,
        valueChunkMetadatas,
        subTaskId);

    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
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
    checkTimeAndMayFlushChunkToCurrentFile(timePageHeader.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    if (!checkIsPageSatisfied(timePageHeader, fileIndex, subTaskId)) {
      // unsealed page is too small or page.endTime > file.endTime, then deserialize the page
      return false;
    }

    flushAlignedPageToChunkWriter(
        (AlignedChunkWriterImpl) chunkWriters[subTaskId],
        compressedTimePageData,
        timePageHeader,
        compressedValuePageDatas,
        valuePageHeaders,
        subTaskId);

    // check chunk size and may open a new chunk
    checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(fileIndex), chunkWriters[subTaskId], subTaskId, true);

    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
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
    checkTimeAndMayFlushChunkToCurrentFile(pageHeader.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    if (!checkIsPageSatisfied(pageHeader, fileIndex, subTaskId)) {
      // unsealed page is too small or page.endTime > file.endTime, then deserialize the page
      return false;
    }

    flushNonAlignedPageToChunkWriter(
        (ChunkWriterImpl) chunkWriters[subTaskId], compressedPageData, pageHeader, subTaskId);

    // check chunk size and may open a new chunk
    checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(fileIndex), chunkWriters[subTaskId], subTaskId, true);

    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    lastTime[subTaskId] = pageHeader.getEndTime();
    return true;
  }

  private boolean checkIsChunkSatisfied(
      IChunkMetadata chunkMetadata, int fileIndex, int subTaskId) {
    boolean isUnsealedChunkLargeEnough =
        chunkWriters[subTaskId].checkIsChunkSizeOverThreshold(
            chunkSizeLowerBoundInCompaction, chunkPointNumLowerBoundInCompaction, true);
    if (!isUnsealedChunkLargeEnough
        || (chunkMetadata.getEndTime() > currentDeviceEndTime[fileIndex]
            && fileIndex != targetFileWriters.size() - 1)) {
      // if unsealed chunk is not large enough or chunk.endTime > file.endTime, then return false
      return false;
    }
    return true;
  }

  private boolean checkIsPageSatisfied(PageHeader pageHeader, int fileIndex, int subTaskId) {
    boolean isUnsealedPageLargeEnough =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            pageSizeLowerBoundInCompaction, pagePointNumLowerBoundInCompaction, true);
    if (!isUnsealedPageLargeEnough
        || (pageHeader.getEndTime() > currentDeviceEndTime[fileIndex]
            && fileIndex != targetFileWriters.size() - 1)) {
      // unsealed page is too small or page.endTime > file.endTime
      return false;
    }
    return true;
  }
}
