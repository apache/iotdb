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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.flushcontroller.AbstractCompactionFlushController;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ReadPointInnerCompactionWriter extends AbstractInnerCompactionWriter {
  public ReadPointInnerCompactionWriter(TsFileResource targetFileResource) throws IOException {
    super(targetFileResource);
  }

  public ReadPointInnerCompactionWriter(List<TsFileResource> targetFileResources)
      throws IOException {
    super(targetFileResources);
  }

  @Override
  public void write(TsBlock tsBlock, int subTaskId) throws IOException {
    TimeColumn timestamps = (TimeColumn) tsBlock.getTimeColumn();
    Column[] columns = tsBlock.getValueColumns();
    int batchSize = tsBlock.getPositionCount();
    AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) this.chunkWriters[subTaskId];
    chunkWriter.write(timestamps, columns, batchSize);
    chunkPointNumArray[subTaskId] += timestamps.getTimes().length;
    checkChunkSizeAndMayOpenANewChunk(fileWriter, chunkWriter, subTaskId);
  }

  @Override
  public boolean flushNonAlignedChunk(Chunk chunk, ChunkMetadata chunkMetadata, int subTaskId) {
    throw new RuntimeException("Does not support this method in ReadPointInnerCompactionWriter");
  }

  @Override
  public boolean flushAlignedChunk(ChunkMetadataElement chunkMetadataElement, int subTaskId) {
    throw new RuntimeException("Does not support this method in ReadPointInnerCompactionWriter");
  }

  @Override
  public boolean flushBatchedValueChunk(
      ChunkMetadataElement chunkMetadataElement,
      int subTaskId,
      AbstractCompactionFlushController flushController) {
    throw new RuntimeException("Does not support this method in ReadPointInnerCompactionWriter");
  }

  @Override
  public boolean flushNonAlignedPage(
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId) {
    throw new RuntimeException("Does not support this method in ReadPointInnerCompactionWriter");
  }

  @Override
  public boolean flushAlignedPage(AlignedPageElement alignedPageElement, int subTaskId) {
    throw new RuntimeException("Does not support this method in ReadPointInnerCompactionWriter");
  }

  @Override
  public boolean flushBatchedValuePage(
      AlignedPageElement alignedPageElement,
      int subTaskId,
      AbstractCompactionFlushController flushController)
      throws PageException, IOException {
    throw new RuntimeException("Does not support this method in ReadPointInnerCompactionWriter");
  }
}
