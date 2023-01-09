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
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ReadPointInnerCompactionWriter extends AbstractInnerCompactionWriter {
  public ReadPointInnerCompactionWriter(TsFileResource targetFileResource) throws IOException {
    super(targetFileResource);
  }

  @Override
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {
    AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) this.chunkWriters[subTaskId];
    chunkWriter.write(timestamps, columns, batchSize);
    chunkPointNumArray[subTaskId] += timestamps.getTimes().length;
    checkChunkSizeAndMayOpenANewChunk(fileWriter, chunkWriter, subTaskId, false);
    isEmptyFile = false;
  }

  @Override
  public boolean flushNonAlignedChunk(Chunk chunk, ChunkMetadata chunkMetadata, int subTaskId) {
    throw new RuntimeException("Does not support this method in ReadPointInnerCompactionWriter");
  }

  @Override
  public boolean flushAlignedChunk(
      Chunk timeChunk,
      IChunkMetadata timeChunkMetadata,
      List<Chunk> valueChunks,
      List<IChunkMetadata> valueChunkMetadatas,
      int subTaskId) {
    throw new RuntimeException("Does not support this method in ReadPointInnerCompactionWriter");
  }

  @Override
  public boolean flushNonAlignedPage(
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId) {
    throw new RuntimeException("Does not support this method in ReadPointInnerCompactionWriter");
  }

  @Override
  public boolean flushAlignedPage(
      ByteBuffer compressedTimePageData,
      PageHeader timePageHeader,
      List<ByteBuffer> compressedValuePageDatas,
      List<PageHeader> valuePageHeaders,
      int subTaskId) {
    throw new RuntimeException("Does not support this method in ReadPointInnerCompactionWriter");
  }
}
