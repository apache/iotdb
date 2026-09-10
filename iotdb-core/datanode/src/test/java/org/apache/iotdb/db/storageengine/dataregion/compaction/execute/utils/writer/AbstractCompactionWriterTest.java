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

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class AbstractCompactionWriterTest {

  private static final int SUB_TASK_ID = 0;

  @Test
  public void testBinarySizeCheckpointTriggersChunkSizeCheckBeforePointCheckpoint()
      throws IOException, PageException {
    TestCompactionWriter compactionWriter = new TestCompactionWriter();
    CountingChunkWriter chunkWriter = new CountingChunkWriter();
    PageHeader pageHeader =
        createPageHeader(compactionWriter.getCompressedSizeToReachSizeCheckpoint());

    compactionWriter.startMeasurement("s1", chunkWriter, SUB_TASK_ID);
    compactionWriter.flushNonAlignedPageToChunkWriter(
        chunkWriter, ByteBuffer.allocate(0), pageHeader, SUB_TASK_ID);
    compactionWriter.checkChunkSizeAndMayOpenANewChunk(null, chunkWriter, SUB_TASK_ID);

    Assert.assertEquals(1, chunkWriter.chunkSizeCheckCount);
  }

  private static PageHeader createPageHeader(int compressedSize) {
    Statistics<?> statistics = Statistics.getStatsByType(TSDataType.TEXT);
    return new PageHeader(compressedSize, compressedSize, statistics);
  }

  private static class CountingChunkWriter extends ChunkWriterImpl {

    private int chunkSizeCheckCount;

    private CountingChunkWriter() {
      super(new MeasurementSchema("s1", TSDataType.TEXT));
    }

    @Override
    public boolean checkIsChunkSizeOverThreshold(
        long size, long pointNum, boolean returnTrueIfChunkEmpty) {
      chunkSizeCheckCount++;
      return false;
    }
  }

  private static class TestCompactionWriter extends AbstractCompactionWriter {

    private int getCompressedSizeToReachSizeCheckpoint() {
      return (int) Math.max(targetChunkSize / 10, 1L);
    }

    @Override
    public void startChunkGroup(IDeviceID deviceId, boolean isAlign) {}

    @Override
    public void endChunkGroup() {}

    @Override
    public void endMeasurement(int subTaskId) {}

    @Override
    public void write(TimeValuePair timeValuePair, int subTaskId) {}

    @Override
    public void write(TsBlock tsBlock, int subTaskId) {}

    @Override
    public void endFile() {}

    @Override
    public long getWriterSize() {
      return 0;
    }

    @Override
    public void checkAndMayFlushChunkMetadata() {}

    @Override
    public EncryptParameter getEncryptParameter() {
      return null;
    }

    @Override
    public boolean flushNonAlignedChunk(Chunk chunk, ChunkMetadata chunkMetadata, int subTaskId) {
      return false;
    }

    @Override
    public boolean flushAlignedChunk(ChunkMetadataElement chunkMetadataElement, int subTaskId) {
      return false;
    }

    @Override
    public boolean flushBatchedValueChunk(
        ChunkMetadataElement chunkMetadataElement,
        int subTaskId,
        AbstractCompactionFlushController flushController) {
      return false;
    }

    @Override
    public boolean flushNonAlignedPage(
        ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId) {
      return false;
    }

    @Override
    public boolean flushAlignedPage(AlignedPageElement alignedPageElement, int subTaskId) {
      return false;
    }

    @Override
    public boolean flushBatchedValuePage(
        AlignedPageElement alignedPageElement,
        int subTaskId,
        AbstractCompactionFlushController flushController) {
      return false;
    }

    @Override
    public void setSchemaForAllTargetFile(List<Schema> schemas) {}

    @Override
    public void close() {}
  }
}
