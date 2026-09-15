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

import org.apache.tsfile.common.conf.TSFileConfig;
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
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
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

  @Test
  public void testEstimateWrittenPointTotalSize() throws Exception {
    TestCompactionWriter compactionWriter = new TestCompactionWriter();
    TsPrimitiveType binaryValue =
        Type.fromTsDataType(TSDataType.TEXT)
            .getTsPrimitiveType(new Binary("abc", TSFileConfig.STRING_CHARSET));
    TsPrimitiveType vector =
        Type.fromTsDataType(TSDataType.VECTOR)
            .getTsPrimitiveType(
                new TsPrimitiveType[] {
                  Type.fromTsDataType(TSDataType.INT32).getTsPrimitiveType(1),
                  Type.fromTsDataType(TSDataType.INT64).getTsPrimitiveType(2L),
                  Type.fromTsDataType(TSDataType.FLOAT).getTsPrimitiveType(3F),
                  Type.fromTsDataType(TSDataType.DOUBLE).getTsPrimitiveType(4D),
                  Type.fromTsDataType(TSDataType.BOOLEAN).getTsPrimitiveType(true),
                  binaryValue,
                  null
                });

    Method estimateMethod =
        AbstractCompactionWriter.class.getDeclaredMethod(
            "estimateWrittenPointTotalSize", TsPrimitiveType.class);
    estimateMethod.setAccessible(true);
    Assert.assertEquals(36L, estimateMethod.invoke(compactionWriter, vector));

    CountingChunkWriter chunkWriter = new CountingChunkWriter();
    compactionWriter.startMeasurement("s1", chunkWriter, SUB_TASK_ID);
    compactionWriter.writePoint(binaryValue, chunkWriter);
    Assert.assertEquals(11L, compactionWriter.getWrittenPointTotalSize());

    TsBlockBuilder builder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.TEXT, TSDataType.BOOLEAN));
    builder.getTimeColumnBuilder().writeLong(1);
    builder.getColumnBuilder(0).writeInt(1);
    builder.getColumnBuilder(1).writeBinary(new Binary("abc", TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(2).writeBoolean(true);
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2);
    builder.getColumnBuilder(0).appendNull();
    builder.getColumnBuilder(1).appendNull();
    builder.getColumnBuilder(2).appendNull();
    builder.declarePosition();

    Assert.assertEquals(29L, compactionWriter.estimate(builder.build()));
  }

  @Test
  public void testWriteDataPointForAllDataTypes() {
    assertWrittenPointTotalSize(TSDataType.BOOLEAN, true, Long.BYTES + Byte.BYTES);
    assertWrittenPointTotalSize(TSDataType.INT32, 1, Long.BYTES + Integer.BYTES);
    assertWrittenPointTotalSize(TSDataType.DATE, 1, Long.BYTES + Integer.BYTES);
    assertWrittenPointTotalSize(TSDataType.INT64, 1L, Long.BYTES + Long.BYTES);
    assertWrittenPointTotalSize(TSDataType.TIMESTAMP, 1L, Long.BYTES + Long.BYTES);
    assertWrittenPointTotalSize(TSDataType.FLOAT, 1F, Long.BYTES + Float.BYTES);
    assertWrittenPointTotalSize(TSDataType.DOUBLE, 1D, Long.BYTES + Double.BYTES);

    Binary binary = new Binary("abc", TSFileConfig.STRING_CHARSET);
    assertWrittenPointTotalSize(TSDataType.TEXT, binary, Long.BYTES + binary.getLength());
    assertWrittenPointTotalSize(TSDataType.STRING, binary, Long.BYTES + binary.getLength());
    assertWrittenPointTotalSize(TSDataType.BLOB, binary, Long.BYTES + binary.getLength());
    assertWrittenPointTotalSize(TSDataType.OBJECT, binary, Long.BYTES + binary.getLength());
  }

  @Test
  public void testWriteAlignedDataPointWithFixedLengthTypes() {
    TestCompactionWriter compactionWriter = new TestCompactionWriter();
    AlignedChunkWriterImpl chunkWriter =
        new AlignedChunkWriterImpl(
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32),
                new MeasurementSchema("s2", TSDataType.BOOLEAN)));
    TsPrimitiveType vector =
        Type.fromTsDataType(TSDataType.VECTOR)
            .getTsPrimitiveType(
                new TsPrimitiveType[] {
                  Type.fromTsDataType(TSDataType.INT32).getTsPrimitiveType(1),
                  Type.fromTsDataType(TSDataType.BOOLEAN).getTsPrimitiveType(true)
                });

    compactionWriter.startMeasurement("aligned", chunkWriter, SUB_TASK_ID);
    compactionWriter.writePoint(vector, chunkWriter);

    Assert.assertEquals(
        Long.BYTES + Integer.BYTES + Byte.BYTES, compactionWriter.getWrittenPointTotalSize());
  }

  private static void assertWrittenPointTotalSize(
      TSDataType dataType, Object value, long expectedSize) {
    TestCompactionWriter compactionWriter = new TestCompactionWriter();
    CountingChunkWriter chunkWriter = new CountingChunkWriter(dataType);
    TsPrimitiveType primitiveType = Type.fromTsDataType(dataType).getTsPrimitiveType(value);

    compactionWriter.startMeasurement("s1", chunkWriter, SUB_TASK_ID);
    compactionWriter.writePoint(primitiveType, chunkWriter);

    Assert.assertEquals(
        dataType.toString(), expectedSize, compactionWriter.getWrittenPointTotalSize());
  }

  private static PageHeader createPageHeader(int compressedSize) {
    Statistics<?> statistics = Statistics.getStatsByType(TSDataType.TEXT);
    return new PageHeader(compressedSize, compressedSize, statistics);
  }

  private static class CountingChunkWriter extends ChunkWriterImpl {

    private int chunkSizeCheckCount;

    private CountingChunkWriter() {
      this(TSDataType.TEXT);
    }

    private CountingChunkWriter(TSDataType dataType) {
      super(new MeasurementSchema("s1", dataType));
    }

    @Override
    public boolean checkIsChunkSizeOverThreshold(
        long size, long pointNum, boolean returnTrueIfChunkEmpty) {
      chunkSizeCheckCount++;
      return false;
    }
  }

  private static class TestCompactionWriter extends AbstractCompactionWriter {

    private void writePoint(TsPrimitiveType value, IChunkWriter chunkWriter) {
      writeDataPoint(1, value, chunkWriter, SUB_TASK_ID);
    }

    private long getWrittenPointTotalSize() {
      return writtenPointTotalSizeArray[SUB_TASK_ID];
    }

    private long estimate(TsBlock tsBlock) {
      return estimateWrittenPointTotalSize(tsBlock);
    }

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
