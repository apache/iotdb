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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.tsfile.common.constant.TsFileConstant.TIME_COLUMN_MASK;
import static org.apache.tsfile.common.constant.TsFileConstant.VALUE_COLUMN_MASK;
import static org.junit.Assert.assertEquals;

public class ChunkDataDirectWriteTest {

  @Test
  public void testNonAlignedChunkDataCanWriteWithoutSerdeRoundTrip() throws Exception {
    final NonAlignedChunkData chunkData = createNonAlignedChunkData();
    chunkData.setNotDecode();
    final IChunkMetadata chunkMetadata = Mockito.mock(IChunkMetadata.class);
    Mockito.doReturn(createInt32Statistics()).when(chunkMetadata).getStatistics();
    chunkData.writeEntireChunk(ByteBuffer.allocate(0), chunkMetadata);

    final TsFileIOWriter writer = Mockito.mock(TsFileIOWriter.class);
    chunkData.writeToFileWriter(writer);

    Mockito.verify(writer).writeChunk(Mockito.any(Chunk.class));
  }

  @Test
  public void testAlignedChunkDataCanWriteWithoutSerdeRoundTrip() throws Exception {
    final AlignedChunkData chunkData = createAlignedChunkData();
    chunkData.setNotDecode();
    final IChunkMetadata chunkMetadata = Mockito.mock(IChunkMetadata.class);
    Mockito.doReturn(createInt32Statistics()).when(chunkMetadata).getStatistics();
    chunkData.writeEntireChunk(ByteBuffer.allocate(0), chunkMetadata);

    final TsFileIOWriter writer = Mockito.mock(TsFileIOWriter.class);
    chunkData.writeToFileWriter(writer);

    Mockito.verify(writer).writeChunk(Mockito.any(Chunk.class));
  }

  @Test
  public void testAlignedChunkDataAcceptsObjectArrayFromTimePageDecode() throws Exception {
    final AlignedChunkData chunkData = createAlignedTimeChunkData();

    chunkData.writeDecodePage(new long[] {1L, 2L}, new Object[] {null, null}, 2);
  }

  @Test
  public void testReencodedNonAlignedChunkRebuildsHeader() {
    final NonAlignedChunkData chunkData = createNonAlignedChunkData();
    chunkData.writeDecodePage(new long[] {1L, 2L}, new Object[] {1, 2}, 2);
    chunkData.endChunk();

    final Chunk chunk = chunkData.getChunks().get(0);
    assertEquals(chunk.getData().remaining(), chunk.getHeader().getDataSize());
    assertEquals(1, chunk.getHeader().getNumOfPages());
  }

  @Test
  public void testAlignedChunkEndBuildsOnlyCurrentPhysicalChunk() throws Exception {
    final AlignedChunkData chunkData = createAlignedTimeChunkData();
    final long[] times = new long[] {1L, 2L};
    chunkData.writeDecodePage(times, new Object[] {null, null}, times.length);
    chunkData.endChunk();

    final Chunk timeChunk = chunkData.getChunks().get(0);
    assertEquals(1, chunkData.getChunks().size());
    assertEquals(timeChunk.getData().remaining(), timeChunk.getHeader().getDataSize());
    assertEquals(TIME_COLUMN_MASK, timeChunk.getHeader().getChunkType() & TIME_COLUMN_MASK);

    chunkData.addValueChunk(createChunkHeader());
    chunkData.writeDecodeValuePage(
        times,
        new TsPrimitiveType[] {
          TsPrimitiveType.getByType(TSDataType.INT32, 1),
          TsPrimitiveType.getByType(TSDataType.INT32, 2)
        },
        TSDataType.INT32);
    chunkData.endChunk();

    final List<Chunk> chunks = chunkData.getChunks();
    assertEquals(2, chunks.size());
    final Chunk valueChunk = chunks.get(1);
    assertEquals(valueChunk.getData().remaining(), valueChunk.getHeader().getDataSize());
    assertEquals(VALUE_COLUMN_MASK, valueChunk.getHeader().getChunkType() & VALUE_COLUMN_MASK);
  }

  private static Statistics<?> createInt32Statistics() {
    final Statistics<?> statistics = Statistics.getStatsByType(TSDataType.INT32);
    statistics.update(1L, 1);
    return statistics;
  }

  private static NonAlignedChunkData createNonAlignedChunkData() {
    final IDeviceID device = new StringArrayDeviceID("root", "sg", "d1");
    return (NonAlignedChunkData)
        ChunkData.createChunkData(false, device, createChunkHeader(), new TTimePartitionSlot(0L));
  }

  private static AlignedChunkData createAlignedChunkData() {
    final IDeviceID device = new StringArrayDeviceID("root", "sg", "d1");
    return (AlignedChunkData)
        ChunkData.createChunkData(true, device, createChunkHeader(), new TTimePartitionSlot(0L));
  }

  private static AlignedChunkData createAlignedTimeChunkData() {
    final IDeviceID device = new StringArrayDeviceID("root", "sg", "d1");
    final ChunkHeader timeHeader =
        new ChunkHeader(
            "",
            1024,
            TSDataType.VECTOR,
            CompressionType.UNCOMPRESSED,
            TSEncoding.PLAIN,
            2,
            TIME_COLUMN_MASK);
    return (AlignedChunkData)
        ChunkData.createChunkData(true, device, timeHeader, new TTimePartitionSlot(0L));
  }

  private static ChunkHeader createChunkHeader() {
    return new ChunkHeader(
        "temperature", 0, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0);
  }
}
