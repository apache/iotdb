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
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

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

  private static ChunkHeader createChunkHeader() {
    return new ChunkHeader(
        "temperature", 0, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0);
  }
}
