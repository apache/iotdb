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
import org.apache.iotdb.db.storageengine.load.splitter.LoadTsFileObjectFileBatch.ObjectFileChunk;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;

public class AbstractChunkDataObjectMetadataTest {

  private static final String MEASUREMENT_ID = "temperature";

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testNonAlignedChunkDataSizeIncludesObjectMetadata() throws Exception {
    final File objectDir = temporaryFolder.newFolder("objects");
    final File objectFile = new File(objectDir, "payload.bin");
    Files.write(objectFile.toPath(), new byte[] {1, 2, 3});

    final NonAlignedChunkData chunkData = createNonAlignedChunkData();
    final long rawDataSize = Whitebox.getInternalState(chunkData, "dataSize");

    chunkData.addObjectRelativePath(objectDir, "payload.bin");
    final long objectMetadataSize = RamUsageEstimator.sizeOf("payload.bin");

    Assert.assertEquals(objectMetadataSize, chunkData.getObjectMetadataSizeInBytes());
    Assert.assertEquals(rawDataSize + objectMetadataSize, chunkData.getDataSize());
  }

  @Test
  public void testAddDuplicateObjectPathDoesNotDoubleCountMetadata() throws Exception {
    final File objectDir = temporaryFolder.newFolder("objects");
    final File objectFile = new File(objectDir, "payload.bin");
    Files.write(objectFile.toPath(), new byte[] {1});

    final NonAlignedChunkData chunkData = createNonAlignedChunkData();

    chunkData.addObjectRelativePath(objectDir, "payload.bin");
    final long metadataSizeAfterFirstAdd = chunkData.getObjectMetadataSizeInBytes();

    chunkData.addObjectRelativePath(objectDir, "payload.bin");

    Assert.assertEquals(metadataSizeAfterFirstAdd, chunkData.getObjectMetadataSizeInBytes());
    Assert.assertEquals(1, chunkData.getObjectFiles().size());
  }

  @Test
  public void testRewrittenObjectPathCountsBothPathsAndDispatchesTargetPath() throws Exception {
    final File objectDir = temporaryFolder.newFolder("rewritten-objects");
    final File objectFile = new File(objectDir, "view/payload.bin");
    Assert.assertTrue(objectFile.getParentFile().mkdirs());
    Files.write(objectFile.toPath(), new byte[] {1});

    final NonAlignedChunkData chunkData = createNonAlignedChunkData();

    chunkData.addObjectRelativePath(objectDir, "view/payload.bin", "source/payload.bin");

    Assert.assertEquals(
        RamUsageEstimator.sizeOf("view/payload.bin")
            + RamUsageEstimator.sizeOf("source/payload.bin"),
        chunkData.getObjectMetadataSizeInBytes());
    Assert.assertEquals(1, chunkData.getObjectFiles().size());
    Assert.assertEquals("view/payload.bin", chunkData.getObjectFiles().iterator().next().right);

    final LoadTsFileObjectFileBatch objectFileBatch =
        chunkData.getObjectFileBatchIterator(Integer.MAX_VALUE).next();
    Assert.assertEquals(1, objectFileBatch.getObjectFileChunks().size());
    final ObjectFileChunk objectFileChunk = objectFileBatch.getObjectFileChunks().get(0);
    Assert.assertEquals("source/payload.bin", objectFileChunk.getObjectRelativePath());
  }

  @Test
  public void testAlignedChunkCopyObjectSidecarCopiesMetadataSize() throws Exception {
    final File objectDir = temporaryFolder.newFolder("objects");
    final File objectFile = new File(objectDir, "payload.bin");
    Files.write(objectFile.toPath(), new byte[] {1});

    final AlignedChunkData source = createAlignedChunkData();
    source.addObjectRelativePath(objectDir, "payload.bin");

    final AlignedChunkData copy = new AlignedChunkData(source);

    final long objectMetadataSize = source.getObjectMetadataSizeInBytes();
    Assert.assertEquals(objectMetadataSize, copy.getObjectMetadataSizeInBytes());
    // Copy constructor only transfers object sidecar, not chunkHeaderList, so getDataSize()
    // is not required to equal source.getDataSize(). It should still include object metadata.
    Assert.assertTrue(copy.getDataSize() >= objectMetadataSize);
  }

  @Test
  public void testNonAlignedChunkDataCanWriteWithoutSerdeRoundTrip() throws Exception {
    final NonAlignedChunkData chunkData = createNonAlignedChunkData();
    chunkData.setNotDecode();
    final Statistics<?> statistics = createInt32Statistics();
    final IChunkMetadata chunkMetadata = Mockito.mock(IChunkMetadata.class);
    Mockito.doReturn(statistics).when(chunkMetadata).getStatistics();
    chunkData.writeEntireChunk(ByteBuffer.allocate(0), chunkMetadata);

    final TsFileIOWriter writer = Mockito.mock(TsFileIOWriter.class);
    chunkData.writeToFileWriter(writer);

    Mockito.verify(writer).writeChunk(Mockito.any(Chunk.class));
  }

  @Test
  public void testAlignedChunkDataCanWriteWithoutSerdeRoundTrip() throws Exception {
    final AlignedChunkData chunkData = createAlignedChunkData();
    chunkData.setNotDecode();
    final Statistics<?> statistics = createInt32Statistics();
    final IChunkMetadata chunkMetadata = Mockito.mock(IChunkMetadata.class);
    Mockito.doReturn(statistics).when(chunkMetadata).getStatistics();
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
        MEASUREMENT_ID, 0, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0);
  }
}
