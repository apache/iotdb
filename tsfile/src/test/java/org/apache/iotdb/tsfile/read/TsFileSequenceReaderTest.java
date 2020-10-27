/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.read;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.utils.FileGenerator;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TsFileSequenceReaderTest {

  private static final String FILE_PATH = FileGenerator.outputDataFile;
  private ReadOnlyTsFile tsFile;

  @Before
  public void before() throws IOException {
    int rowCount = 100;
    FileGenerator.generateFile(rowCount, 10000);
    TsFileSequenceReader fileReader = new TsFileSequenceReader(FILE_PATH);
    tsFile = new ReadOnlyTsFile(fileReader);
  }

  @After
  public void after() throws IOException {
    tsFile.close();
    FileGenerator.after();
  }

  @Test
  public void testReadTsFileSequently() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);
    reader.position(TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.VERSION_NUMBER
        .getBytes().length);
    Map<String, List<Pair<Long, Long>>> deviceChunkGroupMetadataOffsets = new HashMap<>();

    long startOffset = reader.position();
    byte marker;
    while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
      switch (marker) {
        case MetaMarker.CHUNK_HEADER:
          ChunkHeader header = reader.readChunkHeader();
          for (int j = 0; j < header.getNumOfPages(); j++) {
            PageHeader pageHeader = reader.readPageHeader(header.getDataType());
            reader.readPage(pageHeader, header.getCompressionType());
          }
          break;
        case MetaMarker.CHUNK_GROUP_FOOTER:
          ChunkGroupFooter footer = reader.readChunkGroupFooter();
          long endOffset = reader.position();
          Pair<Long, Long> pair = new Pair<>(startOffset, endOffset);
          deviceChunkGroupMetadataOffsets.putIfAbsent(footer.getDeviceID(), new ArrayList<>());
          List<Pair<Long, Long>> metadatas = deviceChunkGroupMetadataOffsets
              .get(footer.getDeviceID());
          metadatas.add(pair);
          startOffset = endOffset;
          break;
        case MetaMarker.VERSION:
          reader.readVersion();
          break;
        default:
          MetaMarker.handleUnexpectedMarker(marker);
      }
    }
    /*
     *
     * for (Entry<String, TsDeviceMetadataIndex> entry:
     * metaData.getDeviceMap().entrySet()) { int chunkGroupIndex = 0;
     * TsDeviceMetadata deviceMetadata =
     * reader.readTsDeviceMetaData(entry.getValue()); List<ChunkGroupMetaData>
     * chunkGroupMetaDataList = deviceMetadata.getChunkGroupMetaDataList();
     * List<Pair<Long, Long>> offsets =
     * deviceChunkGroupMetadataOffsets.get(entry.getKey()); for (ChunkGroupMetaData
     * chunkGroupMetaData : chunkGroupMetaDataList) { Pair<Long, Long> pair =
     * offsets.get(chunkGroupIndex++);
     * Assert.assertEquals(chunkGroupMetaData.getStartOffsetOfChunkGroup(), (long)
     * pair.left);
     * Assert.assertEquals(chunkGroupMetaData.getEndOffsetOfChunkGroup(), (long)
     * pair.right); } }
     */
    reader.close();
  }

  @Test
  public void readChunksInDevice() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);

    for (String device : reader.getAllDevices()) {
      Map<String, Set<Chunk>> expectedChunksInDevice = new HashMap<>();
      for (Entry<String, List<ChunkMetadata>> entry : reader.readChunkMetadataInDevice(device)
          .entrySet()) {
        expectedChunksInDevice.putIfAbsent(entry.getKey(), new HashSet<>());
        for (ChunkMetadata chunkMetadata : entry.getValue()) {
          expectedChunksInDevice.get(entry.getKey()).add(reader.readMemChunk(chunkMetadata));
        }
      }

      Map<String, List<Chunk>> actualChunksInDevice = reader.readChunksInDevice(device);

      for (Entry<String, Set<Chunk>> entry : expectedChunksInDevice.entrySet()) {
        Set<String> expectedChunkStrings = entry.getValue().stream()
            .map(chunk -> chunk.getHeader().toString()).collect(Collectors.toSet());

        Assert.assertTrue(actualChunksInDevice.containsKey(entry.getKey()));
        Set<String> actualChunkStrings = actualChunksInDevice.get(entry.getKey()).stream()
            .map(chunk -> chunk.getHeader().toString()).collect(Collectors.toSet());

        Assert.assertEquals(expectedChunkStrings, actualChunkStrings);
      }
    }

    reader.close();
  }
}
