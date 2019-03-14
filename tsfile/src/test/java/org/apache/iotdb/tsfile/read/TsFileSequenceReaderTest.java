/**
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.writer.IncompleteFileTestUtil;
import org.apache.iotdb.tsfile.write.writer.NativeRestorableIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TsFileSequenceReaderTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private TsFileSequenceReader fileReader;
  private int rowCount = 1000;
  private ReadOnlyTsFile tsFile;

  @Before
  public void before() throws InterruptedException, WriteProcessException, IOException {
    TsFileGeneratorForTest.generateFile(rowCount, 16 * 1024 * 1024, 10000);
    fileReader = new TsFileSequenceReader(FILE_PATH);
    tsFile = new ReadOnlyTsFile(fileReader);
  }

  @After
  public void after() throws IOException {
    tsFile.close();
    TsFileGeneratorForTest.after();
  }

  @Test
  public void testReadTsFileSequently() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);
    TsFileMetaData metaData = reader.readFileMetadata();
    List<Pair<Long, Long>> offsetList = new ArrayList<>();
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
          reader.readChunkGroupFooter();
          long endOffset = reader.position();
          offsetList.add(new Pair<>(startOffset, endOffset));
          startOffset = endOffset;
          break;
        default:
          MetaMarker.handleUnexpectedMarker(marker);
      }
    }
    int offsetListIndex = 0;
    List<TsDeviceMetadataIndex> deviceMetadataIndexList = metaData.getDeviceMap().values().stream()
        .sorted((x, y) -> (int) (x.getOffset() - y.getOffset())).collect(Collectors.toList());
    for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
      TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
      List<ChunkGroupMetaData> chunkGroupMetaDataList = deviceMetadata.getChunkGroupMetaDataList();
      for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
        Pair<Long, Long> pair = offsetList.get(offsetListIndex++);
        Assert.assertEquals(chunkGroupMetaData.getStartOffsetOfChunkGroup(), (long) pair.left);
        Assert.assertEquals(chunkGroupMetaData.getEndOffsetOfChunkGroup(), (long) pair.right);
      }
    }
    reader.close();
  }

  @Test
  public void testToReadDamagedFileAndRepair() throws IOException {
    File file = new File(FILE_PATH);

    IncompleteFileTestUtil.writeFileWithOneIncompleteChunkHeader(file);

    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH, true, true);
    String tailMagic = reader.readTailMagic();
    reader.close();

    // Check if the file was repaired
    assertEquals(TSFileConfig.MAGIC_STRING, tailMagic);
    assertTrue(file.delete());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToReadDamagedFileNoRepair() throws IOException {
    File file = new File(FILE_PATH);

    IncompleteFileTestUtil.writeFileWithOneIncompleteChunkHeader(file);

    // This should throw an Illegal Argument Exception
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH, true, false);
  }
}