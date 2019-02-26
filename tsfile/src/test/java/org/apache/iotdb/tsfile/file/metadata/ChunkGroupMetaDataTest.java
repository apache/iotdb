/**
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
package org.apache.iotdb.tsfile.file.metadata;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.InvalidJsonSchemaException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.utils.TestHelper;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.RecordUtils;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ChunkGroupMetaDataTest {

  public static final String DELTA_OBJECT_UID = "delta-3312";
  final String PATH = "target/outputChunkGroup.tsfile";
  private String testDataFile;

  @Before
  public void setUp() throws WriteProcessException, IOException, InterruptedException {
    testDataFile = TsFileGeneratorForTest.outputDataFile;
    TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";

    TsFileGeneratorForTest.generateFileWithoutText(1000, 16 * 1024 * 1024, 10000);
  }

  @After
  public void tearDown() {
    File file = new File(PATH);
    if (file.exists()) {
      file.delete();
    }

    TsFileGeneratorForTest.after();
  }

  @Test
  public void testOffset() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(testDataFile);
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
  public void testWriteIntoFile() {
    // serialize metadata to a file
    ChunkGroupMetaData metaData = TestHelper.createSimpleChunkGroupMetaData();
    serialized(metaData);
    ChunkGroupMetaData readMetaData = deSerialized();
    serialized(readMetaData);
  }

  private ChunkGroupMetaData deSerialized() {
    FileInputStream fis = null;
    ChunkGroupMetaData metaData = null;
    try {
      fis = new FileInputStream(new File(PATH));
      metaData = ChunkGroupMetaData.deserializeFrom(fis);
      return metaData;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return metaData;
  }

  private void serialized(ChunkGroupMetaData metaData) {
    File file = new File(PATH);
    if (file.exists()) {
      file.delete();
    }
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(file);
      metaData.serializeTo(fos);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

}
