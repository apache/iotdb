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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.FloatStatistics;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileSequenceReaderSelfCheckTest {
  private static final String FILE_NAME =
      TestConstant.BASE_OUTPUT_PATH.concat(System.currentTimeMillis() + "-1-0-0.tsfile");
  File file = new File(FILE_NAME);

  @Before
  public void setUp() throws IOException {
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
  }

  @After
  public void tearDown() {
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
  }

  // File with wrong head magic string
  @Test
  public void testCheckWithBadHeadMagic() throws Exception {
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("TEST_STRING");
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long truncateSize = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(truncateSize, TsFileCheckStatus.INCOMPATIBLE_FILE);
      Assert.assertEquals(newSchema.size(), 0);
      Assert.assertEquals(chunkGroupMetadataList.size(), 0);
    }
  }

  // test file with magic string and version number
  @Test
  public void testCheckWithOnlyHeadMagic() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    // we have to flush using inner API.
    writer.close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(checkStatus, TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES);
      Assert.assertEquals(newSchema.size(), 0);
      Assert.assertEquals(chunkGroupMetadataList.size(), 0);
    }
  }

  // test file with magic string, version number and a marker of ChunkHeader
  @Test
  public void testCheckWithOnlyFirstMask() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    // we have to flush using inner API.
    writer.getIOWriter().getIOWriterOut().write(new byte[] {MetaMarker.CHUNK_HEADER});
    writer.getIOWriter().close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(checkStatus, TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES);
      Assert.assertEquals(newSchema.size(), 0);
      Assert.assertEquals(chunkGroupMetadataList.size(), 0);
    }
  }

  // Test file with magic string, version number, marker of ChunkHeader, incomplete chunk header
  @Test
  public void testCheckWithIncompleteChunkHeader() throws Exception {
    TsFileGeneratorForTest.writeFileWithOneIncompleteChunkHeader(file);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(checkStatus, TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES);
      Assert.assertEquals(newSchema.size(), 0);
      Assert.assertEquals(chunkGroupMetadataList.size(), 0);
    }
  }

  // Test file with magic string, version number, marker of ChunkHeader, one chunkHeader
  // And do not load the last chunk group metadata
  @Test
  public void testCheckWithOnlyOneChunkHeader() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.getIOWriter().startChunkGroup("root.sg1.d1");
    writer
        .getIOWriter()
        .startFlushChunk(
            new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.PLAIN),
            CompressionType.SNAPPY,
            TSDataType.FLOAT,
            TSEncoding.PLAIN,
            new FloatStatistics(),
            100,
            10);
    writer.getIOWriter().close();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, false);
      Assert.assertEquals(checkStatus, TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES);
      Assert.assertEquals(newSchema.size(), 0);
      Assert.assertEquals(chunkGroupMetadataList.size(), 0);
    }
  }

  // Test file with magic string, version number, marker of ChunkHeader, one chunkHeader
  // And load the last chunk group metadata
  @Test
  public void testCheckWithOnlyOneChunkHeaderAndLoadLastChunkGroupMetadata() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.getIOWriter().startChunkGroup("root.sg1.d1");
    writer
        .getIOWriter()
        .startFlushChunk(
            new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.PLAIN),
            CompressionType.SNAPPY,
            TSDataType.FLOAT,
            TSEncoding.PLAIN,
            new FloatStatistics(),
            100,
            10);
    writer.getIOWriter().close();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(checkStatus, TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES);
      Assert.assertEquals(newSchema.size(), 0);
      Assert.assertEquals(chunkGroupMetadataList.size(), 0);
    }
  }

  /*
   * test check with tsfile with one ChunkGroup, and not marker behind this chunk group
   */
  @Test
  public void testCheckWithOnlyOneChunkGroupAndNoMarker() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    writer.getIOWriter().close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, false);
      Assert.assertEquals(TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES, checkStatus);
      Assert.assertEquals(newSchema.size(), 0);
      Assert.assertEquals(chunkGroupMetadataList.size(), 0);
    }
  }

  /*
   * test check with tsfile with one ChunkGroup, and not marker behind this chunk group. And load the last chunk group metadata.
   */
  @Test
  public void testCheckWithOnlyOneChunkGroupAndNoMarkerAndLoadLastChunkGroupMetadata()
      throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    writer.getIOWriter().close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES, checkStatus);
      Assert.assertEquals(newSchema.size(), 2);
      Assert.assertEquals(chunkGroupMetadataList.size(), 1);
      ChunkGroupMetadata chunkGroupMetadata = chunkGroupMetadataList.get(0);
      Assert.assertEquals(chunkGroupMetadata.getChunkMetadataList().size(), 2);
    }
  }

  /** Test check with one complete chunk group and marker */
  @Test
  public void testCheckWithOneCompleteChunkGroupAndMarker() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos = writer.getIOWriter().getPos();
    writer.getIOWriter().writeChunkGroupMarkerForTest();
    writer.getIOWriter().close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, false);
      Assert.assertEquals(pos, checkStatus);
      Assert.assertEquals(newSchema.size(), 2);
      Assert.assertEquals(chunkGroupMetadataList.size(), 1);
      ChunkGroupMetadata chunkGroupMetadata = chunkGroupMetadataList.get(0);
      Assert.assertEquals(chunkGroupMetadata.getChunkMetadataList().size(), 2);
    }
  }

  /** Test check with one complete chunk group and marker, and load the last chunk group */
  @Test
  public void testCheckWithOneCompleteChunkGroupAndMarkerAndLoadLastChunkGroup() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos = writer.getIOWriter().getPos();
    writer.getIOWriter().writeChunkGroupMarkerForTest();
    writer.getIOWriter().close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(pos, checkStatus);
      Assert.assertEquals(newSchema.size(), 2);
      Assert.assertEquals(chunkGroupMetadataList.size(), 1);
      ChunkGroupMetadata chunkGroupMetadata = chunkGroupMetadataList.get(0);
      Assert.assertEquals(chunkGroupMetadata.getChunkMetadataList().size(), 2);
    }
  }

  /*
   * Test file with magic string, version number, marker of ChunkHeader, one complete chunk group, one chunk group with the last chunk uncompleted. And do not load the last chunk metadata.
   */
  @Test
  public void testCheckWithOneCompleteChunkGroupAndOneUncompletedChunkGroup() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos1 = writer.getIOWriter().getPos();
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos2 = writer.getIOWriter().getPos();
    // let's delete one byte. the version is broken
    writer.getIOWriter().getIOWriterOut().truncate(pos2 - 1);
    writer.getIOWriter().close();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, false);
      Assert.assertEquals(checkStatus, pos1);
      Assert.assertEquals(newSchema.size(), 2);
      Assert.assertEquals(chunkGroupMetadataList.size(), 1);
      ChunkGroupMetadata chunkGroupMetadata = chunkGroupMetadataList.get(0);
      Assert.assertEquals(chunkGroupMetadata.getChunkMetadataList().size(), 2);
    }
  }

  /*
   * Test file with magic string, version number, marker of ChunkHeader, one complete chunk group, one chunk group with the last chunk uncompleted. And load the last chunk metadata.
   */
  @Test
  public void
      testCheckWithOneCompleteChunkGroupAndOneUncompletedChunkGroupAndLoadLastChunkGroupMetadata()
          throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos1 = writer.getIOWriter().getPos();
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos2 = writer.getIOWriter().getPos();
    // let's delete one byte. the version is broken
    writer.getIOWriter().getIOWriterOut().truncate(pos2 - 1);
    writer.getIOWriter().close();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(checkStatus, pos1);
      Assert.assertEquals(newSchema.size(), 2);
      Assert.assertEquals(chunkGroupMetadataList.size(), 2);
      ChunkGroupMetadata firstChunkGroupMetadata = chunkGroupMetadataList.get(0);
      ChunkGroupMetadata secondChunkGroupMetadata = chunkGroupMetadataList.get(1);
      // the first chunk group contains the chunk metadata for two chunks
      Assert.assertEquals(2, firstChunkGroupMetadata.getChunkMetadataList().size());
      // the second chunk group contains only chunk metadata for the first chunk
      Assert.assertEquals(1, secondChunkGroupMetadata.getChunkMetadataList().size());
    }
  }
  /*
   * Test check with two chunk group and marker
   */
  @Test
  public void testCheckWithTwoChunkGroupAndMarker() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(
        new TSRecord(1, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos1 = writer.getIOWriter().getPos();
    writer.getIOWriter().writeChunkGroupMarkerForTest();
    writer.getIOWriter().close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, false);
      Assert.assertEquals(checkStatus, pos1);
      Assert.assertEquals(newSchema.size(), 4);
      Assert.assertEquals(chunkGroupMetadataList.size(), 2);
      ChunkGroupMetadata firstChunkGroupMetadata = chunkGroupMetadataList.get(0);
      ChunkGroupMetadata secondChunkGroupMetadata = chunkGroupMetadataList.get(1);
      // the first chunk group contains the chunk metadata for two chunks
      Assert.assertEquals(2, firstChunkGroupMetadata.getChunkMetadataList().size());
      // the second chunk group contains only chunk metadata for the first chunk
      Assert.assertEquals(2, secondChunkGroupMetadata.getChunkMetadataList().size());
    }
  }

  /*
   * Test check with two chunk group and marker, and load last chunk group
   */
  @Test
  public void testCheckWithTwoChunkGroupAndMarkerAndLoadLastChunkGroup() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(
        new TSRecord(1, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos1 = writer.getIOWriter().getPos();
    writer.getIOWriter().writeChunkGroupMarkerForTest();
    writer.getIOWriter().close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(checkStatus, pos1);
      Assert.assertEquals(newSchema.size(), 4);
      Assert.assertEquals(chunkGroupMetadataList.size(), 2);
      ChunkGroupMetadata firstChunkGroupMetadata = chunkGroupMetadataList.get(0);
      ChunkGroupMetadata secondChunkGroupMetadata = chunkGroupMetadataList.get(1);
      // the first chunk group contains the chunk metadata for two chunks
      Assert.assertEquals(2, firstChunkGroupMetadata.getChunkMetadataList().size());
      // the second chunk group contains only chunk metadata for the first chunk
      Assert.assertEquals(2, secondChunkGroupMetadata.getChunkMetadataList().size());
    }
  }

  /*
   * Test check with two chunk group, and no marker behind the last chunk group
   */
  @Test
  public void testCheckWithTwoChunkGroupAndNotMarkerBehind() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(
        new TSRecord(1, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos1 = writer.getIOWriter().getPos();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, false);
      Assert.assertEquals(checkStatus, pos1);
      Assert.assertEquals(newSchema.size(), 4);
      Assert.assertEquals(chunkGroupMetadataList.size(), 2);
      ChunkGroupMetadata firstChunkGroupMetadata = chunkGroupMetadataList.get(0);
      ChunkGroupMetadata secondChunkGroupMetadata = chunkGroupMetadataList.get(1);
      // the first chunk group contains the chunk metadata for two chunks
      Assert.assertEquals(2, firstChunkGroupMetadata.getChunkMetadataList().size());
      // the second chunk group contains only chunk metadata for the first chunk
      Assert.assertEquals(2, secondChunkGroupMetadata.getChunkMetadataList().size());
    }
  }

  /**
   * Test check with two chunk group, and no marker behind the last chunk group, and load the last
   * chunk group metadata
   */
  @Test
  public void testCheckWithTwoChunkGroupAndNotMarkerBehindAndLoadLastChunkGroupMetadata()
      throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(
        new TSRecord(1, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos1 = writer.getIOWriter().getPos();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(checkStatus, pos1);
      Assert.assertEquals(newSchema.size(), 4);
      Assert.assertEquals(chunkGroupMetadataList.size(), 2);
      ChunkGroupMetadata firstChunkGroupMetadata = chunkGroupMetadataList.get(0);
      ChunkGroupMetadata secondChunkGroupMetadata = chunkGroupMetadataList.get(1);
      // the first chunk group contains the chunk metadata for two chunks
      Assert.assertEquals(2, firstChunkGroupMetadata.getChunkMetadataList().size());
      // the second chunk group contains only chunk metadata for the first chunk
      Assert.assertEquals(2, secondChunkGroupMetadata.getChunkMetadataList().size());
    }
  }

  /**
   * Test check with two chunk group, and a marker of FileMetadata
   *
   * @throws Exception
   */
  @Test
  public void testCheckWithTwoChunkGroupAndFileMetadataMarker() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(
        new TSRecord(1, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos1 = writer.getIOWriter().getPos();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, false);
      Assert.assertEquals(checkStatus, pos1);
      Assert.assertEquals(newSchema.size(), 4);
      Assert.assertEquals(chunkGroupMetadataList.size(), 2);
      ChunkGroupMetadata firstChunkGroupMetadata = chunkGroupMetadataList.get(0);
      ChunkGroupMetadata secondChunkGroupMetadata = chunkGroupMetadataList.get(1);
      // the first chunk group contains the chunk metadata for two chunks
      Assert.assertEquals(2, firstChunkGroupMetadata.getChunkMetadataList().size());
      // the second chunk group contains only chunk metadata for the first chunk
      Assert.assertEquals(2, secondChunkGroupMetadata.getChunkMetadataList().size());
    }
  }

  /**
   * Test check with two chunk group, and a marker of FileMetadata, and load the last chunk group
   * metadata
   *
   * @throws Exception
   */
  @Test
  public void testCheckWithTwoChunkGroupAndFileMetadataMarkerAndLoadLastChunkGroup()
      throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(
        new TSRecord(1, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos1 = writer.getIOWriter().getPos();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(checkStatus, pos1);
      Assert.assertEquals(newSchema.size(), 4);
      Assert.assertEquals(chunkGroupMetadataList.size(), 2);
      ChunkGroupMetadata firstChunkGroupMetadata = chunkGroupMetadataList.get(0);
      ChunkGroupMetadata secondChunkGroupMetadata = chunkGroupMetadataList.get(1);
      // the first chunk group contains the chunk metadata for two chunks
      Assert.assertEquals(2, firstChunkGroupMetadata.getChunkMetadataList().size());
      // the second chunk group contains only chunk metadata for the first chunk
      Assert.assertEquals(2, secondChunkGroupMetadata.getChunkMetadataList().size());
    }
  }

  /**
   * Test check with a complete file
   *
   * @throws Exception
   */
  @Test
  public void testCheckWithCompleteFile() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos = writer.getIOWriter().getPos();
    writer.close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, false);
      Assert.assertEquals(checkStatus, pos);
      Assert.assertEquals(newSchema.size(), 2);
      Assert.assertEquals(chunkGroupMetadataList.size(), 1);
      ChunkGroupMetadata firstChunkGroupMetadata = chunkGroupMetadataList.get(0);
      Assert.assertEquals(2, firstChunkGroupMetadata.getChunkMetadataList().size());
    }
  }

  /**
   * Test check with a complete file, and load last chunk group metadata
   *
   * @throws Exception
   */
  @Test
  public void testCheckWithCompleteFileAndLoadLastChunkGroupMetadata() throws Exception {
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos = writer.getIOWriter().getPos();
    writer.close();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      Map<Path, MeasurementSchema> newSchema = new HashMap<>();
      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      long checkStatus = reader.selfCheck(newSchema, chunkGroupMetadataList, false, true);
      Assert.assertEquals(checkStatus, pos);
      Assert.assertEquals(newSchema.size(), 2);
      Assert.assertEquals(chunkGroupMetadataList.size(), 1);
      ChunkGroupMetadata firstChunkGroupMetadata = chunkGroupMetadataList.get(0);
      Assert.assertEquals(2, firstChunkGroupMetadata.getChunkMetadataList().size());
    }
  }
}
