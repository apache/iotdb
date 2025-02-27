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

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.validate.TsFileValidator;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class TsFileValidationCorrectnessTests {
  final String dir = TestConstant.OUTPUT_DATA_DIR + "test-validation";

  @Before
  public void setUp() throws IOException {
    FileUtils.forceMkdir(new File(dir));
  }

  @After
  public void tearDown() throws IOException {
    File[] files = new File(dir).listFiles();
    if (files != null) {
      for (File f : files) {
        FileUtils.delete(f);
      }
    }

    FileUtils.forceDelete(new File(dir));
  }

  // 1. empty tsfile
  @Test
  public void testTsFileHasNoData() throws IOException {
    TsFileResource tsFileResource =
        new TsFileResource(new File(dir + File.separator + "test1.tsfile"));
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(tsFileResource)) {
      writer.endFile();
    }
    boolean success = TsFileValidator.getInstance().validateTsFile(tsFileResource);
    Assert.assertFalse(success);
  }

  @Test
  public void testAlignedTsFileHasOnePageData() throws IOException {
    String path = dir + File.separator + "test2.tsfile";
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    TsFileGeneratorUtils.generateSingleAlignedSeriesFile(
        "d1",
        Collections.singletonList("s1"),
        new TimeRange[] {new TimeRange(1, 100)},
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        path);
    tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 1);
    tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 100);
    tsFileResource.serialize();
    boolean success = TsFileValidator.getInstance().validateTsFile(tsFileResource);
    Assert.assertTrue(success);
  }

  @Test
  public void testAlignedTsFileHasManyPage() throws IOException {
    String path = dir + File.separator + "test3.tsfile";
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    TsFileGeneratorUtils.generateSingleAlignedSeriesFile(
        "d1",
        Collections.singletonList("s1"),
        new TimeRange[] {new TimeRange(1, 100), new TimeRange(22, 110)},
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        path);
    tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 1);
    tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 110);
    tsFileResource.serialize();
    boolean success = TsFileValidator.getInstance().validateTsFile(tsFileResource);
    Assert.assertTrue(success);
  }

  @Test
  public void testAlignedTimestampRepeatedOrNotIncremented() throws IOException {
    String path = dir + File.separator + "test4.tsfile";
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(tsFileResource)) {
      writer.startChunkGroup("d1");
      VectorMeasurementSchema vectorMeasurementSchema =
          new VectorMeasurementSchema(
              "d1", new String[] {"s1"}, new TSDataType[] {TSDataType.INT32});
      AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(vectorMeasurementSchema);
      chunkWriter.getTimeChunkWriter().write(1);
      chunkWriter.getTimeChunkWriter().write(2);
      chunkWriter.getTimeChunkWriter().write(2);
      chunkWriter.getTimeChunkWriter().write(3);
      chunkWriter.sealCurrentPage();
      chunkWriter.writeToFileWriter(writer.getFileWriter());
      writer.endChunkGroup();
      writer.endFile();
    }
    boolean success = TsFileValidator.getInstance().validateTsFile(tsFileResource);
    Assert.assertFalse(success);
  }

  @Test
  public void testAlignedTimestampHasOverlapBetweenPages() throws IOException {
    String path = dir + File.separator + "test5.tsfile";
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(tsFileResource)) {
      writer.startChunkGroup("d1");
      VectorMeasurementSchema vectorMeasurementSchema =
          new VectorMeasurementSchema(
              "d1", new String[] {"s1"}, new TSDataType[] {TSDataType.INT32});
      AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(vectorMeasurementSchema);
      chunkWriter.getTimeChunkWriter().write(1);
      chunkWriter.getTimeChunkWriter().write(2);
      chunkWriter.getTimeChunkWriter().write(3);
      chunkWriter.sealCurrentPage();
      chunkWriter.getTimeChunkWriter().write(3);
      chunkWriter.getTimeChunkWriter().write(4);
      chunkWriter.getTimeChunkWriter().write(5);
      chunkWriter.sealCurrentPage();
      chunkWriter.writeToFileWriter(writer.getFileWriter());
      writer.endChunkGroup();
      writer.endFile();
    }
    boolean success = TsFileValidator.getInstance().validateTsFile(tsFileResource);
    Assert.assertFalse(success);
  }

  @Test
  public void testAlignedTimestampTimeChunkOffsetEqualsMetadata() throws IOException {
    String path = dir + File.separator + "test6.tsfile";
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(tsFileResource)) {
      writer.startChunkGroup("d1");
      VectorMeasurementSchema vectorMeasurementSchema =
          new VectorMeasurementSchema(
              "d1", new String[] {"s1"}, new TSDataType[] {TSDataType.INT32});
      AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(vectorMeasurementSchema);
      chunkWriter.getTimeChunkWriter().write(1);
      chunkWriter.getTimeChunkWriter().write(2);
      chunkWriter.getTimeChunkWriter().write(3);
      chunkWriter.getValueChunkWriterByIndex(0).getPageWriter().write(1, 1, false);
      chunkWriter.getValueChunkWriterByIndex(0).getPageWriter().write(2, 1, false);
      chunkWriter.getValueChunkWriterByIndex(0).getPageWriter().write(3, 1, false);
      chunkWriter.sealCurrentPage();
      chunkWriter.writeToFileWriter(writer.getFileWriter());
      writer.endChunkGroup();
      writer.endFile();
    }
    tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 1);
    tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 3);
    tsFileResource.serialize();
    boolean success = TsFileValidator.getInstance().validateTsFile(tsFileResource);
    Assert.assertTrue(success);
  }

  @Test
  public void testNonAlignedTsFileHasOnePageData() throws IOException {
    String path = dir + File.separator + "test7.tsfile";
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    TsFileGeneratorUtils.generateSingleNonAlignedSeriesFile(
        "d1",
        "s1",
        new TimeRange[] {new TimeRange(1, 100)},
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        path);
    tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 1);
    tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 100);
    tsFileResource.serialize();
    boolean success = TsFileValidator.getInstance().validateTsFile(tsFileResource);
    Assert.assertTrue(success);
  }

  @Test
  public void testNonAlignedTsFileHasManyPage() throws IOException {
    String path = dir + File.separator + "test8.tsfile";
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    TsFileGeneratorUtils.generateSingleNonAlignedSeriesFile(
        "d1",
        "s1",
        new TimeRange[] {new TimeRange(1, 100), new TimeRange(22, 110)},
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        path);
    tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 1);
    tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 110);
    tsFileResource.serialize();
    boolean success = TsFileValidator.getInstance().validateTsFile(tsFileResource);
    Assert.assertTrue(success);
  }

  @Test
  public void testNonAlignedTimestampRepeatedOrNotIncremented() throws IOException {
    String path = dir + File.separator + "test9.tsfile";
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(tsFileResource)) {
      writer.startChunkGroup("d1");
      ChunkWriterImpl chunkWriter =
          new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT32));
      chunkWriter.getPageWriter().write(1, 2);
      chunkWriter.getPageWriter().write(2, 2);
      chunkWriter.getPageWriter().write(2, 2);
      chunkWriter.getPageWriter().write(3, 2);
      chunkWriter.sealCurrentPage();
      chunkWriter.writeToFileWriter(writer.getFileWriter());
      writer.endChunkGroup();
      writer.endFile();
    }
    boolean success = TsFileValidator.getInstance().validateTsFile(tsFileResource);
    Assert.assertFalse(success);
  }

  @Test
  public void testNonAlignedTimestampHasOverlapBetweenPages() throws IOException {
    String path = dir + File.separator + "test10.tsfile";
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(tsFileResource)) {
      writer.startChunkGroup("d1");
      ChunkWriterImpl chunkWriter =
          new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT32));
      chunkWriter.getPageWriter().write(1, 2);
      chunkWriter.getPageWriter().write(2, 2);
      chunkWriter.getPageWriter().write(3, 2);
      chunkWriter.sealCurrentPage();
      chunkWriter.getPageWriter().write(3, 4);
      chunkWriter.getPageWriter().write(4, 4);
      chunkWriter.writeToFileWriter(writer.getFileWriter());
      writer.endChunkGroup();
      writer.endFile();
    }
    boolean success = TsFileValidator.getInstance().validateTsFile(tsFileResource);
    Assert.assertFalse(success);
  }

  @Test
  public void testNonAlignedTimestampTimeChunkOffsetEqualsMetadata() throws IOException {
    String path = dir + File.separator + "test11.tsfile";
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(tsFileResource)) {
      writer.startChunkGroup("d1");
      VectorMeasurementSchema vectorMeasurementSchema =
          new VectorMeasurementSchema(
              "d1", new String[] {"s1"}, new TSDataType[] {TSDataType.INT32});
      AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(vectorMeasurementSchema);
      chunkWriter.getTimeChunkWriter().write(1);
      chunkWriter.getTimeChunkWriter().write(2);
      chunkWriter.getTimeChunkWriter().write(3);
      chunkWriter.getValueChunkWriterByIndex(0).getPageWriter().write(1, 1, false);
      chunkWriter.getValueChunkWriterByIndex(0).getPageWriter().write(2, 1, false);
      chunkWriter.getValueChunkWriterByIndex(0).getPageWriter().write(3, 1, false);
      chunkWriter.sealCurrentPage();
      chunkWriter.writeToFileWriter(writer.getFileWriter());
      writer.endChunkGroup();
      writer.endFile();
    }
    tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 1);
    tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 3);
    tsFileResource.serialize();
    boolean success = TsFileValidator.getInstance().validateTsFile(tsFileResource);
    Assert.assertTrue(success);
  }

  @Test
  public void testDeletedFile() throws IOException {
    String path = dir + File.separator + "test12.tsfile";
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(tsFileResource)) {
      writer.startChunkGroup("d1");
      VectorMeasurementSchema vectorMeasurementSchema =
          new VectorMeasurementSchema(
              "d1", new String[] {"s1"}, new TSDataType[] {TSDataType.INT32});
      AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(vectorMeasurementSchema);
      chunkWriter.getTimeChunkWriter().write(1);
      chunkWriter.getTimeChunkWriter().write(2);
      chunkWriter.getTimeChunkWriter().write(3);
      chunkWriter.getValueChunkWriterByIndex(0).getPageWriter().write(1, 1, false);
      chunkWriter.getValueChunkWriterByIndex(0).getPageWriter().write(2, 1, false);
      chunkWriter.getValueChunkWriterByIndex(0).getPageWriter().write(3, 1, false);
      chunkWriter.sealCurrentPage();
      chunkWriter.writeToFileWriter(writer.getFileWriter());
      writer.endChunkGroup();
      writer.endFile();
    }
    tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 1);
    tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 3);
    tsFileResource.serialize();
    tsFileResource.remove();
    Assert.assertTrue(TsFileValidator.getInstance().validateTsFile(tsFileResource));
  }
}
