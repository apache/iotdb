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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.series;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.page.TimePageWriter;
import org.apache.tsfile.write.page.ValuePageWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createChunkWriter;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeAlignedChunk;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeAlignedPoint;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeNullPoint;

public abstract class AbstractAlignedSeriesScanTest {

  protected static final String TEST_DATABASE = "root.sg_pd";
  protected static final IDeviceID TEST_DEVICE =
      IDeviceID.Factory.DEFAULT_FACTORY.create(TEST_DATABASE + ".d1_aligned");

  /**
   * The data distribution is as follows.
   *
   * <pre>
   *  time    root.sg_pd.d1_aligned(s1, s2, s3)
   *      ┌──────────┐
   *   0  │ f1-c1-p1 │ (s1, s2, s3 align on time column)
   *      ╞══════════╡
   *  10  │ f2-c1-p1 │ (s1, s2, s3 non-align on time column)
   *      ╞══════════╡
   *  20  │ f3-c1-p1 │ (s1, s2, s3 align on time column)
   *      │==========│
   *  30  │ f3-c2-p1 │ (s1, s2, s3 non-align on time column)
   *      ╞══════════╡
   *  40  │ f4-c1-p1 │ (s1, s2, s3 align on time column)
   *      │----------│
   *  50  │ f4-c1-p2 │ (s1, s2, s3 non-align on time column)
   *      │----------│
   *  60  │ f4-c1-p3 │ (s1, s2, s3 align on time column)
   *      │==========│──────────┐
   *  70  │ f4-c2-p1 │ f5-c1-p1 │
   *      └──────────│----------│
   *  80             │ f5-c1-p2 │
   *                 └──────────┘
   * </pre>
   */
  protected static final List<TsFileResource> seqResources = new ArrayList<>();

  protected static final List<TsFileResource> unSeqResources = new ArrayList<>();

  @BeforeClass
  public static void setUp() throws IOException, WriteProcessException, IllegalPathException {
    List<PartialPath> writtenPaths =
        Arrays.asList(
            new AlignedPath(TEST_DEVICE.toString(), "s1"),
            new AlignedPath(TEST_DEVICE.toString(), "s2"),
            new AlignedPath(TEST_DEVICE.toString(), "s3"));
    List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32);
    List<TSEncoding> encodings =
        Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN);
    List<CompressionType> compressionTypes =
        Arrays.asList(
            CompressionType.UNCOMPRESSED,
            CompressionType.UNCOMPRESSED,
            CompressionType.UNCOMPRESSED);

    // prepare file 1
    File seqFile1 = new File(TestConstant.getTestTsFilePath(TEST_DATABASE, 0, 0, 1));
    TsFileResource seqFileResource1 = new TsFileResource(seqFile1);
    if (!seqFile1.getParentFile().exists()) {
      Assert.assertTrue(seqFile1.getParentFile().mkdirs());
    }
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(seqFileResource1.getTsFile())) {
      // prepare f1-c1
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      List<TimeRange> pages = new ArrayList<>();
      // prepare f1-c1-p1
      pages.add(new TimeRange(0L, 9L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      seqFileResource1.updateStartTime(TEST_DEVICE, 0);
      seqFileResource1.updateEndTime(TEST_DEVICE, 9);
      tsFileIOWriter.endFile();
    }
    seqFileResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    seqResources.add(seqFileResource1);

    // prepare file 2
    File seqFile2 = new File(TestConstant.getTestTsFilePath(TEST_DATABASE, 0, 0, 2));
    TsFileResource seqFileResource2 = new TsFileResource(seqFile2);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(seqFileResource2.getTsFile())) {
      // prepare f2-c1
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);

      // prepare f2-c1-p1
      AlignedChunkWriterImpl alignedChunkWriter =
          (AlignedChunkWriterImpl)
              createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, true).get(0);
      TimePageWriter timePageWriter = alignedChunkWriter.getTimeChunkWriter().getPageWriter();
      // write time page
      for (long timestamp = 10L; timestamp <= 19L; timestamp++) {
        timePageWriter.write(timestamp);
      }
      // seal time page
      alignedChunkWriter.getTimeChunkWriter().sealCurrentPage();

      List<ValueChunkWriter> valueChunkWriterList = alignedChunkWriter.getValueChunkWriterList();
      // write value page-1
      ValuePageWriter valuePageWriter1 = valueChunkWriterList.get(0).getPageWriter();
      for (long timestamp = 10L; timestamp <= 15L; timestamp++) {
        writeAlignedPoint(valuePageWriter1, timestamp, true);
      }
      for (long timestamp = 16L; timestamp <= 19L; timestamp++) {
        writeNullPoint(valuePageWriter1, timestamp);
      }
      // seal sub value page
      valueChunkWriterList.get(0).sealCurrentPage();

      // write value page-2
      ValuePageWriter valuePageWriter2 = valueChunkWriterList.get(1).getPageWriter();
      for (long timestamp = 10L; timestamp <= 13L; timestamp++) {
        writeNullPoint(valuePageWriter2, timestamp);
      }
      for (long timestamp = 14L; timestamp <= 19L; timestamp++) {
        writeAlignedPoint(valuePageWriter2, timestamp, true);
      }
      // seal sub value page
      valueChunkWriterList.get(1).sealCurrentPage();

      // write value page-3
      ValuePageWriter valuePageWriter3 = valueChunkWriterList.get(2).getPageWriter();
      for (long timestamp = 10L; timestamp <= 11L; timestamp++) {
        writeNullPoint(valuePageWriter3, timestamp);
      }
      for (long timestamp = 12L; timestamp <= 17L; timestamp++) {
        writeAlignedPoint(valuePageWriter3, timestamp, true);
      }
      for (long timestamp = 18L; timestamp <= 19L; timestamp++) {
        writeNullPoint(valuePageWriter3, timestamp);
      }
      // seal sub value page
      valueChunkWriterList.get(2).sealCurrentPage();

      // seal time chunk and value chunks
      alignedChunkWriter.writeToFileWriter(tsFileIOWriter);

      tsFileIOWriter.endChunkGroup();

      seqFileResource2.updateStartTime(TEST_DEVICE, 10);
      seqFileResource2.updateEndTime(TEST_DEVICE, 19);
      tsFileIOWriter.endFile();
    }
    seqFileResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    seqResources.add(seqFileResource2);

    // prepare file 3
    File seqFile3 = new File(TestConstant.getTestTsFilePath(TEST_DATABASE, 0, 0, 3));
    TsFileResource seqFileResource3 = new TsFileResource(seqFile3);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(seqFileResource3.getTsFile())) {
      // prepare f3-c1
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);

      // prepare f3-c1-p1
      List<TimeRange> pages = new ArrayList<>();
      pages.add(new TimeRange(20L, 29L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      // prepare f3-c2
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);

      // prepare f3-c2-p1
      AlignedChunkWriterImpl alignedChunkWriter =
          (AlignedChunkWriterImpl)
              createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, true).get(0);
      TimePageWriter timePageWriter = alignedChunkWriter.getTimeChunkWriter().getPageWriter();
      // write time page
      for (long timestamp = 30L; timestamp <= 39L; timestamp++) {
        timePageWriter.write(timestamp);
      }
      // seal time page
      alignedChunkWriter.getTimeChunkWriter().sealCurrentPage();

      List<ValueChunkWriter> valueChunkWriterList = alignedChunkWriter.getValueChunkWriterList();
      // write value page-1
      ValuePageWriter valuePageWriter1 = valueChunkWriterList.get(0).getPageWriter();
      for (long timestamp = 30L; timestamp <= 35L; timestamp++) {
        writeAlignedPoint(valuePageWriter1, timestamp, true);
      }
      for (long timestamp = 36L; timestamp <= 39L; timestamp++) {
        writeNullPoint(valuePageWriter1, timestamp);
      }
      // seal sub value page
      valueChunkWriterList.get(0).sealCurrentPage();

      // write value page-2
      ValuePageWriter valuePageWriter2 = valueChunkWriterList.get(1).getPageWriter();
      for (long timestamp = 30L; timestamp <= 33L; timestamp++) {
        writeNullPoint(valuePageWriter2, timestamp);
      }
      for (long timestamp = 34L; timestamp <= 39L; timestamp++) {
        writeAlignedPoint(valuePageWriter2, timestamp, true);
      }
      // seal sub value page
      valueChunkWriterList.get(1).sealCurrentPage();

      // write value page-3
      ValuePageWriter valuePageWriter3 = valueChunkWriterList.get(2).getPageWriter();
      for (long timestamp = 30L; timestamp <= 31L; timestamp++) {
        writeNullPoint(valuePageWriter3, timestamp);
      }
      for (long timestamp = 32L; timestamp <= 37L; timestamp++) {
        writeAlignedPoint(valuePageWriter3, timestamp, true);
      }
      for (long timestamp = 38L; timestamp <= 39L; timestamp++) {
        writeNullPoint(valuePageWriter3, timestamp);
      }
      // seal sub value page
      valueChunkWriterList.get(2).sealCurrentPage();

      // seal time chunk and value chunks
      alignedChunkWriter.writeToFileWriter(tsFileIOWriter);

      tsFileIOWriter.endChunkGroup();

      seqFileResource3.updateStartTime(TEST_DEVICE, 20);
      seqFileResource3.updateEndTime(TEST_DEVICE, 39);
      tsFileIOWriter.endFile();
    }
    seqFileResource3.setStatusForTest(TsFileResourceStatus.NORMAL);
    seqResources.add(seqFileResource3);

    // prepare file 4
    File seqFile4 = new File(TestConstant.getTestTsFilePath(TEST_DATABASE, 0, 0, 4));
    TsFileResource seqFileResource4 = new TsFileResource(seqFile4);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(seqFileResource4.getTsFile())) {
      // prepare f4-c1
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);

      // prepare f4-c1-p1
      List<TimeRange> pages = new ArrayList<>();
      pages.add(new TimeRange(40L, 49L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }

      // prepare f4-c1-p2
      AlignedChunkWriterImpl alignedChunkWriter =
          (AlignedChunkWriterImpl)
              createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, true).get(0);
      TimePageWriter timePageWriter = alignedChunkWriter.getTimeChunkWriter().getPageWriter();
      // write time page
      for (long timestamp = 50L; timestamp <= 59L; timestamp++) {
        timePageWriter.write(timestamp);
      }
      // seal time page
      alignedChunkWriter.getTimeChunkWriter().sealCurrentPage();

      List<ValueChunkWriter> valueChunkWriterList = alignedChunkWriter.getValueChunkWriterList();
      // write value page-1
      ValuePageWriter valuePageWriter1 = valueChunkWriterList.get(0).getPageWriter();
      for (long timestamp = 50L; timestamp <= 55L; timestamp++) {
        writeAlignedPoint(valuePageWriter1, timestamp, true);
      }
      for (long timestamp = 56L; timestamp <= 59L; timestamp++) {
        writeNullPoint(valuePageWriter1, timestamp);
      }
      // seal sub value page
      valueChunkWriterList.get(0).sealCurrentPage();

      // write value page-2
      ValuePageWriter valuePageWriter2 = valueChunkWriterList.get(1).getPageWriter();
      for (long timestamp = 50L; timestamp <= 53L; timestamp++) {
        writeNullPoint(valuePageWriter2, timestamp);
      }
      for (long timestamp = 54L; timestamp <= 59L; timestamp++) {
        writeAlignedPoint(valuePageWriter2, timestamp, true);
      }
      // seal sub value page
      valueChunkWriterList.get(1).sealCurrentPage();

      // write value page-3
      ValuePageWriter valuePageWriter3 = valueChunkWriterList.get(2).getPageWriter();
      for (long timestamp = 50L; timestamp <= 51L; timestamp++) {
        writeNullPoint(valuePageWriter3, timestamp);
      }
      for (long timestamp = 52L; timestamp <= 57L; timestamp++) {
        writeAlignedPoint(valuePageWriter3, timestamp, true);
      }
      for (long timestamp = 58L; timestamp <= 59L; timestamp++) {
        writeNullPoint(valuePageWriter3, timestamp);
      }
      // seal sub value page
      valueChunkWriterList.get(2).sealCurrentPage();

      // seal time chunk and value chunks
      alignedChunkWriter.writeToFileWriter(tsFileIOWriter);

      // prepare f4-c1-p3
      pages.clear();
      pages.add(new TimeRange(60L, 69L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }

      tsFileIOWriter.endChunkGroup();

      // prepare f4-c2
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      pages.clear();
      // prepare f4-c2-p1
      pages.add(new TimeRange(70L, 79L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      seqFileResource4.updateStartTime(TEST_DEVICE, 40);
      seqFileResource4.updateEndTime(TEST_DEVICE, 79);
      tsFileIOWriter.endFile();
    }
    seqFileResource4.setStatusForTest(TsFileResourceStatus.NORMAL);
    seqResources.add(seqFileResource4);

    // prepare file 5
    File unseqFile5 = new File(TestConstant.getTestTsFilePath(TEST_DATABASE, 0, 0, 5));
    TsFileResource unseqFileResource5 = new TsFileResource(unseqFile5);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(unseqFileResource5.getTsFile())) {
      // prepare f5-c1
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      List<TimeRange> pages = new ArrayList<>();
      // prepare f5-c1-p1
      pages.add(new TimeRange(70L, 79L));
      // prepare f5-c1-p2
      pages.add(new TimeRange(80L, 89L));
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) chunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      unseqFileResource5.updateStartTime(TEST_DEVICE, 70);
      unseqFileResource5.updateEndTime(TEST_DEVICE, 89);
      tsFileIOWriter.endFile();
    }
    unseqFileResource5.setStatusForTest(TsFileResourceStatus.NORMAL);
    unSeqResources.add(unseqFileResource5);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    for (TsFileResource tsFileResource : seqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    for (TsFileResource tsFileResource : unSeqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    File[] files = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".tsfile");
    for (File file : files) {
      file.delete();
    }
    File[] resourceFiles =
        FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".resource");
    for (File resourceFile : resourceFiles) {
      resourceFile.delete();
    }
    seqResources.clear();
    unSeqResources.clear();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
  }
}
