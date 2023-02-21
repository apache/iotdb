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

package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.mpp.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;
import org.apache.iotdb.tsfile.write.page.TimePageWriter;
import org.apache.iotdb.tsfile.write.page.ValuePageWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.utils.TsFileGeneratorUtils.createChunkWriter;
import static org.apache.iotdb.db.engine.compaction.utils.TsFileGeneratorUtils.writeAlignedChunk;
import static org.apache.iotdb.db.engine.compaction.utils.TsFileGeneratorUtils.writeAlignedPoint;
import static org.apache.iotdb.db.engine.compaction.utils.TsFileGeneratorUtils.writeNullPoint;

public class AlignedSeriesScanLimitOffsetPushDownTest {

  private static final String TEST_DATABASE = "root.sg_pd";
  private static final String TEST_DEVICE = TEST_DATABASE + ".d1_aligned";

  /**
   * The data distribution is as follows:
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
  private static final List<TsFileResource> seqResources = new ArrayList<>();

  private static final List<TsFileResource> unSeqResources = new ArrayList<>();

  @BeforeClass
  public static void setUp() throws IOException, WriteProcessException, IllegalPathException {
    List<PartialPath> writtenPaths =
        Arrays.asList(
            new AlignedPath(TEST_DEVICE, "s1"),
            new AlignedPath(TEST_DEVICE, "s2"),
            new AlignedPath(TEST_DEVICE, "s3"));
    List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32);

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
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      seqFileResource1.updateStartTime(TEST_DEVICE, 0);
      seqFileResource1.updateEndTime(TEST_DEVICE, 9);
      tsFileIOWriter.endFile();
    }
    seqFileResource1.setStatus(TsFileResourceStatus.CLOSED);
    seqResources.add(seqFileResource1);

    // prepare file 2
    File seqFile2 = new File(TestConstant.getTestTsFilePath(TEST_DATABASE, 0, 0, 2));
    TsFileResource seqFileResource2 = new TsFileResource(seqFile2);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(seqFileResource2.getTsFile())) {
      // prepare f2-c1
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);

      // prepare f2-c1-p1
      AlignedChunkWriterImpl alignedChunkWriter =
          (AlignedChunkWriterImpl) createChunkWriter(writtenPaths, dataTypes, true).get(0);
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
    seqFileResource2.setStatus(TsFileResourceStatus.CLOSED);
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
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      // prepare f3-c2
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);

      // prepare f3-c2-p1
      AlignedChunkWriterImpl alignedChunkWriter =
          (AlignedChunkWriterImpl) createChunkWriter(writtenPaths, dataTypes, true).get(0);
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
    seqFileResource3.setStatus(TsFileResourceStatus.CLOSED);
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
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
      }

      // prepare f4-c1-p2
      AlignedChunkWriterImpl alignedChunkWriter =
          (AlignedChunkWriterImpl) createChunkWriter(writtenPaths, dataTypes, true).get(0);
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
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
      }

      tsFileIOWriter.endChunkGroup();

      // prepare f4-c2
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      pages.clear();
      // prepare f4-c2-p1
      pages.add(new TimeRange(70L, 79L));
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      seqFileResource4.updateStartTime(TEST_DEVICE, 40);
      seqFileResource4.updateEndTime(TEST_DEVICE, 79);
      tsFileIOWriter.endFile();
    }
    seqFileResource4.setStatus(TsFileResourceStatus.CLOSED);
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
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, true)) {
        writeAlignedChunk((AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      unseqFileResource5.updateStartTime(TEST_DEVICE, 70);
      unseqFileResource5.updateEndTime(TEST_DEVICE, 89);
      tsFileIOWriter.endFile();
    }
    unseqFileResource5.setStatus(TsFileResourceStatus.CLOSED);
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
    EnvironmentUtils.cleanAllDir();
  }

  private AlignedSeriesScanUtil getAlignedSeriesScanUtil(long limit, long offset)
      throws IllegalPathException {
    AlignedPath scanPath =
        new AlignedPath(
            TEST_DEVICE,
            Arrays.asList("s1", "s2"),
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32),
                new MeasurementSchema("s2", TSDataType.INT32)));

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(new HashSet<>(scanPath.getMeasurementList()));
    scanOptionsBuilder.withLimit(limit);
    scanOptionsBuilder.withOffset(offset);
    AlignedSeriesScanUtil seriesScanUtil =
        new AlignedSeriesScanUtil(
            scanPath,
            Ordering.ASC,
            scanOptionsBuilder.build(),
            EnvironmentUtils.TEST_QUERY_FI_CONTEXT);
    seriesScanUtil.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
    return seriesScanUtil;
  }

  @Test
  public void testSkipFile() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 10);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 10;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testCannotSkipFile() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 20);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 20;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }

  @Test
  public void testSkipChunk() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 30);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 30;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }

  @Test
  public void testCannotSkipChunk() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 40);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 40;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }

  @Test
  public void testSkipPage() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 50);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 50;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }

  @Test
  public void testCannotSkipPage() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(5, 60);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 60;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }

  @Test
  public void testSkipPoint() throws IllegalPathException, IOException {
    AlignedSeriesScanUtil seriesScanUtil = getAlignedSeriesScanUtil(10, 75);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 75;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertTrue(seriesScanUtil.hasNextPage());

    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    expectedTime = 80;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }
  }
}
