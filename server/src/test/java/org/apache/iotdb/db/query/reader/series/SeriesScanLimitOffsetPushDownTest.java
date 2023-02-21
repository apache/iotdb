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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.utils.TsFileGeneratorUtils.createChunkWriter;
import static org.apache.iotdb.db.engine.compaction.utils.TsFileGeneratorUtils.writeNonAlignedChunk;

public class SeriesScanLimitOffsetPushDownTest {

  private static final String TEST_DATABASE = "root.sg_pd";
  private static final String TEST_DEVICE = TEST_DATABASE + ".d1";
  private static final String TEST_PATH = TEST_DEVICE + ".s1";

  /**
   * The data distribution is as follows:
   *
   * <pre>
   *  time    root.sg_pd.d1.s1
   *      ┌──────────┐
   *   0  │ f1-c1-p1 │
   *      ╞══════════╡
   *  10  │ f2-c1-p1 │
   *      │==========│
   *  20  │ f2-c2-p1 │
   *      ╞══════════╡
   *  30  │ f3-c1-p1 │
   *      │----------│
   *  40  │ f3-c1-p2 │
   *      │==========│──────────┐
   *  50  │ f3-c2-p1 │ f4-c1-p1 │
   *      └──────────│----------│
   *  60             │ f4-c1-p2 │
   *                 └──────────┘
   * </pre>
   */
  private static final List<TsFileResource> seqResources = new ArrayList<>();

  private static final List<TsFileResource> unSeqResources = new ArrayList<>();

  @BeforeClass
  public static void setUp() throws IOException, WriteProcessException, IllegalPathException {
    List<PartialPath> writtenPaths = Collections.singletonList(new PartialPath(TEST_PATH));
    List<TSDataType> dataTypes = Collections.singletonList(TSDataType.INT32);

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
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
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
      List<TimeRange> pages = new ArrayList<>();
      // prepare f2-c1-p1
      pages.add(new TimeRange(10L, 19L));
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      // prepare f2-c2
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      pages.clear();
      // prepare f2-c2-p1
      pages.add(new TimeRange(20L, 29L));
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      seqFileResource2.updateStartTime(TEST_DEVICE, 10);
      seqFileResource2.updateEndTime(TEST_DEVICE, 29);
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
      List<TimeRange> pages = new ArrayList<>();
      // prepare f3-c1-p1
      pages.add(new TimeRange(30L, 39L));
      // prepare f3-c1-p2
      pages.add(new TimeRange(40L, 49L));
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      // prepare f3-c2
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      pages.clear();
      // prepare f3-c2-p1
      pages.add(new TimeRange(50L, 59L));
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      seqFileResource3.updateStartTime(TEST_DEVICE, 30);
      seqFileResource3.updateEndTime(TEST_DEVICE, 59);
      tsFileIOWriter.endFile();
    }
    seqFileResource3.setStatus(TsFileResourceStatus.CLOSED);
    seqResources.add(seqFileResource3);

    // prepare file 4
    File unseqFile4 = new File(TestConstant.getTestTsFilePath(TEST_DATABASE, 0, 0, 4));
    TsFileResource unseqFileResource4 = new TsFileResource(unseqFile4);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(unseqFileResource4.getTsFile())) {
      // prepare f4-c1
      tsFileIOWriter.startChunkGroup(TEST_DEVICE);
      List<TimeRange> pages = new ArrayList<>();
      // prepare f4-c1-p1
      pages.add(new TimeRange(50L, 59L));
      // prepare f4-c1-p2
      pages.add(new TimeRange(60L, 69L));
      for (IChunkWriter iChunkWriter : createChunkWriter(writtenPaths, dataTypes, false)) {
        writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
      }
      tsFileIOWriter.endChunkGroup();

      unseqFileResource4.updateStartTime(TEST_DEVICE, 50);
      unseqFileResource4.updateEndTime(TEST_DEVICE, 69);
      tsFileIOWriter.endFile();
    }
    unseqFileResource4.setStatus(TsFileResourceStatus.CLOSED);
    unSeqResources.add(unseqFileResource4);
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

  private SeriesScanUtil getSeriesScanUtil(long limit, long offset) throws IllegalPathException {
    MeasurementPath scanPath = new MeasurementPath(TEST_PATH, TSDataType.INT32);

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(Collections.singleton(scanPath.getMeasurement()));
    scanOptionsBuilder.withLimit(limit);
    scanOptionsBuilder.withOffset(offset);
    SeriesScanUtil seriesScanUtil =
        new SeriesScanUtil(
            scanPath,
            Ordering.ASC,
            scanOptionsBuilder.build(),
            EnvironmentUtils.TEST_QUERY_FI_CONTEXT);
    seriesScanUtil.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
    return seriesScanUtil;
  }

  @Test
  public void testSkipFile() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(5, 10);

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
  public void testSkipChunk() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(5, 20);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 20;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipPage() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(5, 30);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 30;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipPoint1() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(10, 45);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(0, tsBlock.getPositionCount());

    Assert.assertTrue(seriesScanUtil.hasNextPage());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 45;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }

  @Test
  public void testSkipPoint2() throws IllegalPathException, IOException {
    SeriesScanUtil seriesScanUtil = getSeriesScanUtil(10, 55);

    Assert.assertTrue(seriesScanUtil.hasNextFile());
    Assert.assertTrue(seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());

    TsBlock tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    long expectedTime = 55;
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertTrue(seriesScanUtil.hasNextPage());
    tsBlock = seriesScanUtil.nextPage();
    Assert.assertEquals(5, tsBlock.getPositionCount());
    for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
      Assert.assertEquals(expectedTime++, tsBlock.getTimeByIndex(i));
    }

    Assert.assertFalse(seriesScanUtil.hasNextPage());
    Assert.assertFalse(seriesScanUtil.hasNextChunk());
    Assert.assertFalse(seriesScanUtil.hasNextFile());
  }
}
