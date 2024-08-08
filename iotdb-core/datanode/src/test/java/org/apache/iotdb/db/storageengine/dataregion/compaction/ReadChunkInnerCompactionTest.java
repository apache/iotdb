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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.page.TimePageWriter;
import org.apache.tsfile.write.page.ValuePageWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createChunkWriter;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createCompressionType;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createDataType;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createEncodingType;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createTimeseries;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeAlignedChunk;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeNonAlignedChunk;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeOneAlignedPage;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class ReadChunkInnerCompactionTest extends AbstractCompactionTest {
  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(512);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(100);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(30);
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    for (TsFileResource tsFileResource : seqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
  }

  @Test
  public void testNonAlignedWithDifferentEncodingAndCompression()
      throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int measurementNum = 20;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < 10; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex));

        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<TSEncoding> encodings = createEncodingType(measurementNum);
        List<CompressionType> compressionTypes = createCompressionType(measurementNum);
        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          timeseriesPath.add(
              new MeasurementPath(
                  COMPACTION_TEST_SG
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i,
                  dataTypes.get(i)));
        }

        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter :
            createChunkWriter(timeseriesPath, dataTypes, encodings, compressionTypes, false)) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }
        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            0);
        resource.updateEndTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            600);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < 12; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex));

        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<TSEncoding> encodings = createEncodingType(measurementNum);
        List<CompressionType> compressionTypes = createCompressionType(measurementNum);
        List<PartialPath> timeseriesPath = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
          timeseriesPath.add(
              new MeasurementPath(
                  COMPACTION_TEST_SG
                      + PATH_SEPARATOR
                      + "d"
                      + deviceIndex
                      + PATH_SEPARATOR
                      + "s"
                      + i,
                  dataTypes.get(i)));
        }

        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(900L, 1400L));
        pages.add(new TimeRange(1550L, 1700L));
        pages.add(new TimeRange(1750L, 2000L));

        for (IChunkWriter iChunkWriter :
            createChunkWriter(timeseriesPath, dataTypes, encodings, compressionTypes, false)) {
          writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }
        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            900);
        resource.updateEndTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            2000);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<IFullPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(maxDeviceNum, maxMeasurementNum, false), tsDataTypes);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);

    task.start();

    validateSeqFiles(true);

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void testAlignedWithDifferentEncodingAndCompression()
      throws MetadataException, IOException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file 1
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex));

        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<TSEncoding> encodings = createEncodingType(measurementNum);
        List<CompressionType> compressionTypes = createCompressionType(measurementNum);
        List<Integer> measurementIndexes = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          measurementIndexes.add(i);
        }
        List<PartialPath> timeseriesPath =
            createTimeseries(deviceIndex, measurementIndexes, dataTypes, true);

        List<IChunkWriter> iChunkWriters =
            createChunkWriter(timeseriesPath, dataTypes, encodings, compressionTypes, true);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeAlignedChunk((AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(900L, 1199L));
        timeRanges.add(new TimeRange(1301L, 1400L));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeOneAlignedPage((AlignedChunkWriterImpl) iChunkWriter, timeRanges, true);
          iChunkWriter.writeToFileWriter(tsFileIOWriter);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            0);
        resource.updateEndTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 500, 600);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 2
    deviceNum = 5;
    measurementNum = 20;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex));

        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<TSEncoding> encodings = createEncodingType(measurementNum);
        List<CompressionType> compressionTypes = createCompressionType(measurementNum);
        List<Integer> measurementIndexes = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          measurementIndexes.add(i);
        }
        List<PartialPath> timeseriesPath =
            createTimeseries(deviceIndex, measurementIndexes, dataTypes, true);
        List<IChunkWriter> iChunkWriters =
            createChunkWriter(timeseriesPath, dataTypes, encodings, compressionTypes, true);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(1800L, 1900L));
        pages.add(new TimeRange(2150L, 2250L));
        pages.add(new TimeRange(2400L, 2500L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeAlignedChunk((AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            1800);
        resource.updateEndTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            2500);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 1840, 1900);
        generateModsFile(timeseriesPath, resource, 2150, 2250);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // seq file 3
    deviceNum = 15;
    measurementNum = 15;
    resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex));

        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<TSEncoding> encodings = createEncodingType(measurementNum);
        List<CompressionType> compressionTypes = createCompressionType(measurementNum);
        List<Integer> measurementIndexes = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          measurementIndexes.add(i);
        }
        List<PartialPath> timeseriesPath =
            createTimeseries(deviceIndex, measurementIndexes, dataTypes, true);
        List<IChunkWriter> iChunkWriters =
            createChunkWriter(timeseriesPath, dataTypes, encodings, compressionTypes, true);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(2801, 2850));
        pages.add(new TimeRange(2851, 2900));
        pages.add(new TimeRange(3300, 3400));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeAlignedChunk((AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        // write second chunk
        pages.clear();
        pages.add(new TimeRange(3950, 4100));
        pages.add(new TimeRange(4200, 4300));
        for (IChunkWriter iChunkWriter : iChunkWriters) {
          writeAlignedChunk((AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            2801);
        resource.updateEndTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            4300);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);

        generateModsFile(timeseriesPath, resource, 2801, 2850);
        generateModsFile(timeseriesPath, resource, 3950, 4100);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<IFullPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(maxDeviceNum, maxMeasurementNum, true), tsDataTypes);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);

    task.start();

    validateSeqFiles(true);

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  @Test
  public void testReadChunkPerformerWithEmptyTargetFile1() throws IOException {
    TsFileResource seqFile1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile1)) {
      writer.endFile();
    }
    TsFileResource seqFile2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile2)) {
      writer.endFile();
    }
    TsFileResource seqFile3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile3)) {
      writer.endFile();
    }
    seqResources.add(seqFile1);
    seqResources.add(seqFile2);
    seqResources.add(seqFile3);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testReadChunkPerformerWithEmptyTargetFile2() throws IOException {
    TsFileResource seqFile1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile1)) {
      writer.endFile();
    }
    TsFileResource seqFile2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile2)) {
      writer.endFile();
    }
    TsFileResource seqFile3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile3)) {
      writer.startChunkGroup("d1");
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(seqFile1);
    seqResources.add(seqFile2);
    seqResources.add(seqFile3);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testReadChunkPerformerWithEmptyTargetFile3()
      throws IOException, IllegalPathException {
    TsFileResource seqFile1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1"),
          new TimeRange[] {new TimeRange(1, 2)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile1
        .getModFile()
        .write(
            new Deletion(new MeasurementPath("root.testsg.d1.s1"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile1.getModFile().close();
    TsFileResource seqFile2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1"),
          new TimeRange[] {new TimeRange(5, 6)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile2
        .getModFile()
        .write(
            new Deletion(new MeasurementPath("root.testsg.d1.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile2.getModFile().close();
    TsFileResource seqFile3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile3)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1"),
          new TimeRange[] {new TimeRange(10, 20)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile3
        .getModFile()
        .write(
            new Deletion(new MeasurementPath("root.testsg.d1.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile3.getModFile().close();
    seqResources.add(seqFile1);
    seqResources.add(seqFile2);
    seqResources.add(seqFile3);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testReadChunkPerformerWithEmptyTargetFile4()
      throws IOException, IllegalPathException {
    TsFileResource seqFile1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(1, 2)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile1
        .getModFile()
        .write(
            new Deletion(new MeasurementPath("root.testsg.d1.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile1.getModFile().close();
    TsFileResource seqFile2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(5, 6)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile2
        .getModFile()
        .write(
            new Deletion(new MeasurementPath("root.testsg.d1.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile2.getModFile().close();
    TsFileResource seqFile3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile3)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(10, 20)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile3
        .getModFile()
        .write(
            new Deletion(new MeasurementPath("root.testsg.d1.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile3.getModFile().close();
    seqResources.add(seqFile1);
    seqResources.add(seqFile2);
    seqResources.add(seqFile3);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testReadChunkPerformerWithEmptyTargetFile5()
      throws IOException, IllegalPathException {
    TsFileResource seqFile1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(1, 2)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile1
        .getModFile()
        .write(new Deletion(new MeasurementPath("root.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile1.getModFile().close();
    TsFileResource seqFile2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(5, 6)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile2
        .getModFile()
        .write(new Deletion(new MeasurementPath("root.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile2.getModFile().close();
    TsFileResource seqFile3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile3)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(10, 20)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile3
        .getModFile()
        .write(new Deletion(new MeasurementPath("root.testsg.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile3.getModFile().close();
    seqResources.add(seqFile1);
    seqResources.add(seqFile2);
    seqResources.add(seqFile3);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testReadChunkPerformerWithEmptyTargetFile6()
      throws IOException, IllegalPathException {
    TsFileResource seqFile1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1"),
          new TimeRange[] {new TimeRange(1, 2)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile1
        .getModFile()
        .write(new Deletion(new MeasurementPath("root.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile1.getModFile().close();
    TsFileResource seqFile2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1"),
          new TimeRange[] {new TimeRange(5, 6)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile2
        .getModFile()
        .write(new Deletion(new MeasurementPath("root.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile2.getModFile().close();
    TsFileResource seqFile3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile3)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1"),
          new TimeRange[] {new TimeRange(10, 20)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile3
        .getModFile()
        .write(new Deletion(new MeasurementPath("root.testsg.**"), Long.MAX_VALUE, Long.MAX_VALUE));
    seqFile3.getModFile().close();
    seqResources.add(seqFile1);
    seqResources.add(seqFile2);
    seqResources.add(seqFile3);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testReadChunkPerformerWithNonEmptyTargetFile() throws IOException {
    TsFileResource seqFile1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile1)) {
      writer.endFile();
    }
    TsFileResource seqFile2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile2)) {
      writer.endFile();
    }
    TsFileResource seqFile3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile3)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(1, 2)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(seqFile1);
    seqResources.add(seqFile2);
    seqResources.add(seqFile3);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testCompactionLogIsDeletedAfterException() throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {
            new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12), new TimeRange(3, 12)}}
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource seqResource2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqResource2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(1, 9)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(seqResource1);
    seqResources.add(seqResource2);
    tsFileManager.addAll(seqResources, true);

    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new FastCompactionPerformer(false), 0);
    Assert.assertFalse(task.start());
    Assert.assertFalse(
        new File(
                seqResource1
                    .getTsFile()
                    .getAbsolutePath()
                    .replace(
                        "0-0" + TsFileConstant.TSFILE_SUFFIX,
                        "1-0.inner" + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX))
            .exists());
    Assert.assertEquals(
        4, Objects.requireNonNull(seqResource1.getTsFile().getParentFile().listFiles()).length);
  }

  @Test
  public void testCompactionWithAllEmptyValueChunks() throws IOException, IllegalPathException {
    List<PartialPath> timeserisPathList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    // seq file with empty aligned device
    int deviceNum = 10;
    int measurementNum = 10;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        tsFileIOWriter.startChunkGroup(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex));

        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<TSEncoding> encodings = createEncodingType(measurementNum);
        List<CompressionType> compressionTypes = createCompressionType(measurementNum);
        List<Integer> measurementIndexes = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          measurementIndexes.add(i);
        }
        List<PartialPath> timeseriesPath =
            createTimeseries(deviceIndex, measurementIndexes, dataTypes, true);

        List<IChunkWriter> iChunkWriters =
            createChunkWriter(timeseriesPath, dataTypes, encodings, compressionTypes, true);

        // write first chunk
        List<TimeRange> pages = new ArrayList<>();
        pages.add(new TimeRange(0L, 300L));
        pages.add(new TimeRange(500L, 600L));

        for (IChunkWriter iChunkWriter : iChunkWriters) {
          if (deviceIndex == 0) {
            writeEmptyAlignedChunk(
                (AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
          } else {
            writeAlignedChunk((AlignedChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
          }
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            0);
        resource.updateEndTime(
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + deviceIndex),
            1400);
        timeserisPathList.addAll(timeseriesPath);
        tsDataTypes.addAll(dataTypes);
        if (deviceIndex == 0) {
          generateModsFile(timeseriesPath, resource, Long.MIN_VALUE, Long.MAX_VALUE);
        }
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

    // start compacting
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<IFullPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(maxDeviceNum, maxMeasurementNum, true), tsDataTypes);

    // execute inner compaction for each seq file to produce file with empty value chunk
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            Collections.singletonList(seqResources.get(0)),
            true,
            new ReadChunkCompactionPerformer(),
            0);
    Assert.assertTrue(task.start());

    validateSeqFiles(true);

    validateTargetDatas(sourceDatas, tsDataTypes);
  }

  private void writeEmptyAlignedChunk(
      AlignedChunkWriterImpl alignedChunkWriter,
      TsFileIOWriter tsFileIOWriter,
      List<TimeRange> pages,
      boolean isSeq)
      throws IOException {
    TimePageWriter timePageWriter = alignedChunkWriter.getTimeChunkWriter().getPageWriter();
    for (TimeRange page : pages) {
      // write time page
      for (long timestamp = page.getMin(); timestamp <= page.getMax(); timestamp++) {
        timePageWriter.write(timestamp);
      }
      // seal time page
      alignedChunkWriter.getTimeChunkWriter().sealCurrentPage();

      // write value page
      for (ValueChunkWriter valueChunkWriter : alignedChunkWriter.getValueChunkWriterList()) {
        ValuePageWriter valuePageWriter = valueChunkWriter.getPageWriter();
        for (long timestamp = page.getMin(); timestamp <= page.getMax(); timestamp++) {
          writeEmptyAlignedPoint(valuePageWriter, timestamp, isSeq);
        }
        // seal sub value page
        valueChunkWriter.sealCurrentPage();
      }
    }
    // seal time chunk and value chunks
    alignedChunkWriter.writeToFileWriter(tsFileIOWriter);
  }

  private void writeEmptyAlignedPoint(
      ValuePageWriter valuePageWriter, long timestamp, boolean isSeq) {
    switch (valuePageWriter.getStatistics().getType()) {
      case TEXT:
        valuePageWriter.write(
            timestamp,
            new Binary(isSeq ? "seqText" : "unSeqText", TSFileConfig.STRING_CHARSET),
            true);
        break;
      case DOUBLE:
        valuePageWriter.write(timestamp, isSeq ? timestamp + 0.01 : 100000.01 + timestamp, true);
        break;
      case BOOLEAN:
        valuePageWriter.write(timestamp, isSeq, true);
        break;
      case INT64:
        valuePageWriter.write(timestamp, isSeq ? timestamp : 100000L + timestamp, true);
        break;
      case INT32:
        valuePageWriter.write(
            timestamp, isSeq ? (int) timestamp : (int) (100000 + timestamp), true);
        break;
      case FLOAT:
        valuePageWriter.write(
            timestamp, isSeq ? timestamp + (float) 0.1 : (float) (100000.1 + timestamp), true);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown data type " + valuePageWriter.getStatistics().getType());
    }
  }
}
