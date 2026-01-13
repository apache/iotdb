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
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.ColumnRename;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.TableRename;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.fileset.TsFileSet;
import org.apache.iotdb.db.utils.EncryptDBUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.page.TimePageWriter;
import org.apache.tsfile.write.page.ValuePageWriter;
import org.apache.tsfile.write.record.Tablet;
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
import java.util.concurrent.CountDownLatch;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createChunkWriter;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createCompressionType;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createDataType;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createEncodingType;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createTimeseries;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeAlignedChunk;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeNonAlignedChunk;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeOneAlignedPage;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFileID());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFileID());
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
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.d1.s1"), Long.MAX_VALUE));
    seqFile1.getModFileForWrite().close();
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
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.d1.**"), Long.MAX_VALUE));
    seqFile2.getModFileForWrite().close();
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
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.d1.**"), Long.MAX_VALUE));
    seqFile3.getModFileForWrite().close();
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
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.d1.**"), Long.MAX_VALUE));
    seqFile1.getModFileForWrite().close();
    TsFileResource seqFile2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(5, 6)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile2
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.d1.**"), Long.MAX_VALUE));
    seqFile2.getModFileForWrite().close();
    TsFileResource seqFile3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile3)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(10, 20)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile3
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.d1.**"), Long.MAX_VALUE));
    seqFile3.getModFileForWrite().close();
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
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.**"), Long.MAX_VALUE));
    seqFile1.getModFileForWrite().close();
    TsFileResource seqFile2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile2)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(5, 6)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile2
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.**"), Long.MAX_VALUE));
    seqFile2.getModFileForWrite().close();
    TsFileResource seqFile3 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(seqFile3)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1", new TimeRange[] {new TimeRange(10, 20)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqFile3
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.**"), Long.MAX_VALUE));
    seqFile3.getModFileForWrite().close();
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
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.**"), Long.MAX_VALUE));
    seqFile1.getModFileForWrite().close();
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
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.**"), Long.MAX_VALUE));
    seqFile2.getModFileForWrite().close();
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
        .getModFileForWrite()
        .write(new TreeDeletionEntry(new MeasurementPath("root.testsg.**"), Long.MAX_VALUE));
    seqFile3.getModFileForWrite().close();
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

  @Test
  public void testCascadedDeletionDuringCompaction() throws IOException, InterruptedException {
    TsFileResource source = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(source)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDevice(
          Arrays.asList("s1"),
          new TimeRange[] {new TimeRange(10, 20)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0,
            tsFileManager,
            Collections.singletonList(source),
            true,
            new TestReadChunkCompactionPerformer(latch1, latch2),
            0);
    new Thread(
            () -> {
              try {
                latch1.await();
                try (ModificationFile modificationFile = source.getModFileForWrite()) {
                  modificationFile.write(
                      new TreeDeletionEntry(new MeasurementPath("root.testsg.d1.s1"), 15));
                }
                latch2.countDown();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            })
        .start();
    Assert.assertTrue(task.start());
    Assert.assertEquals(1, tsFileManager.getTsFileList(true).size());
    tsFileManager.getTsFileList(true).get(0).getExclusiveModFile();
    Assert.assertEquals(1, FileMetrics.getInstance().getModFileNum());
  }

  private static class TestReadChunkCompactionPerformer extends ReadChunkCompactionPerformer {

    private final CountDownLatch latch1;
    private final CountDownLatch latch2;

    public TestReadChunkCompactionPerformer(CountDownLatch latch1, CountDownLatch latch2) {
      this.latch1 = latch1;
      this.latch2 = latch2;
    }

    @Override
    public void perform()
        throws IOException,
            MetadataException,
            InterruptedException,
            StorageEngineException,
            PageException {
      super.perform();
      latch1.countDown();
      latch2.await();
    }
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

  @Test
  public void testWithSevoFile() throws Exception {
    String fileSetDir =
        TestConstant.BASE_OUTPUT_PATH + File.separator + TsFileSet.FILE_SET_DIR_NAME;
    // file1:
    // table1[s1, s2, s3]
    // table2[s1, s2, s3]
    File f1 = new File(SEQ_DIRS, "0-1-0-0.tsfile");
    TableSchema tableSchema1_1 =
        new TableSchema(
            "table1",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    TableSchema tableSchema1_2 =
        new TableSchema(
            "table2",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    try (TsFileWriter tsFileWriter = new TsFileWriter(f1)) {
      tsFileWriter.registerTableSchema(tableSchema1_1);
      tsFileWriter.registerTableSchema(tableSchema1_2);

      Tablet tablet1 = new Tablet(tableSchema1_1.getTableName(), tableSchema1_1.getColumnSchemas());
      tablet1.addTimestamp(0, 0);
      tablet1.addValue(0, 0, 1);
      tablet1.addValue(0, 1, 2);
      tablet1.addValue(0, 2, 3);

      Tablet tablet2 = new Tablet(tableSchema1_2.getTableName(), tableSchema1_2.getColumnSchemas());
      tablet2.addTimestamp(0, 0);
      tablet2.addValue(0, 0, 101);
      tablet2.addValue(0, 1, 102);
      tablet2.addValue(0, 2, 103);

      tsFileWriter.writeTable(tablet1);
      tsFileWriter.writeTable(tablet2);
    }
    TsFileResource resource1 = new TsFileResource(f1);
    resource1.setTsFileManager(tsFileManager);
    resource1.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table1"}), 0);
    resource1.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table1"}), 0);
    resource1.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 0);
    resource1.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 0);
    resource1.close();

    // rename table1 -> table0
    TsFileSet tsFileSet1 = new TsFileSet(1, fileSetDir, false);
    tsFileSet1.appendSchemaEvolution(
        Collections.singletonList(new TableRename("table1", "table0")));
    tsFileManager.addTsFileSet(tsFileSet1, 0);

    // file2:
    // table0[s1, s2, s3]
    // table2[s1, s2, s3]
    File f2 = new File(SEQ_DIRS, "0-2-0-0.tsfile");
    TableSchema tableSchema2_1 =
        new TableSchema(
            "table0",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    TableSchema tableSchema2_2 =
        new TableSchema(
            "table2",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    try (TsFileWriter tsFileWriter = new TsFileWriter(f2)) {
      tsFileWriter.registerTableSchema(tableSchema2_1);
      tsFileWriter.registerTableSchema(tableSchema2_2);

      Tablet tablet1 = new Tablet(tableSchema2_1.getTableName(), tableSchema2_1.getColumnSchemas());
      tablet1.addTimestamp(0, 1);
      tablet1.addValue(0, 0, 11);
      tablet1.addValue(0, 1, 12);
      tablet1.addValue(0, 2, 13);

      Tablet tablet2 = new Tablet(tableSchema2_2.getTableName(), tableSchema2_2.getColumnSchemas());
      tablet2.addTimestamp(0, 1);
      tablet2.addValue(0, 0, 111);
      tablet2.addValue(0, 1, 112);
      tablet2.addValue(0, 2, 113);

      tsFileWriter.writeTable(tablet1);
      tsFileWriter.writeTable(tablet2);
    }
    TsFileResource resource2 = new TsFileResource(f2);
    resource2.setTsFileManager(tsFileManager);
    resource2.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table0"}), 1);
    resource2.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table0"}), 1);
    resource2.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 1);
    resource2.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 1);
    resource2.close();


    // rename table0.s1 -> table0.s0
    TsFileSet tsFileSet2 = new TsFileSet(2, fileSetDir, false);
    tsFileSet2.appendSchemaEvolution(
        Collections.singletonList(new ColumnRename("table0", "s1", "s0")));
    tsFileManager.addTsFileSet(tsFileSet2, 0);

    // file3:
    // table0[s0, s2, s3]
    // table2[s1, s2, s3]
    File f3 = new File(SEQ_DIRS, "0-3-0-0.tsfile");
    TableSchema tableSchema3_1 =
        new TableSchema(
            "table0",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s0")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    TableSchema tableSchema3_2 =
        new TableSchema(
            "table2",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    try (TsFileWriter tsFileWriter = new TsFileWriter(f3)) {
      tsFileWriter.registerTableSchema(tableSchema3_1);
      tsFileWriter.registerTableSchema(tableSchema3_2);

      Tablet tablet1 = new Tablet(tableSchema3_1.getTableName(), tableSchema3_1.getColumnSchemas());
      tablet1.addTimestamp(0, 2);
      tablet1.addValue(0, 0, 21);
      tablet1.addValue(0, 1, 22);
      tablet1.addValue(0, 2, 23);

      Tablet tablet2 = new Tablet(tableSchema3_2.getTableName(), tableSchema3_2.getColumnSchemas());
      tablet2.addTimestamp(0, 2);
      tablet2.addValue(0, 0, 121);
      tablet2.addValue(0, 1, 122);
      tablet2.addValue(0, 2, 123);

      tsFileWriter.writeTable(tablet1);
      tsFileWriter.writeTable(tablet2);
    }
    TsFileResource resource3 = new TsFileResource(f3);
    resource3.setTsFileManager(tsFileManager);
    resource3.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table0"}), 2);
    resource3.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table0"}), 2);
    resource3.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 2);
    resource3.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 2);
    resource3.close();

    // rename table2 -> table1
    TsFileSet tsFileSet3 = new TsFileSet(3, fileSetDir, false);
    tsFileSet3.appendSchemaEvolution(
        Collections.singletonList(new TableRename("table2", "table1")));
    tsFileManager.addTsFileSet(tsFileSet3, 0);

    // perform compaction
    seqResources.add(resource1);
    seqResources.add(resource2);
    seqResources.add(resource3);

    List<TsFileResource> targetResources =
        CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    targetResources.forEach(s -> s.setTsFileManager(tsFileManager));

    ICompactionPerformer performer =
        new ReadChunkCompactionPerformer(seqResources, targetResources, EncryptDBUtils.getDefaultFirstEncryptParam());
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();

    // target(version=1):
    // table1[s1, s2, s3]
    // table2[s1, s2, s3]
    try (ITsFileReader tsFileReader =
        new TsFileReaderBuilder().file(targetResources.get(0).getTsFile()).build()) {
      // table1 should not exist
      try {
        tsFileReader.query("table0", Collections.singletonList("s2"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table0 should not exist");
      } catch (NoTableException e) {
        assertEquals("Table table0 not found", e.getMessage());
      }

      // table1.s0 should not exist
      try {
        tsFileReader.query("table1", Collections.singletonList("s0"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table1.s0 should not exist");
      } catch (NoMeasurementException e) {
        assertEquals("No measurement for s0", e.getMessage());
      }

      // check data of table1
      ResultSet resultSet = tsFileReader.query("table1", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      for (int i = 0; i < 3; i++) {
        assertTrue(resultSet.next());
        assertEquals(i, resultSet.getLong(1));
        for (int j = 0; j < 3; j++) {
          assertEquals(i * 10 + j + 1, resultSet.getLong(j + 2));
        }
      }

      // check data of table2
      resultSet = tsFileReader.query("table2", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      for (int i = 0; i < 3; i++) {
        assertTrue(resultSet.next());
        assertEquals(i, resultSet.getLong(1));
        for (int j = 0; j < 3; j++) {
          assertEquals(100 + i * 10 + j + 1, resultSet.getLong(j + 2));
        }
      }
    }
  }
}
