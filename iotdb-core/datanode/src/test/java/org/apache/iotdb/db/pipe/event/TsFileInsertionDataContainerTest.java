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

package org.apache.iotdb.db.pipe.event;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBPipePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixPipePattern;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.TsFileInsertionDataContainer;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.query.TsFileInsertionQueryDataContainer;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.scan.AlignedSinglePageWholeChunkReader;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.scan.SinglePageWholeChunkReader;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.scan.TsFileInsertionScanDataContainer;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.pipe.api.access.Row;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.fail;

public class TsFileInsertionDataContainerTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TsFileInsertionDataContainerTest.class);

  private static final long TSFILE_START_TIME = 300L;

  private static final String PREFIX_FORMAT = "prefix";
  private static final String IOTDB_FORMAT = "iotdb";

  private File alignedTsFile;
  private File nonalignedTsFile;
  private TsFileResource resource;
  private boolean isPipeMemoryManagementEnabled;

  @Before
  public void setUp() throws Exception {
    isPipeMemoryManagementEnabled = PipeConfig.getInstance().getPipeMemoryManagementEnabled();
    CommonDescriptor.getInstance().getConfig().setPipeMemoryManagementEnabled(false);
  }

  @After
  public void tearDown() throws Exception {
    CommonDescriptor.getInstance()
        .getConfig()
        .setPipeMemoryManagementEnabled(isPipeMemoryManagementEnabled);
    if (alignedTsFile != null) {
      alignedTsFile.delete();
    }
    if (nonalignedTsFile != null) {
      nonalignedTsFile.delete();
    }
    if (Objects.nonNull(resource)) {
      resource.remove();
    }
  }

  @Test
  public void testQueryContainer() throws Exception {
    final long startTime = System.currentTimeMillis();
    testToTabletInsertionEvents(true);
    System.out.println(System.currentTimeMillis() - startTime);
  }

  @Test
  public void testScanContainer() throws Exception {
    final long startTime = System.currentTimeMillis();
    testToTabletInsertionEvents(false);
    System.out.println(System.currentTimeMillis() - startTime);
  }

  @Test
  public void testScanParserSplitNonAlignedSinglePageChunkByEstimatedPageMemory() throws Exception {
    final long originalPipeMaxReaderChunkSize =
        CommonDescriptor.getInstance().getConfig().getPipeMaxReaderChunkSize();
    final int originalPageSizeInByte =
        TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    final int originalMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

    try {
      TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(64 * 1024);
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10000);

      final int measurementCount = 16;
      final int rowCount = 64;
      final List<MeasurementSchema> schemaList = new ArrayList<>();
      for (int i = 0; i < measurementCount; ++i) {
        schemaList.add(
            new MeasurementSchema(
                "s" + i, TSDataType.STRING, TSEncoding.PLAIN, CompressionType.LZ4));
      }

      nonalignedTsFile = new File("nonaligned-single-page-high-compression.tsfile");
      final Tablet tablet = new Tablet("root.sg.d", schemaList, rowCount);
      final Binary value =
          new Binary(new String(new char[512]).replace('\0', 'a'), TSFileConfig.STRING_CHARSET);
      for (int row = 0; row < rowCount; ++row) {
        tablet.addTimestamp(row, row);
        for (int measurementIndex = 0; measurementIndex < measurementCount; ++measurementIndex) {
          tablet.addValue("s" + measurementIndex, row, value);
        }
      }
      tablet.rowSize = rowCount;

      try (final TsFileWriter writer = new TsFileWriter(nonalignedTsFile)) {
        writer.registerTimeseries(new Path("root.sg.d"), schemaList);
        writer.write(tablet);
      }

      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(
              calculatePipeMaxReaderChunkSizeForSinglePageNonAlignedChunk(nonalignedTsFile));

      int tabletCount = 0;
      int maxMeasurementCount = 0;
      int pointCount = 0;
      try (final TsFileInsertionScanDataContainer parser =
          new TsFileInsertionScanDataContainer(
              nonalignedTsFile,
              new PrefixPipePattern("root"),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null,
              false)) {
        for (final Pair<Tablet, Boolean> tabletWithIsAligned : parser.toTabletWithIsAligneds()) {
          Assert.assertFalse(tabletWithIsAligned.getRight());
          final Tablet parsedTablet = tabletWithIsAligned.getLeft();
          tabletCount++;
          maxMeasurementCount = Math.max(maxMeasurementCount, parsedTablet.getSchemas().size());
          pointCount += getNonNullSize(parsedTablet);
        }
      }

      Assert.assertTrue(tabletCount > 1);
      Assert.assertTrue(maxMeasurementCount < measurementCount);
      Assert.assertEquals(measurementCount * rowCount, pointCount);
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(originalPipeMaxReaderChunkSize);
      TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(originalPageSizeInByte);
      TSFileDescriptor.getInstance()
          .getConfig()
          .setMaxNumberOfPointsInPage(originalMaxNumberOfPointsInPage);
    }
  }

  @Test
  public void testScanParserSplitAlignedSinglePageChunkByEstimatedPageMemory() throws Exception {
    final long originalPipeMaxReaderChunkSize =
        CommonDescriptor.getInstance().getConfig().getPipeMaxReaderChunkSize();
    final int originalPageSizeInByte =
        TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    final int originalMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

    try {
      TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(64 * 1024);
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10000);

      final int measurementCount = 16;
      final int rowCount = 64;
      final List<MeasurementSchema> schemaList = new ArrayList<>();
      for (int i = 0; i < measurementCount; ++i) {
        schemaList.add(
            new MeasurementSchema(
                "s" + i, TSDataType.STRING, TSEncoding.PLAIN, CompressionType.LZ4));
      }

      alignedTsFile = new File("aligned-single-page-high-compression.tsfile");
      final Tablet tablet = new Tablet("root.sg.d", schemaList, rowCount);
      final Binary value =
          new Binary(new String(new char[512]).replace('\0', 'a'), TSFileConfig.STRING_CHARSET);
      for (int row = 0; row < rowCount; ++row) {
        tablet.addTimestamp(row, row);
        for (int measurementIndex = 0; measurementIndex < measurementCount; ++measurementIndex) {
          tablet.addValue("s" + measurementIndex, row, value);
        }
      }
      tablet.rowSize = rowCount;

      try (final TsFileWriter writer = new TsFileWriter(alignedTsFile)) {
        writer.registerAlignedTimeseries(new Path("root.sg.d"), schemaList);
        writer.writeAligned(tablet);
      }

      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(
              calculatePipeMaxReaderChunkSizeForSinglePageAlignedChunk(alignedTsFile));

      int tabletCount = 0;
      int maxMeasurementCount = 0;
      int pointCount = 0;
      try (final TsFileInsertionScanDataContainer parser =
          new TsFileInsertionScanDataContainer(
              alignedTsFile,
              new PrefixPipePattern("root"),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null,
              false)) {
        for (final Pair<Tablet, Boolean> tabletWithIsAligned : parser.toTabletWithIsAligneds()) {
          Assert.assertTrue(tabletWithIsAligned.getRight());
          final Tablet parsedTablet = tabletWithIsAligned.getLeft();
          tabletCount++;
          maxMeasurementCount = Math.max(maxMeasurementCount, parsedTablet.getSchemas().size());
          pointCount += getNonNullSize(parsedTablet);
        }
      }

      Assert.assertTrue(tabletCount > 1);
      Assert.assertTrue(maxMeasurementCount < measurementCount);
      Assert.assertEquals(measurementCount * rowCount, pointCount);
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(originalPipeMaxReaderChunkSize);
      TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(originalPageSizeInByte);
      TSFileDescriptor.getInstance()
          .getConfig()
          .setMaxNumberOfPointsInPage(originalMaxNumberOfPointsInPage);
    }
  }

  @Test
  public void testScanParserSplitAlignedMultiPageChunkByEstimatedPageMemory() throws Exception {
    final long originalPipeMaxReaderChunkSize =
        CommonDescriptor.getInstance().getConfig().getPipeMaxReaderChunkSize();
    final int originalPageSizeInByte =
        TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    final int originalMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

    try {
      TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(64 * 1024);
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(32);

      final int measurementCount = 16;
      final int rowCount = 64;
      final List<MeasurementSchema> schemaList = new ArrayList<>();
      for (int i = 0; i < measurementCount; ++i) {
        schemaList.add(
            new MeasurementSchema(
                "s" + i, TSDataType.STRING, TSEncoding.PLAIN, CompressionType.LZ4));
      }

      alignedTsFile = new File("aligned-multi-page-high-compression.tsfile");
      final Tablet tablet = new Tablet("root.sg.d", schemaList, rowCount);
      final Binary value =
          new Binary(new String(new char[512]).replace('\0', 'a'), TSFileConfig.STRING_CHARSET);
      for (int row = 0; row < rowCount; ++row) {
        tablet.addTimestamp(row, row);
        for (int measurementIndex = 0; measurementIndex < measurementCount; ++measurementIndex) {
          tablet.addValue("s" + measurementIndex, row, value);
        }
      }
      tablet.rowSize = rowCount;

      try (final TsFileWriter writer = new TsFileWriter(alignedTsFile)) {
        writer.registerAlignedTimeseries(new Path("root.sg.d"), schemaList);
        writer.writeAligned(tablet);
      }

      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(
              calculatePipeMaxReaderChunkSizeForMultiPageAlignedChunk(alignedTsFile));

      int tabletCount = 0;
      int maxMeasurementCount = 0;
      int pointCount = 0;
      try (final TsFileInsertionScanDataContainer parser =
          new TsFileInsertionScanDataContainer(
              alignedTsFile,
              new PrefixPipePattern("root"),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null,
              false)) {
        for (final Pair<Tablet, Boolean> tabletWithIsAligned : parser.toTabletWithIsAligneds()) {
          Assert.assertTrue(tabletWithIsAligned.getRight());
          final Tablet parsedTablet = tabletWithIsAligned.getLeft();
          tabletCount++;
          maxMeasurementCount = Math.max(maxMeasurementCount, parsedTablet.getSchemas().size());
          pointCount += getNonNullSize(parsedTablet);
        }
      }

      Assert.assertTrue(tabletCount > 1);
      Assert.assertTrue(maxMeasurementCount < measurementCount);
      Assert.assertEquals(measurementCount * rowCount, pointCount);
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(originalPipeMaxReaderChunkSize);
      TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(originalPageSizeInByte);
      TSFileDescriptor.getInstance()
          .getConfig()
          .setMaxNumberOfPointsInPage(originalMaxNumberOfPointsInPage);
    }
  }

  @Test
  public void testScanParserResizesChunkMemoryForFirstAlignedValueChunk() throws Exception {
    final long originalPipeMaxReaderChunkSize =
        PipeConfig.getInstance().getPipeMaxReaderChunkSize();
    final int originalPipeDataStructureTabletSizeInBytes =
        PipeConfig.getInstance().getPipeDataStructureTabletSizeInBytes();
    final int configuredBatchMemorySize = 1024 * 1024;
    CommonDescriptor.getInstance().getConfig().setPipeMaxReaderChunkSize(0);
    CommonDescriptor.getInstance()
        .getConfig()
        .setPipeDataStructureTabletSizeInBytes(configuredBatchMemorySize);

    alignedTsFile = new File("single-aligned-value-chunk.tsfile");
    final List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));

    final Tablet tablet = new Tablet("root.sg.d", schemaList, 2);
    tablet.rowSize = 2;
    tablet.addTimestamp(0, 1);
    tablet.addValue("s1", 0, 1L);
    tablet.addTimestamp(1, 2);
    tablet.addValue("s1", 1, 2L);

    try {
      try (final TsFileWriter writer = new TsFileWriter(alignedTsFile)) {
        writer.registerAlignedTimeseries(new Path("root.sg.d"), schemaList);
        writer.writeAligned(tablet);
      }

      try (final TsFileInsertionScanDataContainer parser =
          new TsFileInsertionScanDataContainer(
              alignedTsFile,
              new PrefixPipePattern("root"),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null,
              false)) {
        Assert.assertTrue(getAllocatedChunkMemory(parser).getMemoryUsageInBytes() > 0);
        Assert.assertTrue(getAllocatedBatchDataMemory(parser).getMemoryUsageInBytes() > 0);
        Assert.assertTrue(
            getAllocatedBatchDataMemory(parser).getMemoryUsageInBytes()
                < configuredBatchMemorySize);
      }
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(originalPipeMaxReaderChunkSize);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeDataStructureTabletSizeInBytes(originalPipeDataStructureTabletSizeInBytes);
    }
  }

  @Test
  public void testScanParserMergesBatchedAlignedValueChunkGroups() throws Exception {
    final long originalPipeMaxReaderChunkSize =
        CommonDescriptor.getInstance().getConfig().getPipeMaxReaderChunkSize();
    final int originalPipeDataStructureTabletRowSize =
        CommonDescriptor.getInstance().getConfig().getPipeDataStructureTabletRowSize();

    final int measurementCount = 20;
    final int batchSize = 10;
    final int rowCount = 4;
    final File sourceTsFile = new File("aligned-source-for-batched-layout.tsfile");
    alignedTsFile = new File("aligned-batched-layout.tsfile");

    try {
      CommonDescriptor.getInstance().getConfig().setPipeMaxReaderChunkSize(1024 * 1024L);
      CommonDescriptor.getInstance().getConfig().setPipeDataStructureTabletRowSize(0);

      final List<MeasurementSchema> schemaList = new ArrayList<>();
      for (int i = 0; i < measurementCount; ++i) {
        schemaList.add(new MeasurementSchema("s" + i, TSDataType.INT64));
      }

      writeAlignedSourceTsFile(sourceTsFile, schemaList, rowCount);
      rewriteAlignedTsFileWithBatchedValueChunks(
          sourceTsFile, alignedTsFile, measurementCount, batchSize);

      int tabletCount = 0;
      int pointCount = 0;
      try (final TsFileInsertionScanDataContainer parser =
          new TsFileInsertionScanDataContainer(
              alignedTsFile,
              new PrefixPipePattern("root"),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null,
              false)) {
        for (final Pair<Tablet, Boolean> tabletWithIsAligned : parser.toTabletWithIsAligneds()) {
          Assert.assertTrue(tabletWithIsAligned.getRight());
          final Tablet tablet = tabletWithIsAligned.getLeft();
          ++tabletCount;
          Assert.assertEquals(measurementCount, tablet.getSchemas().size());
          Assert.assertEquals(rowCount / 2, tablet.rowSize);
          pointCount += getNonNullSize(tablet);
        }
      }

      Assert.assertEquals(measurementCount * rowCount, pointCount);
      Assert.assertEquals(2, tabletCount);
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(originalPipeMaxReaderChunkSize);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeDataStructureTabletRowSize(originalPipeDataStructureTabletRowSize);
      sourceTsFile.delete();
    }
  }

  @Test
  public void testScanParserFlushesBatchedAlignedValueChunkGroupsByMemoryLimit() throws Exception {
    final long originalPipeMaxReaderChunkSize =
        CommonDescriptor.getInstance().getConfig().getPipeMaxReaderChunkSize();
    final int originalPipeDataStructureTabletRowSize =
        CommonDescriptor.getInstance().getConfig().getPipeDataStructureTabletRowSize();

    final int measurementCount = 20;
    final int batchSize = 10;
    final int rowCount = 4;
    final File sourceTsFile = new File("aligned-source-for-batched-layout-memory-limit.tsfile");
    alignedTsFile = new File("aligned-batched-layout-memory-limit.tsfile");

    try {
      CommonDescriptor.getInstance().getConfig().setPipeDataStructureTabletRowSize(0);

      final List<MeasurementSchema> schemaList = new ArrayList<>();
      for (int i = 0; i < measurementCount; ++i) {
        schemaList.add(new MeasurementSchema("s" + i, TSDataType.INT64));
      }

      writeAlignedSourceTsFile(sourceTsFile, schemaList, rowCount);
      rewriteAlignedTsFileWithBatchedValueChunks(
          sourceTsFile, alignedTsFile, measurementCount, batchSize);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(
              calculateFirstBatchedAlignedValueChunkGroupMemoryLimit(alignedTsFile, batchSize));

      int tabletCount = 0;
      int maxMeasurementCount = 0;
      int pointCount = 0;
      try (final TsFileInsertionScanDataContainer parser =
          new TsFileInsertionScanDataContainer(
              alignedTsFile,
              new PrefixPipePattern("root"),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null,
              false)) {
        for (final Pair<Tablet, Boolean> tabletWithIsAligned : parser.toTabletWithIsAligneds()) {
          Assert.assertTrue(tabletWithIsAligned.getRight());
          final Tablet tablet = tabletWithIsAligned.getLeft();
          ++tabletCount;
          maxMeasurementCount = Math.max(maxMeasurementCount, tablet.getSchemas().size());
          Assert.assertTrue(tablet.getSchemas().size() <= batchSize);
          Assert.assertEquals(rowCount / 2, tablet.rowSize);
          pointCount += getNonNullSize(tablet);
        }
      }

      Assert.assertEquals(batchSize, maxMeasurementCount);
      Assert.assertEquals(measurementCount * rowCount, pointCount);
      Assert.assertEquals(measurementCount / batchSize * 2, tabletCount);
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(originalPipeMaxReaderChunkSize);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeDataStructureTabletRowSize(originalPipeDataStructureTabletRowSize);
      sourceTsFile.delete();
    }
  }

  @Test
  public void testPipeTabletRowSizeCanBeDisabledByNonPositiveValue() {
    final int originalPipeDataStructureTabletRowSize =
        CommonDescriptor.getInstance().getConfig().getPipeDataStructureTabletRowSize();
    final int originalPipeDataStructureTabletSizeInBytes =
        CommonDescriptor.getInstance().getConfig().getPipeDataStructureTabletSizeInBytes();

    try {
      CommonDescriptor.getInstance().getConfig().setPipeDataStructureTabletSizeInBytes(1024 * 1024);

      final BatchData batchData = new BatchData(TSDataType.INT64);
      for (int i = 0; i < 1000; ++i) {
        batchData.putAnObject(i, (long) i);
      }

      CommonDescriptor.getInstance().getConfig().setPipeDataStructureTabletRowSize(2);
      final int rowCountWithLimit =
          PipeMemoryWeightUtil.calculateTabletRowCountAndMemory(batchData).getLeft();

      CommonDescriptor.getInstance().getConfig().setPipeDataStructureTabletRowSize(0);
      final int rowCountWithoutLimit =
          PipeMemoryWeightUtil.calculateTabletRowCountAndMemory(batchData).getLeft();

      CommonDescriptor.getInstance().getConfig().setPipeDataStructureTabletRowSize(-1);
      final int rowCountWithNegativeLimit =
          PipeMemoryWeightUtil.calculateTabletRowCountAndMemory(batchData).getLeft();

      Assert.assertEquals(rowCountWithoutLimit, rowCountWithNegativeLimit);
      Assert.assertEquals(2, rowCountWithLimit);
      Assert.assertTrue(rowCountWithoutLimit > rowCountWithLimit);
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeDataStructureTabletRowSize(originalPipeDataStructureTabletRowSize);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeDataStructureTabletSizeInBytes(originalPipeDataStructureTabletSizeInBytes);
    }
  }

  public void testToTabletInsertionEvents(final boolean isQuery) throws Exception {
    // Test empty chunk
    testMixedTsFileWithEmptyChunk(isQuery);

    // Test partial null value
    testPartialNullValue(isQuery);

    // Test the combinations of pipe and tsFile settings
    final Set<Integer> deviceNumbers = new HashSet<>();
    deviceNumbers.add(1);
    deviceNumbers.add(2);

    final Set<Integer> measurementNumbers = new HashSet<>();
    measurementNumbers.add(1);
    measurementNumbers.add(2);

    final Set<String> patternFormats = new HashSet<>();
    patternFormats.add(PREFIX_FORMAT);
    patternFormats.add(IOTDB_FORMAT);

    final Set<Pair<Long, Long>> startEndTimes = new HashSet<>();
    startEndTimes.add(new Pair<>(100L, TSFILE_START_TIME - 1));
    startEndTimes.add(new Pair<>(100L, TSFILE_START_TIME));
    startEndTimes.add(new Pair<>(100L, TSFILE_START_TIME + 1));

    startEndTimes.add(new Pair<>(TSFILE_START_TIME - 1, TSFILE_START_TIME - 1));
    startEndTimes.add(new Pair<>(TSFILE_START_TIME, TSFILE_START_TIME));
    startEndTimes.add(new Pair<>(TSFILE_START_TIME + 1, TSFILE_START_TIME + 1));

    startEndTimes.add(new Pair<>(TSFILE_START_TIME + 1, TSFILE_START_TIME + 1));
    startEndTimes.add(new Pair<>(TSFILE_START_TIME + 1, TSFILE_START_TIME + 10));
    startEndTimes.add(new Pair<>(TSFILE_START_TIME + 1, TSFILE_START_TIME + 100));
    startEndTimes.add(new Pair<>(TSFILE_START_TIME + 1, TSFILE_START_TIME + 10000));

    startEndTimes.add(new Pair<>(TSFILE_START_TIME + 1000000, TSFILE_START_TIME + 2000000));

    startEndTimes.add(new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE));

    for (final int deviceNumber : deviceNumbers) {
      for (final int measurementNumber : measurementNumbers) {
        for (final String patternFormat : patternFormats) {
          for (final Pair<Long, Long> startEndTime : startEndTimes) {
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                0,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                2,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);

            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                999,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1000,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1001,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);

            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                999 * 2 + 1,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1000,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1001 * 2 - 1,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);

            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1023,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1024,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1025,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);

            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1023 * 2 + 1,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1024 * 2,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1025 * 2 - 1,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);

            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                10001,
                patternFormat,
                startEndTime.left,
                startEndTime.right,
                isQuery);
          }
        }
      }
    }
  }

  private void testToTabletInsertionEvents(
      final int deviceNumber,
      final int measurementNumber,
      final int rowNumberInOneDevice,
      final String patternFormat,
      final long startTime,
      final long endTime,
      final boolean isQuery)
      throws Exception {
    LOGGER.debug(
        "testToTabletInsertionEvents: deviceNumber: {}, measurementNumber: {}, rowNumberInOneDevice: {}, patternFormat: {}, startTime: {}, endTime: {}",
        deviceNumber,
        measurementNumber,
        rowNumberInOneDevice,
        patternFormat,
        startTime,
        endTime);

    alignedTsFile =
        TsFileGeneratorUtils.generateAlignedTsFile(
            "aligned.tsfile",
            deviceNumber,
            measurementNumber,
            rowNumberInOneDevice,
            (int) TSFILE_START_TIME,
            10000,
            700,
            50);
    nonalignedTsFile =
        TsFileGeneratorUtils.generateNonAlignedTsFile(
            "nonaligned.tsfile",
            deviceNumber,
            measurementNumber,
            rowNumberInOneDevice,
            (int) TSFILE_START_TIME,
            10000,
            700,
            50);

    final int tsfileEndTime = (int) TSFILE_START_TIME + rowNumberInOneDevice - 1;

    int expectedRowNumber = rowNumberInOneDevice;
    Assert.assertTrue(startTime <= endTime);
    if (startTime != Long.MIN_VALUE && endTime != Long.MAX_VALUE) {
      if (startTime < TSFILE_START_TIME) {
        if (endTime < TSFILE_START_TIME) {
          expectedRowNumber = 0;
        } else {
          expectedRowNumber =
              Math.min((int) (endTime - TSFILE_START_TIME + 1), rowNumberInOneDevice);
        }
      } else if (tsfileEndTime < startTime) {
        expectedRowNumber = 0;
      } else {
        expectedRowNumber =
            Math.min(
                (int) (Math.min(endTime, tsfileEndTime) - startTime + 1), rowNumberInOneDevice);
      }
    }

    final PipePattern rootPattern;
    switch (patternFormat) {
      case PREFIX_FORMAT:
        rootPattern = new PrefixPipePattern("root");
        break;
      case IOTDB_FORMAT:
      default:
        rootPattern = new IoTDBPipePattern("root.**");
        break;
    }

    testTsFilePointNum(
        alignedTsFile,
        rootPattern,
        startTime,
        endTime,
        isQuery,
        deviceNumber * expectedRowNumber * measurementNumber);
    testTsFilePointNum(
        nonalignedTsFile,
        rootPattern,
        startTime,
        endTime,
        isQuery,
        deviceNumber * expectedRowNumber * measurementNumber);

    final AtomicReference<String> oneDeviceInAlignedTsFile = new AtomicReference<>();
    final AtomicReference<String> oneMeasurementInAlignedTsFile = new AtomicReference<>();

    final AtomicReference<String> oneDeviceInUnalignedTsFile = new AtomicReference<>();
    final AtomicReference<String> oneMeasurementInUnalignedTsFile = new AtomicReference<>();

    try (final TsFileSequenceReader alignedReader =
            new TsFileSequenceReader(alignedTsFile.getAbsolutePath());
        final TsFileSequenceReader nonalignedReader =
            new TsFileSequenceReader(nonalignedTsFile.getAbsolutePath())) {

      alignedReader
          .getDeviceMeasurementsMap()
          .forEach(
              (k, v) ->
                  v.stream()
                      .filter(p -> p != null && !p.isEmpty())
                      .forEach(
                          p -> {
                            oneDeviceInAlignedTsFile.set(((PlainDeviceID) k).toStringID());
                            oneMeasurementInAlignedTsFile.set(new Path(k, p, false).toString());
                          }));
      nonalignedReader
          .getDeviceMeasurementsMap()
          .forEach(
              (k, v) ->
                  v.stream()
                      .filter(p -> p != null && !p.isEmpty())
                      .forEach(
                          p -> {
                            oneDeviceInUnalignedTsFile.set(((PlainDeviceID) k).toStringID());
                            oneMeasurementInUnalignedTsFile.set(new Path(k, p, false).toString());
                          }));
    }

    final PipePattern oneAlignedDevicePattern;
    final PipePattern oneNonAlignedDevicePattern;
    switch (patternFormat) {
      case PREFIX_FORMAT:
        oneAlignedDevicePattern = new PrefixPipePattern(oneDeviceInAlignedTsFile.get());
        oneNonAlignedDevicePattern = new PrefixPipePattern(oneDeviceInUnalignedTsFile.get());
        break;
      case IOTDB_FORMAT:
      default:
        oneAlignedDevicePattern = new IoTDBPipePattern(oneDeviceInAlignedTsFile.get() + ".**");
        oneNonAlignedDevicePattern = new IoTDBPipePattern(oneDeviceInUnalignedTsFile.get() + ".**");
        break;
    }

    testTsFilePointNum(
        alignedTsFile,
        oneAlignedDevicePattern,
        startTime,
        endTime,
        isQuery,
        expectedRowNumber * measurementNumber);
    testTsFilePointNum(
        nonalignedTsFile,
        oneNonAlignedDevicePattern,
        startTime,
        endTime,
        isQuery,
        expectedRowNumber * measurementNumber);

    final PipePattern oneAlignedMeasurementPattern;
    final PipePattern oneNonAlignedMeasurementPattern;
    switch (patternFormat) {
      case PREFIX_FORMAT:
        oneAlignedMeasurementPattern = new PrefixPipePattern(oneMeasurementInAlignedTsFile.get());
        oneNonAlignedMeasurementPattern =
            new PrefixPipePattern(oneMeasurementInUnalignedTsFile.get());
        break;
      case IOTDB_FORMAT:
      default:
        oneAlignedMeasurementPattern = new IoTDBPipePattern(oneMeasurementInAlignedTsFile.get());
        oneNonAlignedMeasurementPattern =
            new IoTDBPipePattern(oneMeasurementInUnalignedTsFile.get());
        break;
    }

    testTsFilePointNum(
        alignedTsFile,
        oneAlignedMeasurementPattern,
        startTime,
        endTime,
        isQuery,
        expectedRowNumber);
    testTsFilePointNum(
        nonalignedTsFile,
        oneNonAlignedMeasurementPattern,
        startTime,
        endTime,
        isQuery,
        expectedRowNumber);

    final PipePattern notExistPattern;
    switch (patternFormat) {
      case PREFIX_FORMAT:
        notExistPattern = new PrefixPipePattern("root.`not-exist-pattern`");
        break;
      case IOTDB_FORMAT:
      default:
        notExistPattern = new IoTDBPipePattern("root.`not-exist-pattern`");
        break;
    }

    testTsFilePointNum(alignedTsFile, notExistPattern, startTime, endTime, isQuery, 0);
    testTsFilePointNum(nonalignedTsFile, notExistPattern, startTime, endTime, isQuery, 0);
  }

  private void testMixedTsFileWithEmptyChunk(final boolean isQuery) throws IOException {
    final File tsFile = new File("0-0-1-0.tsfile");
    resource = new TsFileResource(tsFile);
    resource.updatePlanIndexes(0);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    try (final CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1", "s2"),
          new TimeRange[] {new TimeRange(10, 40)},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false, true));
      writer.endChunkGroup();
      writer.startChunkGroup("d2");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s0", new TimeRange[] {new TimeRange(10, 40)}, TSEncoding.PLAIN, CompressionType.LZ4);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[] {new TimeRange(40, 40), new TimeRange(50, 70)},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    testTsFilePointNum(
        resource.getTsFile(),
        new PrefixPipePattern("root"),
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        isQuery,
        115);
    resource.remove();
    resource = null;
  }

  private void testPartialNullValue(final boolean isQuery)
      throws IOException, WriteProcessException {
    alignedTsFile = new File("0-0-2-0.tsfile");

    final List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.TIMESTAMP));
    schemaList.add(new MeasurementSchema("s2", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s3", TSDataType.DATE));

    final Tablet t = new Tablet("root.sg.d", schemaList, 1024);
    t.rowSize = 2;
    t.addTimestamp(0, 1000);
    t.addTimestamp(1, 2000);
    t.addValue("s1", 0, 2L);
    t.addValue("s2", 0, null);
    t.addValue("s3", 0, LocalDate.of(2020, 8, 1));
    t.addValue("s1", 1, null);
    t.addValue("s2", 1, new Binary("test", TSFileConfig.STRING_CHARSET));
    t.addValue("s3", 1, LocalDate.of(2024, 8, 1));

    try (final TsFileWriter writer = new TsFileWriter(alignedTsFile)) {
      writer.registerAlignedTimeseries(new Path("root.sg.d"), schemaList);
      writer.writeAligned(t);
    }
    testTsFilePointNum(
        alignedTsFile, new PrefixPipePattern("root"), Long.MIN_VALUE, Long.MAX_VALUE, isQuery, 4);
  }

  private void testTsFilePointNum(
      final File tsFile,
      final PipePattern pattern,
      final long startTime,
      final long endTime,
      final boolean isQuery,
      final int expectedCount) {
    try (final TsFileInsertionDataContainer tsFileContainer =
        isQuery
            ? new TsFileInsertionQueryDataContainer(tsFile, pattern, startTime, endTime)
            : new TsFileInsertionScanDataContainer(
                tsFile, pattern, startTime, endTime, null, null, false)) {
      final AtomicInteger count1 = new AtomicInteger(0);
      final AtomicInteger count2 = new AtomicInteger(0);
      final AtomicInteger count3 = new AtomicInteger(0);

      tsFileContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processRowByRow(
                          (row, collector) -> {
                            try {
                              collector.collectRow(row);
                              count1.addAndGet(getNonNullSize(row));
                            } catch (final IOException e) {
                              throw new RuntimeException(e);
                            }
                          })
                      .forEach(
                          tabletInsertionEvent1 ->
                              tabletInsertionEvent1
                                  .processRowByRow(
                                      (row, collector) -> {
                                        try {
                                          collector.collectRow(row);
                                          count2.addAndGet(getNonNullSize(row));
                                        } catch (final IOException e) {
                                          throw new RuntimeException(e);
                                        }
                                      })
                                  .forEach(
                                      tabletInsertionEvent2 ->
                                          tabletInsertionEvent2.processTabletWithCollect(
                                              (tablet, collector) -> {
                                                try {
                                                  collector.collectTablet(tablet);
                                                  count3.addAndGet(getNonNullSize(tablet));
                                                } catch (final IOException e) {
                                                  throw new RuntimeException(e);
                                                }
                                              }))));

      Assert.assertEquals(expectedCount, count1.get());
      Assert.assertEquals(expectedCount, count2.get());
      Assert.assertEquals(expectedCount, count3.get());
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private int getNonNullSize(final Row row) {
    int count = 0;
    for (int i = 0; i < row.size(); ++i) {
      if (!row.isNull(i)) {
        ++count;
      }
    }
    return count;
  }

  private int getNonNullSize(final Tablet tablet) {
    int count = 0;
    for (int i = 0; i < tablet.rowSize; ++i) {
      for (int j = 0; j < tablet.getSchemas().size(); ++j) {
        if (tablet.bitMaps == null || tablet.bitMaps[j] == null || !tablet.bitMaps[j].isMarked(i)) {
          ++count;
        }
      }
    }
    return count;
  }

  private PipeMemoryBlock getAllocatedChunkMemory(final TsFileInsertionScanDataContainer parser)
      throws NoSuchFieldException, IllegalAccessException {
    final Field field =
        TsFileInsertionScanDataContainer.class.getDeclaredField("allocatedMemoryBlockForChunk");
    field.setAccessible(true);
    return (PipeMemoryBlock) field.get(parser);
  }

  private PipeMemoryBlock getAllocatedBatchDataMemory(final TsFileInsertionScanDataContainer parser)
      throws NoSuchFieldException, IllegalAccessException {
    final Field field =
        TsFileInsertionScanDataContainer.class.getDeclaredField("allocatedMemoryBlockForBatchData");
    field.setAccessible(true);
    return (PipeMemoryBlock) field.get(parser);
  }

  private long calculatePipeMaxReaderChunkSizeForSinglePageAlignedChunk(final File tsFile)
      throws Exception {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      final List<IDeviceID> deviceIDList = reader.getAllDevices();
      Assert.assertEquals(1, deviceIDList.size());
      final IDeviceID deviceID = deviceIDList.get(0);
      final List<AlignedChunkMetadata> alignedChunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID);
      Assert.assertEquals(1, alignedChunkMetadataList.size());

      final AlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(0);
      final Chunk timeChunk =
          reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
      Assert.assertEquals(
          MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER, timeChunk.getHeader().getChunkType() & 0x3F);

      final List<Chunk> valueChunkList = new ArrayList<>();
      long chunkSizeLimit = PipeMemoryWeightUtil.calculateChunkRamBytesUsed(timeChunk);
      for (final IChunkMetadata valueChunkMetadata :
          alignedChunkMetadata.getValueChunkMetadataList()) {
        final Chunk valueChunk = reader.readMemChunk((ChunkMetadata) valueChunkMetadata);
        Assert.assertEquals(
            MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER, valueChunk.getHeader().getChunkType() & 0x3F);
        valueChunkList.add(valueChunk);
        chunkSizeLimit += valueChunk.getHeader().getDataSize();
      }

      final long estimatedPageMemorySize =
          AlignedSinglePageWholeChunkReader.calculatePageEstimatedMemoryUsageInBytes(
              timeChunk, valueChunkList);
      Assert.assertTrue(estimatedPageMemorySize > chunkSizeLimit);
      return chunkSizeLimit;
    }
  }

  private long calculatePipeMaxReaderChunkSizeForSinglePageNonAlignedChunk(final File tsFile)
      throws Exception {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      final IDeviceID deviceID = reader.getDeviceMeasurementsMap().keySet().iterator().next();
      final List<String> measurements = reader.getDeviceMeasurementsMap().get(deviceID);
      Assert.assertFalse(measurements.isEmpty());

      long chunkSizeLimit = 0;
      long estimatedPageMemorySize = 0;
      for (final String measurement : measurements) {
        final List<ChunkMetadata> chunkMetadataList =
            reader.getChunkMetadataList(new Path(deviceID, measurement, false));
        Assert.assertEquals(1, chunkMetadataList.size());

        final Chunk chunk = reader.readMemChunk(chunkMetadataList.get(0));
        Assert.assertEquals(
            MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER, chunk.getHeader().getChunkType() & 0x3F);
        chunkSizeLimit += chunk.getHeader().getDataSize();
        estimatedPageMemorySize +=
            SinglePageWholeChunkReader.calculateMaxPageEstimatedMemoryUsageInBytesWithBatchData(
                chunk);
      }

      Assert.assertTrue(estimatedPageMemorySize > chunkSizeLimit);
      return chunkSizeLimit;
    }
  }

  private long calculatePipeMaxReaderChunkSizeForMultiPageAlignedChunk(final File tsFile)
      throws Exception {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      final List<IDeviceID> deviceIDList = reader.getAllDevices();
      Assert.assertEquals(1, deviceIDList.size());
      final IDeviceID deviceID = deviceIDList.get(0);
      final List<AlignedChunkMetadata> alignedChunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID);
      Assert.assertEquals(1, alignedChunkMetadataList.size());

      final AlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(0);
      final Chunk timeChunk =
          reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
      Assert.assertEquals(MetaMarker.CHUNK_HEADER, timeChunk.getHeader().getChunkType() & 0x3F);

      long chunkSizeLimit = PipeMemoryWeightUtil.calculateChunkRamBytesUsed(timeChunk);
      long estimatedMaxPageMemorySize =
          SinglePageWholeChunkReader.calculateMaxPageEstimatedMemoryUsageInBytes(timeChunk);
      for (final IChunkMetadata valueChunkMetadata :
          alignedChunkMetadata.getValueChunkMetadataList()) {
        final Chunk valueChunk = reader.readMemChunk((ChunkMetadata) valueChunkMetadata);
        Assert.assertEquals(MetaMarker.CHUNK_HEADER, valueChunk.getHeader().getChunkType() & 0x3F);
        chunkSizeLimit += valueChunk.getHeader().getDataSize();
        estimatedMaxPageMemorySize +=
            SinglePageWholeChunkReader.calculateMaxPageEstimatedMemoryUsageInBytes(valueChunk);
      }

      Assert.assertTrue(estimatedMaxPageMemorySize > chunkSizeLimit);
      return chunkSizeLimit;
    }
  }

  private long calculateFirstBatchedAlignedValueChunkGroupMemoryLimit(
      final File tsFile, final int batchSize) throws Exception {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      final IDeviceID deviceID = reader.getDeviceMeasurementsMap().keySet().iterator().next();
      final List<AlignedChunkMetadata> alignedChunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID);
      Assert.assertEquals(2, alignedChunkMetadataList.size());

      final AlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(0);
      final Chunk timeChunk =
          reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
      final List<IChunkMetadata> valueChunkMetadataList =
          alignedChunkMetadata.getValueChunkMetadataList();
      Assert.assertTrue(valueChunkMetadataList.size() >= batchSize * 2);

      final List<Chunk> firstValueChunkBatch = new ArrayList<>();
      final List<Chunk> firstTwoValueChunkBatches = new ArrayList<>();
      long firstBatchChunkSize = PipeMemoryWeightUtil.calculateChunkRamBytesUsed(timeChunk);
      long firstTwoBatchChunkSize = firstBatchChunkSize;
      for (int index = 0; index < batchSize * 2; ++index) {
        final Chunk valueChunk =
            reader.readMemChunk((ChunkMetadata) valueChunkMetadataList.get(index));
        if (index < batchSize) {
          firstValueChunkBatch.add(valueChunk);
          firstBatchChunkSize += valueChunk.getHeader().getDataSize();
        }
        firstTwoValueChunkBatches.add(valueChunk);
        firstTwoBatchChunkSize += valueChunk.getHeader().getDataSize();
      }

      final long firstBatchPageMemorySize =
          AlignedSinglePageWholeChunkReader
              .calculateMaxPageEstimatedMemoryUsageInBytesWithBatchData(
                  timeChunk, firstValueChunkBatch);
      final long firstTwoBatchPageMemorySize =
          AlignedSinglePageWholeChunkReader
              .calculateMaxPageEstimatedMemoryUsageInBytesWithBatchData(
                  timeChunk, firstTwoValueChunkBatches);
      Assert.assertTrue(firstTwoBatchChunkSize > firstBatchChunkSize);
      Assert.assertTrue(firstTwoBatchPageMemorySize > firstBatchPageMemorySize);
      return Math.max(firstBatchChunkSize, firstBatchPageMemorySize);
    }
  }

  private void writeAlignedSourceTsFile(
      final File tsFile, final List<MeasurementSchema> schemaList, final int rowCount)
      throws IOException {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }
    Assert.assertEquals(0, rowCount % 2);

    final IDeviceID deviceID = new PlainDeviceID("root.sg.d");
    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.startChunkGroup(deviceID);
      final int rowCountPerChunk = rowCount / 2;
      for (int chunkIndex = 0; chunkIndex < 2; ++chunkIndex) {
        final AlignedChunkWriterImpl alignedChunkWriter =
            new AlignedChunkWriterImpl(new ArrayList<IMeasurementSchema>(schemaList));
        for (int row = 0; row < rowCountPerChunk; ++row) {
          final long time = (long) chunkIndex * rowCountPerChunk + row;
          alignedChunkWriter.getTimeChunkWriter().write(time);
          for (int measurementIndex = 0; measurementIndex < schemaList.size(); ++measurementIndex) {
            alignedChunkWriter
                .getValueChunkWriterByIndex(measurementIndex)
                .write(time, time * 100 + measurementIndex, false);
          }
        }
        alignedChunkWriter.writeToFileWriter(writer);
      }
      writer.endChunkGroup();
      writer.endFile();
    }
  }

  private void rewriteAlignedTsFileWithBatchedValueChunks(
      final File sourceTsFile,
      final File targetTsFile,
      final int measurementCount,
      final int batchSize)
      throws Exception {
    if (targetTsFile.exists()) {
      Assert.assertTrue(targetTsFile.delete());
    }

    try (final TsFileSequenceReader reader =
        new TsFileSequenceReader(sourceTsFile.getAbsolutePath())) {
      final IDeviceID deviceID = reader.getDeviceMeasurementsMap().keySet().iterator().next();
      final List<AlignedChunkMetadata> sourceAlignedChunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID);
      Assert.assertEquals(2, sourceAlignedChunkMetadataList.size());
      for (final AlignedChunkMetadata sourceAlignedChunkMetadata : sourceAlignedChunkMetadataList) {
        Assert.assertEquals(
            measurementCount, sourceAlignedChunkMetadata.getValueChunkMetadataList().size());
      }

      try (final CompactionTsFileWriter writer =
          new CompactionTsFileWriter(
              targetTsFile, Long.MAX_VALUE, CompactionType.INNER_SEQ_COMPACTION)) {
        writer.startChunkGroup(deviceID);
        writer.markStartingWritingAligned();
        for (final AlignedChunkMetadata sourceAlignedChunkMetadata :
            sourceAlignedChunkMetadataList) {
          final ChunkMetadata timeChunkMetadata =
              (ChunkMetadata) sourceAlignedChunkMetadata.getTimeChunkMetadata();
          writer.writeChunk(reader.readMemChunk(timeChunkMetadata), timeChunkMetadata);
        }

        for (int start = 0; start < measurementCount; start += batchSize) {
          writeValueChunkBatch(
              reader,
              writer,
              sourceAlignedChunkMetadataList,
              start,
              Math.min(start + batchSize, measurementCount));
        }
        writer.markEndingWritingAligned();
        writer.endChunkGroup();
        writer.endFile();
      }
    }
  }

  private void writeValueChunkBatch(
      final TsFileSequenceReader reader,
      final CompactionTsFileWriter writer,
      final List<AlignedChunkMetadata> alignedChunkMetadataList,
      final int start,
      final int end)
      throws IOException {
    for (final AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
      final List<IChunkMetadata> valueChunkMetadataList =
          alignedChunkMetadata.getValueChunkMetadataList();
      for (int index = start; index < end; ++index) {
        final ChunkMetadata valueChunkMetadata = (ChunkMetadata) valueChunkMetadataList.get(index);
        writer.writeChunk(reader.readMemChunk(valueChunkMetadata), valueChunkMetadata);
      }
    }
  }
}
