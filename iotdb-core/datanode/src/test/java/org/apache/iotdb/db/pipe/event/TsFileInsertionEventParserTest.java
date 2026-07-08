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
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.query.TsFileInsertionEventQueryParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.AlignedSinglePageWholeChunkReader;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.SinglePageWholeChunkReader;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.TsFileInsertionEventScanParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.table.TsFileInsertionEventTableParser;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
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
import org.junit.Assume;
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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.fail;

public class TsFileInsertionEventParserTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TsFileInsertionEventParserTest.class);

  private static final long TSFILE_START_TIME = 300L;

  private static final String PREFIX_FORMAT = "prefix";
  private static final String IOTDB_FORMAT = "iotdb";
  private static final String MANUAL_SCAN_PARSER_PERFORMANCE_TEST =
      "iotdb.scan.parser.performance.enabled";
  private static final String MANUAL_QUERY_PARSER_PERFORMANCE_TEST =
      "iotdb.query.parser.performance.enabled";
  private static final String MANUAL_TABLE_PARSER_PERFORMANCE_TEST =
      "iotdb.table.parser.performance.enabled";
  private static final long BITMAP_TEST_PIPE_MAX_READER_CHUNK_SIZE = 1024 * 1024L;

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
  public void testQueryParser() throws Exception {
    final long startTime = System.currentTimeMillis();
    testToTabletInsertionEvents(true);
    System.out.println(System.currentTimeMillis() - startTime);
  }

  @Test
  public void testScanParser() throws Exception {
    final long startTime = System.currentTimeMillis();
    testToTabletInsertionEvents(false);
    System.out.println(System.currentTimeMillis() - startTime);
  }

  @Test
  public void testScanParserReleasesTabletMemoryAfterRawTabletGenerated() throws Exception {
    nonalignedTsFile =
        TsFileGeneratorUtils.generateNonAlignedTsFile(
            "nonaligned-release-tablet-memory.tsfile", 1, 1, 10, 0, 100, 10, 10);

    try (final TsFileInsertionEventScanParser parser =
        new TsFileInsertionEventScanParser(
            nonalignedTsFile,
            new PrefixTreePattern("root"),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            null,
            null,
            false)) {
      final Iterator<TabletInsertionEvent> iterator = parser.toTabletInsertionEvents().iterator();

      Assert.assertTrue(iterator.hasNext());
      final TabletInsertionEvent event = iterator.next();
      Assert.assertTrue(event instanceof PipeRawTabletInsertionEvent);
      Assert.assertEquals(0, getAllocatedTabletMemory(parser).getMemoryUsageInBytes());

      ((PipeRawTabletInsertionEvent) event).clearReferenceCount(getClass().getName());
    }
  }

  @Test
  public void testConsumeTabletInsertionEventsWithRetryReleasesParserOnOutOfMemory()
      throws Exception {
    nonalignedTsFile =
        TsFileGeneratorUtils.generateNonAlignedTsFile(
            "nonaligned-consume-oom.tsfile", 1, 1, 10, 0, 100, 10, 10);
    resource = new TsFileResource(nonalignedTsFile);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);

    // The TsFile generator only creates the file, so mark the resource non-empty explicitly.
    final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d0");
    resource.updateStartTime(deviceID, 0);
    resource.updateEndTime(deviceID, 9);

    final PipeTsFileInsertionEvent event =
        new PipeTsFileInsertionEvent(
            false,
            "root",
            resource,
            null,
            false,
            false,
            false,
            null,
            null,
            0,
            null,
            new PrefixTreePattern("root"),
            null,
            null,
            null,
            null,
            true,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    final AtomicReference<PipeRawTabletInsertionEvent> parsedEventReference =
        new AtomicReference<>();

    final PipeRuntimeOutOfMemoryCriticalException exception =
        Assert.assertThrows(
            PipeRuntimeOutOfMemoryCriticalException.class,
            () ->
                event.consumeTabletInsertionEventsWithRetry(
                    parsedEvent -> {
                      parsedEventReference.set(parsedEvent);
                      throw new PipeRuntimeOutOfMemoryCriticalException("expected oom");
                    },
                    "test"));

    Assert.assertEquals("expected oom", exception.getMessage());
    Assert.assertNotNull(parsedEventReference.get());
    Assert.assertTrue(parsedEventReference.get().isReleased());
    Assert.assertNull(getEventParser(event).get());
  }

  @Test
  public void testConsumeTabletInsertionEventsWithRetryKeepsParserForTransientOutOfMemory()
      throws Exception {
    nonalignedTsFile =
        TsFileGeneratorUtils.generateNonAlignedTsFile(
            "nonaligned-consume-transient-oom.tsfile", 1, 1, 10, 0, 100, 10, 10);
    resource = new TsFileResource(nonalignedTsFile);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);

    final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d0");
    resource.updateStartTime(deviceID, 0);
    resource.updateEndTime(deviceID, 9);

    final PipeTsFileInsertionEvent event =
        new PipeTsFileInsertionEvent(
            false,
            "root",
            resource,
            null,
            false,
            false,
            false,
            null,
            null,
            0,
            null,
            new PrefixTreePattern("root"),
            null,
            null,
            null,
            null,
            true,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    final AtomicInteger retryCount = new AtomicInteger(0);
    final AtomicReference<PipeRawTabletInsertionEvent> parsedEventReference =
        new AtomicReference<>();

    event.consumeTabletInsertionEventsWithRetry(
        parsedEvent -> {
          parsedEventReference.set(parsedEvent);
          if (retryCount.getAndIncrement() == 0) {
            throw new PipeRuntimeOutOfMemoryCriticalException("transient oom");
          }
          parsedEvent.clearReferenceCount(getClass().getName());
        },
        "test");

    Assert.assertEquals(2, retryCount.get());
    Assert.assertNotNull(parsedEventReference.get());
    Assert.assertTrue(parsedEventReference.get().isReleased());
    Assert.assertNotNull(getEventParser(event).get());

    event.close();
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
      final List<IMeasurementSchema> schemaList = new ArrayList<>();
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

      try (final TsFileWriter writer = new TsFileWriter(nonalignedTsFile)) {
        writer.registerTimeseries(new PartialPath("root.sg.d"), schemaList);
        writer.writeTree(tablet);
      }

      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(
              calculatePipeMaxReaderChunkSizeForSinglePageNonAlignedChunk(nonalignedTsFile));

      int tabletCount = 0;
      int maxMeasurementCount = 0;
      int pointCount = 0;
      try (final TsFileInsertionEventScanParser parser =
          new TsFileInsertionEventScanParser(
              nonalignedTsFile,
              new PrefixTreePattern("root"),
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
      final List<IMeasurementSchema> schemaList = new ArrayList<>();
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

      try (final TsFileWriter writer = new TsFileWriter(alignedTsFile)) {
        writer.registerAlignedTimeseries(new PartialPath("root.sg.d"), schemaList);
        writer.writeAligned(tablet);
      }

      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(
              calculatePipeMaxReaderChunkSizeForSinglePageAlignedChunk(alignedTsFile));

      int tabletCount = 0;
      int maxMeasurementCount = 0;
      int pointCount = 0;
      try (final TsFileInsertionEventScanParser parser =
          new TsFileInsertionEventScanParser(
              alignedTsFile,
              new PrefixTreePattern("root"),
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
      final List<IMeasurementSchema> schemaList = new ArrayList<>();
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

      try (final TsFileWriter writer = new TsFileWriter(alignedTsFile)) {
        writer.registerAlignedTimeseries(new PartialPath("root.sg.d"), schemaList);
        writer.writeAligned(tablet);
      }

      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(
              calculatePipeMaxReaderChunkSizeForMultiPageAlignedChunk(alignedTsFile));

      int tabletCount = 0;
      int maxMeasurementCount = 0;
      int pointCount = 0;
      try (final TsFileInsertionEventScanParser parser =
          new TsFileInsertionEventScanParser(
              alignedTsFile,
              new PrefixTreePattern("root"),
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
    final List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));

    final Tablet tablet = new Tablet("root.sg.d", schemaList, 2);
    tablet.addTimestamp(0, 1);
    tablet.addValue("s1", 0, 1L);
    tablet.addTimestamp(1, 2);
    tablet.addValue("s1", 1, 2L);

    try {
      try (final TsFileWriter writer = new TsFileWriter(alignedTsFile)) {
        writer.registerAlignedTimeseries(new PartialPath("root.sg.d"), schemaList);
        writer.writeAligned(tablet);
      }

      try (final TsFileInsertionEventScanParser parser =
          new TsFileInsertionEventScanParser(
              alignedTsFile,
              new PrefixTreePattern("root"),
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

      final List<IMeasurementSchema> schemaList = new ArrayList<>();
      for (int i = 0; i < measurementCount; ++i) {
        schemaList.add(new MeasurementSchema("s" + i, TSDataType.INT64));
      }

      writeAlignedSourceTsFile(sourceTsFile, schemaList, rowCount);
      rewriteAlignedTsFileWithBatchedValueChunks(
          sourceTsFile, alignedTsFile, measurementCount, batchSize);

      int tabletCount = 0;
      int pointCount = 0;
      try (final TsFileInsertionEventScanParser parser =
          new TsFileInsertionEventScanParser(
              alignedTsFile,
              new PrefixTreePattern("root"),
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
          Assert.assertEquals(rowCount / 2, tablet.getRowSize());
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

      final List<IMeasurementSchema> schemaList = new ArrayList<>();
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
      try (final TsFileInsertionEventScanParser parser =
          new TsFileInsertionEventScanParser(
              alignedTsFile,
              new PrefixTreePattern("root"),
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
          Assert.assertEquals(rowCount / 2, tablet.getRowSize());
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

  @Test
  public void testQueryParserSkipsUnnecessaryBitMaps() throws Exception {
    testTreeParserSkipsUnnecessaryBitMaps(true);
  }

  @Test
  public void testScanParserSkipsUnnecessaryBitMaps() throws Exception {
    testTreeParserSkipsUnnecessaryBitMaps(false);
  }

  @Test
  public void testTableParserSkipsUnnecessaryBitMaps() throws Exception {
    final long originalPipeMaxReaderChunkSize =
        PipeConfig.getInstance().getPipeMaxReaderChunkSize();
    CommonDescriptor.getInstance()
        .getConfig()
        .setPipeMaxReaderChunkSize(BITMAP_TEST_PIPE_MAX_READER_CHUNK_SIZE);

    try {
      alignedTsFile = new File("table-parser-bitmap.tsfile");
      if (alignedTsFile.exists()) {
        Assert.assertTrue(alignedTsFile.delete());
      }

      final List<IMeasurementSchema> schemaList =
          Arrays.asList(
              new MeasurementSchema("tag0", TSDataType.STRING),
              new MeasurementSchema("dense", TSDataType.INT64),
              new MeasurementSchema("sparse", TSDataType.INT64));
      final List<String> columnNameList = Arrays.asList("tag0", "dense", "sparse");
      final List<TSDataType> dataTypeList =
          Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.INT64);
      final List<ColumnCategory> columnCategoryList =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);

      final Tablet tablet =
          new Tablet("bitmap_table", columnNameList, dataTypeList, columnCategoryList, 2);
      for (int rowIndex = 0; rowIndex < 2; ++rowIndex) {
        tablet.addTimestamp(rowIndex, rowIndex);
        tablet.addValue(rowIndex, 0, "tag-value");
        tablet.addValue(rowIndex, 1, (long) rowIndex);
        tablet.addValue("sparse", rowIndex, rowIndex == 0 ? 100L : null);
      }

      try (final TsFileWriter writer = new TsFileWriter(alignedTsFile)) {
        writer.registerTableSchema(new TableSchema("bitmap_table", schemaList, columnCategoryList));
        writer.writeTable(tablet);
      }

      try (final TsFileInsertionEventTableParser parser =
          new TsFileInsertionEventTableParser(
              alignedTsFile,
              new TablePattern(true, null, null),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null,
              null,
              false)) {
        final Iterator<TabletInsertionEvent> iterator = parser.toTabletInsertionEvents().iterator();
        Assert.assertTrue(iterator.hasNext());
        Tablet parsedTablet = ((PipeRawTabletInsertionEvent) iterator.next()).convertToTablet();
        if (parsedTablet.getSchemas().size() == 3) {
          assertBitMapExistence(parsedTablet, false, false, true);
          Assert.assertTrue(parsedTablet.isNull(1, 2));
          Assert.assertFalse(iterator.hasNext());
        } else {
          if (parsedTablet.getSchemas().get(1).getMeasurementName().equals("dense")) {
            Assert.assertNull(parsedTablet.getBitMaps());
          } else {
            Assert.assertTrue(parsedTablet.isNull(1, 1));
          }
          while (iterator.hasNext()) {
            parsedTablet = ((PipeRawTabletInsertionEvent) iterator.next()).convertToTablet();
            if (parsedTablet.getSchemas().get(1).getMeasurementName().equals("dense")) {
              Assert.assertNull(parsedTablet.getBitMaps());
            } else {
              Assert.assertTrue(parsedTablet.isNull(1, 1));
            }
          }
        }
      }
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(originalPipeMaxReaderChunkSize);
    }
  }

  @Test
  public void testTableParserWithTablePatternReportsLastNonEmptyTablet() throws Exception {
    final int originalPipeDataStructureTabletRowSize =
        PipeConfig.getInstance().getPipeDataStructureTabletRowSize();
    CommonDescriptor.getInstance().getConfig().setPipeDataStructureTabletRowSize(2);

    try {
      alignedTsFile = new File("table-parser-table-pattern.tsfile");
      if (alignedTsFile.exists()) {
        Assert.assertTrue(alignedTsFile.delete());
      }

      final List<IMeasurementSchema> schemaList =
          Arrays.asList(
              new MeasurementSchema("tag0", TSDataType.STRING),
              new MeasurementSchema("s0", TSDataType.INT64));
      final List<String> columnNameList = Arrays.asList("tag0", "s0");
      final List<TSDataType> dataTypeList = Arrays.asList(TSDataType.STRING, TSDataType.INT64);
      final List<ColumnCategory> columnCategoryList =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);

      try (final TsFileWriter writer = new TsFileWriter(alignedTsFile)) {
        writer.registerTableSchema(new TableSchema("test", schemaList, columnCategoryList));
        writer.registerTableSchema(new TableSchema("test1", schemaList, columnCategoryList));
        writer.writeTable(
            generateSimpleTableTablet(
                "test", columnNameList, dataTypeList, columnCategoryList, "ignored", 0, 2));
        writer.writeTable(
            generateSimpleTableTablet(
                "test1", columnNameList, dataTypeList, columnCategoryList, "matched", 3, 4));
        writer.writeTable(
            generateSimpleTableTablet(
                "test1", columnNameList, dataTypeList, columnCategoryList, "unmatched", 2, 10));
      }

      try (final TsFileInsertionEventTableParser parser =
          new TsFileInsertionEventTableParser(
              alignedTsFile,
              new TablePattern(true, null, "test1"),
              3,
              5,
              null,
              null,
              null,
              false)) {
        final Iterator<TabletInsertionEvent> iterator = parser.toTabletInsertionEvents().iterator();
        int rowCount = 0;
        PipeRawTabletInsertionEvent lastEvent = null;
        while (iterator.hasNext()) {
          final PipeRawTabletInsertionEvent event = (PipeRawTabletInsertionEvent) iterator.next();
          final Tablet tablet = event.convertToTablet();
          Assert.assertEquals("test1", tablet.getTableName());
          Assert.assertFalse(PipeRawTabletInsertionEvent.isTabletEmpty(tablet));
          rowCount += tablet.getRowSize();
          if (lastEvent != null) {
            Assert.assertFalse(lastEvent.isNeedToReport());
          }
          lastEvent = event;
        }

        Assert.assertEquals(2, rowCount);
        Assert.assertNotNull(lastEvent);
        Assert.assertTrue(lastEvent.isNeedToReport());
      }
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeDataStructureTabletRowSize(originalPipeDataStructureTabletRowSize);
    }
  }

  @Test
  public void manualTestScanParserSplitPerformance() throws Exception {
    Assume.assumeTrue(
        "Set -D" + MANUAL_SCAN_PARSER_PERFORMANCE_TEST + "=true to run this manual test.",
        Boolean.getBoolean(MANUAL_SCAN_PARSER_PERFORMANCE_TEST));

    final int deviceCount =
        getManualPerformanceIntProperty("iotdb.scan.parser.performance.device.count", 1);
    final int measurementCount =
        getManualPerformanceIntProperty("iotdb.scan.parser.performance.measurement.count", 256);
    final int rowCountPerDevice =
        getManualPerformanceIntProperty("iotdb.scan.parser.performance.row.count", 200_000);
    final int tabletRowCount =
        getManualPerformanceIntProperty("iotdb.scan.parser.performance.tablet.row.count", 1024);
    final long pipeMaxReaderChunkSize =
        getManualPerformanceLongProperty(
            "iotdb.scan.parser.performance.reader.chunk.size", 1024 * 1024L);
    final long expectedPointCount = (long) deviceCount * measurementCount * rowCountPerDevice;
    final long originalPipeMaxReaderChunkSize =
        PipeConfig.getInstance().getPipeMaxReaderChunkSize();

    CommonDescriptor.getInstance().getConfig().setPipeMaxReaderChunkSize(pipeMaxReaderChunkSize);

    alignedTsFile = new File("scan-parser-split-performance.tsfile");
    try {
      final List<IMeasurementSchema> schemaList = new ArrayList<>();
      for (int i = 0; i < measurementCount; ++i) {
        schemaList.add(
            new MeasurementSchema(
                "s" + i, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.LZ4));
      }

      final long writeStartTime = System.nanoTime();
      generateLargeAlignedTsFile(
          alignedTsFile, schemaList, deviceCount, rowCountPerDevice, tabletRowCount);
      final long writeElapsedNanos = System.nanoTime() - writeStartTime;

      long pointCount = 0;
      long tabletRowCountSum = 0;
      int tabletCount = 0;
      int alignedTabletCount = 0;
      int minMeasurementCountInTablet = Integer.MAX_VALUE;
      int maxMeasurementCountInTablet = 0;

      final long parseStartTime = System.nanoTime();
      try (final TsFileInsertionEventScanParser parser =
          new TsFileInsertionEventScanParser(
              alignedTsFile,
              new PrefixTreePattern("root"),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null,
              false)) {
        for (final Pair<Tablet, Boolean> tabletWithIsAligned : parser.toTabletWithIsAligneds()) {
          final Tablet tablet = tabletWithIsAligned.getLeft();
          ++tabletCount;
          if (tabletWithIsAligned.getRight()) {
            ++alignedTabletCount;
          }
          tabletRowCountSum += tablet.getRowSize();
          // The generated performance file is dense and mod-free. Avoid scanning every cell here,
          // otherwise parseTime also includes the test-side validation cost.
          pointCount += (long) tablet.getRowSize() * tablet.getSchemas().size();
          minMeasurementCountInTablet =
              Math.min(minMeasurementCountInTablet, tablet.getSchemas().size());
          maxMeasurementCountInTablet =
              Math.max(maxMeasurementCountInTablet, tablet.getSchemas().size());
        }
      }
      final long parseElapsedNanos = System.nanoTime() - parseStartTime;

      Assert.assertEquals(expectedPointCount, pointCount);
      Assert.assertTrue(
          "Expected TsFileInsertionEventScanParser to split tablets.", tabletCount > 1);
      Assert.assertTrue(
          "Expected measurement split by pipe max reader chunk size.",
          maxMeasurementCountInTablet < measurementCount);

      printScanParserPerformanceResult(
          alignedTsFile.length(),
          deviceCount,
          measurementCount,
          rowCountPerDevice,
          tabletRowCount,
          pipeMaxReaderChunkSize,
          expectedPointCount,
          writeElapsedNanos,
          parseElapsedNanos,
          tabletCount,
          alignedTabletCount,
          tabletRowCountSum,
          pointCount,
          minMeasurementCountInTablet,
          maxMeasurementCountInTablet);
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(originalPipeMaxReaderChunkSize);
    }
  }

  @Test
  public void manualTestQueryParserPerformance() throws Exception {
    Assume.assumeTrue(
        "Set -D" + MANUAL_QUERY_PARSER_PERFORMANCE_TEST + "=true to run this manual test.",
        Boolean.getBoolean(MANUAL_QUERY_PARSER_PERFORMANCE_TEST));

    final int deviceCount =
        getManualPerformanceIntProperty("iotdb.query.parser.performance.device.count", 1);
    final int measurementCount =
        getManualPerformanceIntProperty("iotdb.query.parser.performance.measurement.count", 256);
    final int rowCountPerDevice =
        getManualPerformanceIntProperty("iotdb.query.parser.performance.row.count", 200_000);
    final int tabletRowCount =
        getManualPerformanceIntProperty("iotdb.query.parser.performance.tablet.row.count", 1024);
    final long expectedPointCount = (long) deviceCount * measurementCount * rowCountPerDevice;

    alignedTsFile = new File("query-parser-performance.tsfile");
    final List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < measurementCount; ++i) {
      schemaList.add(
          new MeasurementSchema("s" + i, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.LZ4));
    }

    final long writeStartTime = System.nanoTime();
    generateLargeAlignedTsFile(
        alignedTsFile, schemaList, deviceCount, rowCountPerDevice, tabletRowCount);
    final long writeElapsedNanos = System.nanoTime() - writeStartTime;

    final ParserPerformanceStats stats;
    final long parseStartTime = System.nanoTime();
    try (final TsFileInsertionEventQueryParser parser =
        new TsFileInsertionEventQueryParser(
            alignedTsFile, new PrefixTreePattern("root"), Long.MIN_VALUE, Long.MAX_VALUE, null)) {
      stats =
          collectTabletInsertionEventParserPerformanceStats(
              parser.toTabletInsertionEvents(), false);
    }
    final long parseElapsedNanos = System.nanoTime() - parseStartTime;

    Assert.assertEquals(expectedPointCount, stats.pointCount);
    Assert.assertTrue(
        "Expected TsFileInsertionEventQueryParser to parse tablets.", stats.tabletCount > 0);

    printTabletInsertionEventParserPerformanceResult(
        "TsFileInsertionEventQueryParser",
        alignedTsFile.length(),
        String.format(
            Locale.ROOT,
            "deviceCount=%d, measurementCount=%d, rowCountPerDevice=%d",
            deviceCount,
            measurementCount,
            rowCountPerDevice),
        tabletRowCount,
        "",
        expectedPointCount,
        writeElapsedNanos,
        parseElapsedNanos,
        stats,
        "measurement");
  }

  @Test
  public void manualTestTableParserSplitPerformance() throws Exception {
    Assume.assumeTrue(
        "Set -D" + MANUAL_TABLE_PARSER_PERFORMANCE_TEST + "=true to run this manual test.",
        Boolean.getBoolean(MANUAL_TABLE_PARSER_PERFORMANCE_TEST));

    final int tableCount =
        getManualPerformanceIntProperty("iotdb.table.parser.performance.table.count", 1);
    final int deviceCount =
        getManualPerformanceIntProperty("iotdb.table.parser.performance.device.count", 1);
    final int tagCount =
        getManualPerformanceIntProperty("iotdb.table.parser.performance.tag.count", 1);
    final int fieldCount =
        getManualPerformanceIntProperty("iotdb.table.parser.performance.field.count", 256);
    final int rowCountPerDevice =
        getManualPerformanceIntProperty("iotdb.table.parser.performance.row.count", 200_000);
    final int tabletRowCount =
        getManualPerformanceIntProperty("iotdb.table.parser.performance.tablet.row.count", 1024);
    final long pipeMaxReaderChunkSize =
        getManualPerformanceLongProperty(
            "iotdb.table.parser.performance.reader.chunk.size", 1024 * 1024L);
    final long expectedPointCount =
        (long) tableCount * deviceCount * fieldCount * rowCountPerDevice;
    final long originalPipeMaxReaderChunkSize =
        PipeConfig.getInstance().getPipeMaxReaderChunkSize();

    CommonDescriptor.getInstance().getConfig().setPipeMaxReaderChunkSize(pipeMaxReaderChunkSize);

    alignedTsFile = new File("table-parser-split-performance.tsfile");
    try {
      final long writeStartTime = System.nanoTime();
      generateLargeTableTsFile(
          alignedTsFile,
          tableCount,
          deviceCount,
          tagCount,
          fieldCount,
          rowCountPerDevice,
          tabletRowCount);
      final long writeElapsedNanos = System.nanoTime() - writeStartTime;

      final ParserPerformanceStats stats;
      final long parseStartTime = System.nanoTime();
      try (final TsFileInsertionEventTableParser parser =
          new TsFileInsertionEventTableParser(
              alignedTsFile,
              new TablePattern(true, null, null),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null,
              null,
              false)) {
        stats =
            collectTabletInsertionEventParserPerformanceStats(
                parser.toTabletInsertionEvents(), true);
      }
      final long parseElapsedNanos = System.nanoTime() - parseStartTime;

      Assert.assertEquals(expectedPointCount, stats.pointCount);
      Assert.assertTrue(
          "Expected TsFileInsertionEventTableParser to split tablets.", stats.tabletCount > 1);
      Assert.assertTrue(
          "Expected field split by pipe max reader chunk size.",
          fieldCount == 1 || stats.maxColumnCountInTablet < fieldCount);

      printTabletInsertionEventParserPerformanceResult(
          "TsFileInsertionEventTableParser split",
          alignedTsFile.length(),
          String.format(
              Locale.ROOT,
              "tableCount=%d, deviceCount=%d, tagCount=%d, fieldCount=%d, rowCountPerDevice=%d",
              tableCount,
              deviceCount,
              tagCount,
              fieldCount,
              rowCountPerDevice),
          tabletRowCount,
          ", pipeMaxReaderChunkSize=" + formatBytes(pipeMaxReaderChunkSize),
          expectedPointCount,
          writeElapsedNanos,
          parseElapsedNanos,
          stats,
          "field");
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(originalPipeMaxReaderChunkSize);
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

    final TreePattern rootPattern;
    switch (patternFormat) {
      case PREFIX_FORMAT:
        rootPattern = new PrefixTreePattern("root");
        break;
      case IOTDB_FORMAT:
      default:
        rootPattern = new IoTDBTreePattern("root.**");
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
                            oneDeviceInAlignedTsFile.set(k.toString());
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
                            oneDeviceInUnalignedTsFile.set((k.toString()));
                            oneMeasurementInUnalignedTsFile.set(new Path(k, p, false).toString());
                          }));
    }

    final TreePattern oneAlignedDevicePattern;
    final TreePattern oneNonAlignedDevicePattern;
    switch (patternFormat) {
      case PREFIX_FORMAT:
        oneAlignedDevicePattern = new PrefixTreePattern(oneDeviceInAlignedTsFile.get());
        oneNonAlignedDevicePattern = new PrefixTreePattern(oneDeviceInUnalignedTsFile.get());
        break;
      case IOTDB_FORMAT:
      default:
        oneAlignedDevicePattern = new IoTDBTreePattern(oneDeviceInAlignedTsFile.get() + ".**");
        oneNonAlignedDevicePattern = new IoTDBTreePattern(oneDeviceInUnalignedTsFile.get() + ".**");
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

    final TreePattern oneAlignedMeasurementPattern;
    final TreePattern oneNonAlignedMeasurementPattern;
    switch (patternFormat) {
      case PREFIX_FORMAT:
        oneAlignedMeasurementPattern = new PrefixTreePattern(oneMeasurementInAlignedTsFile.get());
        oneNonAlignedMeasurementPattern =
            new PrefixTreePattern(oneMeasurementInUnalignedTsFile.get());
        break;
      case IOTDB_FORMAT:
      default:
        oneAlignedMeasurementPattern = new IoTDBTreePattern(oneMeasurementInAlignedTsFile.get());
        oneNonAlignedMeasurementPattern =
            new IoTDBTreePattern(oneMeasurementInUnalignedTsFile.get());
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

    final TreePattern notExistPattern;
    switch (patternFormat) {
      case PREFIX_FORMAT:
        notExistPattern = new PrefixTreePattern("root.`not-exist-pattern`");
        break;
      case IOTDB_FORMAT:
      default:
        notExistPattern = new IoTDBTreePattern("root.`not-exist-pattern`");
        break;
    }

    testTsFilePointNum(alignedTsFile, notExistPattern, startTime, endTime, isQuery, 0);
    testTsFilePointNum(nonalignedTsFile, notExistPattern, startTime, endTime, isQuery, 0);
  }

  private void testMixedTsFileWithEmptyChunk(final boolean isQuery) throws Exception {
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
        new PrefixTreePattern("root"),
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        isQuery,
        115);
    resource.remove();
    resource = null;
  }

  private void testPartialNullValue(final boolean isQuery) throws Exception {
    alignedTsFile = new File("0-0-2-0.tsfile");

    final List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.TIMESTAMP));
    schemaList.add(new MeasurementSchema("s2", TSDataType.STRING));
    schemaList.add(new MeasurementSchema("s3", TSDataType.DATE));

    final Tablet t = new Tablet("root.sg.d", schemaList, 1024);
    t.addTimestamp(0, 1000);
    t.addTimestamp(1, 2000);
    t.addValue("s1", 0, 2L);
    t.addValue("s2", 0, null);
    t.addValue("s3", 0, LocalDate.of(2020, 8, 1));
    t.addValue("s1", 1, null);
    t.addValue("s2", 1, new Binary("test", TSFileConfig.STRING_CHARSET));
    t.addValue("s3", 1, LocalDate.of(2024, 8, 1));

    try (final TsFileWriter writer = new TsFileWriter(alignedTsFile)) {
      writer.registerAlignedTimeseries(new PartialPath("root.sg.d"), schemaList);
      writer.writeAligned(t);
    }
    testTsFilePointNum(
        alignedTsFile, new PrefixTreePattern("root"), Long.MIN_VALUE, Long.MAX_VALUE, isQuery, 4);
  }

  private void testTreeParserSkipsUnnecessaryBitMaps(final boolean isQuery) throws Exception {
    final long originalPipeMaxReaderChunkSize =
        PipeConfig.getInstance().getPipeMaxReaderChunkSize();
    CommonDescriptor.getInstance()
        .getConfig()
        .setPipeMaxReaderChunkSize(BITMAP_TEST_PIPE_MAX_READER_CHUNK_SIZE);

    try {
      alignedTsFile =
          new File(isQuery ? "query-parser-bitmap.tsfile" : "scan-parser-bitmap.tsfile");
      if (alignedTsFile.exists()) {
        Assert.assertTrue(alignedTsFile.delete());
      }

      final List<IMeasurementSchema> schemaList =
          Arrays.asList(
              new MeasurementSchema("dense", TSDataType.INT64),
              new MeasurementSchema("sparse", TSDataType.INT64));
      final Tablet tablet = new Tablet("root.sg.d", schemaList, 2);
      for (int rowIndex = 0; rowIndex < 2; ++rowIndex) {
        tablet.addTimestamp(rowIndex, rowIndex);
        tablet.addValue("dense", rowIndex, (long) rowIndex);
        tablet.addValue("sparse", rowIndex, rowIndex == 0 ? 100L : null);
      }

      try (final TsFileWriter writer = new TsFileWriter(alignedTsFile)) {
        writer.registerAlignedTimeseries(new PartialPath("root.sg.d"), schemaList);
        writer.writeAligned(tablet);
      }

      try (final TsFileInsertionEventParser parser =
          isQuery
              ? new TsFileInsertionEventQueryParser(
                  alignedTsFile,
                  new PrefixTreePattern("root"),
                  Long.MIN_VALUE,
                  Long.MAX_VALUE,
                  null)
              : new TsFileInsertionEventScanParser(
                  alignedTsFile,
                  new PrefixTreePattern("root"),
                  Long.MIN_VALUE,
                  Long.MAX_VALUE,
                  null,
                  null,
                  false)) {
        final Iterator<TabletInsertionEvent> iterator = parser.toTabletInsertionEvents().iterator();
        Assert.assertTrue(iterator.hasNext());
        Tablet parsedTablet = ((PipeRawTabletInsertionEvent) iterator.next()).convertToTablet();
        if (parsedTablet.getSchemas().size() > 1) {
          assertBitMapExistenceByMeasurement(parsedTablet, Map.of("dense", false, "sparse", true));
          Assert.assertTrue(parsedTablet.isNull(1, getColumnIndex(parsedTablet, "sparse")));
          Assert.assertFalse(iterator.hasNext());
        } else {
          Assert.assertNull(parsedTablet.getBitMaps());
          Assert.assertTrue(iterator.hasNext());
          while (iterator.hasNext()) {
            parsedTablet = ((PipeRawTabletInsertionEvent) iterator.next()).convertToTablet();
            Assert.assertNull(parsedTablet.getBitMaps());
          }
        }
      }
    } finally {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeMaxReaderChunkSize(originalPipeMaxReaderChunkSize);
    }
  }

  private void assertBitMapExistence(
      final Tablet tablet, final boolean... expectedColumnHasBitMap) {
    final BitMap[] bitMaps = tablet.getBitMaps();
    Assert.assertNotNull(bitMaps);
    Assert.assertEquals(expectedColumnHasBitMap.length, bitMaps.length);
    for (int i = 0; i < expectedColumnHasBitMap.length; ++i) {
      if (expectedColumnHasBitMap[i]) {
        Assert.assertNotNull(bitMaps[i]);
      } else {
        Assert.assertNull(bitMaps[i]);
      }
    }
  }

  private void assertBitMapExistenceByMeasurement(
      final Tablet tablet, final Map<String, Boolean> expectedMeasurementHasBitMap) {
    final BitMap[] bitMaps = tablet.getBitMaps();
    Assert.assertNotNull(bitMaps);
    Assert.assertEquals(tablet.getSchemas().size(), bitMaps.length);
    Assert.assertEquals(expectedMeasurementHasBitMap.size(), tablet.getSchemas().size());
    for (int i = 0; i < tablet.getSchemas().size(); ++i) {
      final String measurement = tablet.getSchemas().get(i).getMeasurementName();
      Assert.assertTrue(expectedMeasurementHasBitMap.containsKey(measurement));
      if (expectedMeasurementHasBitMap.get(measurement)) {
        Assert.assertNotNull(bitMaps[i]);
      } else {
        Assert.assertNull(bitMaps[i]);
      }
    }
  }

  private int getColumnIndex(final Tablet tablet, final String measurement) {
    for (int i = 0; i < tablet.getSchemas().size(); ++i) {
      if (tablet.getSchemas().get(i).getMeasurementName().equals(measurement)) {
        return i;
      }
    }
    fail(String.format("Measurement %s does not exist in tablet.", measurement));
    return -1;
  }

  private void generateLargeAlignedTsFile(
      final File tsFile,
      final List<IMeasurementSchema> schemaList,
      final int deviceCount,
      final int rowCountPerDevice,
      final int tabletRowCount)
      throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }

    try (final TsFileWriter writer = new TsFileWriter(tsFile)) {
      for (int deviceIndex = 0; deviceIndex < deviceCount; ++deviceIndex) {
        final String device = "root.sg.performance.d" + deviceIndex;
        writer.registerAlignedTimeseries(new PartialPath(device), schemaList);

        final Tablet tablet = new Tablet(device, schemaList, tabletRowCount);
        for (int row = 0; row < rowCountPerDevice; ++row) {
          int rowIndex = tablet.getRowSize();
          if (rowIndex == tablet.getMaxRowNumber()) {
            writer.writeAligned(tablet);
            tablet.reset();
            rowIndex = 0;
          }

          tablet.addTimestamp(rowIndex, row);
          for (int measurementIndex = 0; measurementIndex < schemaList.size(); ++measurementIndex) {
            tablet.addValue(
                rowIndex,
                measurementIndex,
                ((long) deviceIndex << 48) + (long) row * schemaList.size() + measurementIndex);
          }
        }

        if (tablet.getRowSize() > 0) {
          writer.writeAligned(tablet);
        }
      }
    }
  }

  private Tablet generateSimpleTableTablet(
      final String tableName,
      final List<String> columnNameList,
      final List<TSDataType> dataTypeList,
      final List<ColumnCategory> columnCategoryList,
      final String tagValue,
      final long... timestamps) {
    final Tablet tablet =
        new Tablet(tableName, columnNameList, dataTypeList, columnCategoryList, timestamps.length);
    for (int rowIndex = 0; rowIndex < timestamps.length; ++rowIndex) {
      tablet.addTimestamp(rowIndex, timestamps[rowIndex]);
      tablet.addValue(rowIndex, 0, tagValue);
      tablet.addValue(rowIndex, 1, (long) rowIndex);
    }
    return tablet;
  }

  private void generateLargeTableTsFile(
      final File tsFile,
      final int tableCount,
      final int deviceCount,
      final int tagCount,
      final int fieldCount,
      final int rowCountPerDevice,
      final int tabletRowCount)
      throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }

    try (final TsFileWriter writer = new TsFileWriter(tsFile)) {
      for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
        final String tableName = "performance_table_" + tableIndex;
        final List<IMeasurementSchema> schemaList = new ArrayList<>();
        final List<String> columnNameList = new ArrayList<>();
        final List<TSDataType> dataTypeList = new ArrayList<>();
        final List<ColumnCategory> columnCategoryList = new ArrayList<>();

        for (int tagIndex = 0; tagIndex < tagCount; ++tagIndex) {
          final String tagName = "tag" + tagIndex;
          schemaList.add(
              new MeasurementSchema(
                  tagName, TSDataType.STRING, TSEncoding.PLAIN, CompressionType.LZ4));
          columnNameList.add(tagName);
          dataTypeList.add(TSDataType.STRING);
          columnCategoryList.add(ColumnCategory.TAG);
        }

        for (int fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
          final String fieldName = "s" + fieldIndex;
          schemaList.add(
              new MeasurementSchema(
                  fieldName, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.LZ4));
          columnNameList.add(fieldName);
          dataTypeList.add(TSDataType.INT64);
          columnCategoryList.add(ColumnCategory.FIELD);
        }

        writer.registerTableSchema(new TableSchema(tableName, schemaList, columnCategoryList));

        for (int deviceIndex = 0; deviceIndex < deviceCount; ++deviceIndex) {
          final Tablet tablet =
              new Tablet(
                  tableName, columnNameList, dataTypeList, columnCategoryList, tabletRowCount);

          for (int row = 0; row < rowCountPerDevice; ++row) {
            int rowIndex = tablet.getRowSize();
            if (rowIndex == tablet.getMaxRowNumber()) {
              writer.writeTable(tablet);
              tablet.reset();
              rowIndex = 0;
            }

            tablet.addTimestamp(rowIndex, row);
            for (int tagIndex = 0; tagIndex < tagCount; ++tagIndex) {
              tablet.addValue(rowIndex, tagIndex, "d" + deviceIndex + "_tag" + tagIndex);
            }
            for (int fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
              tablet.addValue(
                  rowIndex,
                  tagCount + fieldIndex,
                  ((long) tableIndex << 56)
                      + ((long) deviceIndex << 48)
                      + (long) row * fieldCount
                      + fieldIndex);
            }
          }

          if (tablet.getRowSize() > 0) {
            writer.writeTable(tablet);
          }
        }
      }
    }
  }

  private ParserPerformanceStats collectTabletInsertionEventParserPerformanceStats(
      final Iterable<TabletInsertionEvent> tabletInsertionEvents,
      final boolean countFieldColumnsOnly) {
    final ParserPerformanceStats stats = new ParserPerformanceStats();

    for (final TabletInsertionEvent tabletInsertionEvent : tabletInsertionEvents) {
      Assert.assertTrue(
          "Expected parser to generate PipeRawTabletInsertionEvent.",
          tabletInsertionEvent instanceof PipeRawTabletInsertionEvent);

      final Tablet tablet = ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
      final int columnCount =
          countFieldColumnsOnly ? getFieldColumnCount(tablet) : tablet.getSchemas().size();

      ++stats.tabletCount;
      stats.tabletRowCountSum += tablet.getRowSize();
      stats.pointCount += (long) tablet.getRowSize() * columnCount;
      stats.minColumnCountInTablet = Math.min(stats.minColumnCountInTablet, columnCount);
      stats.maxColumnCountInTablet = Math.max(stats.maxColumnCountInTablet, columnCount);
    }

    return stats;
  }

  private int getFieldColumnCount(final Tablet tablet) {
    if (Objects.isNull(tablet.getColumnTypes())) {
      return tablet.getSchemas().size();
    }

    int fieldCount = 0;
    for (final ColumnCategory columnCategory : tablet.getColumnTypes()) {
      if (ColumnCategory.FIELD.equals(columnCategory)) {
        ++fieldCount;
      }
    }
    return fieldCount;
  }

  private int getManualPerformanceIntProperty(final String propertyName, final int defaultValue) {
    final int value = Integer.getInteger(propertyName, defaultValue);
    Assert.assertTrue(propertyName + " should be positive.", value > 0);
    return value;
  }

  private long getManualPerformanceLongProperty(
      final String propertyName, final long defaultValue) {
    final Long value = Long.getLong(propertyName, defaultValue);
    Assert.assertTrue(propertyName + " should be positive.", value > 0);
    return value;
  }

  private void printScanParserPerformanceResult(
      final long tsFileSizeInBytes,
      final int deviceCount,
      final int measurementCount,
      final int rowCountPerDevice,
      final int inputTabletRowCount,
      final long pipeMaxReaderChunkSize,
      final long expectedPointCount,
      final long writeElapsedNanos,
      final long parseElapsedNanos,
      final int tabletCount,
      final int alignedTabletCount,
      final long parsedTabletRowCount,
      final long pointCount,
      final int minMeasurementCountInTablet,
      final int maxMeasurementCountInTablet) {
    final double writeElapsedSeconds = nanosToSeconds(writeElapsedNanos);
    final double parseElapsedSeconds = nanosToSeconds(parseElapsedNanos);
    final double pointThroughput = pointCount / Math.max(parseElapsedSeconds, 1.0e-9);
    final double fileThroughputInMiBPerSecond =
        tsFileSizeInBytes / 1024.0 / 1024.0 / Math.max(parseElapsedSeconds, 1.0e-9);

    System.out.printf(
        Locale.ROOT,
        "%nTsFileInsertionEventScanParser split performance:%n"
            + "  fileSize=%s%n"
            + "  deviceCount=%d, measurementCount=%d, rowCountPerDevice=%d, expectedPoints=%d%n"
            + "  inputTabletRowCount=%d, pipeMaxReaderChunkSize=%s%n"
            + "  writeTime=%.3fs, parseTime=%.3fs%n"
            + "  tablets=%d, alignedTablets=%d, parsedTabletRows=%d, points=%d%n"
            + "  measurementCountInTablet[min=%d, max=%d]%n"
            + "  pointThroughput=%.2f points/s, fileThroughput=%.2f MiB/s%n",
        formatBytes(tsFileSizeInBytes),
        deviceCount,
        measurementCount,
        rowCountPerDevice,
        expectedPointCount,
        inputTabletRowCount,
        formatBytes(pipeMaxReaderChunkSize),
        writeElapsedSeconds,
        parseElapsedSeconds,
        tabletCount,
        alignedTabletCount,
        parsedTabletRowCount,
        pointCount,
        minMeasurementCountInTablet,
        maxMeasurementCountInTablet,
        pointThroughput,
        fileThroughputInMiBPerSecond);
  }

  private void printTabletInsertionEventParserPerformanceResult(
      final String parserName,
      final long tsFileSizeInBytes,
      final String dataShape,
      final int inputTabletRowCount,
      final String extraConfig,
      final long expectedPointCount,
      final long writeElapsedNanos,
      final long parseElapsedNanos,
      final ParserPerformanceStats stats,
      final String columnName) {
    final double writeElapsedSeconds = nanosToSeconds(writeElapsedNanos);
    final double parseElapsedSeconds = nanosToSeconds(parseElapsedNanos);
    final double pointThroughput = stats.pointCount / Math.max(parseElapsedSeconds, 1.0e-9);
    final double fileThroughputInMiBPerSecond =
        tsFileSizeInBytes / 1024.0 / 1024.0 / Math.max(parseElapsedSeconds, 1.0e-9);
    final int minColumnCountInTablet = stats.tabletCount == 0 ? 0 : stats.minColumnCountInTablet;

    System.out.printf(
        Locale.ROOT,
        "%n%s performance:%n"
            + "  fileSize=%s%n"
            + "  %s, expectedPoints=%d%n"
            + "  inputTabletRowCount=%d%s%n"
            + "  writeTime=%.3fs, parseTime=%.3fs%n"
            + "  tablets=%d, parsedTabletRows=%d, points=%d%n"
            + "  %sCountInTablet[min=%d, max=%d]%n"
            + "  pointThroughput=%.2f points/s, fileThroughput=%.2f MiB/s%n",
        parserName,
        formatBytes(tsFileSizeInBytes),
        dataShape,
        expectedPointCount,
        inputTabletRowCount,
        extraConfig,
        writeElapsedSeconds,
        parseElapsedSeconds,
        stats.tabletCount,
        stats.tabletRowCountSum,
        stats.pointCount,
        columnName,
        minColumnCountInTablet,
        stats.maxColumnCountInTablet,
        pointThroughput,
        fileThroughputInMiBPerSecond);
  }

  private double nanosToSeconds(final long nanos) {
    return nanos / 1_000_000_000.0;
  }

  private String formatBytes(final long bytes) {
    double value = bytes;
    final String[] units = {"B", "KiB", "MiB", "GiB"};
    int unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      ++unitIndex;
    }
    return String.format(Locale.ROOT, "%.2f %s", value, units[unitIndex]);
  }

  private void testTsFilePointNum(
      final File tsFile,
      final TreePattern pattern,
      final long startTime,
      final long endTime,
      final boolean isQuery,
      final int expectedCount) {
    final PipeTsFileInsertionEvent tsFileInsertionEvent =
        new PipeTsFileInsertionEvent(
            false,
            "",
            new TsFileResource(tsFile),
            null,
            true,
            false,
            false,
            null,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    try (final TsFileInsertionEventParser tsFileContainer =
        isQuery
            ? new TsFileInsertionEventQueryParser(
                tsFile, pattern, startTime, endTime, tsFileInsertionEvent)
            : new TsFileInsertionEventScanParser(
                tsFile, pattern, startTime, endTime, null, tsFileInsertionEvent, false)) {
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
    for (int i = 0; i < tablet.getRowSize(); ++i) {
      for (int j = 0; j < tablet.getSchemas().size(); ++j) {
        if (!tablet.isNull(i, j)) {
          ++count;
        }
      }
    }
    return count;
  }

  private PipeMemoryBlock getAllocatedChunkMemory(final TsFileInsertionEventScanParser parser)
      throws NoSuchFieldException, IllegalAccessException {
    final Field field =
        TsFileInsertionEventScanParser.class.getDeclaredField("allocatedMemoryBlockForChunk");
    field.setAccessible(true);
    return (PipeMemoryBlock) field.get(parser);
  }

  private PipeMemoryBlock getAllocatedBatchDataMemory(final TsFileInsertionEventScanParser parser)
      throws NoSuchFieldException, IllegalAccessException {
    final Field field =
        TsFileInsertionEventScanParser.class.getDeclaredField("allocatedMemoryBlockForBatchData");
    field.setAccessible(true);
    return (PipeMemoryBlock) field.get(parser);
  }

  private PipeMemoryBlock getAllocatedTabletMemory(final TsFileInsertionEventParser parser)
      throws NoSuchFieldException, IllegalAccessException {
    final Field field =
        TsFileInsertionEventParser.class.getDeclaredField("allocatedMemoryBlockForTablet");
    field.setAccessible(true);
    return (PipeMemoryBlock) field.get(parser);
  }

  @SuppressWarnings("unchecked")
  private AtomicReference<TsFileInsertionEventParser> getEventParser(
      final PipeTsFileInsertionEvent event) throws NoSuchFieldException, IllegalAccessException {
    final Field field = PipeTsFileInsertionEvent.class.getDeclaredField("eventParser");
    field.setAccessible(true);
    return (AtomicReference<TsFileInsertionEventParser>) field.get(event);
  }

  private long calculatePipeMaxReaderChunkSizeForSinglePageAlignedChunk(final File tsFile)
      throws Exception {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      final IDeviceID deviceID = reader.getDeviceMeasurementsMap().keySet().iterator().next();
      final List<AbstractAlignedChunkMetadata> alignedChunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID, true);
      Assert.assertEquals(1, alignedChunkMetadataList.size());

      final AbstractAlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(0);
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
      final IDeviceID deviceID = reader.getDeviceMeasurementsMap().keySet().iterator().next();
      final List<AbstractAlignedChunkMetadata> alignedChunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID, true);
      Assert.assertEquals(1, alignedChunkMetadataList.size());

      final AbstractAlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(0);
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
      final List<AbstractAlignedChunkMetadata> alignedChunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID, true);
      Assert.assertEquals(2, alignedChunkMetadataList.size());

      final AbstractAlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(0);
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
      final File tsFile, final List<IMeasurementSchema> schemaList, final int rowCount)
      throws IOException {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }
    Assert.assertEquals(0, rowCount % 2);

    final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d");
    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.startChunkGroup(deviceID);
      final int rowCountPerChunk = rowCount / 2;
      for (int chunkIndex = 0; chunkIndex < 2; ++chunkIndex) {
        final AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(schemaList);
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
      final List<AbstractAlignedChunkMetadata> sourceAlignedChunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID, true);
      Assert.assertEquals(2, sourceAlignedChunkMetadataList.size());
      for (final AbstractAlignedChunkMetadata sourceAlignedChunkMetadata :
          sourceAlignedChunkMetadataList) {
        Assert.assertEquals(
            measurementCount, sourceAlignedChunkMetadata.getValueChunkMetadataList().size());
      }

      try (final CompactionTsFileWriter writer =
          new CompactionTsFileWriter(
              targetTsFile, Long.MAX_VALUE, CompactionType.INNER_SEQ_COMPACTION)) {
        writer.startChunkGroup(deviceID);
        writer.markStartingWritingAligned();
        for (final AbstractAlignedChunkMetadata sourceAlignedChunkMetadata :
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
      final List<AbstractAlignedChunkMetadata> alignedChunkMetadataList,
      final int start,
      final int end)
      throws IOException {
    for (final AbstractAlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
      final List<IChunkMetadata> valueChunkMetadataList =
          alignedChunkMetadata.getValueChunkMetadataList();
      for (int index = start; index < end; ++index) {
        final ChunkMetadata valueChunkMetadata = (ChunkMetadata) valueChunkMetadataList.get(index);
        writer.writeChunk(reader.readMemChunk(valueChunkMetadata), valueChunkMetadata);
      }
    }
  }

  private static class ParserPerformanceStats {

    private long pointCount;
    private long tabletRowCountSum;
    private int tabletCount;
    private int minColumnCountInTablet = Integer.MAX_VALUE;
    private int maxColumnCountInTablet;
  }
}
