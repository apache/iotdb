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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBPipePattern;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.scan.TsFileInsertionScanDataContainer;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileParserMemoryManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class LoadTreeStatementDataTypeConvertExecutionVisitorTest {

  private static final String DEVICE_0 = "root.sg.d0";
  private static final String DEVICE_1 = "root.sg.d1";
  private static final String DEVICE_2 = "root.sg.d2";
  private static final String ALIGNED_DEVICE = "root.sg.ad0";
  private static final int ROW_COUNT_PER_DEVICE = 2048;
  private File tsFile;
  private boolean isPipeMemoryManagementEnabled;
  private long pipeMaxReaderChunkSize;

  @Before
  public void setUp() {
    isPipeMemoryManagementEnabled = PipeConfigAccessor.getPipeMemoryManagementEnabled();
    PipeConfigAccessor.setPipeMemoryManagementEnabled(false);
    pipeMaxReaderChunkSize = CommonDescriptor.getInstance().getConfig().getPipeMaxReaderChunkSize();
  }

  @After
  public void tearDown() {
    PipeConfigAccessor.setPipeMemoryManagementEnabled(isPipeMemoryManagementEnabled);
    CommonDescriptor.getInstance().getConfig().setPipeMaxReaderChunkSize(pipeMaxReaderChunkSize);
    if (tsFile != null && tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }
    if (tsFile != null) {
      final File modsFile = new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
      if (modsFile.exists()) {
        Assert.assertTrue(modsFile.delete());
      }
    }
  }

  @Test
  public void testFallbackToQueryForRemainingDevicesWhenScanParserHitsCorruption()
      throws Exception {
    tsFile = new File("load-tree-query-fallback-corrupted.tsfile");
    writeTsFile(tsFile);
    corruptMeasurementChunk(tsFile, DEVICE_1, "s0");

    Assert.assertTrue("Expected scan parser to fail after corruption.", scanParserFails(tsFile));

    final Map<String, Integer> pointCountByDevice = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedPoints(statement, pointCountByDevice);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });

    final Optional<TSStatus> status =
        visitor.visitLoadFile(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()).setDeleteAfterLoad(true),
            null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.get().getCode());
    Assert.assertTrue("A partial load must retain its source", tsFile.exists());
    final int loadedPointCountBeforeCorruption = pointCountByDevice.getOrDefault(DEVICE_0, 0);
    final int loadedPointCountAfterFallback = pointCountByDevice.getOrDefault(DEVICE_2, 0);
    Assert.assertTrue(loadedPointCountBeforeCorruption > 0);
    Assert.assertEquals(loadedPointCountBeforeCorruption, loadedPointCountAfterFallback);
  }

  @Test
  public void testFullyRecoveredQueryFallbackCanDeleteSource() throws Exception {
    tsFile = new File("load-tree-query-fallback-recoverable-marker.tsfile");
    final long groupMarkerOffset = writeTsFile(tsFile);
    // Keep the first marker valid for the reader's encryption-header check. Indexed queries can
    // recover every point without reading the second device's chunk-group marker.
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      reader.position(groupMarkerOffset);
      Assert.assertEquals(MetaMarker.CHUNK_GROUP_HEADER, reader.readMarker());
    }
    try (final RandomAccessFile file = new RandomAccessFile(tsFile, "rw")) {
      file.seek(groupMarkerOffset);
      file.writeByte(0x7f);
    }
    Assert.assertTrue("Expected scan parser to fail after corruption.", scanParserFails(tsFile));

    final Map<String, Integer> pointCountByTimeseries = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedPointsByTimeseries(statement, pointCountByTimeseries);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });
    final Optional<TSStatus> status =
        visitor.visitLoadFile(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()).setDeleteAfterLoad(true),
            null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());
    for (final String device : Arrays.asList(DEVICE_0, DEVICE_1, DEVICE_2)) {
      assertMeasurementLoadedCompletely(pointCountByTimeseries, device, 0);
      assertMeasurementLoadedCompletely(pointCountByTimeseries, device, 1);
    }
    Assert.assertFalse("A fully recovered load can delete its source", tsFile.exists());
  }

  @Test
  public void testFallbackRetainsSourceWhenFirstDeviceMetadataIsUnreadable() throws Exception {
    assertFallbackRetainsSourceWhenMetadataIsUnreadable(DEVICE_0);
  }

  @Test
  public void testFallbackRetainsSourceWhenLaterDeviceMetadataIsUnreadable() throws Exception {
    assertFallbackRetainsSourceWhenMetadataIsUnreadable(DEVICE_1);
  }

  private void assertFallbackRetainsSourceWhenMetadataIsUnreadable(final String corruptedDevice)
      throws Exception {
    tsFile = File.createTempFile("load-tree-unreadable-metadata-", ".tsfile");
    final long groupMarkerOffset = writeTsFile(tsFile);
    final IDeviceID deviceId = new PlainDeviceID(corruptedDevice);
    final long measurementIndexEndOffsetPosition;
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      final MetadataIndexNode deviceIndex = reader.readFileMetadata().getMetadataIndex();
      Assert.assertEquals(MetadataIndexNodeType.LEAF_DEVICE, deviceIndex.getNodeType());
      final Pair<IMetadataIndexEntry, Long> deviceEntry =
          deviceIndex.getChildIndexEntry(deviceId, true);
      Assert.assertNotNull(deviceEntry);
      final MetadataIndexNode measurementIndex =
          reader.readMetadataIndexNode(deviceEntry.left.getOffset(), deviceEntry.right, false);
      Assert.assertEquals(MetadataIndexNodeType.LEAF_MEASUREMENT, measurementIndex.getNodeType());
      measurementIndexEndOffsetPosition = deviceEntry.right - Long.BYTES - Byte.BYTES;
    }
    // Preserve the footer and device index, but make the measurement metadata read reach EOF.
    try (final RandomAccessFile bytes = new RandomAccessFile(tsFile, "rw")) {
      bytes.seek(measurementIndexEndOffsetPosition);
      bytes.writeLong(bytes.length() + 1);
    }
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      Assert.assertThrows(IOException.class, () -> reader.readDeviceMetadata(deviceId));
    }
    corruptGroupMarker(tsFile, groupMarkerOffset);
    Assert.assertTrue("Expected scan parser to fail after corruption.", scanParserFails(tsFile));

    final Map<String, Integer> points = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedPointsByTimeseries(statement, points);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });
    final Optional<TSStatus> status =
        visitor.visitLoadFile(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()).setDeleteAfterLoad(true),
            null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.get().getCode());
    Assert.assertTrue("Unreadable metadata must retain the source", tsFile.exists());
    assertMeasurementLoadedCompletely(points, DEVICE_0, 0);
    assertMeasurementLoadedCompletely(points, DEVICE_0, 1);
  }

  @Test
  public void testFallbackResumesRepeatedNonAlignedDevicesAcrossChunkGroups() throws Exception {
    assertFallbackResumesRepeatedDevices(false);
  }

  @Test
  public void testFallbackResumesRepeatedAlignedDevicesAcrossChunkGroups() throws Exception {
    assertFallbackResumesRepeatedDevices(true);
  }

  @Test
  public void testFallbackRetainsMeasurementsSplitAcrossMetadataBatches() throws Exception {
    tsFile = File.createTempFile("load-tree-metadata-batches-", ".tsfile");
    final int measurementCount = 4001;
    final List<MeasurementSchema> schemas = new java.util.ArrayList<>();
    for (int measurement = 0; measurement < measurementCount; measurement++) {
      schemas.add(new MeasurementSchema("s" + measurement, TSDataType.INT64, TSEncoding.PLAIN));
    }

    final long secondGroupMarkerOffset;
    try (final TsFileWriter writer = new TsFileWriter(tsFile)) {
      writer.registerAlignedTimeseries(new Path(DEVICE_0), schemas);
      writeWideAlignedRow(writer, schemas, 0);
      writer.flushAllChunkGroups();
      secondGroupMarkerOffset = writer.getIOWriter().getPos();
      writeWideAlignedRow(writer, schemas, 1);
    }
    corruptGroupMarker(tsFile, secondGroupMarkerOffset);
    Assert.assertTrue("Expected scan parser to fail after corruption.", scanParserFails(tsFile));

    final Map<String, Map<Long, Long>> points = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedLongPoints(statement, points);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });
    final Optional<TSStatus> status =
        visitor.visitLoadFile(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()).setDeleteAfterLoad(true),
            null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());
    Assert.assertEquals(measurementCount, points.size());
    for (int measurement = 0; measurement < measurementCount; measurement++) {
      final String path = DEVICE_0 + ".s" + measurement;
      final Map<Long, Long> values = points.get(path);
      Assert.assertNotNull(path, values);
      Assert.assertEquals(path, 2, values.size());
      Assert.assertEquals(Long.valueOf(measurement * 10_000L), values.get(0L));
      Assert.assertEquals(Long.valueOf(measurement * 10_000L + 1), values.get(1L));
    }
    Assert.assertFalse("A fully recovered load can delete its source", tsFile.exists());
  }

  @Test
  public void testFallbackDoesNotReplayMinimumTimestamp() throws Exception {
    assertFallbackResumesExtremeTimestamps(Long.MIN_VALUE);
  }

  @Test
  public void testFallbackResumesAtMaximumTimestamp() throws Exception {
    assertFallbackResumesExtremeTimestamps(Long.MAX_VALUE - 1);
  }

  private void assertFallbackResumesExtremeTimestamps(final long firstTimestamp) throws Exception {
    tsFile = File.createTempFile("load-tree-extreme-timestamps-", ".tsfile");
    final List<MeasurementSchema> schemas =
        Arrays.asList(new MeasurementSchema("s0", TSDataType.INT64, TSEncoding.PLAIN));
    final long secondFlushOffset;
    try (final TsFileWriter writer = new TsFileWriter(tsFile)) {
      writer.registerTimeseries(new Path(DEVICE_0), schemas);
      writeRows(writer, schemas, DEVICE_0, firstTimestamp, 1, false);
      writer.flushAllChunkGroups();
      secondFlushOffset = writer.getIOWriter().getPos();
      writeRows(writer, schemas, DEVICE_0, firstTimestamp + 1, 1, false);
    }
    corruptGroupMarker(tsFile, secondFlushOffset);

    final Map<String, Map<Long, Long>> points = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedLongPoints(statement, points);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });
    final Optional<TSStatus> status =
        visitor.visitLoadFile(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()).setDeleteAfterLoad(true),
            null);
    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());
    final Map<Long, Long> values = points.get(DEVICE_0 + ".s0");
    Assert.assertNotNull(values);
    Assert.assertEquals(2, values.size());
    Assert.assertEquals(Long.valueOf(firstTimestamp), values.get(firstTimestamp));
    Assert.assertEquals(Long.valueOf(firstTimestamp + 1), values.get(firstTimestamp + 1));
    Assert.assertFalse("A fully recovered load can delete its source", tsFile.exists());
  }

  private void assertFallbackResumesRepeatedDevices(final boolean aligned) throws Exception {
    tsFile = File.createTempFile("load-tree-repeated-devices-", ".tsfile");
    final List<MeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    final long secondFlushOffset;
    try (final TsFileWriter writer = new TsFileWriter(tsFile)) {
      for (final String device : Arrays.asList(DEVICE_0, DEVICE_1)) {
        if (aligned) {
          writer.registerAlignedTimeseries(new Path(device), schemas);
        } else {
          writer.registerTimeseries(new Path(device), schemas);
        }
        writeRows(writer, schemas, device, 0, 4, aligned);
      }
      writer.flushAllChunkGroups();
      secondFlushOffset = writer.getIOWriter().getPos();
      for (final String device : Arrays.asList(DEVICE_0, DEVICE_1)) {
        writeRows(writer, schemas, device, 4, 4, aligned);
      }
    }
    corruptGroupMarker(tsFile, secondFlushOffset);

    final Map<String, Map<Long, Long>> points = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedLongPoints(statement, points);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });
    final Optional<TSStatus> status =
        visitor.visitLoadFile(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()).setDeleteAfterLoad(true),
            null);
    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());
    for (final String device : Arrays.asList(DEVICE_0, DEVICE_1)) {
      for (int measurement = 0; measurement < 2; measurement++) {
        final String path = device + ".s" + measurement;
        final Map<Long, Long> values = points.get(path);
        Assert.assertNotNull(path, values);
        Assert.assertEquals(path, 8, values.size());
        for (long timestamp = 0; timestamp < 8; timestamp++) {
          Assert.assertEquals(
              path, Long.valueOf(timestamp + measurement * 100), values.get(timestamp));
        }
      }
    }
    Assert.assertFalse("A fully recovered load can delete its source", tsFile.exists());
  }

  private static void corruptGroupMarker(final File tsFile, final long offset) throws Exception {
    try (final RandomAccessFile file = new RandomAccessFile(tsFile, "rw")) {
      file.seek(offset);
      Assert.assertEquals(MetaMarker.CHUNK_GROUP_HEADER, file.readByte());
      file.seek(offset);
      file.writeByte(0x7f);
    }
  }

  private static void writeRows(
      final TsFileWriter writer,
      final List<MeasurementSchema> schemas,
      final String device,
      final long firstTimestamp,
      final int rowCount,
      final boolean aligned)
      throws Exception {
    final Tablet tablet = new Tablet(device, schemas, rowCount);
    for (int row = 0; row < rowCount; row++) {
      tablet.addTimestamp(row, firstTimestamp + row);
      for (int column = 0; column < schemas.size(); column++) {
        tablet.addValue(
            schemas.get(column).getMeasurementId(), row, firstTimestamp + row + column * 100);
      }
    }
    tablet.rowSize = rowCount;
    if (aligned) {
      writer.writeAligned(tablet);
    } else {
      writer.write(tablet);
    }
  }

  @Test
  public void testRecoversPartiallyScannedAlignedDeviceAtNormalEof() throws Exception {
    tsFile = new File("load-tree-query-fallback-partial-aligned.tsfile");
    writeAlignedBooleanTsFile(tsFile);

    final List<MeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.BOOLEAN, TSEncoding.PLAIN),
            new MeasurementSchema("s1", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    final Tablet partialTablet =
        new Tablet(ALIGNED_DEVICE, Collections.singletonList(schemas.get(0)), 1);
    partialTablet.addTimestamp(0, 0);
    partialTablet.addValue("s0", 0, true);
    partialTablet.rowSize = 1;

    final Map<String, Integer> pointCountByTimeseries = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedPointsByTimeseries(statement, pointCountByTimeseries);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            },
            file ->
                new LoadTreeTsFileTabletIterator(
                    file,
                    false,
                    sourceFile ->
                        new TsFileInsertionScanDataContainer(
                            sourceFile,
                            new IoTDBPipePattern(null),
                            Long.MIN_VALUE,
                            Long.MAX_VALUE,
                            null,
                            null,
                            false,
                            LoadTsFileParserMemoryManager.getInstance(),
                            true) {
                          @Override
                          public Iterable<Pair<Tablet, Boolean>> toTabletWithIsAligneds() {
                            return Collections.singletonList(new Pair<>(partialTablet, true));
                          }

                          @Override
                          public Map<IDeviceID, Map<String, Long>> getParsedPointCounts() {
                            return Collections.singletonMap(
                                new PlainDeviceID(ALIGNED_DEVICE),
                                Collections.singletonMap("s0", 1L));
                          }
                        }));

    final Optional<TSStatus> status =
        visitor.visitLoadFile(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()).setDeleteAfterLoad(true),
            null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());
    Assert.assertEquals(
        4, pointCountByTimeseries.getOrDefault(ALIGNED_DEVICE + ".s0", 0).intValue());
    Assert.assertEquals(
        4, pointCountByTimeseries.getOrDefault(ALIGNED_DEVICE + ".s1", 0).intValue());
    Assert.assertFalse("Successfully loaded TsFile should be deleted.", tsFile.exists());
  }

  @Test
  public void testScanParserFailureWithModsUsesParsedPointCounts() throws Exception {
    tsFile = new File("load-tree-query-fallback-aligned-mods.tsfile");
    writeAlignedBooleanTsFile(tsFile);
    try (final ModificationFile modificationFile =
        new ModificationFile(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX)) {
      modificationFile.write(new Deletion(new MeasurementPath(ALIGNED_DEVICE + ".s0"), 0, 0, 0));
    }

    final List<MeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.BOOLEAN, TSEncoding.PLAIN),
            new MeasurementSchema("s1", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    final Tablet partialTablet =
        new Tablet(ALIGNED_DEVICE, Collections.singletonList(schemas.get(0)), 1);
    partialTablet.addTimestamp(0, 1);
    partialTablet.addValue("s0", 0, false);
    partialTablet.rowSize = 1;

    final Map<IDeviceID, Map<String, Long>> parsedPointCounts =
        Collections.singletonMap(new PlainDeviceID(ALIGNED_DEVICE), Map.of("s0", 4L, "s1", 4L));
    final Map<IDeviceID, Map<String, Long>> expectedPointCounts =
        Collections.singletonMap(new PlainDeviceID(ALIGNED_DEVICE), Map.of("s0", 3L, "s1", 4L));
    final Map<String, Integer> pointCountByTimeseries = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedPointsByTimeseries(statement, pointCountByTimeseries);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            },
            file ->
                new LoadTreeTsFileTabletIterator(
                    file,
                    true,
                    sourceFile ->
                        new TsFileInsertionScanDataContainer(
                            sourceFile,
                            new IoTDBPipePattern(null),
                            Long.MIN_VALUE,
                            Long.MAX_VALUE,
                            null,
                            null,
                            true,
                            LoadTsFileParserMemoryManager.getInstance(),
                            true) {
                          private boolean parsedPointCountsRead;
                          private boolean expectedPointCountsRead;
                          private boolean closed;

                          @Override
                          public Iterable<Pair<Tablet, Boolean>> toTabletWithIsAligneds() {
                            return () ->
                                new Iterator<Pair<Tablet, Boolean>>() {
                                  private boolean tabletReturned;

                                  @Override
                                  public boolean hasNext() {
                                    if (tabletReturned) {
                                      throw new IllegalStateException("synthetic parser failure");
                                    }
                                    return true;
                                  }

                                  @Override
                                  public Pair<Tablet, Boolean> next() {
                                    tabletReturned = true;
                                    return new Pair<>(partialTablet, true);
                                  }
                                };
                          }

                          @Override
                          public Map<IDeviceID, Map<String, Long>> getParsedPointCounts() {
                            Assert.assertFalse(closed);
                            parsedPointCountsRead = true;
                            return parsedPointCounts;
                          }

                          @Override
                          public Map<IDeviceID, Map<String, Long>> getExpectedPointCounts() {
                            Assert.assertFalse(closed);
                            expectedPointCountsRead = true;
                            return expectedPointCounts;
                          }

                          @Override
                          public void close() {
                            Assert.assertTrue(parsedPointCountsRead);
                            Assert.assertTrue(expectedPointCountsRead);
                            closed = true;
                            super.close();
                          }
                        }));

    final Optional<TSStatus> status =
        visitor.visitLoadFile(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()).setDeleteAfterLoad(true),
            null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());
    Assert.assertEquals(
        3, pointCountByTimeseries.getOrDefault(ALIGNED_DEVICE + ".s0", 0).intValue());
    Assert.assertEquals(
        4, pointCountByTimeseries.getOrDefault(ALIGNED_DEVICE + ".s1", 0).intValue());
    Assert.assertFalse("Successfully loaded TsFile should be deleted.", tsFile.exists());
  }

  @Test
  public void testModifiedAlignedScanPreservesValueChunkOrderAfterSkippingDeletedChunk()
      throws Exception {
    tsFile = new File("load-tree-aligned-scan-skipped-value-chunk.tsfile");
    writeAlignedBooleanTsFileWithMultipleChunksInGroup(tsFile);
    try (final ModificationFile modificationFile =
        new ModificationFile(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX)) {
      modificationFile.write(new Deletion(new MeasurementPath(ALIGNED_DEVICE + ".s0"), 0, 0, 0));
    }

    final List<Long> visibleTimestamps = new ArrayList<>();
    try (final TsFileInsertionScanDataContainer parser =
        new TsFileInsertionScanDataContainer(
            tsFile, new IoTDBPipePattern(null), Long.MIN_VALUE, Long.MAX_VALUE, null, null, true)) {
      for (final Pair<Tablet, Boolean> tabletWithIsAligned : parser.toTabletWithIsAligneds()) {
        final Tablet tablet = tabletWithIsAligned.getLeft();
        for (int column = 0; column < tablet.getSchemas().size(); column++) {
          if (!"s0".equals(tablet.getSchemas().get(column).getMeasurementId())) {
            continue;
          }
          final BitMap bitMap = tablet.bitMaps == null ? null : tablet.bitMaps[column];
          for (int row = 0; row < tablet.rowSize; row++) {
            if (bitMap == null || !bitMap.isMarked(row)) {
              visibleTimestamps.add(tablet.timestamps[row]);
            }
          }
        }
      }
    }

    Assert.assertEquals(Collections.singletonList(1L), visibleTimestamps);
  }

  @Test
  public void testFlushesPendingTabletsWhenIteratorFails() throws Exception {
    tsFile = File.createTempFile("load-tree-pending-tablet", ".tsfile");
    final List<MeasurementSchema> schemaList =
        Arrays.asList(new MeasurementSchema("s0", TSDataType.INT64, TSEncoding.PLAIN));
    final Tablet tablet = new Tablet(DEVICE_0, schemaList, 1);
    tablet.addTimestamp(0, 1);
    tablet.addValue("s0", 0, 1L);
    tablet.rowSize = 1;

    final Map<String, Integer> pointCountByDevice = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedPoints(statement, pointCountByDevice);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            },
            file -> new ThrowingTabletIterator(file, tablet));

    final Optional<TSStatus> status =
        visitor.visitLoadFile(LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()), null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.get().getCode());
    Assert.assertEquals(1, pointCountByDevice.getOrDefault(DEVICE_0, 0).intValue());
  }

  @Test
  public void testFallbackToQueryWhenFirstNonAlignedDeviceIsCorrupted() throws Exception {
    tsFile = new File("load-tree-query-fallback-corrupted-first-non-aligned-device.tsfile");
    writeTsFile(tsFile);
    corruptMeasurementChunk(tsFile, DEVICE_0, "s0");

    Assert.assertTrue("Expected scan parser to fail after corruption.", scanParserFails(tsFile));

    final Map<String, Integer> pointCountByTimeseries = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedPointsByTimeseries(statement, pointCountByTimeseries);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });

    final Optional<TSStatus> status =
        visitor.visitLoadFile(LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()), null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.get().getCode());

    Assert.assertTrue(
        pointCountByTimeseries.getOrDefault(DEVICE_0 + ".s0", 0) < ROW_COUNT_PER_DEVICE);
    assertMeasurementLoadedCompletely(pointCountByTimeseries, DEVICE_0, 1);
    assertMeasurementLoadedCompletely(pointCountByTimeseries, DEVICE_1, 0);
    assertMeasurementLoadedCompletely(pointCountByTimeseries, DEVICE_1, 1);
    assertMeasurementLoadedCompletely(pointCountByTimeseries, DEVICE_2, 0);
    assertMeasurementLoadedCompletely(pointCountByTimeseries, DEVICE_2, 1);
  }

  @Test
  public void testFallbackDoesNotReloadCompletedMeasurementsOfCurrentNonAlignedDevice()
      throws Exception {
    tsFile = new File("load-tree-query-fallback-corrupted-current-non-aligned-device.tsfile");
    writeTsFile(tsFile);
    corruptMeasurementChunk(tsFile, DEVICE_1, "s1");

    Assert.assertTrue("Expected scan parser to fail after corruption.", scanParserFails(tsFile));

    final Map<String, Integer> pointCountByTimeseries = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedPointsByTimeseries(statement, pointCountByTimeseries);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });

    final Optional<TSStatus> status =
        visitor.visitLoadFile(LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()), null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.get().getCode());

    assertMeasurementLoadedCompletely(pointCountByTimeseries, DEVICE_1, 0);
    Assert.assertTrue(
        pointCountByTimeseries.getOrDefault(DEVICE_1 + ".s1", 0) < ROW_COUNT_PER_DEVICE);
    assertMeasurementLoadedCompletely(pointCountByTimeseries, DEVICE_2, 0);
    assertMeasurementLoadedCompletely(pointCountByTimeseries, DEVICE_2, 1);
  }

  @Test
  public void testFallbackToQueryForRemainingMeasurementsOfCurrentAlignedDevice() throws Exception {
    CommonDescriptor.getInstance().getConfig().setPipeMaxReaderChunkSize(0);

    tsFile = new File("load-tree-query-fallback-corrupted-aligned-current-device.tsfile");
    writeWideAlignedTsFile(tsFile, ALIGNED_DEVICE, 16);
    corruptMeasurementChunk(tsFile, ALIGNED_DEVICE, "s8");
    corruptMeasurementChunk(tsFile, ALIGNED_DEVICE, "s12");

    Assert.assertTrue("Expected scan parser to fail after corruption.", scanParserFails(tsFile));

    final Map<String, Integer> pointCountByTimeseries = new HashMap<>();
    final LoadTreeStatementDataTypeConvertExecutionVisitor visitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(
            statement -> {
              collectLoadedPointsByTimeseries(statement, pointCountByTimeseries);
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });

    final Optional<TSStatus> status =
        visitor.visitLoadFile(LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()), null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.LOAD_FILE_ERROR.getStatusCode(), status.get().getCode());

    for (int measurementIndex = 0; measurementIndex < 8; ++measurementIndex) {
      assertMeasurementLoadedCompletely(pointCountByTimeseries, ALIGNED_DEVICE, measurementIndex);
    }
    for (int measurementIndex : Arrays.asList(9, 10, 11, 13, 14, 15)) {
      assertMeasurementLoadedCompletely(pointCountByTimeseries, ALIGNED_DEVICE, measurementIndex);
    }

    Assert.assertTrue(
        pointCountByTimeseries.getOrDefault(ALIGNED_DEVICE + ".s8", 0) < ROW_COUNT_PER_DEVICE);
    Assert.assertTrue(
        pointCountByTimeseries.getOrDefault(ALIGNED_DEVICE + ".s12", 0) < ROW_COUNT_PER_DEVICE);
  }

  private long writeTsFile(final File file) throws Exception {
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }

    final List<MeasurementSchema> schemaList =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));

    final long secondGroupMarkerOffset;
    try (final TsFileWriter writer = new TsFileWriter(file)) {
      writeDevice(writer, schemaList, DEVICE_0, 0);
      writer.flushAllChunkGroups();
      secondGroupMarkerOffset = writer.getIOWriter().getPos();
      writeDevice(writer, schemaList, DEVICE_1, 10_000);
      writeDevice(writer, schemaList, DEVICE_2, 20_000);
    }
    return secondGroupMarkerOffset;
  }

  private void writeAlignedBooleanTsFile(final File file) throws Exception {
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }

    final List<MeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.BOOLEAN, TSEncoding.PLAIN),
            new MeasurementSchema("s1", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    try (final TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerAlignedTimeseries(new Path(ALIGNED_DEVICE), schemas);
      final Tablet tablet = new Tablet(ALIGNED_DEVICE, schemas, 4);
      for (int row = 0; row < 4; row++) {
        tablet.addTimestamp(row, row);
        tablet.addValue("s0", row, row % 2 == 0);
        tablet.addValue("s1", row, row % 2 != 0);
      }
      tablet.rowSize = 4;
      writer.writeAligned(tablet);
    }
  }

  private void writeAlignedBooleanTsFileWithMultipleChunksInGroup(final File file)
      throws Exception {
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }

    final List<MeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.BOOLEAN, TSEncoding.PLAIN),
            new MeasurementSchema("s1", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    final List<IMeasurementSchema> alignedSchemas = new ArrayList<>(schemas);
    try (final TsFileIOWriter writer = new TsFileIOWriter(file)) {
      writer.startChunkGroup(new PlainDeviceID(ALIGNED_DEVICE));
      for (int timestamp = 0; timestamp < 2; timestamp++) {
        final AlignedChunkWriterImpl chunkWriter = new AlignedChunkWriterImpl(alignedSchemas);
        chunkWriter.getTimeChunkWriter().write(timestamp);
        chunkWriter.getValueChunkWriterByIndex(0).write(timestamp, timestamp == 0, false);
        chunkWriter.getValueChunkWriterByIndex(1).write(timestamp, timestamp != 0, false);
        chunkWriter.writeToFileWriter(writer);
      }
      writer.endChunkGroup();
      writer.endFile();
    }
  }

  private void writeWideAlignedTsFile(
      final File file, final String device, final int measurementCount) throws Exception {
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }

    final List<MeasurementSchema> schemaList = new java.util.ArrayList<>();
    for (int measurementIndex = 0; measurementIndex < measurementCount; ++measurementIndex) {
      schemaList.add(
          new MeasurementSchema("s" + measurementIndex, TSDataType.INT64, TSEncoding.PLAIN));
    }

    try (final TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerAlignedTimeseries(new Path(device), schemaList);

      final Tablet tablet = new Tablet(device, schemaList, ROW_COUNT_PER_DEVICE);
      for (int row = 0; row < ROW_COUNT_PER_DEVICE; ++row) {
        tablet.addTimestamp(row, row);
        for (int measurementIndex = 0; measurementIndex < measurementCount; ++measurementIndex) {
          tablet.addValue("s" + measurementIndex, row, (long) measurementIndex * 10_000 + row);
        }
      }
      tablet.rowSize = ROW_COUNT_PER_DEVICE;
      writer.writeAligned(tablet);
    }
  }

  private void writeWideAlignedRow(
      final TsFileWriter writer, final List<MeasurementSchema> schemas, final long timestamp)
      throws Exception {
    final Tablet tablet = new Tablet(DEVICE_0, schemas, 1);
    tablet.addTimestamp(0, timestamp);
    for (int measurement = 0; measurement < schemas.size(); measurement++) {
      tablet.addValue(
          schemas.get(measurement).getMeasurementId(), 0, measurement * 10_000L + timestamp);
    }
    tablet.rowSize = 1;
    writer.writeAligned(tablet);
  }

  private void writeDevice(
      final TsFileWriter writer,
      final List<MeasurementSchema> schemaList,
      final String device,
      final long valueBase)
      throws Exception {
    writer.registerTimeseries(new Path(device), schemaList);

    final Tablet tablet = new Tablet(device, schemaList, ROW_COUNT_PER_DEVICE);
    for (int row = 0; row < ROW_COUNT_PER_DEVICE; ++row) {
      tablet.addTimestamp(row, row);
      tablet.addValue("s0", row, valueBase + row);
      tablet.addValue("s1", row, valueBase + ROW_COUNT_PER_DEVICE + row);
    }
    tablet.rowSize = ROW_COUNT_PER_DEVICE;
    writer.write(tablet);
  }

  private void corruptMeasurementChunk(
      final File file, final String device, final String measurement) throws Exception {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      final IDeviceID deviceId = new PlainDeviceID(device);
      final List<IChunkMetadata> chunkMetadataList =
          reader.getIChunkMetadataList(new Path(deviceId, measurement, false));
      Assert.assertFalse(chunkMetadataList.isEmpty());

      final long chunkHeaderOffset =
          getTargetChunkMetadata(chunkMetadataList.get(0), measurement).getOffsetOfChunkHeader();
      try (final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
        randomAccessFile.seek(chunkHeaderOffset + 64);
        randomAccessFile.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
      }
    }
  }

  private IChunkMetadata getTargetChunkMetadata(
      final IChunkMetadata chunkMetadata, final String measurement) {
    if (!(chunkMetadata instanceof AlignedChunkMetadata)) {
      return chunkMetadata;
    }

    final IChunkMetadata valueChunkMetadata =
        ((AlignedChunkMetadata) chunkMetadata)
            .getValueChunkMetadataList().stream()
                .filter(Objects::nonNull)
                .filter(metadata -> measurement.equals(metadata.getMeasurementUid()))
                .findFirst()
                .orElse(null);
    Assert.assertNotNull(valueChunkMetadata);
    return valueChunkMetadata;
  }

  private void assertMeasurementLoadedCompletely(
      final Map<String, Integer> pointCountByTimeseries,
      final String device,
      final int measurementIndex) {
    Assert.assertEquals(
        ROW_COUNT_PER_DEVICE,
        pointCountByTimeseries.getOrDefault(device + ".s" + measurementIndex, 0).intValue());
  }

  private boolean scanParserFails(final File file) throws Exception {
    try (final TsFileInsertionScanDataContainer parser =
        new TsFileInsertionScanDataContainer(
            file, new IoTDBPipePattern(null), Long.MIN_VALUE, Long.MAX_VALUE, null, null, true)) {
      parser.toTabletWithIsAligneds().forEach(tabletWithIsAligned -> {});
      return false;
    } catch (final Exception e) {
      return true;
    }
  }

  private void collectLoadedLongPoints(
      final Statement statement, final Map<String, Map<Long, Long>> points) {
    Assert.assertTrue(statement instanceof InsertMultiTabletsStatement);
    for (final InsertTabletStatement tablet :
        ((InsertMultiTabletsStatement) statement).getInsertTabletStatementList()) {
      for (int column = 0; column < tablet.getMeasurements().length; column++) {
        final String path =
            tablet.getDevicePath().getFullPath() + "." + tablet.getMeasurements()[column];
        final Map<Long, Long> values = points.computeIfAbsent(path, key -> new HashMap<>());
        for (int row = 0; row < tablet.getRowCount(); row++) {
          if (!isNull(tablet, row, column)) {
            final long timestamp = tablet.getTimes()[row];
            Assert.assertNull(
                "Duplicate point: " + path + " at " + timestamp,
                values.put(timestamp, ((long[]) tablet.getColumns()[column])[row]));
          }
        }
      }
    }
  }

  private void collectLoadedPointsByTimeseries(
      final Statement statement, final Map<String, Integer> pointCountByTimeseries) {
    Assert.assertTrue(statement instanceof InsertMultiTabletsStatement);
    for (final InsertTabletStatement insertTabletStatement :
        ((InsertMultiTabletsStatement) statement).getInsertTabletStatementList()) {
      for (int row = 0; row < insertTabletStatement.getRowCount(); ++row) {
        for (int column = 0; column < insertTabletStatement.getMeasurements().length; ++column) {
          final String measurement = insertTabletStatement.getMeasurements()[column];
          if (measurement == null || isNull(insertTabletStatement, row, column)) {
            continue;
          }
          pointCountByTimeseries.merge(
              insertTabletStatement.getDevicePath().getFullPath() + "." + measurement,
              1,
              Integer::sum);
        }
      }
    }
  }

  private void collectLoadedPoints(
      final Statement statement, final Map<String, Integer> pointCountByDevice) {
    Assert.assertTrue(statement instanceof InsertMultiTabletsStatement);
    for (final InsertTabletStatement insertTabletStatement :
        ((InsertMultiTabletsStatement) statement).getInsertTabletStatementList()) {
      pointCountByDevice.merge(
          insertTabletStatement.getDevicePath().getFullPath(),
          countNonNullPoints(insertTabletStatement),
          Integer::sum);
    }
  }

  private int countNonNullPoints(final InsertTabletStatement insertTabletStatement) {
    int pointCount = 0;
    for (int row = 0; row < insertTabletStatement.getRowCount(); ++row) {
      for (int column = 0; column < insertTabletStatement.getMeasurements().length; ++column) {
        if (insertTabletStatement.getMeasurements()[column] != null
            && !isNull(insertTabletStatement, row, column)) {
          ++pointCount;
        }
      }
    }
    return pointCount;
  }

  private boolean isNull(
      final InsertTabletStatement insertTabletStatement, final int row, final int column) {
    final Object[] columns = insertTabletStatement.getColumns();
    if (columns == null || column >= columns.length || columns[column] == null) {
      return true;
    }

    final BitMap[] bitMaps = insertTabletStatement.getBitMaps();
    return bitMaps != null
        && column < bitMaps.length
        && bitMaps[column] != null
        && bitMaps[column].isMarked(row);
  }

  private static class PipeConfigAccessor {
    private static boolean getPipeMemoryManagementEnabled() {
      return CommonDescriptor.getInstance().getConfig().getPipeMemoryManagementEnabled();
    }

    private static void setPipeMemoryManagementEnabled(final boolean enabled) {
      CommonDescriptor.getInstance().getConfig().setPipeMemoryManagementEnabled(enabled);
    }
  }

  private static class ThrowingTabletIterator extends LoadTreeTsFileTabletIterator {
    private final Pair<Tablet, Boolean> tabletWithIsAligned;
    private boolean tabletAvailable = true;

    private ThrowingTabletIterator(final File file, final Tablet tablet) {
      super(file, true);
      tabletWithIsAligned = new Pair<>(tablet, false);
    }

    @Override
    public boolean hasNext() {
      if (tabletAvailable) {
        return true;
      }
      throw new IllegalStateException("synthetic parser failure");
    }

    @Override
    public Pair<Tablet, Boolean> next() {
      if (!tabletAvailable) {
        throw new IllegalStateException("synthetic parser failure");
      }
      tabletAvailable = false;
      return tabletWithIsAligned;
    }

    @Override
    public void close() {
      // No parser resources are allocated by this test iterator.
    }
  }
}
