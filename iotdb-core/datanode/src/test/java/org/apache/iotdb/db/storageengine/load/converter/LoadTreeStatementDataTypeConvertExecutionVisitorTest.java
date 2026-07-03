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
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBPipePattern;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.scan.TsFileInsertionScanDataContainer;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
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
        visitor.visitLoadFile(LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()), null);

    Assert.assertTrue(status.isPresent());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());
    final int loadedPointCountBeforeCorruption = pointCountByDevice.getOrDefault(DEVICE_0, 0);
    final int loadedPointCountAfterFallback = pointCountByDevice.getOrDefault(DEVICE_2, 0);
    Assert.assertTrue(loadedPointCountBeforeCorruption > 0);
    Assert.assertEquals(loadedPointCountBeforeCorruption, loadedPointCountAfterFallback);
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
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());

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
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());

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
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.get().getCode());

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

  private void writeTsFile(final File file) throws Exception {
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }

    final List<MeasurementSchema> schemaList =
        Arrays.asList(
            new MeasurementSchema("s0", TSDataType.INT64, TSEncoding.PLAIN),
            new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));

    try (final TsFileWriter writer = new TsFileWriter(file)) {
      writeDevice(writer, schemaList, DEVICE_0, 0);
      writeDevice(writer, schemaList, DEVICE_1, 10_000);
      writeDevice(writer, schemaList, DEVICE_2, 20_000);
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
}
