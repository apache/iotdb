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
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.TsFileInsertionEventScanParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.table.TsFileInsertionEventTableParser;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
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
import org.apache.tsfile.write.schema.Schema;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

public class TsFileInsertionEventParserTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TsFileInsertionEventParserTest.class);

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
      ModificationFile.getExclusiveMods(alignedTsFile).delete();
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
  public void testTableParserWithAllNullFields() throws Exception {
    alignedTsFile = new File("table-all-null.tsfile");
    writeTableTsFileWithNullableFields(true);

    assertParsedTablets(parseTableTablets(false), Arrays.asList("tag1", "s1", "s2"), 3, true);
  }

  @Test
  public void testTableParserWithMixedAllNullFields() throws Exception {
    alignedTsFile = new File("table-mixed-all-null.tsfile");
    writeTableTsFileWithNullableFields(false);

    assertParsedTablets(parseTableTablets(false), Arrays.asList("tag1", "s1", "s2"), 3, false);
  }

  @Test
  public void testTableParserWithAllNullFieldsAndDeletedValueChunk() throws Exception {
    alignedTsFile = new File("table-all-null-with-mod.tsfile");
    writeTableTsFileWithNullableFields(false);
    try (final ModificationFile modificationFile =
        new ModificationFile(ModificationFile.getExclusiveMods(alignedTsFile), false)) {
      modificationFile.write(
          new TableDeletionEntry(
              new DeletionPredicate(
                  "table1", new IDPredicate.NOP(), Collections.singletonList("s2")),
              new TimeRange(100, 102)));
    }

    assertParsedTablets(parseTableTablets(true), Arrays.asList("tag1", "s1"), 3, true);
  }

  private void writeTableTsFileWithNullableFields(final boolean allFieldsNull) throws Exception {
    final List<IMeasurementSchema> tableSchemaList =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("s1", TSDataType.INT64),
            new MeasurementSchema("s2", TSDataType.DOUBLE));
    final List<ColumnCategory> columnCategoryList =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);

    final Schema schema = new Schema();
    schema.registerTableSchema(new TableSchema("table1", tableSchemaList, columnCategoryList));
    try (final TsFileIOWriter writer = new TsFileIOWriter(alignedTsFile)) {
      writer.setSchema(schema);
      final IDeviceID deviceID = new StringArrayDeviceID(new String[] {"table1", "tagA"});
      writer.startChunkGroup(deviceID);

      final AlignedChunkWriterImpl chunkWriter =
          new AlignedChunkWriterImpl(tableSchemaList.subList(1, tableSchemaList.size()));
      for (int i = 0; i < 3; i++) {
        final long time = 100 + i;
        chunkWriter.getTimeChunkWriter().write(time);
        chunkWriter.getValueChunkWriterByIndex(0).write(time, 0L, true);
        chunkWriter.getValueChunkWriterByIndex(1).write(time, 1.0 + i, allFieldsNull);
      }
      chunkWriter.writeToFileWriter(writer);
      writer.endChunkGroup();
      writer.endFile();
    }
  }

  private List<Tablet> parseTableTablets(final boolean isWithMod) throws IOException {
    final List<Tablet> parsedTablets = new ArrayList<>();
    try (final TsFileInsertionEventTableParser parser =
        new TsFileInsertionEventTableParser(
            alignedTsFile,
            new TablePattern(true, null, null),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            null,
            null,
            null,
            isWithMod)) {
      for (final TabletInsertionEvent event : parser.toTabletInsertionEvents()) {
        Assert.assertTrue(event instanceof PipeRawTabletInsertionEvent);
        parsedTablets.add(((PipeRawTabletInsertionEvent) event).convertToTablet());
      }
    }
    return parsedTablets;
  }

  private void assertParsedTablets(
      final List<Tablet> tablets,
      final List<String> expectedColumns,
      final int expectedRowCount,
      final boolean expectS2Null) {
    Assert.assertFalse(tablets.isEmpty());
    int rowCount = 0;
    for (final Tablet tablet : tablets) {
      if (tablet.getRowSize() == 0) {
        continue;
      }

      Assert.assertEquals("table1", tablet.getTableName());
      Assert.assertEquals(
          expectedColumns,
          tablet.getSchemas().stream()
              .map(IMeasurementSchema::getMeasurementName)
              .collect(Collectors.toList()));
      Assert.assertEquals(ColumnCategory.TAG, tablet.getColumnTypes().get(0));
      for (int i = 1; i < tablet.getColumnTypes().size(); i++) {
        Assert.assertEquals(ColumnCategory.FIELD, tablet.getColumnTypes().get(i));
      }

      for (int i = 0; i < tablet.getRowSize(); i++, rowCount++) {
        Assert.assertEquals(100 + rowCount, tablet.getTimestamp(i));
        Assert.assertFalse(tablet.isNull(i, 0));
        Assert.assertTrue(tablet.isNull(i, 1));
        if (expectedColumns.size() > 2) {
          Assert.assertEquals(expectS2Null, tablet.isNull(i, 2));
          if (!expectS2Null) {
            Assert.assertEquals(1.0 + rowCount, (double) tablet.getValue(i, 2), 0.0);
          }
        }
      }
    }
    Assert.assertEquals(expectedRowCount, rowCount);
  }

  public void testScanParserResizesChunkMemoryForFirstAlignedValueChunk() throws Exception {
    final long originalPipeMaxReaderChunkSize =
        PipeConfig.getInstance().getPipeMaxReaderChunkSize();
    CommonDescriptor.getInstance().getConfig().setPipeMaxReaderChunkSize(0);

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
      }
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
}
