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

import org.apache.iotdb.commons.pipe.pattern.IoTDBPipePattern;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.pattern.PrefixPipePattern;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.TsFileInsertionDataContainer;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.query.TsFileInsertionQueryDataContainer;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.scan.TsFileInsertionScanDataContainer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.pipe.api.access.Row;

import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
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

  @After
  public void tearDown() throws Exception {
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

  public void testToTabletInsertionEvents(final boolean isQuery) throws Exception {
    // Test empty chunk
    testMixedTsFileWithEmptyChunk(isQuery);

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
                tsFile, pattern, startTime, endTime, null, null)) {
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
                                          tabletInsertionEvent2.processTablet(
                                              (tablet, rowCollector) ->
                                                  new PipeRawTabletInsertionEvent(tablet, false)
                                                      .processRowByRow(
                                                          (row, collector) -> {
                                                            try {
                                                              rowCollector.collectRow(row);
                                                              count3.addAndGet(getNonNullSize(row));
                                                            } catch (final IOException e) {
                                                              throw new RuntimeException(e);
                                                            }
                                                          })))));

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
}
