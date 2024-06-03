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
import org.apache.iotdb.db.pipe.event.common.tsfile.TsFileInsertionDataContainer;

import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
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

  @After
  public void tearDown() throws Exception {
    if (alignedTsFile != null) {
      alignedTsFile.delete();
    }
    if (nonalignedTsFile != null) {
      nonalignedTsFile.delete();
    }
  }

  @Test
  public void testToTabletInsertionEvents() throws Exception {
    Set<Integer> deviceNumbers = new HashSet<>();
    deviceNumbers.add(1);
    deviceNumbers.add(2);

    Set<Integer> measurementNumbers = new HashSet<>();
    measurementNumbers.add(1);
    measurementNumbers.add(2);

    Set<String> patternFormats = new HashSet<>();
    patternFormats.add(PREFIX_FORMAT);
    patternFormats.add(IOTDB_FORMAT);

    Set<Pair<Long, Long>> startEndTimes = new HashSet<>();
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

    for (int deviceNumber : deviceNumbers) {
      for (int measurementNumber : measurementNumbers) {
        for (String patternFormat : patternFormats) {
          for (Pair<Long, Long> startEndTime : startEndTimes) {
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                0,
                patternFormat,
                startEndTime.left,
                startEndTime.right);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1,
                patternFormat,
                startEndTime.left,
                startEndTime.right);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                2,
                patternFormat,
                startEndTime.left,
                startEndTime.right);

            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                999,
                patternFormat,
                startEndTime.left,
                startEndTime.right);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1000,
                patternFormat,
                startEndTime.left,
                startEndTime.right);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1001,
                patternFormat,
                startEndTime.left,
                startEndTime.right);

            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                999 * 2 + 1,
                patternFormat,
                startEndTime.left,
                startEndTime.right);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1000,
                patternFormat,
                startEndTime.left,
                startEndTime.right);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1001 * 2 - 1,
                patternFormat,
                startEndTime.left,
                startEndTime.right);

            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1023,
                patternFormat,
                startEndTime.left,
                startEndTime.right);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1024,
                patternFormat,
                startEndTime.left,
                startEndTime.right);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1025,
                patternFormat,
                startEndTime.left,
                startEndTime.right);

            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1023 * 2 + 1,
                patternFormat,
                startEndTime.left,
                startEndTime.right);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1024 * 2,
                patternFormat,
                startEndTime.left,
                startEndTime.right);
            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                1025 * 2 - 1,
                patternFormat,
                startEndTime.left,
                startEndTime.right);

            testToTabletInsertionEvents(
                deviceNumber,
                measurementNumber,
                10001,
                patternFormat,
                startEndTime.left,
                startEndTime.right);
          }
        }
      }
    }
  }

  private void testToTabletInsertionEvents(
      int deviceNumber,
      int measurementNumber,
      int rowNumberInOneDevice,
      String patternFormat,
      long startTime,
      long endTime)
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

    try (final TsFileInsertionDataContainer alignedContainer =
            new TsFileInsertionDataContainer(alignedTsFile, rootPattern, startTime, endTime);
        final TsFileInsertionDataContainer nonalignedContainer =
            new TsFileInsertionDataContainer(nonalignedTsFile, rootPattern, startTime, endTime)) {
      AtomicInteger count1 = new AtomicInteger(0);
      AtomicInteger count2 = new AtomicInteger(0);
      AtomicInteger count3 = new AtomicInteger(0);

      alignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processRowByRow(
                          (row, collector) -> {
                            try {
                              collector.collectRow(row);
                              Assert.assertEquals(measurementNumber, row.size());
                              count1.incrementAndGet();
                            } catch (IOException e) {
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
                                          Assert.assertEquals(measurementNumber, row.size());
                                          count2.incrementAndGet();
                                        } catch (IOException e) {
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
                                                              Assert.assertEquals(
                                                                  measurementNumber, row.size());
                                                              count3.incrementAndGet();
                                                            } catch (IOException e) {
                                                              throw new RuntimeException(e);
                                                            }
                                                          })))));

      Assert.assertEquals(count1.getAndSet(0), deviceNumber * expectedRowNumber);
      Assert.assertEquals(count2.getAndSet(0), deviceNumber * expectedRowNumber);
      Assert.assertEquals(count3.getAndSet(0), deviceNumber * expectedRowNumber);

      nonalignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processTablet(
                          (tablet, rowCollector) ->
                              new PipeRawTabletInsertionEvent(tablet, false)
                                  .processRowByRow(
                                      (row, collector) -> {
                                        try {
                                          rowCollector.collectRow(row);
                                          Assert.assertEquals(measurementNumber, row.size());
                                          count1.incrementAndGet();
                                        } catch (IOException e) {
                                          throw new RuntimeException(e);
                                        }
                                      }))
                      .forEach(
                          tabletInsertionEvent1 ->
                              tabletInsertionEvent1
                                  .processRowByRow(
                                      (row, collector) -> {
                                        try {
                                          collector.collectRow(row);
                                          Assert.assertEquals(measurementNumber, row.size());
                                          count2.incrementAndGet();
                                        } catch (IOException e) {
                                          throw new RuntimeException(e);
                                        }
                                      })
                                  .forEach(
                                      tabletInsertionEvent2 ->
                                          tabletInsertionEvent2.processRowByRow(
                                              (row, collector) -> {
                                                try {
                                                  collector.collectRow(row);
                                                  Assert.assertEquals(
                                                      measurementNumber, row.size());
                                                  count3.incrementAndGet();
                                                } catch (IOException e) {
                                                  throw new RuntimeException(e);
                                                }
                                              }))));

      Assert.assertEquals(deviceNumber * expectedRowNumber, count1.get());
      Assert.assertEquals(deviceNumber * expectedRowNumber, count2.get());
      Assert.assertEquals(deviceNumber * expectedRowNumber, count3.get());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    AtomicReference<String> oneDeviceInAlignedTsFile = new AtomicReference<>();
    AtomicReference<String> oneMeasurementInAlignedTsFile = new AtomicReference<>();

    AtomicReference<String> oneDeviceInUnalignedTsFile = new AtomicReference<>();
    AtomicReference<String> oneMeasurementInUnalignedTsFile = new AtomicReference<>();

    try (TsFileSequenceReader alignedReader =
            new TsFileSequenceReader(alignedTsFile.getAbsolutePath());
        TsFileSequenceReader nonalignedReader =
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

    try (final TsFileInsertionDataContainer alignedContainer =
            new TsFileInsertionDataContainer(
                alignedTsFile, oneAlignedDevicePattern, startTime, endTime);
        final TsFileInsertionDataContainer nonalignedContainer =
            new TsFileInsertionDataContainer(
                nonalignedTsFile, oneNonAlignedDevicePattern, startTime, endTime)) {
      AtomicInteger count1 = new AtomicInteger(0);
      AtomicInteger count2 = new AtomicInteger(0);
      AtomicInteger count3 = new AtomicInteger(0);

      alignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processRowByRow(
                          (row, collector) -> {
                            try {
                              collector.collectRow(row);
                              Assert.assertEquals(measurementNumber, row.size());
                              count1.incrementAndGet();
                            } catch (IOException e) {
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
                                          Assert.assertEquals(measurementNumber, row.size());
                                          count2.incrementAndGet();
                                        } catch (IOException e) {
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
                                                              Assert.assertEquals(
                                                                  measurementNumber, row.size());
                                                              count3.incrementAndGet();
                                                            } catch (IOException e) {
                                                              throw new RuntimeException(e);
                                                            }
                                                          })))));

      Assert.assertEquals(expectedRowNumber, count1.getAndSet(0));
      Assert.assertEquals(expectedRowNumber, count2.getAndSet(0));
      Assert.assertEquals(expectedRowNumber, count3.getAndSet(0));

      nonalignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processTablet(
                          (tablet, rowCollector) ->
                              new PipeRawTabletInsertionEvent(tablet, false)
                                  .processRowByRow(
                                      (row, collector) -> {
                                        try {
                                          rowCollector.collectRow(row);
                                          Assert.assertEquals(measurementNumber, row.size());
                                          count1.incrementAndGet();
                                        } catch (IOException e) {
                                          throw new RuntimeException(e);
                                        }
                                      }))
                      .forEach(
                          tabletInsertionEvent1 ->
                              tabletInsertionEvent1
                                  .processRowByRow(
                                      (row, collector) -> {
                                        try {
                                          collector.collectRow(row);
                                          Assert.assertEquals(measurementNumber, row.size());
                                          count2.incrementAndGet();
                                        } catch (IOException e) {
                                          throw new RuntimeException(e);
                                        }
                                      })
                                  .forEach(
                                      tabletInsertionEvent2 ->
                                          tabletInsertionEvent2.processRowByRow(
                                              (row, collector) -> {
                                                try {
                                                  collector.collectRow(row);
                                                  Assert.assertEquals(
                                                      measurementNumber, row.size());
                                                  count3.incrementAndGet();
                                                } catch (IOException e) {
                                                  throw new RuntimeException(e);
                                                }
                                              }))));

      Assert.assertEquals(expectedRowNumber, count1.get());
      Assert.assertEquals(expectedRowNumber, count2.get());
      Assert.assertEquals(expectedRowNumber, count3.get());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

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

    try (final TsFileInsertionDataContainer alignedContainer =
            new TsFileInsertionDataContainer(
                alignedTsFile, oneAlignedMeasurementPattern, startTime, endTime);
        final TsFileInsertionDataContainer nonalignedContainer =
            new TsFileInsertionDataContainer(
                nonalignedTsFile, oneNonAlignedMeasurementPattern, startTime, endTime)) {
      AtomicInteger count1 = new AtomicInteger(0);
      AtomicInteger count2 = new AtomicInteger(0);
      AtomicInteger count3 = new AtomicInteger(0);

      alignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processRowByRow(
                          (row, collector) -> {
                            try {
                              collector.collectRow(row);
                              Assert.assertEquals(1, row.size());
                              count1.incrementAndGet();
                            } catch (IOException e) {
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
                                          Assert.assertEquals(1, row.size());
                                          count2.incrementAndGet();
                                        } catch (IOException e) {
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
                                                              Assert.assertEquals(1, row.size());
                                                              count3.incrementAndGet();
                                                            } catch (IOException e) {
                                                              throw new RuntimeException(e);
                                                            }
                                                          })))));

      Assert.assertEquals(expectedRowNumber, count1.getAndSet(0));
      Assert.assertEquals(expectedRowNumber, count2.getAndSet(0));
      Assert.assertEquals(expectedRowNumber, count3.getAndSet(0));

      nonalignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processTablet(
                          (tablet, rowCollector) ->
                              new PipeRawTabletInsertionEvent(tablet, false)
                                  .processRowByRow(
                                      (row, collector) -> {
                                        try {
                                          rowCollector.collectRow(row);
                                          Assert.assertEquals(1, row.size());
                                          count1.incrementAndGet();
                                        } catch (IOException e) {
                                          throw new RuntimeException(e);
                                        }
                                      }))
                      .forEach(
                          tabletInsertionEvent1 ->
                              tabletInsertionEvent1
                                  .processRowByRow(
                                      (row, collector) -> {
                                        try {
                                          collector.collectRow(row);
                                          Assert.assertEquals(1, row.size());
                                          count2.incrementAndGet();
                                        } catch (IOException e) {
                                          throw new RuntimeException(e);
                                        }
                                      })
                                  .forEach(
                                      tabletInsertionEvent2 ->
                                          tabletInsertionEvent2.processRowByRow(
                                              (row, collector) -> {
                                                try {
                                                  collector.collectRow(row);
                                                  Assert.assertEquals(1, row.size());
                                                  count3.incrementAndGet();
                                                } catch (IOException e) {
                                                  throw new RuntimeException(e);
                                                }
                                              }))));

      Assert.assertEquals(expectedRowNumber, count1.get());
      Assert.assertEquals(expectedRowNumber, count2.get());
      Assert.assertEquals(expectedRowNumber, count3.get());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

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

    try (final TsFileInsertionDataContainer alignedContainer =
            new TsFileInsertionDataContainer(alignedTsFile, notExistPattern, startTime, endTime);
        final TsFileInsertionDataContainer nonalignedContainer =
            new TsFileInsertionDataContainer(
                nonalignedTsFile, notExistPattern, startTime, endTime)) {
      AtomicInteger count1 = new AtomicInteger(0);
      AtomicInteger count2 = new AtomicInteger(0);
      AtomicInteger count3 = new AtomicInteger(0);

      alignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processRowByRow(
                          (row, collector) -> {
                            try {
                              collector.collectRow(row);
                              Assert.assertEquals(0, row.size());
                              count1.incrementAndGet();
                            } catch (IOException e) {
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
                                          Assert.assertEquals(0, row.size());
                                          count2.incrementAndGet();
                                        } catch (IOException e) {
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
                                                              Assert.assertEquals(0, row.size());
                                                              count3.incrementAndGet();
                                                            } catch (IOException e) {
                                                              throw new RuntimeException(e);
                                                            }
                                                          })))));

      Assert.assertEquals(0, count1.getAndSet(0));
      Assert.assertEquals(0, count2.getAndSet(0));
      Assert.assertEquals(0, count3.getAndSet(0));

      nonalignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processTablet(
                          (tablet, rowCollector) ->
                              new PipeRawTabletInsertionEvent(tablet, false)
                                  .processRowByRow(
                                      (row, collector) -> {
                                        try {
                                          rowCollector.collectRow(row);
                                          Assert.assertEquals(0, row.size());
                                          count1.incrementAndGet();
                                        } catch (IOException e) {
                                          throw new RuntimeException(e);
                                        }
                                      }))
                      .forEach(
                          tabletInsertionEvent1 ->
                              tabletInsertionEvent1
                                  .processRowByRow(
                                      (row, collector) -> {
                                        try {
                                          collector.collectRow(row);
                                          Assert.assertEquals(0, row.size());
                                          count2.incrementAndGet();
                                        } catch (IOException e) {
                                          throw new RuntimeException(e);
                                        }
                                      })
                                  .forEach(
                                      tabletInsertionEvent2 ->
                                          tabletInsertionEvent2.processRowByRow(
                                              (row, collector) -> {
                                                try {
                                                  collector.collectRow(row);
                                                  Assert.assertEquals(0, row.size());
                                                  count3.incrementAndGet();
                                                } catch (IOException e) {
                                                  throw new RuntimeException(e);
                                                }
                                              }))));

      Assert.assertEquals(0, count1.get());
      Assert.assertEquals(0, count2.get());
      Assert.assertEquals(0, count3.get());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
