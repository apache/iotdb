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

import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.TsFileInsertionDataContainer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorUtils;

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
        for (Pair<Long, Long> startEndTime : startEndTimes) {
          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 0, startEndTime.left, startEndTime.right);
          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 1, startEndTime.left, startEndTime.right);
          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 2, startEndTime.left, startEndTime.right);

          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 999, startEndTime.left, startEndTime.right);
          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 1000, startEndTime.left, startEndTime.right);
          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 1001, startEndTime.left, startEndTime.right);

          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 999 * 2 + 1, startEndTime.left, startEndTime.right);
          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 1000, startEndTime.left, startEndTime.right);
          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 1001 * 2 - 1, startEndTime.left, startEndTime.right);

          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 1023, startEndTime.left, startEndTime.right);
          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 1024, startEndTime.left, startEndTime.right);
          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 1025, startEndTime.left, startEndTime.right);

          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 1023 * 2 + 1, startEndTime.left, startEndTime.right);
          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 1024 * 2, startEndTime.left, startEndTime.right);
          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 1025 * 2 - 1, startEndTime.left, startEndTime.right);

          testToTabletInsertionEvents(
              deviceNumber, measurementNumber, 10001, startEndTime.left, startEndTime.right);
        }
      }
    }
  }

  private void testToTabletInsertionEvents(
      int deviceNumber,
      int measurementNumber,
      int rowNumberInOneDevice,
      long startTime,
      long endTime)
      throws Exception {
    LOGGER.info(
        "testToTabletInsertionEvents: deviceNumber: {}, measurementNumber: {}, rowNumberInOneDevice: {}, startTime: {}, endTime: {}",
        deviceNumber,
        measurementNumber,
        rowNumberInOneDevice,
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

    try (final TsFileInsertionDataContainer alignedContainer =
            new TsFileInsertionDataContainer(alignedTsFile, "root", startTime, endTime);
        final TsFileInsertionDataContainer nonalignedContainer =
            new TsFileInsertionDataContainer(nonalignedTsFile, "root", startTime, endTime); ) {
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
                                              (tablet, rowCollector) -> {
                                                new PipeRawTabletInsertionEvent(tablet)
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
                                                        });
                                              }))));

      Assert.assertEquals(count1.getAndSet(0), deviceNumber * expectedRowNumber);
      Assert.assertEquals(count2.getAndSet(0), deviceNumber * expectedRowNumber);
      Assert.assertEquals(count3.getAndSet(0), deviceNumber * expectedRowNumber);

      nonalignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processTablet(
                          (tablet, rowCollector) -> {
                            new PipeRawTabletInsertionEvent(tablet)
                                .processRowByRow(
                                    (row, collector) -> {
                                      try {
                                        rowCollector.collectRow(row);
                                        Assert.assertEquals(measurementNumber, row.size());
                                        count1.incrementAndGet();
                                      } catch (IOException e) {
                                        throw new RuntimeException(e);
                                      }
                                    });
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

      Assert.assertEquals(count1.get(), deviceNumber * expectedRowNumber);
      Assert.assertEquals(count2.get(), deviceNumber * expectedRowNumber);
      Assert.assertEquals(count3.get(), deviceNumber * expectedRowNumber);
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
                            oneDeviceInAlignedTsFile.set(k);
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
                            oneDeviceInUnalignedTsFile.set(k);
                            oneMeasurementInUnalignedTsFile.set(new Path(k, p, false).toString());
                          }));
    }

    try (final TsFileInsertionDataContainer alignedContainer =
            new TsFileInsertionDataContainer(
                alignedTsFile, oneDeviceInAlignedTsFile.get(), startTime, endTime);
        final TsFileInsertionDataContainer nonalignedContainer =
            new TsFileInsertionDataContainer(
                nonalignedTsFile, oneDeviceInUnalignedTsFile.get(), startTime, endTime); ) {
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
                                              (tablet, rowCollector) -> {
                                                new PipeRawTabletInsertionEvent(tablet)
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
                                                        });
                                              }))));

      Assert.assertEquals(count1.getAndSet(0), expectedRowNumber);
      Assert.assertEquals(count2.getAndSet(0), expectedRowNumber);
      Assert.assertEquals(count3.getAndSet(0), expectedRowNumber);

      nonalignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processTablet(
                          (tablet, rowCollector) -> {
                            new PipeRawTabletInsertionEvent(tablet)
                                .processRowByRow(
                                    (row, collector) -> {
                                      try {
                                        rowCollector.collectRow(row);
                                        Assert.assertEquals(measurementNumber, row.size());
                                        count1.incrementAndGet();
                                      } catch (IOException e) {
                                        throw new RuntimeException(e);
                                      }
                                    });
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

      Assert.assertEquals(count1.get(), expectedRowNumber);
      Assert.assertEquals(count2.get(), expectedRowNumber);
      Assert.assertEquals(count3.get(), expectedRowNumber);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (final TsFileInsertionDataContainer alignedContainer =
            new TsFileInsertionDataContainer(
                alignedTsFile, oneMeasurementInAlignedTsFile.get(), startTime, endTime);
        final TsFileInsertionDataContainer nonalignedContainer =
            new TsFileInsertionDataContainer(
                nonalignedTsFile, oneMeasurementInUnalignedTsFile.get(), startTime, endTime); ) {
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
                                              (tablet, rowCollector) -> {
                                                new PipeRawTabletInsertionEvent(tablet)
                                                    .processRowByRow(
                                                        (row, collector) -> {
                                                          try {
                                                            rowCollector.collectRow(row);
                                                            Assert.assertEquals(1, row.size());
                                                            count3.incrementAndGet();
                                                          } catch (IOException e) {
                                                            throw new RuntimeException(e);
                                                          }
                                                        });
                                              }))));

      Assert.assertEquals(count1.getAndSet(0), expectedRowNumber);
      Assert.assertEquals(count2.getAndSet(0), expectedRowNumber);
      Assert.assertEquals(count3.getAndSet(0), expectedRowNumber);

      nonalignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processTablet(
                          (tablet, rowCollector) -> {
                            new PipeRawTabletInsertionEvent(tablet)
                                .processRowByRow(
                                    (row, collector) -> {
                                      try {
                                        rowCollector.collectRow(row);
                                        Assert.assertEquals(1, row.size());
                                        count1.incrementAndGet();
                                      } catch (IOException e) {
                                        throw new RuntimeException(e);
                                      }
                                    });
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

      Assert.assertEquals(count1.get(), expectedRowNumber);
      Assert.assertEquals(count2.get(), expectedRowNumber);
      Assert.assertEquals(count3.get(), expectedRowNumber);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try (final TsFileInsertionDataContainer alignedContainer =
            new TsFileInsertionDataContainer(
                alignedTsFile, "not-exist-pattern", startTime, endTime);
        final TsFileInsertionDataContainer nonalignedContainer =
            new TsFileInsertionDataContainer(
                nonalignedTsFile, "not-exist-pattern", startTime, endTime); ) {
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
                                              (tablet, rowCollector) -> {
                                                new PipeRawTabletInsertionEvent(tablet)
                                                    .processRowByRow(
                                                        (row, collector) -> {
                                                          try {
                                                            rowCollector.collectRow(row);
                                                            Assert.assertEquals(0, row.size());
                                                            count3.incrementAndGet();
                                                          } catch (IOException e) {
                                                            throw new RuntimeException(e);
                                                          }
                                                        });
                                              }))));

      Assert.assertEquals(count1.getAndSet(0), 0);
      Assert.assertEquals(count2.getAndSet(0), 0);
      Assert.assertEquals(count3.getAndSet(0), 0);

      nonalignedContainer
          .toTabletInsertionEvents()
          .forEach(
              event ->
                  event
                      .processTablet(
                          (tablet, rowCollector) -> {
                            new PipeRawTabletInsertionEvent(tablet)
                                .processRowByRow(
                                    (row, collector) -> {
                                      try {
                                        rowCollector.collectRow(row);
                                        Assert.assertEquals(0, row.size());
                                        count1.incrementAndGet();
                                      } catch (IOException e) {
                                        throw new RuntimeException(e);
                                      }
                                    });
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

      Assert.assertEquals(count1.get(), 0);
      Assert.assertEquals(count2.get(), 0);
      Assert.assertEquals(count3.get(), 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
