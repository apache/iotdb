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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;

@RunWith(Parameterized.class)
public class WritableMemChunkRegionScanTest {

  private static final int LAZY_ALIGNED_TEST_ROW_COUNT = ARRAY_SIZE * 8 + 123;
  private static final int LAZY_ALIGNED_S1_INTERVAL = 17;
  private static final int LAZY_ALIGNED_S2_INTERVAL = 19;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{0}, {1000}, {10000}, {20000}});
  }

  private int defaultTvListThreshold;
  private int tvListSortThreshold;

  public WritableMemChunkRegionScanTest(int tvListSortThreshold) {
    this.tvListSortThreshold = tvListSortThreshold;
  }

  @Before
  public void setup() {
    defaultTvListThreshold = IoTDBDescriptor.getInstance().getConfig().getTvListSortThreshold();
    IoTDBDescriptor.getInstance().getConfig().setTVListSortThreshold(tvListSortThreshold);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setTVListSortThreshold(defaultTvListThreshold);
  }

  @Test
  public void testAlignedWritableMemChunkRegionScan() {
    PrimitiveMemTable memTable = new PrimitiveMemTable("root.test", "0");
    try {
      List<IMeasurementSchema> measurementSchemas =
          Arrays.asList(
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT32),
              new MeasurementSchema("s3", TSDataType.INT32));
      AlignedWritableMemChunk writableMemChunk = null;
      int size = 100000;
      for (int i = 0; i < size; i++) {
        if (i <= 10000) {
          memTable.writeAlignedRow(
              new StringArrayDeviceID("root.test.d1"),
              measurementSchemas,
              i,
              new Object[] {1, null, 1});
        } else if (i <= 20000) {
          memTable.writeAlignedRow(
              new StringArrayDeviceID("root.test.d1"),
              measurementSchemas,
              i,
              new Object[] {null, null, 2});
        } else if (i <= 30000) {
          memTable.writeAlignedRow(
              new StringArrayDeviceID("root.test.d1"),
              measurementSchemas,
              i,
              new Object[] {3, null, null});
        } else {
          memTable.writeAlignedRow(
              new StringArrayDeviceID("root.test.d1"),
              measurementSchemas,
              i,
              new Object[] {4, 4, 4});
        }
      }
      writableMemChunk =
          (AlignedWritableMemChunk)
              memTable.getWritableMemChunk(new StringArrayDeviceID("root.test.d1"), "");
      List<BitMap> bitMaps = new ArrayList<>();
      long[] timestamps =
          writableMemChunk.getAnySatisfiedTimestamp(
              Arrays.asList(
                  Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
              bitMaps,
              true,
              null);
      Assert.assertEquals(2, timestamps.length);
      Assert.assertEquals(0, timestamps[0]);
      Assert.assertFalse(bitMaps.get(0).isMarked(0));
      Assert.assertTrue(bitMaps.get(0).isMarked(1));
      Assert.assertFalse(bitMaps.get(0).isMarked(2));
      Assert.assertTrue(bitMaps.get(1).isMarked(0));
      Assert.assertFalse(bitMaps.get(1).isMarked(1));
      Assert.assertTrue(bitMaps.get(1).isMarked(2));
      Assert.assertEquals(30001, timestamps[1]);

      bitMaps = new ArrayList<>();
      timestamps =
          writableMemChunk.getAnySatisfiedTimestamp(
              Arrays.asList(
                  Collections.emptyList(),
                  Collections.emptyList(),
                  Collections.singletonList(new TimeRange(0, 12000))),
              bitMaps,
              true,
              new TimeFilterOperators.TimeGt(10000000));
      Assert.assertEquals(0, timestamps.length);

      bitMaps = new ArrayList<>();
      timestamps =
          writableMemChunk.getAnySatisfiedTimestamp(
              Arrays.asList(
                  Collections.emptyList(),
                  Collections.emptyList(),
                  Collections.singletonList(new TimeRange(0, 12000))),
              bitMaps,
              true,
              new TimeFilterOperators.TimeGt(11000));

      Assert.assertEquals(3, timestamps.length);
      Assert.assertEquals(12001, timestamps[0]);
      Assert.assertTrue(bitMaps.get(0).isMarked(0));
      Assert.assertTrue(bitMaps.get(0).isMarked(1));
      Assert.assertFalse(bitMaps.get(0).isMarked(2));
      Assert.assertEquals(20001, timestamps[1]);
      Assert.assertFalse(bitMaps.get(1).isMarked(0));
      Assert.assertTrue(bitMaps.get(1).isMarked(1));
      Assert.assertTrue(bitMaps.get(1).isMarked(2));
      Assert.assertEquals(30001, timestamps[2]);
      Assert.assertTrue(bitMaps.get(2).isMarked(0));
      Assert.assertFalse(bitMaps.get(2).isMarked(1));
      Assert.assertTrue(bitMaps.get(2).isMarked(2));

      writableMemChunk.writeAlignedPoints(
          1000001, new Object[] {1, null, null}, measurementSchemas);
      writableMemChunk.writeAlignedPoints(
          1000002, new Object[] {null, 1, null}, measurementSchemas);
      writableMemChunk.writeAlignedPoints(1000002, new Object[] {1, 1, null}, measurementSchemas);
      writableMemChunk.writeAlignedPoints(
          1000003, new Object[] {1, null, null}, measurementSchemas);
      writableMemChunk.writeAlignedPoints(1000004, new Object[] {1, null, 1}, measurementSchemas);
      bitMaps = new ArrayList<>();
      timestamps =
          writableMemChunk.getAnySatisfiedTimestamp(
              Arrays.asList(
                  Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
              bitMaps,
              true,
              new TimeFilterOperators.TimeGt(1000000));
      Assert.assertEquals(3, timestamps.length);
      Assert.assertEquals(1000001, timestamps[0]);
      Assert.assertFalse(bitMaps.get(0).isMarked(0));
      Assert.assertTrue(bitMaps.get(0).isMarked(1));
      Assert.assertTrue(bitMaps.get(0).isMarked(2));
      Assert.assertEquals(1000002, timestamps[1]);
      Assert.assertTrue(bitMaps.get(1).isMarked(0));
      Assert.assertFalse(bitMaps.get(1).isMarked(1));
      Assert.assertTrue(bitMaps.get(1).isMarked(2));
      Assert.assertEquals(1000004, timestamps[2]);
      Assert.assertTrue(bitMaps.get(2).isMarked(0));
      Assert.assertTrue(bitMaps.get(2).isMarked(1));
      Assert.assertFalse(bitMaps.get(2).isMarked(2));

      Map<String, List<IChunkHandle>> chunkHandleMap = new HashMap<>();
      memTable.queryForDeviceRegionScan(
          new StringArrayDeviceID("root.test.d1"),
          true,
          Long.MIN_VALUE,
          new HashMap<>(),
          chunkHandleMap,
          Collections.emptyList(),
          new TimeFilterOperators.TimeGt(1000000));
      Assert.assertEquals(3, chunkHandleMap.size());
      Assert.assertArrayEquals(
          new long[] {1000001, 1000001}, chunkHandleMap.get("s1").get(0).getPageStatisticsTime());
      Assert.assertArrayEquals(
          new long[] {1000002, 1000002}, chunkHandleMap.get("s2").get(0).getPageStatisticsTime());
      Assert.assertArrayEquals(
          new long[] {1000004, 1000004}, chunkHandleMap.get("s3").get(0).getPageStatisticsTime());

      memTable.queryForSeriesRegionScan(
          new AlignedFullPath(
              new StringArrayDeviceID("root.test.d1"),
              IMeasurementSchema.getMeasurementNameList(measurementSchemas),
              measurementSchemas),
          Long.MIN_VALUE,
          new HashMap<>(),
          chunkHandleMap,
          Collections.emptyList(),
          new TimeFilterOperators.TimeGt(1000000));
      Assert.assertEquals(3, chunkHandleMap.size());
      Assert.assertArrayEquals(
          new long[] {1000001, 1000001}, chunkHandleMap.get("s1").get(0).getPageStatisticsTime());
      Assert.assertArrayEquals(
          new long[] {1000002, 1000002}, chunkHandleMap.get("s2").get(0).getPageStatisticsTime());
      Assert.assertArrayEquals(
          new long[] {1000004, 1000004}, chunkHandleMap.get("s3").get(0).getPageStatisticsTime());
    } finally {
      memTable.release();
    }
  }

  @Test
  public void testAlignedWritableMemChunkRegionScan2() throws IllegalPathException {
    PrimitiveMemTable memTable = new PrimitiveMemTable("root.test", "0");
    try {
      List<IMeasurementSchema> measurementSchemas =
          Arrays.asList(
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT32),
              new MeasurementSchema("s3", TSDataType.INT32));
      AlignedWritableMemChunk writableMemChunk = null;
      for (int i = 1000; i < 2000; i++) {
        memTable.writeAlignedRow(
            new StringArrayDeviceID("root.test.d1"), measurementSchemas, i, new Object[] {i, i, i});
      }
      for (int i = 1; i < 100; i++) {
        memTable.writeAlignedRow(
            new StringArrayDeviceID("root.test.d1"),
            measurementSchemas,
            i,
            new Object[] {i, null, i});
      }

      memTable.delete(
          new TreeDeletionEntry(
              new MeasurementPath(new StringArrayDeviceID("root.test.d1"), "s1"),
              new TimeRange(1, 1500)));
      writableMemChunk =
          (AlignedWritableMemChunk)
              memTable.getWritableMemChunk(new StringArrayDeviceID("root.test.d1"), "");
      writableMemChunk.sortTvListForFlush();
      List<BitMap> bitMaps = new ArrayList<>();
      long[] timestamps =
          writableMemChunk.getAnySatisfiedTimestamp(
              Arrays.asList(
                  Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
              bitMaps,
              true,
              null);
      Assert.assertEquals(3, timestamps.length);
      Assert.assertEquals(1, timestamps[0]);
      Assert.assertEquals(1000, timestamps[1]);
      Assert.assertEquals(1501, timestamps[2]);
    } finally {
      memTable.release();
    }
  }

  @Test
  public void testLazyAlignedRegionScanWithSortedIndex() {
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s3", TSDataType.INT32));
    AlignedWritableMemChunk writableMemChunk =
        new AlignedWritableMemChunk(measurementSchemas, false);
    IoTDBDescriptor.getInstance().getConfig().setTVListSortThreshold(0);

    int rowCount = LAZY_ALIGNED_TEST_ROW_COUNT;
    for (int i = rowCount - 1; i >= 0; i--) {
      Object[] values = new Object[] {null, null, null};
      if (i % LAZY_ALIGNED_S1_INTERVAL == 0) {
        values[0] = i;
      }
      if (i % LAZY_ALIGNED_S2_INTERVAL == 0) {
        values[1] = i;
      }
      writableMemChunk.writeAlignedPoints(i, values, measurementSchemas);
    }
    writableMemChunk.sortTvListForFlush();

    List<BitMap> bitMaps = new ArrayList<>();
    long[] timestamps =
        writableMemChunk.getAnySatisfiedTimestamp(
            Arrays.asList(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
            bitMaps,
            true,
            null);

    assertLazyAlignedQueryResult(timestamps, bitMaps);
  }

  @Test
  public void testLazyAlignedFlushSkipsUnallocatedAllNullRows() throws InterruptedException {
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s3", TSDataType.INT32));
    AlignedWritableMemChunk writableMemChunk =
        new AlignedWritableMemChunk(measurementSchemas, false);
    IoTDBDescriptor.getInstance().getConfig().setTVListSortThreshold(0);

    int rowCount = LAZY_ALIGNED_TEST_ROW_COUNT;
    int s1Count = 0;
    int s2Count = 0;
    int expectedTimestampCount = countExpectedLazyAlignedTimestamps(rowCount);
    for (int i = rowCount - 1; i >= 0; i--) {
      Object[] values = new Object[] {null, null, null};
      if (i % LAZY_ALIGNED_S1_INTERVAL == 0) {
        values[0] = i;
        s1Count++;
      }
      if (i % LAZY_ALIGNED_S2_INTERVAL == 0) {
        values[1] = i;
        s2Count++;
      }
      writableMemChunk.writeAlignedPoints(i, values, measurementSchemas);
    }
    writableMemChunk.sortTvListForFlush();

    BlockingQueue<Object> ioTaskQueue = new LinkedBlockingQueue<>();
    writableMemChunk.encodeWorkingAlignedTVList(ioTaskQueue, rowCount, rowCount);

    Assert.assertEquals(1, ioTaskQueue.size());
    AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) ioTaskQueue.take();
    Assert.assertEquals(
        expectedTimestampCount, chunkWriter.getTimeChunkWriter().getStatistics().getCount());
    Assert.assertEquals(
        s1Count, chunkWriter.getValueChunkWriterByIndex(0).getStatistics().getCount());
    Assert.assertEquals(
        s2Count, chunkWriter.getValueChunkWriterByIndex(1).getStatistics().getCount());
    Assert.assertEquals(0, chunkWriter.getValueChunkWriterByIndex(2).getStatistics().getCount());
  }

  @Test
  public void testLazyAlignedFlushWithMultipleChunksAndPages() throws InterruptedException {
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s3", TSDataType.INT32));
    AlignedWritableMemChunk writableMemChunk =
        new AlignedWritableMemChunk(measurementSchemas, false);
    IoTDBDescriptor.getInstance().getConfig().setTVListSortThreshold(0);

    int rowCount = LAZY_ALIGNED_TEST_ROW_COUNT;
    int s1Count = 0;
    int s2Count = 0;
    int expectedTimestampCount = countExpectedLazyAlignedTimestamps(rowCount);
    for (int i = rowCount - 1; i >= 0; i--) {
      Object[] values = new Object[] {null, null, null};
      if (i % LAZY_ALIGNED_S1_INTERVAL == 0) {
        values[0] = i;
        s1Count++;
      }
      if (i % LAZY_ALIGNED_S2_INTERVAL == 0) {
        values[1] = i;
        s2Count++;
      }
      writableMemChunk.writeAlignedPoints(i, values, measurementSchemas);
    }
    writableMemChunk.sortTvListForFlush();

    BlockingQueue<Object> ioTaskQueue = new LinkedBlockingQueue<>();
    writableMemChunk.encodeWorkingAlignedTVList(ioTaskQueue, 37, 11);

    Assert.assertTrue(ioTaskQueue.size() > 1);
    long actualTimeCount = 0;
    long actualS1Count = 0;
    long actualS2Count = 0;
    long actualS3Count = 0;
    while (!ioTaskQueue.isEmpty()) {
      AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) ioTaskQueue.take();
      actualTimeCount += chunkWriter.getTimeChunkWriter().getStatistics().getCount();
      actualS1Count += chunkWriter.getValueChunkWriterByIndex(0).getStatistics().getCount();
      actualS2Count += chunkWriter.getValueChunkWriterByIndex(1).getStatistics().getCount();
      actualS3Count += chunkWriter.getValueChunkWriterByIndex(2).getStatistics().getCount();
    }
    Assert.assertEquals(expectedTimestampCount, actualTimeCount);
    Assert.assertEquals(s1Count, actualS1Count);
    Assert.assertEquals(s2Count, actualS2Count);
    Assert.assertEquals(0, actualS3Count);
  }

  @Test
  public void testLazyAlignedInsertTabletRegionScanAndFlush() throws InterruptedException {
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s3", TSDataType.INT32));
    AlignedWritableMemChunk writableMemChunk =
        new AlignedWritableMemChunk(measurementSchemas, false);
    IoTDBDescriptor.getInstance().getConfig().setTVListSortThreshold(0);

    int rowCount = LAZY_ALIGNED_TEST_ROW_COUNT;
    long[] times = new long[rowCount];
    Object[] columns = new Object[] {new int[rowCount], new int[rowCount], new int[rowCount]};
    BitMap[] bitMaps =
        new BitMap[] {new BitMap(rowCount), new BitMap(rowCount), new BitMap(rowCount)};
    int s1Count = 0;
    int s2Count = 0;
    for (int i = 0; i < rowCount; i++) {
      times[i] = rowCount - i - 1;
      ((int[]) columns[0])[i] = (int) times[i];
      ((int[]) columns[1])[i] = (int) times[i];
      ((int[]) columns[2])[i] = (int) times[i];
      bitMaps[0].mark(i);
      bitMaps[1].mark(i);
      bitMaps[2].mark(i);
      if (times[i] % LAZY_ALIGNED_S1_INTERVAL == 0) {
        bitMaps[0].unmark(i);
        s1Count++;
      }
      if (times[i] % LAZY_ALIGNED_S2_INTERVAL == 0) {
        bitMaps[1].unmark(i);
        s2Count++;
      }
    }

    writableMemChunk.writeAlignedTablet(
        times, columns, bitMaps, measurementSchemas, 0, rowCount, null);
    writableMemChunk.sortTvListForFlush();

    List<BitMap> resultBitMaps = new ArrayList<>();
    long[] timestamps =
        writableMemChunk.getAnySatisfiedTimestamp(
            Arrays.asList(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
            resultBitMaps,
            true,
            null);
    assertLazyAlignedQueryResult(timestamps, resultBitMaps);

    int expectedTimestampCount = countExpectedLazyAlignedTimestamps(rowCount);

    BlockingQueue<Object> ioTaskQueue = new LinkedBlockingQueue<>();
    writableMemChunk.encodeWorkingAlignedTVList(ioTaskQueue, rowCount, rowCount);

    Assert.assertEquals(1, ioTaskQueue.size());
    AlignedChunkWriterImpl chunkWriter = (AlignedChunkWriterImpl) ioTaskQueue.take();
    Assert.assertEquals(
        expectedTimestampCount, chunkWriter.getTimeChunkWriter().getStatistics().getCount());
    Assert.assertEquals(
        s1Count, chunkWriter.getValueChunkWriterByIndex(0).getStatistics().getCount());
    Assert.assertEquals(
        s2Count, chunkWriter.getValueChunkWriterByIndex(1).getStatistics().getCount());
    Assert.assertEquals(0, chunkWriter.getValueChunkWriterByIndex(2).getStatistics().getCount());
  }

  @Test
  public void testLazyAlignedAllNullRowsRegionScanAndFlush() {
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s3", TSDataType.INT32));
    AlignedWritableMemChunk writableMemChunk =
        new AlignedWritableMemChunk(measurementSchemas, false);
    IoTDBDescriptor.getInstance().getConfig().setTVListSortThreshold(0);

    int rowCount = LAZY_ALIGNED_TEST_ROW_COUNT;
    for (int i = rowCount - 1; i >= 0; i--) {
      writableMemChunk.writeAlignedPoints(i, new Object[] {null, null, null}, measurementSchemas);
    }
    writableMemChunk.sortTvListForFlush();

    List<BitMap> bitMaps = new ArrayList<>();
    long[] timestamps =
        writableMemChunk.getAnySatisfiedTimestamp(
            Arrays.asList(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
            bitMaps,
            true,
            null);
    Assert.assertEquals(0, timestamps.length);
    Assert.assertTrue(bitMaps.isEmpty());

    BlockingQueue<Object> ioTaskQueue = new LinkedBlockingQueue<>();
    writableMemChunk.encodeWorkingAlignedTVList(ioTaskQueue, 37, 11);
    Assert.assertTrue(ioTaskQueue.isEmpty());
  }

  @Test
  public void testLazyAlignedAllNullInsertTabletRegionScanAndFlush() {
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s3", TSDataType.INT32));
    AlignedWritableMemChunk writableMemChunk =
        new AlignedWritableMemChunk(measurementSchemas, false);
    IoTDBDescriptor.getInstance().getConfig().setTVListSortThreshold(0);

    int rowCount = LAZY_ALIGNED_TEST_ROW_COUNT;
    long[] times = new long[rowCount];
    Object[] columns = new Object[] {new int[rowCount], new int[rowCount], new int[rowCount]};
    BitMap[] bitMaps =
        new BitMap[] {new BitMap(rowCount), new BitMap(rowCount), new BitMap(rowCount)};
    for (int i = 0; i < rowCount; i++) {
      times[i] = rowCount - i - 1;
      bitMaps[0].mark(i);
      bitMaps[1].mark(i);
      bitMaps[2].mark(i);
    }

    writableMemChunk.writeAlignedTablet(
        times, columns, bitMaps, measurementSchemas, 0, rowCount, null);
    writableMemChunk.sortTvListForFlush();

    List<BitMap> resultBitMaps = new ArrayList<>();
    long[] timestamps =
        writableMemChunk.getAnySatisfiedTimestamp(
            Arrays.asList(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
            resultBitMaps,
            true,
            null);
    Assert.assertEquals(0, timestamps.length);
    Assert.assertTrue(resultBitMaps.isEmpty());

    BlockingQueue<Object> ioTaskQueue = new LinkedBlockingQueue<>();
    writableMemChunk.encodeWorkingAlignedTVList(ioTaskQueue, 37, 11);
    Assert.assertTrue(ioTaskQueue.isEmpty());
  }

  private void assertLazyAlignedQueryResult(long[] timestamps, List<BitMap> bitMaps) {
    Assert.assertTrue(timestamps.length > 0);
    Assert.assertEquals(timestamps.length, bitMaps.size());
    long previousTimestamp = Long.MIN_VALUE;
    for (int i = 0; i < timestamps.length; i++) {
      long timestamp = timestamps[i];
      Assert.assertTrue(timestamp > previousTimestamp);
      Assert.assertTrue(isExpectedLazyAlignedTimestamp(timestamp));
      Assert.assertEquals(timestamp % LAZY_ALIGNED_S1_INTERVAL != 0, bitMaps.get(i).isMarked(0));
      Assert.assertEquals(timestamp % LAZY_ALIGNED_S2_INTERVAL != 0, bitMaps.get(i).isMarked(1));
      Assert.assertTrue(bitMaps.get(i).isMarked(2));
      previousTimestamp = timestamp;
    }
  }

  private int countExpectedLazyAlignedTimestamps(int rowCount) {
    int expectedTimestampCount = 0;
    for (int i = 0; i < rowCount; i++) {
      if (isExpectedLazyAlignedTimestamp(i)) {
        expectedTimestampCount++;
      }
    }
    return expectedTimestampCount;
  }

  private boolean isExpectedLazyAlignedTimestamp(long timestamp) {
    return timestamp % LAZY_ALIGNED_S1_INTERVAL == 0 || timestamp % LAZY_ALIGNED_S2_INTERVAL == 0;
  }

  @Test
  public void testTableWritableMemChunkRegionScan() {
    List<IMeasurementSchema> measurementSchemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s3", TSDataType.INT32));
    AlignedWritableMemChunk writableMemChunk =
        new AlignedWritableMemChunk(measurementSchemas, true);
    int size = 100000;
    for (int i = 0; i < size; i++) {
      if (i <= 10000) {
        writableMemChunk.writeAlignedPoints(i, new Object[] {1, null, 1}, measurementSchemas);
      } else if (i <= 20000) {
        writableMemChunk.writeAlignedPoints(i, new Object[] {null, null, 2}, measurementSchemas);
      } else if (i <= 30000) {
        writableMemChunk.writeAlignedPoints(i, new Object[] {3, null, null}, measurementSchemas);
      } else {
        writableMemChunk.writeAlignedPoints(i, new Object[] {4, 4, 4}, measurementSchemas);
      }
    }
    List<BitMap> bitMaps = new ArrayList<>();
    long[] timestamps =
        writableMemChunk.getAnySatisfiedTimestamp(
            Arrays.asList(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
            bitMaps,
            false,
            null);
    Assert.assertEquals(1, timestamps.length);
    Assert.assertEquals(0, timestamps[0]);
    Assert.assertFalse(bitMaps.get(0).isMarked(0));
    Assert.assertTrue(bitMaps.get(0).isMarked(1));
    Assert.assertFalse(bitMaps.get(0).isMarked(2));

    bitMaps = new ArrayList<>();
    timestamps =
        writableMemChunk.getAnySatisfiedTimestamp(
            Arrays.asList(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(new TimeRange(0, 12000))),
            bitMaps,
            false,
            new TimeFilterOperators.TimeGt(11000));

    Assert.assertEquals(1, timestamps.length);
    Assert.assertEquals(11001, timestamps[0]);
    Assert.assertTrue(bitMaps.get(0).isMarked(0));
    Assert.assertTrue(bitMaps.get(0).isMarked(1));
    Assert.assertTrue(bitMaps.get(0).isMarked(2));
  }

  @Test
  public void testNonAlignedWritableMemChunkRegionScan() {
    PrimitiveMemTable memTable = new PrimitiveMemTable("root.test", "0");
    try {
      MeasurementSchema measurementSchema = new MeasurementSchema("s1", TSDataType.INT32);
      int size = 100000;
      for (int i = 0; i < size; i++) {
        memTable.write(
            new StringArrayDeviceID("root.test.d1"),
            Collections.singletonList(measurementSchema),
            i,
            new Object[] {i});
      }
      WritableMemChunk writableMemChunk =
          (WritableMemChunk)
              memTable.getWritableMemChunk(new StringArrayDeviceID("root.test.d1"), "s1");
      Optional<Long> timestamp = writableMemChunk.getAnySatisfiedTimestamp(null, null);
      Assert.assertTrue(timestamp.isPresent());
      Assert.assertEquals(0, timestamp.get().longValue());

      timestamp =
          writableMemChunk.getAnySatisfiedTimestamp(
              null, new TimeFilterOperators.TimeBetweenAnd(1000L, 2000L));
      Assert.assertTrue(timestamp.isPresent());
      Assert.assertEquals(1000, timestamp.get().longValue());

      timestamp =
          writableMemChunk.getAnySatisfiedTimestamp(
              Collections.singletonList(new TimeRange(1, 1500)),
              new TimeFilterOperators.TimeBetweenAnd(1000L, 2000L));
      Assert.assertTrue(timestamp.isPresent());
      Assert.assertEquals(1501, timestamp.get().longValue());

      timestamp =
          writableMemChunk.getAnySatisfiedTimestamp(
              Collections.singletonList(new TimeRange(1, 1500)),
              new TimeFilterOperators.TimeBetweenAnd(100000L, 200000L));
      Assert.assertFalse(timestamp.isPresent());

      Map<String, List<IChunkHandle>> chunkHandleMap = new HashMap<>();
      memTable.queryForDeviceRegionScan(
          new StringArrayDeviceID("root.test.d1"),
          false,
          Long.MIN_VALUE,
          new HashMap<>(),
          chunkHandleMap,
          Collections.emptyList(),
          new TimeFilterOperators.TimeGt(1));
      Assert.assertEquals(1, chunkHandleMap.size());
      Assert.assertArrayEquals(
          new long[] {2, 2}, chunkHandleMap.get("s1").get(0).getPageStatisticsTime());
      memTable.queryForSeriesRegionScan(
          new NonAlignedFullPath(new StringArrayDeviceID("root.test.d1"), measurementSchema),
          Long.MIN_VALUE,
          new HashMap<>(),
          chunkHandleMap,
          Collections.emptyList(),
          new TimeFilterOperators.TimeGt(1));
      Assert.assertEquals(1, chunkHandleMap.size());
      Assert.assertArrayEquals(
          new long[] {2, 2}, chunkHandleMap.get("s1").get(0).getPageStatisticsTime());
    } finally {
      memTable.release();
    }
  }

  @Test
  public void testNonAlignedWritableMemChunkRegionScan2() throws IllegalPathException {
    PrimitiveMemTable memTable = new PrimitiveMemTable("root.test", "0");
    try {
      MeasurementSchema measurementSchema = new MeasurementSchema("s1", TSDataType.INT32);
      for (int i = 1000; i < 2000; i++) {
        memTable.write(
            new StringArrayDeviceID("root.test.d1"),
            Collections.singletonList(measurementSchema),
            i,
            new Object[] {i});
      }
      for (int i = 1; i < 100; i++) {
        memTable.write(
            new StringArrayDeviceID("root.test.d1"),
            Collections.singletonList(measurementSchema),
            i,
            new Object[] {i});
      }
      memTable.delete(
          new TreeDeletionEntry(
              new MeasurementPath(new StringArrayDeviceID("root.test.d1"), "s1"),
              new TimeRange(1, 1500)));
      WritableMemChunk writableMemChunk =
          (WritableMemChunk)
              memTable.getWritableMemChunk(new StringArrayDeviceID("root.test.d1"), "s1");
      writableMemChunk.sortTvListForFlush();
      Optional<Long> timestamp = writableMemChunk.getAnySatisfiedTimestamp(null, null);
      Assert.assertTrue(timestamp.isPresent());
      Assert.assertEquals(1501, timestamp.get().longValue());

    } finally {
      memTable.release();
    }
  }
}
