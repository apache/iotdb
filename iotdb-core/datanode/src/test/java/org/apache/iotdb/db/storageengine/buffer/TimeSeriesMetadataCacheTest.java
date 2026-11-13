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

package org.apache.iotdb.db.storageengine.buffer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.LongConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TimeSeriesMetadataCacheTest {

  private void testCachePlaceHolderInternal()
      throws IOException, WriteProcessException, ExecutionException, InterruptedException {
    File file = new File("target/test.tsfile");
    TsFileID tsFileID = new TsFileID();

    int deviceCnt = 2000;
    int seriesPerDevice = 2000;
    double nonExistSeriesRatio = 1.0;
    int concurrency = 10;
    List<IDeviceID> deviceIDList = new ArrayList<>();
    for (int i = 0; i < deviceCnt; i++) {
      deviceIDList.add(Factory.DEFAULT_FACTORY.create("root.d" + i));
    }
    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    List<Future<Void>> futures = new ArrayList<>();

    try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      // 100*100 series in the file

      for (int i = 0; i < deviceCnt; i++) {
        for (int j = 0; j < seriesPerDevice; j++) {
          tsFileWriter.registerTimeseries(
              deviceIDList.get(i), new MeasurementSchema("s" + j, TSDataType.INT32));
        }
      }
      for (int i = 0; i < deviceCnt; i++) {
        TSRecord rec = new TSRecord(deviceIDList.get(i), 0);
        for (int j = 0; j < seriesPerDevice; j++) {
          rec.addPoint("s" + j, 0);
        }
        tsFileWriter.writeRecord(rec);
      }
      tsFileWriter.close();

      // read 100*200 series each 10 times in the file
      long start = System.currentTimeMillis();
      QueryContext queryContext = new QueryContext(false, false);
      // put k in outer loop
      int devicePerThread = deviceCnt / concurrency;
      for (int c = 0; c < concurrency; c++) {
        final int finalC = c;
        QueryContext finalQueryContext = queryContext;
        futures.add(
            executor.submit(
                () -> {
                  for (int k = 0; k < 10; k++) {
                    for (int i = devicePerThread * finalC;
                        i < devicePerThread * (finalC + 1);
                        i++) {
                      for (int j = 0;
                          j < seriesPerDevice + (int) (seriesPerDevice * nonExistSeriesRatio);
                          j++) {
                        TimeSeriesMetadataCacheKey key =
                            new TimeSeriesMetadataCacheKey(tsFileID, deviceIDList.get(i), "s" + j);
                        TimeseriesMetadata timeseriesMetadata =
                            TimeSeriesMetadataCache.getInstance()
                                .get(
                                    file.getPath(),
                                    key,
                                    Collections.singleton("s" + j),
                                    true,
                                    false,
                                    finalQueryContext);
                        if (j < seriesPerDevice) {
                          assertNotNull(timeseriesMetadata);
                          assertEquals("s" + j, timeseriesMetadata.getMeasurementId());
                        } else {
                          assertNull(timeseriesMetadata);
                        }
                      }
                    }
                  }
                  return null;
                }));
      }
      for (Future<Void> future : futures) {
        future.get();
      }
      futures.clear();
      System.out.println("time cost with outer k: " + (System.currentTimeMillis() - start) + "ms");
      TimeSeriesMetadataCache.getInstance().clear();

      start = System.currentTimeMillis();
      queryContext = new QueryContext(false, false);
      // put k in inner loop
      for (int c = 0; c < concurrency; c++) {
        final int finalC = c;
        QueryContext finalQueryContext = queryContext;
        futures.add(
            executor.submit(
                () -> {
                  for (int i = devicePerThread * finalC; i < devicePerThread * (finalC + 1); i++) {
                    for (int j = 0;
                        j < seriesPerDevice + (int) (seriesPerDevice * nonExistSeriesRatio);
                        j++) {
                      TimeSeriesMetadataCacheKey key =
                          new TimeSeriesMetadataCacheKey(tsFileID, deviceIDList.get(i), "s" + j);
                      for (int k = 0; k < 10; k++) {
                        TimeseriesMetadata timeseriesMetadata =
                            TimeSeriesMetadataCache.getInstance()
                                .get(
                                    file.getPath(),
                                    key,
                                    Collections.singleton("s" + j),
                                    true,
                                    false,
                                    finalQueryContext);
                        if (j < seriesPerDevice) {
                          assertNotNull(timeseriesMetadata);
                          assertEquals("s" + j, timeseriesMetadata.getMeasurementId());
                        } else {
                          assertNull(timeseriesMetadata);
                        }
                      }
                    }
                  }
                  return null;
                }));
      }
      for (Future<Void> future : futures) {
        future.get();
      }
      futures.clear();

      System.out.println("time cost with inner k: " + (System.currentTimeMillis() - start) + "ms");
      TimeSeriesMetadataCache.getInstance().clear();
    } finally {
      file.delete();
    }
  }

  @Ignore("Performance")
  @Test
  public void testCachePlaceHolder()
      throws IOException, WriteProcessException, ExecutionException, InterruptedException {
    boolean mayCacheNonExistSeries =
        IoTDBDescriptor.getInstance().getMemoryConfig().isMayCacheNonExistSeries();
    try {
      System.out.println("warming up");
      System.out.println("Do not cache non-exist series");
      IoTDBDescriptor.getInstance().getMemoryConfig().setMayCacheNonExistSeries(false);
      testCachePlaceHolderInternal();
      System.out.println("Cache non-exist series");
      IoTDBDescriptor.getInstance().getMemoryConfig().setMayCacheNonExistSeries(true);
      testCachePlaceHolderInternal();

      System.out.println("actual test");
      System.out.println("Do not cache non-exist series");
      IoTDBDescriptor.getInstance().getMemoryConfig().setMayCacheNonExistSeries(false);
      testCachePlaceHolderInternal();
      System.out.println("Cache non-exist series");
      IoTDBDescriptor.getInstance().getMemoryConfig().setMayCacheNonExistSeries(true);
      testCachePlaceHolderInternal();
    } finally {
      IoTDBDescriptor.getInstance()
          .getMemoryConfig()
          .setMayCacheNonExistSeries(mayCacheNonExistSeries);
    }
  }

  @Test
  public void testIgnoreNotExistsResultIsNotCached() throws Exception {
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    FileReaderManager fileReaderManager = FileReaderManager.getInstance();
    boolean mayCacheNonExistSeries =
        IoTDBDescriptor.getInstance().getMemoryConfig().isMayCacheNonExistSeries();
    TsFileID tsFileID = new TsFileID();
    cache.clear();
    bloomFilterCache.clear();
    try {
      IoTDBDescriptor.getInstance().getMemoryConfig().setMayCacheNonExistSeries(true);

      String filePath = "target/missing-device.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.missingDevice");
      String measurement = "s1";
      Set<String> allSensors = Collections.singleton(measurement);
      TimeSeriesMetadataCacheKey key =
          new TimeSeriesMetadataCacheKey(tsFileID, deviceID, measurement);

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      Mockito.when(reader.readBloomFilter(Mockito.any(LongConsumer.class))).thenReturn(null);
      Mockito.when(
              reader.readTimeseriesMetadata(
                  Mockito.eq(deviceID),
                  Mockito.<long[]>isNull(),
                  Mockito.eq(measurement),
                  Mockito.eq(allSensors),
                  Mockito.eq(true),
                  Mockito.any(LongConsumer.class)))
          .thenReturn(Collections.emptyList());
      Mockito.when(
              reader.readTimeseriesMetadata(
                  Mockito.eq(deviceID),
                  Mockito.<long[]>isNull(),
                  Mockito.eq(measurement),
                  Mockito.eq(allSensors),
                  Mockito.eq(false),
                  Mockito.any(LongConsumer.class)))
          .thenThrow(new IOException("missing device"));

      assertNull(cache.get(filePath, key, allSensors, true, false, new QueryContext(false, false)));
      assertThrows(
          IOException.class,
          () -> cache.get(filePath, key, allSensors, false, false, new QueryContext(false, false)));
      Mockito.verify(reader)
          .readTimeseriesMetadata(
              Mockito.eq(deviceID),
              Mockito.<long[]>isNull(),
              Mockito.eq(measurement),
              Mockito.eq(allSensors),
              Mockito.eq(false),
              Mockito.any(LongConsumer.class));
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
      IoTDBDescriptor.getInstance()
          .getMemoryConfig()
          .setMayCacheNonExistSeries(mayCacheNonExistSeries);
    }
  }

  @Test
  public void testGetDeviceTimeSeriesMetadata() throws Exception {
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    FileReaderManager fileReaderManager = FileReaderManager.getInstance();
    TsFileID tsFileID = new TsFileID();
    cache.clear();
    bloomFilterCache.clear();
    try {
      String filePath = "target/batch-device.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.device");
      Set<String> allSensors = new HashSet<>(Arrays.asList("", "s1", "s2"));

      Statistics<?> timeStatistics = Statistics.getStatsByType(TSDataType.INT64);
      timeStatistics.setCount(1);
      timeStatistics.setStartTime(0);
      timeStatistics.setEndTime(10);
      TimeseriesMetadata timeColumn =
          new TimeseriesMetadata(
              (byte) 0, 0, "", TSDataType.INT64, timeStatistics, new PublicBAOS());

      Statistics<?> s1Statistics = Statistics.getStatsByType(TSDataType.INT32);
      s1Statistics.setCount(1);
      s1Statistics.setStartTime(0);
      s1Statistics.setEndTime(10);
      TimeseriesMetadata s1 =
          new TimeseriesMetadata(
              (byte) 0, 0, "s1", TSDataType.INT32, s1Statistics, new PublicBAOS());

      // an empty series (count == 0) is mapped to null and cached as the null placeholder
      TimeseriesMetadata emptyS2 =
          new TimeseriesMetadata(
              (byte) 0,
              0,
              "s2",
              TSDataType.INT32,
              Statistics.getStatsByType(TSDataType.INT32),
              new PublicBAOS());

      List<TimeseriesMetadata> deviceMetadata = new ArrayList<>();
      deviceMetadata.add(timeColumn);
      deviceMetadata.add(s1);
      deviceMetadata.add(emptyS2);
      // the batch read must carry chunk metadata, otherwise the downstream chunk loader NPEs
      s1.setChunkMetadataList(Collections.singletonList(new ChunkMetadata()));

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      Mockito.when(reader.getDeviceTimeseriesMetadata(deviceID)).thenReturn(deviceMetadata);

      Map<String, TimeseriesMetadata> result =
          cache.getDeviceTimeSeriesMetadata(
              filePath, tsFileID, deviceID, allSensors, new QueryContext(false, false), false);

      assertNotNull(result.get(""));
      assertEquals("", result.get("").getMeasurementId());
      assertNotNull(result.get("s1"));
      assertEquals("s1", result.get("s1").getMeasurementId());
      // chunk metadata must flow through to the result (and the cached copy) unchanged
      assertNotNull(result.get("s1").getChunkMetadataList());
      assertNull(result.get("s2"));

      // a second lookup of cached measurements must be served without touching the reader again
      Set<String> cachedSensors = new HashSet<>(Arrays.asList("", "s1"));
      Map<String, TimeseriesMetadata> cachedResult =
          cache.getDeviceTimeSeriesMetadata(
              filePath, tsFileID, deviceID, cachedSensors, new QueryContext(false, false), false);
      assertNotNull(cachedResult.get("s1"));
      Mockito.verify(reader, Mockito.times(1)).getDeviceTimeseriesMetadata(deviceID);

      // the batch read also populated the per-measurement cache
      TimeseriesMetadata cached =
          cache.get(
              filePath,
              new TimeSeriesMetadataCacheKey(tsFileID, deviceID, "s1"),
              Collections.singleton("s1"),
              true,
              false,
              new QueryContext(false, false));
      assertNotNull(cached);
      assertEquals("s1", cached.getMeasurementId());
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }

  @Test
  public void testAbsentDeviceSkippedWhenBloomFilterExcludesIt() throws Exception {
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    FileReaderManager fileReaderManager = FileReaderManager.getInstance();
    TsFileID tsFileID = new TsFileID();
    cache.clear();
    bloomFilterCache.clear();
    try {
      String filePath = "target/batch-absent-device-bloom.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.absent");
      Set<String> allSensors = Collections.singleton("");

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      // the bloom filter proves the device is missing, so the device must not be read at all
      Mockito.when(reader.readBloomFilter(Mockito.any(LongConsumer.class)))
          .thenReturn(BloomFilter.getEmptyBloomFilter(0.05, 1));

      // an absent device must not be read: the filtered-out measurements are returned as null,
      // which the caller distinguishes from "present but empty time column"
      Map<String, TimeseriesMetadata> result =
          cache.getDeviceTimeSeriesMetadata(
              filePath, tsFileID, deviceID, allSensors, new QueryContext(false, false), false);
      assertNull(result.get(""));
      Mockito.verify(reader, Mockito.never()).getDeviceTimeseriesMetadata(deviceID);
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }

  @Test
  public void testAbsentDeviceThrowsWhenBloomFilterCannotExcludeIt() throws Exception {
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    FileReaderManager fileReaderManager = FileReaderManager.getInstance();
    TsFileID tsFileID = new TsFileID();
    cache.clear();
    bloomFilterCache.clear();
    try {
      String filePath = "target/batch-absent-device-throw.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.absent");
      // more than MAX_PER_COLUMN_FALLBACK entries so the batch read (not the per-column fallback)
      // is used
      Set<String> allSensors = new HashSet<>(Arrays.asList("", "s1", "s2"));

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      Mockito.when(reader.getDeviceTimeseriesMetadata(deviceID))
          .thenReturn(Collections.emptyList());
      BloomFilter bloomFilter = BloomFilter.getEmptyBloomFilter(0.05, 1);
      bloomFilter.add(deviceID.toString() + ".");
      bloomFilter.add(deviceID.toString() + ".s1");
      bloomFilter.add(deviceID.toString() + ".s2");
      Mockito.when(reader.readBloomFilter(Mockito.any(LongConsumer.class))).thenReturn(bloomFilter);

      assertThrows(
          IOException.class,
          () ->
              cache.getDeviceTimeSeriesMetadata(
                  filePath, tsFileID, deviceID, allSensors, new QueryContext(false, false), false));

      assertTrue(
          cache
              .getDeviceTimeSeriesMetadata(
                  filePath, tsFileID, deviceID, allSensors, new QueryContext(false, false), true)
              .isEmpty());
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }

  @Test
  public void testBatchMetadataMatchesPerColumn() throws Exception {
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    FileReaderManager fileReaderManager = FileReaderManager.getInstance();
    TsFileID tsFileID = new TsFileID();
    int columnCount = 700;
    File file = new File("target/batch-compare.tsfile");
    file.delete();
    IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.compare.d1");
    List<IMeasurementSchema> schemas = new ArrayList<>(columnCount);
    for (int i = 0; i < columnCount; i++) {
      schemas.add(new MeasurementSchema("s" + i, TSDataType.INT32));
    }
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerAlignedTimeseries(deviceID, schemas);
      for (long time = 0; time < 3; time++) {
        TSRecord record = new TSRecord(deviceID, time);
        for (int i = 0; i < columnCount; i++) {
          record.addPoint("s" + i, (int) time);
        }
        writer.writeRecord(record);
      }
    }
    cache.clear();
    bloomFilterCache.clear();
    try {
      String filePath = file.getAbsolutePath();
      Set<String> allSensors = new HashSet<>();
      for (int i = 0; i < columnCount; i++) {
        allSensors.add("s" + i);
      }
      allSensors.add("");
      allSensors.add("absent");

      Map<String, TimeseriesMetadata> perColumn = new HashMap<>();
      for (String measurement : allSensors) {
        perColumn.put(
            measurement,
            cache.get(
                filePath,
                new TimeSeriesMetadataCacheKey(tsFileID, deviceID, measurement),
                allSensors,
                false,
                false,
                new QueryContext(false, false)));
      }
      cache.clear();

      Map<String, TimeseriesMetadata> batch =
          cache.getDeviceTimeSeriesMetadata(
              filePath, tsFileID, deviceID, allSensors, new QueryContext(false, false), false);

      assertEquals(allSensors.size(), batch.size());
      for (String measurement : allSensors) {
        TimeseriesMetadata expected = perColumn.get(measurement);
        TimeseriesMetadata actual = batch.get(measurement);
        if (expected == null) {
          assertNull(measurement, actual);
          continue;
        }
        assertNotNull(measurement, actual);
        assertEquals(measurement, actual.getMeasurementId());
        assertEquals(
            measurement, expected.getStatistics().getCount(), actual.getStatistics().getCount());
        assertEquals(
            measurement,
            expected.getStatistics().getStartTime(),
            actual.getStatistics().getStartTime());
        assertEquals(
            measurement,
            expected.getStatistics().getEndTime(),
            actual.getStatistics().getEndTime());
        assertEquals(
            measurement,
            expected.getChunkMetadataList().size(),
            actual.getChunkMetadataList().size());
      }
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
      assertTrue(file.delete());
    }
  }

  @Test
  public void testAbsentTimeColumnSkipsDeviceRead() throws Exception {
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    FileReaderManager fileReaderManager = FileReaderManager.getInstance();
    TsFileID tsFileID = new TsFileID();
    cache.clear();
    bloomFilterCache.clear();
    try {
      String filePath = "target/batch-time-column-filtered.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.absent");
      Set<String> allSensors = new HashSet<>(Arrays.asList("", "s1"));

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      // the time column is filtered out, while s1 is a bloom filter false positive
      BloomFilter bloomFilter = BloomFilter.getEmptyBloomFilter(0.05, 1);
      bloomFilter.add(deviceID.toString() + ".s1");
      Mockito.when(reader.readBloomFilter(Mockito.any(LongConsumer.class))).thenReturn(bloomFilter);

      Map<String, TimeseriesMetadata> result =
          cache.getDeviceTimeSeriesMetadata(
              filePath, tsFileID, deviceID, allSensors, new QueryContext(false, false), false);

      assertNull(result.get(""));
      assertNull(result.get("s1"));
      Mockito.verify(reader, Mockito.never()).getDeviceTimeseriesMetadata(deviceID);
      Mockito.verify(reader, Mockito.never())
          .readTimeseriesMetadata(
              Mockito.any(),
              Mockito.<long[]>any(),
              Mockito.any(),
              Mockito.any(),
              Mockito.anyBoolean(),
              Mockito.any());
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }

  @Test
  public void testSingleMissingMeasurementUsesPerColumnFallback() throws Exception {
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    FileReaderManager fileReaderManager = FileReaderManager.getInstance();
    TsFileID tsFileID = new TsFileID();
    cache.clear();
    bloomFilterCache.clear();
    try {
      String filePath = "target/batch-single-miss.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.device");
      Set<String> allSensors = new HashSet<>(Arrays.asList("", "s1", "s2"));

      Statistics<?> statistics = Statistics.getStatsByType(TSDataType.INT32);
      statistics.setCount(1);
      statistics.setStartTime(0);
      statistics.setEndTime(10);
      TimeseriesMetadata s2 =
          new TimeseriesMetadata((byte) 0, 0, "s2", TSDataType.INT32, statistics, new PublicBAOS());
      Statistics<?> timeStatistics = Statistics.getStatsByType(TSDataType.INT64);
      timeStatistics.setCount(1);
      timeStatistics.setStartTime(0);
      timeStatistics.setEndTime(10);
      Statistics<?> s1Statistics = Statistics.getStatsByType(TSDataType.INT32);
      s1Statistics.setCount(1);
      s1Statistics.setStartTime(0);
      s1Statistics.setEndTime(10);

      List<TimeseriesMetadata> deviceMetadata = new ArrayList<>();
      deviceMetadata.add(
          new TimeseriesMetadata(
              (byte) 0, 0, "", TSDataType.INT64, timeStatistics, new PublicBAOS()));
      deviceMetadata.add(
          new TimeseriesMetadata(
              (byte) 0, 0, "s1", TSDataType.INT32, s1Statistics, new PublicBAOS()));
      deviceMetadata.add(s2);

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      Mockito.when(reader.getDeviceTimeseriesMetadata(deviceID)).thenReturn(deviceMetadata);

      Map<String, TimeseriesMetadata> first =
          cache.getDeviceTimeSeriesMetadata(
              filePath, tsFileID, deviceID, allSensors, new QueryContext(false, false), false);
      assertNotNull(first.get("s2"));

      // evict one measurement: the next call must load only that one, not the whole device
      cache.remove(new TimeSeriesMetadataCacheKey(tsFileID, deviceID, "s2"));
      Mockito.when(
              reader.readTimeseriesMetadata(
                  Mockito.eq(deviceID),
                  Mockito.<long[]>isNull(),
                  Mockito.eq("s2"),
                  Mockito.eq(allSensors),
                  Mockito.eq(false),
                  Mockito.any(LongConsumer.class)))
          .thenReturn(Collections.singletonList(s2));

      Map<String, TimeseriesMetadata> second =
          cache.getDeviceTimeSeriesMetadata(
              filePath, tsFileID, deviceID, allSensors, new QueryContext(false, false), false);

      assertNotNull(second.get("s2"));
      Mockito.verify(reader, Mockito.times(1)).getDeviceTimeseriesMetadata(deviceID);
      Mockito.verify(reader, Mockito.times(1))
          .readTimeseriesMetadata(
              Mockito.eq(deviceID),
              Mockito.<long[]>isNull(),
              Mockito.eq("s2"),
              Mockito.eq(allSensors),
              Mockito.eq(false),
              Mockito.any(LongConsumer.class));
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }
}
