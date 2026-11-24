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
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceType;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.LazyTsFileDeviceIterator;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
  public void testSingleCachesFileIndexMissesAfterRecovery() throws Exception {
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
              reader.readTimeseriesMetadataForMeasurements(
                  Mockito.eq(deviceID),
                  Mockito.<long[]>isNull(),
                  Mockito.anyCollection(),
                  Mockito.anySet(),
                  Mockito.eq(true),
                  Mockito.any(LongConsumer.class)))
          .thenReturn(Collections.emptyMap());
      Mockito.when(
              reader.readTimeseriesMetadataForMeasurements(
                  Mockito.eq(deviceID),
                  Mockito.<long[]>isNull(),
                  Mockito.anyCollection(),
                  Mockito.anySet(),
                  Mockito.eq(false),
                  Mockito.any(LongConsumer.class)))
          .thenThrow(new IOException("missing device"));

      assertNull(cache.get(filePath, key, allSensors, true, false, recoveringQueryContext()));
      assertThrows(
          IOException.class,
          () -> cache.get(filePath, key, allSensors, false, false, new QueryContext(false, false)));

      QueryContext fileIndexQuery = new QueryContext(false, false);
      assertNull(cache.get(filePath, key, allSensors, true, false, fileIndexQuery));
      assertNull(cache.get(filePath, key, allSensors, true, false, fileIndexQuery));
      assertEquals(
          1, fileIndexQuery.getQueryStatistics().getLoadTimeSeriesMetadataFromCacheCount().get());
      Mockito.verify(reader, Mockito.times(2))
          .readTimeseriesMetadataForMeasurements(
              Mockito.eq(deviceID),
              Mockito.<long[]>isNull(),
              Mockito.anyCollection(),
              Mockito.anySet(),
              Mockito.eq(true),
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

  private static QueryContext recoveringQueryContext() {
    QueryContext context = Mockito.spy(new QueryContext(false, false));
    Mockito.when(context.isStorageEngineRecovering()).thenReturn(true);
    return context;
  }

  private static Map<String, TimeseriesMetadata> getTimeseriesMetadata(
      TimeSeriesMetadataCache cache,
      String filePath,
      TsFileID tsFileID,
      IDeviceID deviceID,
      Set<String> allSensors,
      boolean ignoreNotExists)
      throws IOException {
    return cache.getTimeseriesMetadata(
        filePath,
        tsFileID,
        deviceID,
        allSensors,
        ignoreNotExists,
        false,
        new QueryContext(false, false),
        null);
  }

  private static void stubGroupRead(
      TsFileSequenceReader reader,
      IDeviceID deviceID,
      boolean ignoreNotExists,
      List<TimeseriesMetadata> group)
      throws IOException {
    Mockito.when(
            reader.readTimeseriesMetadataForMeasurements(
                Mockito.eq(deviceID),
                Mockito.<long[]>isNull(),
                Mockito.anyCollection(),
                Mockito.anySet(),
                Mockito.eq(ignoreNotExists),
                Mockito.any(LongConsumer.class)))
        .thenAnswer(
            invocation -> {
              Set<String> returnedMeasurements = invocation.getArgument(3);
              Map<String, TimeseriesMetadata> loaded = new HashMap<>();
              for (TimeseriesMetadata metadata : group) {
                if (returnedMeasurements.contains(metadata.getMeasurementId())) {
                  loaded.put(metadata.getMeasurementId(), metadata);
                }
              }
              return loaded;
            });
  }

  @Test
  public void testGetTimeseriesMetadata() throws Exception {
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

      List<TimeseriesMetadata> group = new ArrayList<>();
      group.add(timeColumn);
      group.add(s1);
      group.add(emptyS2);
      // the group read must carry chunk metadata, otherwise the downstream chunk loader NPEs
      s1.setChunkMetadataList(Collections.singletonList(new ChunkMetadata()));

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      stubGroupRead(reader, deviceID, false, group);

      Map<String, TimeseriesMetadata> result =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);
      // This file has no BloomFilter. The device and value checks share that result.
      Mockito.verify(reader).readBloomFilter(Mockito.any(LongConsumer.class));
      assertNotNull(result.get(""));
      assertEquals("", result.get("").getMeasurementId());
      assertNotNull(result.get("s1"));
      assertEquals("s1", result.get("s1").getMeasurementId());
      // chunk metadata must flow through to the result (and the cached copy) unchanged
      assertNotNull(result.get("s1").getChunkMetadataList());
      assertNull(result.get("s2"));

      // a second lookup of cached measurements must be served without touching the reader again
      Set<String> cachedSensors = new HashSet<>(Arrays.asList("", "s1"));
      long requestsBefore = cache.getCacheRequestCount();
      Map<String, TimeseriesMetadata> cachedResult =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, cachedSensors, false);
      assertEquals(cachedSensors.size(), cache.getCacheRequestCount() - requestsBefore);
      assertNotNull(cachedResult.get("s1"));
      Mockito.verify(reader, Mockito.times(1))
          .readTimeseriesMetadataForMeasurements(
              Mockito.any(),
              Mockito.<long[]>any(),
              Mockito.anyCollection(),
              Mockito.anySet(),
              Mockito.anyBoolean(),
              Mockito.any(LongConsumer.class));

      // the group read also populated the per-measurement cache
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

      // the cache hands out copies: mutating a returned entry must not poison later reads
      result.get("s1").setModified(true);
      TimeseriesMetadata cachedAgain =
          cache.get(
              filePath,
              new TimeSeriesMetadataCacheKey(tsFileID, deviceID, "s1"),
              Collections.singleton("s1"),
              true,
              false,
              new QueryContext(false, false));
      assertNotNull(cachedAgain);
      assertFalse(cachedAgain.isModified());
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

      Map<String, TimeseriesMetadata> result =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);
      assertNull(result);
      Mockito.verify(reader, Mockito.never())
          .readTimeseriesMetadataForMeasurements(
              Mockito.any(),
              Mockito.<long[]>any(),
              Mockito.anyCollection(),
              Mockito.anySet(),
              Mockito.anyBoolean(),
              Mockito.any(LongConsumer.class));
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
    boolean mayCacheNonExistSeries =
        IoTDBDescriptor.getInstance().getMemoryConfig().isMayCacheNonExistSeries();
    TsFileID tsFileID = new TsFileID();
    cache.clear();
    bloomFilterCache.clear();
    try {
      IoTDBDescriptor.getInstance().getMemoryConfig().setMayCacheNonExistSeries(false);
      String filePath = "target/batch-absent-device-throw.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.absent");
      Set<String> allSensors = new HashSet<>(Arrays.asList("", "s1", "s2"));

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      stubGroupRead(reader, deviceID, true, Collections.emptyList());
      Mockito.when(
              reader.readTimeseriesMetadataForMeasurements(
                  Mockito.eq(deviceID),
                  Mockito.<long[]>isNull(),
                  Mockito.anyCollection(),
                  Mockito.anySet(),
                  Mockito.eq(false),
                  Mockito.any(LongConsumer.class)))
          .thenThrow(new IOException("device absent"));
      BloomFilter bloomFilter = BloomFilter.getEmptyBloomFilter(0.05, 1);
      bloomFilter.add(deviceID.toString() + ".");
      bloomFilter.add(deviceID.toString() + ".s1");
      bloomFilter.add(deviceID.toString() + ".s2");
      Mockito.when(reader.readBloomFilter(Mockito.any(LongConsumer.class))).thenReturn(bloomFilter);

      // With negative caching disabled, a file-index lookup must leave the next read uncached.
      Map<String, TimeseriesMetadata> ignored =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, true);
      assertNull(ignored);

      // a normal query must surface the reader's failure instead of a silently empty device
      assertThrows(
          IOException.class,
          () -> getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false));
    } finally {
      IoTDBDescriptor.getInstance()
          .getMemoryConfig()
          .setMayCacheNonExistSeries(mayCacheNonExistSeries);
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

      // Reading s0 also caches s1 from the same metadata group.
      QueryContext queryContext = new QueryContext(false, false);
      List<String> measurements = Arrays.asList("s0", "s1");
      Set<String> selectedMeasurements = new HashSet<>(measurements);
      for (String measurement : measurements) {
        assertNotNull(
            cache.get(
                filePath,
                new TimeSeriesMetadataCacheKey(tsFileID, deviceID, measurement),
                selectedMeasurements,
                false,
                false,
                queryContext));
      }
      assertEquals(
          1, queryContext.getQueryStatistics().getLoadTimeSeriesMetadataFromDiskCount().get());
      assertEquals(
          1, queryContext.getQueryStatistics().getLoadTimeSeriesMetadataFromCacheCount().get());
      cache.clear();

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
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);

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
        for (int i = 0; i < expected.getChunkMetadataList().size(); i++) {
          ChunkMetadata expectedChunk = (ChunkMetadata) expected.getChunkMetadataList().get(i);
          ChunkMetadata actualChunk = (ChunkMetadata) actual.getChunkMetadataList().get(i);
          assertEquals(
              measurement,
              expectedChunk.getOffsetOfChunkHeader(),
              actualChunk.getOffsetOfChunkHeader());
          assertEquals(
              measurement,
              expectedChunk.getStatistics().getStartTime(),
              actualChunk.getStatistics().getStartTime());
          assertEquals(
              measurement,
              expectedChunk.getStatistics().getEndTime(),
              actualChunk.getStatistics().getEndTime());
        }
      }
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
      assertTrue(file.delete());
    }
  }

  @Test
  public void testBloomFilterExcludedMeasurementsSkipRead() throws Exception {
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
      // both the time column and s1 are excluded, so no leaf group needs to be read
      BloomFilter bloomFilter = BloomFilter.getEmptyBloomFilter(0.05, 1);
      Mockito.when(reader.readBloomFilter(Mockito.any(LongConsumer.class))).thenReturn(bloomFilter);

      Map<String, TimeseriesMetadata> result =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);

      assertNull(result);
      Mockito.verify(reader, Mockito.never())
          .readTimeseriesMetadataForMeasurements(
              Mockito.any(),
              Mockito.<long[]>any(),
              Mockito.anyCollection(),
              Mockito.anySet(),
              Mockito.anyBoolean(),
              Mockito.any(LongConsumer.class));
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }

  @Test
  public void testEvictedMeasurementReloadsOnlyItsGroup() throws Exception {
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

      List<TimeseriesMetadata> group = new ArrayList<>();
      group.add(
          new TimeseriesMetadata(
              (byte) 0, 0, "", TSDataType.INT64, timeStatistics, new PublicBAOS()));
      group.add(
          new TimeseriesMetadata(
              (byte) 0, 0, "s1", TSDataType.INT32, s1Statistics, new PublicBAOS()));
      group.add(s2);

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      stubGroupRead(reader, deviceID, false, group);

      Map<String, TimeseriesMetadata> first =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);
      assertNotNull(first.get("s2"));

      // evict one measurement: the next call reads only the group that holds it, and that one
      // read also resolves the other members of the group
      cache.remove(new TimeSeriesMetadataCacheKey(tsFileID, deviceID, "s2"));
      Map<String, TimeseriesMetadata> second =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);

      assertNotNull(second.get("s2"));
      Mockito.verify(reader, Mockito.times(2))
          .readTimeseriesMetadataForMeasurements(
              Mockito.eq(deviceID),
              Mockito.<long[]>isNull(),
              Mockito.anyCollection(),
              Mockito.anySet(),
              Mockito.eq(false),
              Mockito.any(LongConsumer.class));
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }

  @Test
  public void testBatchCachesFileIndexMissesAfterRecovery() throws Exception {
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
      String filePath = "target/batch-ignore-not-exists.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.absent");
      Set<String> allSensors = new HashSet<>(Arrays.asList("", "s1", "s2"));

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      stubGroupRead(reader, deviceID, true, Collections.emptyList());
      Mockito.when(
              reader.readTimeseriesMetadataForMeasurements(
                  Mockito.eq(deviceID),
                  Mockito.<long[]>isNull(),
                  Mockito.anyCollection(),
                  Mockito.anySet(),
                  Mockito.eq(false),
                  Mockito.any(LongConsumer.class)))
          .thenThrow(new IOException("device absent"));

      QueryContext recovering = recoveringQueryContext();
      Map<String, TimeseriesMetadata> ignored =
          cache.getTimeseriesMetadata(
              filePath, tsFileID, deviceID, allSensors, true, false, recovering, null);
      assertNull(ignored);

      // A strict query after recovery must still reach the reader.
      assertThrows(
          IOException.class,
          () -> getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false));

      assertNull(getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, true));
      QueryContext fileIndexQuery = new QueryContext(false, false);
      Map<String, TimeseriesMetadata> cached =
          cache.getTimeseriesMetadata(
              filePath, tsFileID, deviceID, allSensors, true, false, fileIndexQuery, null);
      assertNull(cached);
      assertEquals(
          1, fileIndexQuery.getQueryStatistics().getLoadTimeSeriesMetadataFromCacheCount().get());
      assertEquals(
          0, fileIndexQuery.getQueryStatistics().getLoadTimeSeriesMetadataFromDiskCount().get());
      assertEquals(
          0, fileIndexQuery.getQueryStatistics().getLoadTimeSeriesMetadataFromDiskTime().get());
      Mockito.verify(reader, Mockito.times(3))
          .readTimeseriesMetadataForMeasurements(
              Mockito.eq(deviceID),
              Mockito.<long[]>isNull(),
              Mockito.anyCollection(),
              Mockito.anySet(),
              Mockito.anyBoolean(),
              Mockito.any(LongConsumer.class));
    } finally {
      IoTDBDescriptor.getInstance()
          .getMemoryConfig()
          .setMayCacheNonExistSeries(mayCacheNonExistSeries);
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }

  @Test
  public void testEvictedMeasurementsShareOneGroupRead() throws Exception {
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    FileReaderManager fileReaderManager = FileReaderManager.getInstance();
    TsFileID tsFileID = new TsFileID();
    cache.clear();
    bloomFilterCache.clear();
    try {
      String filePath = "target/batch-few-missing.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.device");
      Set<String> allSensors = new HashSet<>(Arrays.asList("", "s1", "s2", "s3", "s4", "s5"));

      List<TimeseriesMetadata> group = new ArrayList<>();
      for (String measurement : allSensors) {
        TSDataType dataType = measurement.isEmpty() ? TSDataType.INT64 : TSDataType.INT32;
        Statistics<?> statistics = Statistics.getStatsByType(dataType);
        statistics.setCount(1);
        statistics.setStartTime(0);
        statistics.setEndTime(10);
        group.add(
            new TimeseriesMetadata(
                (byte) 0, 0, measurement, dataType, statistics, new PublicBAOS()));
      }

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      stubGroupRead(reader, deviceID, false, group);

      Map<String, TimeseriesMetadata> first =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);
      assertNotNull(first.get("s5"));

      // evict three measurements: they share one leaf group, so a single read must resolve them
      for (String measurement : Arrays.asList("s2", "s3", "s4")) {
        cache.remove(new TimeSeriesMetadataCacheKey(tsFileID, deviceID, measurement));
      }

      Map<String, TimeseriesMetadata> second =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);
      assertNotNull(second.get("s2"));
      assertNotNull(second.get("s3"));
      assertNotNull(second.get("s4"));
      Mockito.verify(reader, Mockito.times(2))
          .readTimeseriesMetadataForMeasurements(
              Mockito.eq(deviceID),
              Mockito.<long[]>isNull(),
              Mockito.anyCollection(),
              Mockito.anySet(),
              Mockito.eq(false),
              Mockito.any(LongConsumer.class));
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }

  @Test
  public void testReaderConfirmedAbsenceIsRemembered() throws Exception {
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    FileReaderManager fileReaderManager = FileReaderManager.getInstance();
    TsFileID tsFileID = new TsFileID();
    cache.clear();
    bloomFilterCache.clear();
    try {
      String filePath = "target/batch-confirmed-absent.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.device");
      Set<String> allSensors = new HashSet<>(Arrays.asList("", "s1", "s2"));

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      BloomFilter bloomFilter = BloomFilter.getEmptyBloomFilter(0.05, 3);
      bloomFilter.add(deviceID.toString() + ".");
      bloomFilter.add(deviceID.toString() + ".s1");
      bloomFilter.add(deviceID.toString() + ".s2");
      Mockito.when(reader.readBloomFilter(Mockito.any(LongConsumer.class))).thenReturn(bloomFilter);

      Statistics<?> timeStatistics = Statistics.getStatsByType(TSDataType.INT64);
      timeStatistics.setCount(1);
      timeStatistics.setStartTime(0);
      timeStatistics.setEndTime(10);
      Statistics<?> s1Statistics = Statistics.getStatsByType(TSDataType.INT32);
      s1Statistics.setCount(1);
      s1Statistics.setStartTime(0);
      s1Statistics.setEndTime(10);
      List<TimeseriesMetadata> group = new ArrayList<>();
      group.add(
          new TimeseriesMetadata(
              (byte) 0, 0, "", TSDataType.INT64, timeStatistics, new PublicBAOS()));
      group.add(
          new TimeseriesMetadata(
              (byte) 0, 0, "s1", TSDataType.INT32, s1Statistics, new PublicBAOS()));
      stubGroupRead(reader, deviceID, false, group);

      Map<String, TimeseriesMetadata> first =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);
      assertNotNull(first.get(""));
      assertNotNull(first.get("s1"));
      assertTrue(first.containsKey("s2"));
      assertNull(first.get("s2"));

      // the reader confirmed s2 absent, so the second query is answered from the cache alone
      Map<String, TimeseriesMetadata> second =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);
      assertNull(second.get("s2"));
      Mockito.verify(reader, Mockito.times(1))
          .readTimeseriesMetadataForMeasurements(
              Mockito.eq(deviceID),
              Mockito.<long[]>isNull(),
              Mockito.anyCollection(),
              Mockito.anySet(),
              Mockito.eq(false),
              Mockito.any(LongConsumer.class));
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }

  @Test
  public void testBloomExcludedMeasurementIsNotRemembered() throws Exception {
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    FileReaderManager fileReaderManager = FileReaderManager.getInstance();
    TsFileID tsFileID = new TsFileID();
    cache.clear();
    bloomFilterCache.clear();
    try {
      String filePath = "target/batch-bloom-excluded.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.device");
      Set<String> allSensors = new HashSet<>(Arrays.asList("", "s1"));

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      // the device exists, but s1 is not in the file
      BloomFilter bloomFilter = BloomFilter.getEmptyBloomFilter(0.05, 1);
      bloomFilter.add(deviceID.toString() + ".");
      Mockito.when(reader.readBloomFilter(Mockito.any(LongConsumer.class))).thenReturn(bloomFilter);

      Statistics<?> timeStatistics = Statistics.getStatsByType(TSDataType.INT64);
      timeStatistics.setCount(1);
      timeStatistics.setStartTime(0);
      timeStatistics.setEndTime(10);
      List<TimeseriesMetadata> group = new ArrayList<>();
      group.add(
          new TimeseriesMetadata(
              (byte) 0, 0, "", TSDataType.INT64, timeStatistics, new PublicBAOS()));
      stubGroupRead(reader, deviceID, false, group);

      Map<String, TimeseriesMetadata> first =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);
      assertNull(first.get("s1"));

      // s1 was excluded by the bloom filter, not read, so it is probed again instead of being
      // remembered; the second query still needs no reader call
      Map<String, TimeseriesMetadata> second =
          getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);
      assertEquals(allSensors, second.keySet());
      assertNull(second.get("s1"));
      Mockito.verify(reader, Mockito.times(1))
          .readTimeseriesMetadataForMeasurements(
              Mockito.eq(deviceID),
              Mockito.<long[]>isNull(),
              Mockito.anyCollection(),
              Mockito.anySet(),
              Mockito.eq(false),
              Mockito.any(LongConsumer.class));
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }

  @Test
  public void testBloomLoadIsExcludedFromItsMetadataLookupPhase() throws Exception {
    TimeSeriesMetadataCache cache = Mockito.spy(TimeSeriesMetadataCache.getInstance());
    AtomicLong now = new AtomicLong(1);
    Mockito.doAnswer(invocation -> now.get()).when(cache).nanoTime();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    FileReaderManager fileReaderManager = FileReaderManager.getInstance();
    TsFileID tsFileID = new TsFileID();
    cache.clear();
    bloomFilterCache.clear();
    try {
      String filePath = "target/batch-bloom-phase.tsfile";
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.device");
      Set<String> allSensors = new HashSet<>(Arrays.asList("", "s1"));

      TsFileSequenceReader reader = Mockito.mock(TsFileSequenceReader.class);
      fileReaderManager.setReaderForTest(tsFileID, true, reader);
      BloomFilter bloomFilter = Mockito.spy(BloomFilter.getEmptyBloomFilter(0.05, 2));
      bloomFilter.add(deviceID.toString() + ".");
      bloomFilter.add(deviceID.toString() + ".s1");
      long bloomReadNanos = 20;
      long deviceCheckNanos = 3;
      Mockito.when(reader.readBloomFilter(Mockito.any(LongConsumer.class)))
          .thenAnswer(
              invocation -> {
                now.addAndGet(bloomReadNanos);
                return bloomFilter;
              });
      Mockito.doAnswer(
              invocation -> {
                now.addAndGet(deviceCheckNanos);
                return invocation.callRealMethod();
              })
          .when(bloomFilter)
          .contains(deviceID + ".");

      Statistics<?> timeStatistics = Statistics.getStatsByType(TSDataType.INT64);
      timeStatistics.setCount(1);
      timeStatistics.setStartTime(0);
      timeStatistics.setEndTime(10);
      Statistics<?> s1Statistics = Statistics.getStatsByType(TSDataType.INT32);
      s1Statistics.setCount(1);
      s1Statistics.setStartTime(0);
      s1Statistics.setEndTime(10);
      List<TimeseriesMetadata> group = new ArrayList<>();
      group.add(
          new TimeseriesMetadata(
              (byte) 0, 0, "", TSDataType.INT64, timeStatistics, new PublicBAOS()));
      group.add(
          new TimeseriesMetadata(
              (byte) 0, 0, "s1", TSDataType.INT32, s1Statistics, new PublicBAOS()));
      stubGroupRead(reader, deviceID, false, group);

      // warm both entries
      getTimeseriesMetadata(cache, filePath, tsFileID, deviceID, allSensors, false);

      // evict only the time column, and drop the bloom cache so the next call reloads it
      cache.remove(new TimeSeriesMetadataCacheKey(tsFileID, deviceID, ""));
      bloomFilterCache.clear();

      QueryContext queryContext = new QueryContext(false, false);
      Map<String, TimeseriesMetadata> result =
          cache.getTimeseriesMetadata(
              filePath, tsFileID, deviceID, allSensors, false, false, queryContext, null);
      assertNotNull(result.get(""));
      assertNotNull(result.get("s1"));
      assertEquals(
          bloomReadNanos, queryContext.getQueryStatistics().getLoadBloomFilterTime().get());
      assertEquals(
          deviceCheckNanos,
          queryContext.getQueryStatistics().getLoadTimeSeriesMetadataFromCacheTime().get());
      assertEquals(
          0, queryContext.getQueryStatistics().getLoadTimeSeriesMetadataFromDiskTime().get());

      // With time cached, the filter loads after the cache pass. Populate s1 during that pass
      // through the filter, so the locked recheck finds it and only "absent" counts as a miss.
      cache.remove(new TimeSeriesMetadataCacheKey(tsFileID, deviceID, "s1"));
      bloomFilterCache.clear();
      long valueCheckNanos = 5;
      Mockito.doAnswer(
              invocation -> {
                getTimeseriesMetadata(
                    cache,
                    filePath,
                    tsFileID,
                    deviceID,
                    new HashSet<>(Arrays.asList("", "s1")),
                    false);
                now.addAndGet(valueCheckNanos);
                return false;
              })
          .when(bloomFilter)
          .contains(deviceID + ".absent");
      allSensors.add("absent");
      QueryContext mixedQuery = new QueryContext(false, false);
      result =
          cache.getTimeseriesMetadata(
              filePath, tsFileID, deviceID, allSensors, false, false, mixedQuery, null);
      assertNotNull(result.get("s1"));
      assertNull(result.get("absent"));
      assertEquals(
          2, mixedQuery.getQueryStatistics().getLoadTimeSeriesMetadataFromCacheCount().get());
      assertEquals(
          1, mixedQuery.getQueryStatistics().getLoadTimeSeriesMetadataFromDiskCount().get());
      assertEquals(bloomReadNanos, mixedQuery.getQueryStatistics().getLoadBloomFilterTime().get());
      assertEquals(
          0, mixedQuery.getQueryStatistics().getLoadTimeSeriesMetadataFromCacheTime().get());
      assertEquals(
          valueCheckNanos,
          mixedQuery.getQueryStatistics().getLoadTimeSeriesMetadataFromDiskTime().get());
    } finally {
      cache.clear();
      bloomFilterCache.clear();
      fileReaderManager.closeFileAndRemoveReader(tsFileID);
    }
  }

  @Test
  public void testExternalTsFileReadsAllRequestedMeasurements() throws Exception {
    TimeSeriesMetadataCache cache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    File file = new File("target/external-cache-disabled.tsfile");
    file.delete();
    IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.external.d1");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerAlignedTimeseries(
          deviceID,
          Arrays.asList(
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT32)));
      for (int time = 0; time < 3; time++) {
        TSRecord record = new TSRecord(deviceID, time);
        record.addPoint("s1", time);
        record.addPoint("s2", time);
        writer.writeRecord(record);
      }
    }
    cache.clear();
    bloomFilterCache.clear();
    long[] offset;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      LazyTsFileDeviceIterator iterator = new LazyTsFileDeviceIterator(reader);
      assertTrue(iterator.hasNext());
      iterator.next();
      offset = iterator.getCurrentDeviceMeasurementNodeOffset();
    }
    // external tsfiles bypass the metadata cache: every requested measurement must still be read
    TsFileResource resource = new TsFileResource(file);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    String filePath = file.getAbsolutePath();
    // the external registry keys readers by path and closes one when its last reference is
    // released, so hold one for the duration of the test
    FileReaderManager.getInstance().increaseExternalFileReaderReference(filePath);
    EnvironmentUtils.TEST_QUERY_FI_CONTEXT.setQueryDataSourceType(
        QueryDataSourceType.EXTERNAL_TSFILE_SCAN);
    try {
      Set<String> allSensors = new HashSet<>(Arrays.asList("", "s1", "s2"));
      Map<String, TimeseriesMetadata> result =
          cache.getTimeseriesMetadata(
              filePath,
              resource.getTsFileID(),
              deviceID,
              allSensors,
              false,
              false,
              EnvironmentUtils.TEST_QUERY_FI_CONTEXT,
              offset);
      assertEquals(allSensors.size(), result.size());
      for (String measurement : allSensors) {
        assertNotNull(measurement, result.get(measurement));
        assertEquals(measurement, result.get(measurement).getMeasurementId());
      }
      assertNull(
          cache.getTimeseriesMetadata(
              filePath,
              resource.getTsFileID(),
              Factory.DEFAULT_FACTORY.create("root.external.missing"),
              allSensors,
              true,
              false,
              EnvironmentUtils.TEST_QUERY_FI_CONTEXT,
              null));
    } finally {
      EnvironmentUtils.TEST_QUERY_FI_CONTEXT.setQueryDataSourceType(
          QueryDataSourceType.SERIES_SCAN);
      cache.clear();
      bloomFilterCache.clear();
      FileReaderManager.getInstance().decreaseExternalFileReaderReference(filePath);
      assertTrue(file.delete());
    }
  }
}
