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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.series;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongConsumer;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createChunkWriter;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeAlignedChunk;

/**
 * Verifies metadata loading on a real device wider than one measurement leaf group: each selected
 * column is resolved from the leaf group that holds it, and one group read serves every selected
 * column of that group.
 */
public class AlignedSeriesScanBatchTest {

  private static final String DATABASE = "root.wide_pd";
  private static final IDeviceID DEVICE =
      IDeviceID.Factory.DEFAULT_FACTORY.create(DATABASE + ".d1");
  private static final int COLUMN_COUNT = 300;
  private static final int PRESENT_COLUMN_COUNT = 30;
  private static final int ROW_COUNT = 10;

  @Test
  public void testScanReadsWideAlignedDevice() throws Exception {
    Assert.assertTrue(
        "the fixture must span more than one measurement leaf group",
        COLUMN_COUNT + 1 > TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode());
    File file = newFile(1);
    TsFileResource resource = writeWideDevice(file, COLUMN_COUNT);
    TsFileSequenceReader fileReader = new TsFileSequenceReader(file.getAbsolutePath());
    TsFileSequenceReader reader = Mockito.spy(fileReader);
    FileReaderManager.getInstance().setReaderForTest(resource.getTsFileID(), true, reader);
    TimeSeriesMetadataCache metadataCache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    metadataCache.clear();
    bloomFilterCache.clear();
    boolean previousIgnoreAllNullRows =
        EnvironmentUtils.TEST_QUERY_FI_CONTEXT.isIgnoreAllNullRows();
    // the unified path is only reachable from table dialect, which forces this flag to false
    EnvironmentUtils.TEST_QUERY_FI_CONTEXT.setIgnoreAllNullRows(false);
    try {
      long cacheHitsBefore =
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataFromCacheCount()
              .get();
      long diskReadsBefore =
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataFromDiskCount()
              .get();

      TsBlock block = scan(alignedPath(columnNames(COLUMN_COUNT)), resource);

      Assert.assertEquals(COLUMN_COUNT, block.getValueColumnCount());
      Assert.assertEquals(ROW_COUNT, block.getPositionCount());
      for (int row = 0; row < ROW_COUNT; row++) {
        Assert.assertEquals(row, block.getTimeByIndex(row));
      }
      for (int column = 0; column < COLUMN_COUNT; column++) {
        Column valueColumn = block.getColumn(column);
        for (int row = 0; row < ROW_COUNT; row++) {
          Assert.assertFalse("s" + column + " row " + row, valueColumn.isNull(row));
          Assert.assertEquals("s" + column + " row " + row, row, valueColumn.getInt(row));
        }
      }

      // a cold cache: every selected series is a disk lookup, counted per series
      Assert.assertEquals(
          cacheHitsBefore,
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataFromCacheCount()
              .get());
      // per-series counters: a cold scan counts one disk lookup per selected measurement
      long diskReads =
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
                  .getQueryStatistics()
                  .getLoadTimeSeriesMetadataFromDiskCount()
                  .get()
              - diskReadsBefore;
      Assert.assertTrue("disk lookups: " + diskReads, diskReads >= 1);
      Assert.assertTrue("disk lookups: " + diskReads, diskReads <= COLUMN_COUNT + 1);
      // the whole projection is resolved with one reader call for this file, not one per column
      Mockito.verify(reader)
          .readTimeseriesMetadataForMeasurements(
              Mockito.any(),
              Mockito.<long[]>isNull(),
              Mockito.anyCollection(),
              Mockito.anySet(),
              Mockito.anyBoolean(),
              Mockito.any(LongConsumer.class));
    } finally {
      EnvironmentUtils.TEST_QUERY_FI_CONTEXT.setIgnoreAllNullRows(previousIgnoreAllNullRows);
      cleanup(file, resource, metadataCache, bloomFilterCache);
    }
  }

  @Test
  public void testSparseProjectionFiltersAbsentColumns() throws Exception {
    File file = newFile(2);
    TsFileResource resource = writeWideDevice(file, PRESENT_COLUMN_COUNT);
    TimeSeriesMetadataCache metadataCache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    AlignedFullPath scanPath = alignedPath(columnNames(COLUMN_COUNT));
    boolean previousIgnoreAllNullRows =
        EnvironmentUtils.TEST_QUERY_FI_CONTEXT.isIgnoreAllNullRows();
    EnvironmentUtils.TEST_QUERY_FI_CONTEXT.setIgnoreAllNullRows(false);
    try {
      metadataCache.clear();
      bloomFilterCache.clear();
      TsFileSequenceReader reader = Mockito.spy(new TsFileSequenceReader(file.getAbsolutePath()));
      FileReaderManager.getInstance().setReaderForTest(resource.getTsFileID(), true, reader);
      BloomFilter filter = reader.readBloomFilter();
      Set<String> expected = new HashSet<>(scanPath.getAllSensors());
      expected.removeIf(measurement -> !filter.contains(DEVICE + "." + measurement));
      Assert.assertTrue(
          "The fixture must exclude at least one projected column",
          expected.size() < COLUMN_COUNT + 1);

      List<Set<String>> readerRequests = new ArrayList<>();
      Mockito.doAnswer(
              invocation -> {
                // The cache removes entries from pending after the reader returns.
                readerRequests.add(new HashSet<>(invocation.<Collection<String>>getArgument(2)));
                return invocation.callRealMethod();
              })
          .when(reader)
          .readTimeseriesMetadataForMeasurements(
              Mockito.eq(DEVICE),
              Mockito.<long[]>isNull(),
              Mockito.anyCollection(),
              Mockito.anySet(),
              Mockito.anyBoolean(),
              Mockito.any(LongConsumer.class));
      long diskReadsBefore =
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataFromDiskCount()
              .get();

      TsBlock block = scan(scanPath, resource);

      Assert.assertEquals(ROW_COUNT, block.getPositionCount());
      Assert.assertEquals(COLUMN_COUNT, block.getValueColumnCount());
      for (int row = 0; row < ROW_COUNT; row++) {
        Assert.assertEquals(row, block.getTimeByIndex(row));
      }
      for (int column = 0; column < COLUMN_COUNT; column++) {
        Column valueColumn = block.getColumn(column);
        boolean present = column < PRESENT_COLUMN_COUNT;
        for (int row = 0; row < ROW_COUNT; row++) {
          if (present) {
            Assert.assertFalse(valueColumn.isNull(row));
            Assert.assertEquals(row, valueColumn.getInt(row));
          } else {
            Assert.assertTrue("s" + column + " row " + row, valueColumn.isNull(row));
          }
        }
      }

      Assert.assertEquals(Collections.singletonList(expected), readerRequests);
      // All projected measurements missed the cache, including those excluded by the filter.
      long diskReads =
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
                  .getQueryStatistics()
                  .getLoadTimeSeriesMetadataFromDiskCount()
                  .get()
              - diskReadsBefore;
      Assert.assertEquals(COLUMN_COUNT + 1, diskReads);
    } finally {
      EnvironmentUtils.TEST_QUERY_FI_CONTEXT.setIgnoreAllNullRows(previousIgnoreAllNullRows);
      cleanup(file, resource, metadataCache, bloomFilterCache);
    }
  }

  @Test
  public void testAllNullProjection() throws Exception {
    File file = newFile(3);
    TsFileResource resource = writeWideDevice(file, PRESENT_COLUMN_COUNT);
    TimeSeriesMetadataCache metadataCache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    // every selected field is absent from the file
    AlignedFullPath absentPath = alignedPath(absentColumnNames(COLUMN_COUNT));
    boolean previousIgnoreAllNullRows =
        EnvironmentUtils.TEST_QUERY_FI_CONTEXT.isIgnoreAllNullRows();
    try {
      // table dialect: all-null rows are kept
      EnvironmentUtils.TEST_QUERY_FI_CONTEXT.setIgnoreAllNullRows(false);
      metadataCache.clear();
      bloomFilterCache.clear();
      TsBlock block = scan(absentPath, resource);
      Assert.assertEquals(ROW_COUNT, block.getPositionCount());
      Assert.assertEquals(COLUMN_COUNT, block.getValueColumnCount());
      for (int column = 0; column < COLUMN_COUNT; column++) {
        for (int row = 0; row < ROW_COUNT; row++) {
          Assert.assertTrue("absent" + column + " row " + row, block.getColumn(column).isNull(row));
        }
      }

      // tree dialect: an all-null projection skips the file
      EnvironmentUtils.TEST_QUERY_FI_CONTEXT.setIgnoreAllNullRows(true);
      metadataCache.clear();
      bloomFilterCache.clear();
      AlignedSeriesScanUtil scan = newScanUtil(absentPath, resource);
      // The first call skips the only file. The next call sees an exhausted data source.
      Assert.assertEquals(Optional.empty(), scan.hasNextFile());
      Assert.assertEquals(Optional.of(false), scan.hasNextFile());
    } finally {
      EnvironmentUtils.TEST_QUERY_FI_CONTEXT.setIgnoreAllNullRows(previousIgnoreAllNullRows);
      cleanup(file, resource, metadataCache, bloomFilterCache);
    }
  }

  private static TsBlock scan(AlignedFullPath scanPath, TsFileResource resource)
      throws IOException {
    AlignedSeriesScanUtil seriesScanUtil = newScanUtil(scanPath, resource);
    Assert.assertEquals(Optional.of(true), seriesScanUtil.hasNextFile());
    Assert.assertEquals(Optional.of(true), seriesScanUtil.hasNextChunk());
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    return seriesScanUtil.nextPage();
  }

  private static AlignedSeriesScanUtil newScanUtil(
      AlignedFullPath scanPath, TsFileResource resource) throws IOException {
    SeriesScanOptions.Builder builder = new SeriesScanOptions.Builder();
    builder.withAllSensors(new HashSet<>(scanPath.getMeasurementList()));
    SeriesScanOptions scanOptions = builder.build();
    AlignedSeriesScanUtil seriesScanUtil =
        new AlignedSeriesScanUtil(
            scanPath, Ordering.ASC, scanOptions, EnvironmentUtils.TEST_QUERY_FI_CONTEXT);
    seriesScanUtil.initQueryDataSource(
        new QueryDataSource(Collections.singletonList(resource), Collections.emptyList()));
    return seriesScanUtil;
  }

  private static AlignedFullPath alignedPath(List<String> measurementNames) {
    List<IMeasurementSchema> schemas = new ArrayList<>(measurementNames.size());
    for (String measurement : measurementNames) {
      schemas.add(new MeasurementSchema(measurement, TSDataType.INT32));
    }
    return new AlignedFullPath(DEVICE, measurementNames, schemas);
  }

  private static List<String> columnNames(int count) {
    List<String> names = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      names.add("s" + i);
    }
    return names;
  }

  private static List<String> absentColumnNames(int count) {
    List<String> names = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      names.add("absent" + i);
    }
    return names;
  }

  private static File newFile(int version) {
    File file = new File(TestConstant.getTestTsFilePath(DATABASE, 0, 0, version));
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }
    return file;
  }

  private static TsFileResource writeWideDevice(File file, int presentColumnCount)
      throws IOException, IllegalPathException {
    TsFileResource resource = new TsFileResource(file);
    List<PartialPath> writtenPaths = new ArrayList<>(presentColumnCount);
    List<TSDataType> dataTypes = new ArrayList<>(presentColumnCount);
    List<TSEncoding> encodings = new ArrayList<>(presentColumnCount);
    List<CompressionType> compressionTypes = new ArrayList<>(presentColumnCount);
    for (int i = 0; i < presentColumnCount; i++) {
      writtenPaths.add(new AlignedPath(DEVICE.toString(), "s" + i));
      dataTypes.add(TSDataType.INT32);
      encodings.add(TSEncoding.PLAIN);
      compressionTypes.add(CompressionType.UNCOMPRESSED);
    }
    try (TsFileIOWriter writer = new TsFileIOWriter(file)) {
      writer.startChunkGroup(DEVICE);
      for (IChunkWriter chunkWriter :
          createChunkWriter(writtenPaths, dataTypes, encodings, compressionTypes, true)) {
        writeAlignedChunk(
            (AlignedChunkWriterImpl) chunkWriter,
            writer,
            Collections.singletonList(new TimeRange(0L, ROW_COUNT - 1L)),
            true);
      }
      writer.endChunkGroup();
      writer.endFile();
    }
    resource.updateStartTime(DEVICE, 0);
    resource.updateEndTime(DEVICE, ROW_COUNT - 1L);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    return resource;
  }

  private static void cleanup(
      File file,
      TsFileResource resource,
      TimeSeriesMetadataCache metadataCache,
      BloomFilterCache bloomFilterCache)
      throws IOException {
    metadataCache.clear();
    bloomFilterCache.clear();
    FileReaderManager.getInstance().closeFileAndRemoveReader(resource.getTsFileID());
    Assert.assertTrue(file.delete());
  }
}
