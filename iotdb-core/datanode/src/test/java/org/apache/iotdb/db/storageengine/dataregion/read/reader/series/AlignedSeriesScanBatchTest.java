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
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createChunkWriter;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeAlignedChunk;

/**
 * Verifies the batch metadata path on a real device wider than one measurement leaf group, and that
 * it matches the per-column path for dense, sparse and all-null projections.
 */
public class AlignedSeriesScanBatchTest {

  private static final String DATABASE = "root.wide_pd";
  private static final IDeviceID DEVICE =
      IDeviceID.Factory.DEFAULT_FACTORY.create(DATABASE + ".d1");
  private static final int COLUMN_COUNT = 300;
  private static final int PRESENT_COLUMN_COUNT = 30;
  private static final int ROW_COUNT = 10;

  @Test
  public void testBatchScanReadsWideAlignedDevice() throws Exception {
    File file = newFile(1);
    TsFileResource resource = writeWideDevice(file, COLUMN_COUNT);
    TimeSeriesMetadataCache metadataCache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    metadataCache.clear();
    bloomFilterCache.clear();
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

      TsBlock block = scan(alignedPath(columnNames(COLUMN_COUNT)), COLUMN_COUNT, resource);

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

      // one batch read per device and file: no per-column cache lookups happened
      Assert.assertEquals(
          "the batch path must not fall back to per-column cache loads",
          cacheHitsBefore,
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataFromCacheCount()
              .get());
      // statistics are counted per series, like the per-column path (time column + all fields)
      Assert.assertEquals(
          diskReadsBefore + COLUMN_COUNT + 1,
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataFromDiskCount()
              .get());
    } finally {
      cleanup(file, resource, metadataCache, bloomFilterCache);
    }
  }

  @Test
  public void testSparseBatchScanMatchesPerColumn() throws Exception {
    File file = newFile(2);
    TsFileResource resource = writeWideDevice(file, PRESENT_COLUMN_COUNT);
    TimeSeriesMetadataCache metadataCache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    AlignedFullPath scanPath = alignedPath(columnNames(COLUMN_COUNT));
    try {
      metadataCache.clear();
      bloomFilterCache.clear();
      long cacheHitsBefore =
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataFromCacheCount()
              .get();
      TsBlock batched = scan(scanPath, COLUMN_COUNT, resource);
      long batchedCacheHits =
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
                  .getQueryStatistics()
                  .getLoadTimeSeriesMetadataFromCacheCount()
                  .get()
              - cacheHitsBefore;

      metadataCache.clear();
      long perColumnCacheHitsBefore =
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataFromCacheCount()
              .get();
      TsBlock perColumn = scan(scanPath, 0, resource);
      long perColumnCacheHits =
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
                  .getQueryStatistics()
                  .getLoadTimeSeriesMetadataFromCacheCount()
                  .get()
              - perColumnCacheHitsBefore;

      // the sparse projection is identical to the per-column scan, including every null field
      Assert.assertEquals(perColumn.getPositionCount(), batched.getPositionCount());
      Assert.assertEquals(perColumn.getValueColumnCount(), batched.getValueColumnCount());
      for (int row = 0; row < batched.getPositionCount(); row++) {
        Assert.assertEquals(perColumn.getTimeByIndex(row), batched.getTimeByIndex(row));
      }
      for (int column = 0; column < batched.getValueColumnCount(); column++) {
        Column batchedColumn = batched.getColumn(column);
        Column perColumnColumn = perColumn.getColumn(column);
        boolean present = column < PRESENT_COLUMN_COUNT;
        for (int row = 0; row < batched.getPositionCount(); row++) {
          Assert.assertEquals(
              "s" + column + " row " + row, perColumnColumn.isNull(row), batchedColumn.isNull(row));
          if (present) {
            Assert.assertFalse(batchedColumn.isNull(row));
            Assert.assertEquals(row, batchedColumn.getInt(row));
            Assert.assertEquals(perColumnColumn.getInt(row), batchedColumn.getInt(row));
          } else {
            Assert.assertTrue(batchedColumn.isNull(row));
          }
        }
      }

      // the batch scan resolved everything from disk, the per-column scan reused leaf groups
      Assert.assertEquals(0, batchedCacheHits);
      Assert.assertTrue(perColumnCacheHits > 0);
    } finally {
      cleanup(file, resource, metadataCache, bloomFilterCache);
    }
  }

  @Test
  public void testBatchScanSkipsAllNullRows() throws Exception {
    File file = newFile(3);
    TsFileResource resource = writeWideDevice(file, PRESENT_COLUMN_COUNT);
    TimeSeriesMetadataCache metadataCache = TimeSeriesMetadataCache.getInstance();
    BloomFilterCache bloomFilterCache = BloomFilterCache.getInstance();
    // every selected field is absent from the file: with ignoreAllNullRows the file must be
    // skipped by both paths
    AlignedFullPath absentPath = alignedPath(absentColumnNames(COLUMN_COUNT));
    boolean previousIgnoreAllNullRows =
        EnvironmentUtils.TEST_QUERY_FI_CONTEXT.isIgnoreAllNullRows();
    EnvironmentUtils.TEST_QUERY_FI_CONTEXT.setIgnoreAllNullRows(true);
    try {
      metadataCache.clear();
      bloomFilterCache.clear();
      Assert.assertFalse(hasFile(absentPath, COLUMN_COUNT, resource));

      metadataCache.clear();
      Assert.assertFalse(hasFile(absentPath, 0, resource));
    } finally {
      EnvironmentUtils.TEST_QUERY_FI_CONTEXT.setIgnoreAllNullRows(previousIgnoreAllNullRows);
      cleanup(file, resource, metadataCache, bloomFilterCache);
    }
  }

  private static TsBlock scan(
      AlignedFullPath scanPath, int totalFieldColumnCount, TsFileResource resource)
      throws IOException {
    AlignedSeriesScanUtil seriesScanUtil = newScanUtil(scanPath, totalFieldColumnCount, resource);
    SeriesReaderTestUtil.assertWithHasNext(seriesScanUtil::hasNextFile, true);
    SeriesReaderTestUtil.assertWithHasNext(seriesScanUtil::hasNextChunk, true);
    Assert.assertTrue(seriesScanUtil.hasNextPage());
    return seriesScanUtil.nextPage();
  }

  private static boolean hasFile(
      AlignedFullPath scanPath, int totalFieldColumnCount, TsFileResource resource)
      throws IOException {
    AlignedSeriesScanUtil seriesScanUtil = newScanUtil(scanPath, totalFieldColumnCount, resource);
    while (true) {
      Optional<Boolean> hasNextFile = seriesScanUtil.hasNextFile();
      if (hasNextFile.isPresent()) {
        return hasNextFile.get();
      }
    }
  }

  private static AlignedSeriesScanUtil newScanUtil(
      AlignedFullPath scanPath, int totalFieldColumnCount, TsFileResource resource)
      throws IOException {
    SeriesScanOptions.Builder builder = new SeriesScanOptions.Builder();
    builder.withAllSensors(new HashSet<>(scanPath.getMeasurementList()));
    SeriesScanOptions scanOptions = builder.build();
    scanOptions.setTotalFieldColumnCount(totalFieldColumnCount);
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
