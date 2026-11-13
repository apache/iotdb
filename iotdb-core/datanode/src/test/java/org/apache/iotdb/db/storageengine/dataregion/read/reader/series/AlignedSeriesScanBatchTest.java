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

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createChunkWriter;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeAlignedChunk;

/**
 * Verifies that a real multi-leaf-group device (more field columns than the measurement index
 * fan-out) is read correctly through the batch metadata path.
 */
public class AlignedSeriesScanBatchTest {

  private static final String DATABASE = "root.wide_pd";
  private static final IDeviceID DEVICE =
      IDeviceID.Factory.DEFAULT_FACTORY.create(DATABASE + ".d1");

  @Test
  public void testBatchScanReadsWideAlignedDevice() throws IOException, IllegalPathException {
    int columnCount = 300;
    File file = new File(TestConstant.getTestTsFilePath(DATABASE, 0, 0, 1));
    if (!file.getParentFile().exists()) {
      Assert.assertTrue(file.getParentFile().mkdirs());
    }
    TsFileResource resource = new TsFileResource(file);
    List<PartialPath> writtenPaths = new ArrayList<>(columnCount);
    List<TSDataType> dataTypes = new ArrayList<>(columnCount);
    List<TSEncoding> encodings = new ArrayList<>(columnCount);
    List<CompressionType> compressionTypes = new ArrayList<>(columnCount);
    List<String> measurementNames = new ArrayList<>(columnCount);
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>(columnCount);

    for (int i = 0; i < columnCount; i++) {
      String measurement = "s" + i;
      measurementNames.add(measurement);
      measurementSchemas.add(new MeasurementSchema(measurement, TSDataType.INT32));
      writtenPaths.add(new AlignedPath(DEVICE.toString(), measurement));
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
            Collections.singletonList(new TimeRange(0L, 9L)),
            true);
      }
      writer.endChunkGroup();
      writer.endFile();
    }
    resource.updateStartTime(DEVICE, 0);
    resource.updateEndTime(DEVICE, 9);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);

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

      AlignedFullPath scanPath = new AlignedFullPath(DEVICE, measurementNames, measurementSchemas);
      SeriesScanOptions.Builder builder = new SeriesScanOptions.Builder();
      builder.withAllSensors(new HashSet<>(scanPath.getMeasurementList()));
      SeriesScanOptions scanOptions = builder.build();
      // all columns are selected and the table is wider than the index fan-out, so batching applies
      scanOptions.setTotalFieldColumnCount(columnCount);

      AlignedSeriesScanUtil seriesScanUtil =
          new AlignedSeriesScanUtil(
              scanPath, Ordering.ASC, scanOptions, EnvironmentUtils.TEST_QUERY_FI_CONTEXT);
      seriesScanUtil.initQueryDataSource(
          new QueryDataSource(Collections.singletonList(resource), Collections.emptyList()));

      SeriesReaderTestUtil.assertWithHasNext(seriesScanUtil::hasNextFile, true);
      SeriesReaderTestUtil.assertWithHasNext(seriesScanUtil::hasNextChunk, true);
      Assert.assertTrue(seriesScanUtil.hasNextPage());
      TsBlock tsBlock = seriesScanUtil.nextPage();
      Assert.assertEquals(10, tsBlock.getPositionCount());

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
          diskReadsBefore + columnCount + 1,
          EnvironmentUtils.TEST_QUERY_FI_CONTEXT
              .getQueryStatistics()
              .getLoadTimeSeriesMetadataFromDiskCount()
              .get());
    } finally {
      metadataCache.clear();
      bloomFilterCache.clear();
      FileReaderManager.getInstance().closeFileAndRemoveReader(resource.getTsFileID());
      Assert.assertTrue(file.delete());
    }
  }
}
