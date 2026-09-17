/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.ChunkOffsetCalculator;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.NonAlignedChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LoadTsFileManagerTest {

  private File tempDir;
  private String[] originalLoadBaseDirs;
  private DataRegion dataRegion;
  private IoTDBConfig config;

  @Before
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("load-tsfile-manager-test").toFile();
    config = IoTDBDescriptor.getInstance().getConfig();
    originalLoadBaseDirs = config.getLoadTsFileDirs();
    config.setLoadTsFileDirs(new String[] {tempDir.getAbsolutePath()});

    dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn("root.load_manager_test");
    Mockito.when(dataRegion.getDataRegionIdString()).thenReturn("0");
    Mockito.when(dataRegion.getNonSystemDatabaseName())
        .thenReturn(Optional.of("root.load_manager_test"));
  }

  @After
  public void tearDown() throws Exception {
    config.setLoadTsFileDirs(originalLoadBaseDirs);
    deleteRecursively(tempDir);
  }

  @Test
  public void testOutOfOrderPieceWriteKeepsProgressAndDataReadable() throws Exception {
    final String uuid = "test-uuid";
    final int pointCount = 100;
    final int deviceCount = 5;
    final int measurementCount = 4;
    final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
    final List<ChunkData> earlierChunks = new ArrayList<>();
    final List<ChunkData> laterChunks = new ArrayList<>();
    final List<SeriesExpectation> expectations = new ArrayList<>();
    int valueBase = 0;

    for (int deviceIndex = 0; deviceIndex < deviceCount; deviceIndex++) {
      final StringArrayDeviceID device =
          new StringArrayDeviceID("root", "load_manager_test", "d" + deviceIndex);
      for (int measurementIndex = 0; measurementIndex < measurementCount; measurementIndex++) {
        final String measurement = "s" + measurementIndex;
        final NonAlignedChunkData chunkData =
            createNonAlignedChunkData(device, measurement, valueBase, pointCount);
        calculator.assign(chunkData);
        if (deviceIndex < 2) {
          earlierChunks.add(chunkData);
        } else {
          laterChunks.add(chunkData);
        }
        expectations.add(
            new SeriesExpectation(device, measurement, pointCount, valueBase, valueBase + 99));
        valueBase += pointCount;
      }
    }

    final LoadTsFileManager manager = new LoadTsFileManager(dataRegion);
    File tsFile = null;
    try {
      // Normally the earlier piece would be sent first. Simulate the opposite: the later piece
      // arrives first, then the earlier piece is filled in afterwards.
      final List<LoadTsFileConsensusNode.PieceRef> laterRefs =
          manager.writePiece(uuid, toTsFileDataList(laterChunks));
      assertEquals(1, laterRefs.size());
      tsFile = new File(laterRefs.get(0).getRelativePath());

      manager.writePiece(uuid, toTsFileDataList(earlierChunks));
      assertTrue(manager.prepare(uuid, 2, 1L, false, Collections.emptyMap()));

      final LoadTsFileProgress progress = new LoadTsFileProgress(tsFile);
      final List<LoadTsFileProgress.ChunkRangeRecord> records = progress.readAllRecords();
      assertEquals(deviceCount * measurementCount, records.size());
      assertTrue(progress.isReady(tsFile.length()));
      assertAllSeriesReadable(tsFile, expectations);
    } finally {
      manager.deleteAll(uuid);
    }
  }

  private static NonAlignedChunkData createNonAlignedChunkData(
      final IDeviceID device, final String measurement, final int valueBase, final int pointCount) {
    final NonAlignedChunkData chunkData =
        (NonAlignedChunkData)
            ChunkData.createChunkData(
                false,
                device,
                new ChunkHeader(
                    measurement,
                    0,
                    TSDataType.INT32,
                    CompressionType.UNCOMPRESSED,
                    TSEncoding.PLAIN,
                    0),
                new TTimePartitionSlot(0L));
    final long[] times = new long[pointCount];
    final Object[] values = new Object[pointCount];
    for (int i = 0; i < pointCount; i++) {
      times[i] = i + 1L;
      values[i] = valueBase + i;
    }
    chunkData.writeDecodePage(times, values, pointCount);
    chunkData.endChunk();
    return chunkData;
  }

  private static List<TsFileData> toTsFileDataList(final List<ChunkData> chunkDataList) {
    final List<TsFileData> result = new ArrayList<>();
    for (final ChunkData chunkData : chunkDataList) {
      result.add(chunkData);
    }
    return result;
  }

  private static void assertAllSeriesReadable(
      final File tsFile, final List<SeriesExpectation> expectations) throws Exception {
    try (final TsFileReader reader = new TsFileReader(tsFile)) {
      for (final SeriesExpectation expectation : expectations) {
        final QueryExpression queryExpression =
            QueryExpression.create(
                Collections.singletonList(
                    new Path(expectation.device, expectation.measurement, false)),
                null);
        final QueryDataSet dataSet = reader.query(queryExpression);
        int count = 0;
        while (dataSet.hasNext()) {
          final RowRecord row = dataSet.next();
          final org.apache.tsfile.read.common.Field field = row.getField(0);
          final int value = field.getIntV();
          if (count == 0) {
            assertEquals(expectation.firstValue, value);
          }
          if (count == expectation.pointCount - 1) {
            assertEquals(expectation.lastValue, value);
          }
          count++;
        }
        assertEquals(expectation.pointCount, count);
      }
    }
  }

  private static void deleteRecursively(final File file) {
    if (file == null || !file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      final File[] files = file.listFiles();
      if (files != null) {
        for (final File child : files) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }

  private static final class SeriesExpectation {
    private final IDeviceID device;
    private final String measurement;
    private final int pointCount;
    private final int firstValue;
    private final int lastValue;

    private SeriesExpectation(
        final IDeviceID device,
        final String measurement,
        final int pointCount,
        final int firstValue,
        final int lastValue) {
      this.device = device;
      this.measurement = measurement;
      this.pointCount = pointCount;
      this.firstValue = firstValue;
      this.lastValue = lastValue;
    }
  }
}
