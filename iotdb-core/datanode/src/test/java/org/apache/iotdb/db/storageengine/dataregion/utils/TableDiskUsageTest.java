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

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.tablemodel.CompactionTableModelTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.DataRegionTableSizeQueryContext;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.TableDiskUsageCache;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.TableDiskUsageCacheReader;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.TimePartitionTableSizeQueryContext;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TableDiskUsageTest extends AbstractCompactionTest {

  private DataRegion mockDataRegion;
  private TsFileManager mockTsFileManager;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    TableDiskUsageCache.getInstance().ensureRunning();
    mockDataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(mockDataRegion.getDatabaseName()).thenReturn("test");
    Mockito.when(mockDataRegion.getDataRegionId()).thenReturn(new DataRegionId(0));
    Mockito.when(mockDataRegion.getDataRegionIdString()).thenReturn("0");
    StorageEngine.getInstance().setDataRegion(new DataRegionId(0), mockDataRegion);
    mockTsFileManager = new TsFileManager("test", "0", "");
    Mockito.when(mockDataRegion.getTsFileManager()).thenReturn(mockTsFileManager);
    TableDiskUsageCache.getInstance().registerRegion(mockDataRegion);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    StorageEngine.getInstance().deleteDataRegion(new DataRegionId(0));
  }

  @Test
  public void test1() throws Exception {
    TsFileResource resource = prepareFile(4);
    mockTsFileManager.add(resource, true);

    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(false);
    // only query table1 and table2
    Map<String, Long> timePartitionTableSizeMap = new HashMap<>();
    timePartitionTableSizeMap.put("table1", 0L);
    timePartitionTableSizeMap.put("table2", 0L);
    context.addTimePartition(0, new TimePartitionTableSizeQueryContext(timePartitionTableSizeMap));
    queryTableSize(context);
    int entryNum = 0;
    for (Map.Entry<Long, TimePartitionTableSizeQueryContext> timePartitionEntry :
        context.getTimePartitionTableSizeQueryContextMap().entrySet()) {
      TimePartitionTableSizeQueryContext timePartitionContext = timePartitionEntry.getValue();
      for (Map.Entry<String, Long> entry :
          timePartitionContext.getTableSizeResultMap().entrySet()) {
        String tableName = entry.getKey();
        long size = entry.getValue();
        Assert.assertNotEquals("table3", tableName);
        Assert.assertNotEquals("table4", tableName);
        Assert.assertTrue(size > 0);
        entryNum++;
      }
    }
    Assert.assertEquals(2, entryNum);
  }

  @Test
  public void test2() throws Exception {
    // cached
    TsFileResource resource1 = prepareFile(4);
    mockTsFileManager.add(resource1, true);
    Map<String, Long> tableSizeMap = new HashMap<>();
    tableSizeMap.put("table1", 10000000L);
    tableSizeMap.put("table2", 10000000L);
    TableDiskUsageCache.getInstance()
        .write(mockDataRegion.getDatabaseName(), resource1.getTsFileID(), tableSizeMap);

    TsFileResource resource2 = prepareFile(4);
    mockTsFileManager.add(resource2, true);

    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(false);
    // only query table1 and table2
    Map<String, Long> timePartitionTableSizeMap = new HashMap<>();
    timePartitionTableSizeMap.put("table1", 0L);
    timePartitionTableSizeMap.put("table2", 0L);
    context.addTimePartition(0, new TimePartitionTableSizeQueryContext(timePartitionTableSizeMap));
    TableDiskUsageCacheReader reader =
        new TableDiskUsageCacheReader(mockDataRegion, context, false);
    queryTableSize(context);
    int entryNum = 0;
    for (Map.Entry<Long, TimePartitionTableSizeQueryContext> timePartitionEntry :
        context.getTimePartitionTableSizeQueryContextMap().entrySet()) {
      TimePartitionTableSizeQueryContext timePartitionContext = timePartitionEntry.getValue();
      for (Map.Entry<String, Long> entry :
          timePartitionContext.getTableSizeResultMap().entrySet()) {
        String tableName = entry.getKey();
        long size = entry.getValue();
        Assert.assertNotEquals("table3", tableName);
        Assert.assertNotEquals("table4", tableName);
        Assert.assertTrue(size > 10000000L);
        entryNum++;
      }
    }
    Assert.assertEquals(2, entryNum);
  }

  @Test
  public void test3() throws Exception {
    // deleted
    TsFileResource resource1 = prepareFile(4);
    Map<String, Long> tableSizeMap = new HashMap<>();
    tableSizeMap.put("table1", 10000000L);
    tableSizeMap.put("table2", 10000000L);
    TableDiskUsageCache.getInstance()
        .write(mockDataRegion.getDatabaseName(), resource1.getTsFileID(), tableSizeMap);

    TsFileResource resource2 = prepareFile(4);
    mockTsFileManager.add(resource2, true);

    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(false);
    // only query table1 and table2
    Map<String, Long> timePartitionTableSizeMap = new HashMap<>();
    timePartitionTableSizeMap.put("table1", 0L);
    timePartitionTableSizeMap.put("table2", 0L);
    context.addTimePartition(0, new TimePartitionTableSizeQueryContext(timePartitionTableSizeMap));
    TableDiskUsageCacheReader reader =
        new TableDiskUsageCacheReader(mockDataRegion, context, false);
    queryTableSize(context);
    int entryNum = 0;
    for (Map.Entry<Long, TimePartitionTableSizeQueryContext> timePartitionEntry :
        context.getTimePartitionTableSizeQueryContextMap().entrySet()) {
      TimePartitionTableSizeQueryContext timePartitionContext = timePartitionEntry.getValue();
      for (Map.Entry<String, Long> entry :
          timePartitionContext.getTableSizeResultMap().entrySet()) {
        String tableName = entry.getKey();
        long size = entry.getValue();
        Assert.assertNotEquals("table3", tableName);
        Assert.assertNotEquals("table4", tableName);
        Assert.assertTrue(size < 10000000L && size > 0);
        entryNum++;
      }
    }
    Assert.assertEquals(2, entryNum);
  }

  @Test
  public void test4() throws Exception {
    TsFileResource resource1 = prepareFile(4);
    Map<String, Long> tableSizeMap = new HashMap<>();
    tableSizeMap.put("table1", 10000000L);
    tableSizeMap.put("table2", 10000000L);
    TableDiskUsageCache.getInstance()
        .write(mockDataRegion.getDatabaseName(), resource1.getTsFileID(), tableSizeMap);

    TsFileResource resource2 = prepareFile(4);
    mockTsFileManager.add(resource2, true);
    // resource1 renamed to resource2 and recorded in cache
    TableDiskUsageCache.getInstance()
        .write(mockDataRegion.getDatabaseName(), resource1.getTsFileID(), resource2.getTsFileID());

    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(false);
    // only query table1 and table2
    Map<String, Long> timePartitionTableSizeMap = new HashMap<>();
    timePartitionTableSizeMap.put("table1", 0L);
    timePartitionTableSizeMap.put("table2", 0L);
    context.addTimePartition(0, new TimePartitionTableSizeQueryContext(timePartitionTableSizeMap));
    TableDiskUsageCacheReader reader =
        new TableDiskUsageCacheReader(mockDataRegion, context, false);
    queryTableSize(context);
    int entryNum = 0;
    for (Map.Entry<Long, TimePartitionTableSizeQueryContext> timePartitionEntry :
        context.getTimePartitionTableSizeQueryContextMap().entrySet()) {
      TimePartitionTableSizeQueryContext timePartitionContext = timePartitionEntry.getValue();
      for (Map.Entry<String, Long> entry :
          timePartitionContext.getTableSizeResultMap().entrySet()) {
        String tableName = entry.getKey();
        long size = entry.getValue();
        Assert.assertNotEquals("table3", tableName);
        Assert.assertNotEquals("table4", tableName);
        Assert.assertEquals(10000000L, size);
        entryNum++;
      }
    }
    Assert.assertEquals(2, entryNum);
  }

  @Test
  public void testCalculateTableSizeFromFile() throws Exception {
    Pair<TsFileResource, Map<String, Long>> resourceTableSizeMapPair =
        prepareFileAndTableSizeMap(10, 100000);
    TsFileResource resource1 = resourceTableSizeMapPair.getLeft();
    Map<String, Long> tableSizeMapGeneratedByWrite = resourceTableSizeMapPair.getRight();
    Assert.assertEquals(10, tableSizeMapGeneratedByWrite.size());
    for (Long value : tableSizeMapGeneratedByWrite.values()) {
      Assert.assertTrue(value > 0);
    }
    mockTsFileManager.add(resource1, true);

    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(true);
    queryTableSize(context);
    Assert.assertEquals(1, context.getTimePartitionTableSizeQueryContextMap().size());
    TimePartitionTableSizeQueryContext timePartitionContext =
        context.getTimePartitionTableSizeQueryContextMap().values().iterator().next();
    Map<String, Long> tableSizeMapGeneratedByQuery = timePartitionContext.getTableSizeResultMap();
    Assert.assertEquals(10, tableSizeMapGeneratedByQuery.size());

    for (Map.Entry<String, Long> entry : tableSizeMapGeneratedByQuery.entrySet()) {
      String tableName = entry.getKey();
      Long size = tableSizeMapGeneratedByWrite.get(tableName);
      Assert.assertNotNull(size);
      Assert.assertTrue(Math.abs(size - entry.getValue()) < 1000);
    }
  }

  private void queryTableSize(DataRegionTableSizeQueryContext queryContext) throws Exception {
    TableDiskUsageCacheReader reader =
        new TableDiskUsageCacheReader(mockDataRegion, queryContext, false);
    try {
      Assert.assertTrue(reader.prepareCacheReader(System.currentTimeMillis(), Long.MAX_VALUE));
      Assert.assertTrue(
          reader.loadObjectFileTableSizeCache(System.currentTimeMillis(), Long.MAX_VALUE));
      Assert.assertTrue(
          reader.prepareCachedTsFileIDKeys(System.currentTimeMillis(), Long.MAX_VALUE));
      Assert.assertTrue(
          reader.checkAllFilesInTsFileManager(System.currentTimeMillis(), Long.MAX_VALUE));
      Assert.assertTrue(
          reader.readCacheValueFilesAndUpdateResultMap(System.currentTimeMillis(), Long.MAX_VALUE));
    } finally {
      reader.close();
    }
  }

  private TsFileResource prepareFile(int tableNum) throws IOException {
    return prepareFileAndTableSizeMap(tableNum, 100).getLeft();
  }

  private Pair<TsFileResource, Map<String, Long>> prepareFileAndTableSizeMap(
      int tableNum, int pointNum) throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    Map<String, Long> tableSizeMap = null;
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      for (int i = 0; i < tableNum; i++) {
        writer.registerTableSchema("table" + i, Collections.singletonList("device"));
        writer.startChunkGroup("table" + i, Collections.singletonList("d1"));
        writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
            Arrays.asList("s0", "s1"),
            new TimeRange[][][] {
              new TimeRange[][] {new TimeRange[] {new TimeRange(10, 10 + pointNum - 1)}}
            },
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            Arrays.asList(false, false));
        writer.endChunkGroup();
      }
      writer.endFile();
      tableSizeMap = writer.getFileWriter().getTableSizeMap();
    }
    return new Pair<>(resource1, tableSizeMap);
  }
}
