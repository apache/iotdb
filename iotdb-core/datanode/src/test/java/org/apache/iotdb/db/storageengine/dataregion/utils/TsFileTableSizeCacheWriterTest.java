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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.AbstractTableSizeCacheWriter;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.DataRegionTableSizeQueryContext;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.TimePartitionTableSizeQueryContext;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.tsfile.TsFileTableDiskUsageCacheWriter;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.tsfile.TsFileTableSizeCacheReader;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileTableSizeCacheWriterTest extends AbstractCompactionTest {

  private DataRegion mockDataRegion;
  private TsFileManager mockTsFileManager;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    mockDataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(mockDataRegion.getDatabaseName()).thenReturn("test");
    Mockito.when(mockDataRegion.getDataRegionIdString()).thenReturn("0");
    StorageEngine.getInstance().setDataRegion(new DataRegionId(0), mockDataRegion);
    mockTsFileManager = new TsFileManager("test", "0", "");
    Mockito.when(mockDataRegion.getTsFileManager()).thenReturn(mockTsFileManager);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    StorageEngine.getInstance().deleteDataRegion(new DataRegionId(0));
  }

  @Test
  public void testCompactEmptyTargetFile() throws IOException {
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    File oldKeyFile = writer.getKeyFile();
    File oldValueFile = writer.getValueFile();
    Assert.assertEquals("TableSizeKeyFile_0", oldKeyFile.getName());
    Assert.assertEquals("TableSizeValueFile_0", oldValueFile.getName());

    TsFileResource resource1 = createEmptyFileAndResourceWithName("1-1-0-0.tsfile", 1, true);
    TsFileResource resource2 = createEmptyFileAndResourceWithName("2-2-0-0.tsfile", 1, true);
    TsFileResource resource3 = createEmptyFileAndResourceWithName("3-3-0-0.tsfile", 1, false);

    writer.write(resource1.getTsFileID(), Collections.singletonMap("table1", 10L));
    writer.write(resource2.getTsFileID(), Collections.singletonMap("table1", 20L));
    writer.write(resource3.getTsFileID(), Collections.singletonMap("table2", 200L));

    writer.compact();

    Assert.assertFalse(oldKeyFile.exists());
    Assert.assertEquals("TableSizeKeyFile_1", writer.getKeyFile().getName());
    Assert.assertEquals("TableSizeValueFile_1", writer.getValueFile().getName());
    Assert.assertEquals(0, writer.keyFileLength());
    Assert.assertEquals(0, writer.valueFileLength());
    writer.close();
  }

  @Test
  public void testCompactTargetFile1() throws IOException {
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    File oldKeyFile = writer.getKeyFile();
    File oldValueFile = writer.getValueFile();
    Assert.assertEquals("TableSizeKeyFile_0", oldKeyFile.getName());
    Assert.assertEquals("TableSizeValueFile_0", oldValueFile.getName());

    TsFileResource resource1 = createEmptyFileAndResourceWithName("1-1-0-0.tsfile", 1, true);
    TsFileResource resource2 = createEmptyFileAndResourceWithName("2-2-0-0.tsfile", 1, true);
    TsFileResource resource3 = createEmptyFileAndResourceWithName("3-3-0-0.tsfile", 1, false);
    mockTsFileManager.add(resource1, true);
    mockTsFileManager.add(resource3, false);

    writer.write(resource1.getTsFileID(), Collections.singletonMap("table1", 10L));
    writer.write(resource2.getTsFileID(), Collections.singletonMap("table1", 10L));
    writer.write(resource3.getTsFileID(), Collections.singletonMap("table2", 200L));

    writer.compact();

    File targetKeyFile = writer.getKeyFile();
    File targetValueFile = writer.getValueFile();
    Assert.assertFalse(oldKeyFile.exists());
    Assert.assertFalse(oldValueFile.exists());
    Assert.assertEquals("TableSizeKeyFile_1", targetKeyFile.getName());
    Assert.assertEquals("TableSizeValueFile_1", targetValueFile.getName());
    writer.close();

    TsFileTableSizeCacheReader reader =
        new TsFileTableSizeCacheReader(
            targetKeyFile.length(), targetKeyFile, targetValueFile.length(), targetValueFile, 1);
    reader.openKeyFile();
    int count = 0;
    while (reader.hasNextEntryInKeyFile()) {
      TsFileTableSizeCacheReader.KeyFileEntry keyFileEntry = reader.readOneEntryFromKeyFile();
      count++;
    }
    reader.closeCurrentFile();
    Assert.assertEquals(2, count);
  }

  @Test
  public void testCompactTargetFile2() throws IOException {
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    File oldKeyFile = writer.getKeyFile();
    File oldValueFile = writer.getValueFile();
    Assert.assertEquals("TableSizeKeyFile_0", oldKeyFile.getName());
    Assert.assertEquals("TableSizeValueFile_0", oldValueFile.getName());

    TsFileResource resource1 = createEmptyFileAndResourceWithName("1-1-0-0.tsfile", 1, true);
    TsFileResource resource2 = createEmptyFileAndResourceWithName("2-2-0-0.tsfile", 1, true);
    TsFileResource resource3 = createEmptyFileAndResourceWithName("3-3-0-0.tsfile", 1, false);
    mockTsFileManager.add(resource1, true);
    mockTsFileManager.add(resource2, true);

    writer.write(resource1.getTsFileID(), Collections.singletonMap("table1", 10L));
    writer.write(resource3.getTsFileID(), Collections.singletonMap("table2", 200L));
    writer.write(resource3.getTsFileID(), resource2.getTsFileID());

    writer.compact();

    File targetKeyFile = writer.getKeyFile();
    File targetValueFile = writer.getValueFile();
    Assert.assertFalse(oldKeyFile.exists());
    Assert.assertFalse(oldValueFile.exists());
    Assert.assertEquals("TableSizeKeyFile_1", targetKeyFile.getName());
    Assert.assertEquals("TableSizeValueFile_1", targetValueFile.getName());
    writer.close();

    TsFileTableSizeCacheReader reader =
        new TsFileTableSizeCacheReader(
            targetKeyFile.length(), targetKeyFile, targetValueFile.length(), targetValueFile, 1);
    reader.openKeyFile();
    int count = 0;
    while (reader.hasNextEntryInKeyFile()) {
      TsFileTableSizeCacheReader.KeyFileEntry keyFileEntry = reader.readOneEntryFromKeyFile();
      Assert.assertNotEquals(3, keyFileEntry.tsFileID.fileVersion);
      count++;
    }
    reader.closeCurrentFile();
    Assert.assertEquals(2, count);
  }

  @Test
  public void testReadPerformance() throws IOException {
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 10000; j++) {
        TsFileResource resource =
            createEmptyFileAndResourceWithName(j + "-" + j + "-0-0.tsfile", i, true);
        writer.write(resource.getTsFileID(), generateTableSizeMap(i));
      }
    }
    Assert.assertNotEquals(writer.getKeyFile().length(), writer.keyFileLength());
    Assert.assertNotEquals(writer.getValueFile().length(), writer.valueFileLength());
    writer.close();
    Assert.assertEquals(writer.getKeyFile().length(), writer.keyFileLength());
    Assert.assertEquals(writer.getValueFile().length(), writer.valueFileLength());
    File keyFile = writer.getKeyFile();
    File valueFile = writer.getValueFile();
    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(true);
    TsFileTableSizeCacheReader reader =
        new TsFileTableSizeCacheReader(keyFile.length(), keyFile, valueFile.length(), valueFile, 0);
    List<Pair<TsFileID, Long>> offsets = new ArrayList<>();
    long start = System.currentTimeMillis();
    reader.openKeyFile();
    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 10000; j++) {
        Assert.assertTrue(reader.hasNextEntryInKeyFile());
        TsFileTableSizeCacheReader.KeyFileEntry entry = reader.readOneEntryFromKeyFile();
        Assert.assertNotNull(entry);
        Assert.assertNull(entry.originTsFileID);
        Assert.assertEquals(new TsFileID(0, i, j, j, 0L), entry.tsFileID);
        Assert.assertTrue(entry.offset >= 0);
        offsets.add(new Pair<>(entry.tsFileID, entry.offset));
      }
    }
    Assert.assertFalse(reader.hasNextEntryInKeyFile());
    reader.closeCurrentFile();
    reader.openValueFile();
    reader.readFromValueFile(
        offsets.iterator(), context, System.currentTimeMillis(), Long.MAX_VALUE);
    reader.closeCurrentFile();
    System.out.println("cost: " + (System.currentTimeMillis() - start) + "ms");
    System.out.println("keyFileLength: " + keyFile.length());
    System.out.println("valueFileLength: " + valueFile.length());

    Assert.assertEquals(10, context.getTimePartitionTableSizeQueryContextMap().size());
    for (Map.Entry<Long, TimePartitionTableSizeQueryContext> entry :
        context.getTimePartitionTableSizeQueryContextMap().entrySet()) {
      Map<String, Long> tableSizeResultMap = entry.getValue().getTableSizeResultMap();
      for (Map.Entry<String, Long> tableSizeEntry : tableSizeResultMap.entrySet()) {
        int i = Integer.parseInt(tableSizeEntry.getKey().substring(5));
        Assert.assertEquals(Long.valueOf(i * 10000L), tableSizeEntry.getValue());
      }
    }
  }

  @Test
  public void testRecoverWriter1() throws IOException {
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 10; j++) {
        TsFileResource resource =
            createEmptyFileAndResourceWithName(j + "-" + j + "-0-0.tsfile", i, true);
        writer.write(resource.getTsFileID(), generateTableSizeMap(i));
      }
    }
    writer.close();
    File keyFile = writer.getKeyFile();
    long keyFileValidLength = keyFile.length();
    File valueFile = writer.getValueFile();
    long valueFileValidLength = valueFile.length();
    Files.write(keyFile.toPath(), new byte[] {1, 2, 3, 4}, StandardOpenOption.APPEND);
    Files.write(valueFile.toPath(), new byte[] {1, 2, 3, 4}, StandardOpenOption.APPEND);
    Assert.assertEquals(keyFileValidLength + 4, keyFile.length());
    Assert.assertEquals(valueFileValidLength + 4, valueFile.length());

    writer = new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertEquals(keyFileValidLength, keyFile.length());
    Assert.assertEquals(valueFileValidLength, valueFile.length());

    Files.write(keyFile.toPath(), new byte[] {0, 0, 0, 0}, StandardOpenOption.APPEND);
    writer = new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertEquals(keyFileValidLength, keyFile.length());
    Assert.assertEquals(valueFileValidLength, valueFile.length());

    ByteBuffer buffer =
        ByteBuffer.allocate(TsFileTableDiskUsageCacheWriter.KEY_FILE_OFFSET_RECORD_LENGTH);
    TsFileID tsFileID = new TsFileID(0, 1, 2, 3, 4);
    ReadWriteIOUtils.write(TsFileTableDiskUsageCacheWriter.KEY_FILE_RECORD_TYPE_OFFSET, buffer);
    ReadWriteIOUtils.write(tsFileID.timePartitionId, buffer);
    ReadWriteIOUtils.write(tsFileID.timestamp, buffer);
    ReadWriteIOUtils.write(tsFileID.fileVersion, buffer);
    ReadWriteIOUtils.write(tsFileID.compactionVersion, buffer);
    ReadWriteIOUtils.write(valueFileValidLength, buffer);
    Files.write(
        keyFile.toPath(),
        Arrays.copyOf(buffer.array(), buffer.position()),
        StandardOpenOption.APPEND);
    writer = new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertEquals(keyFileValidLength, keyFile.length());
    Assert.assertEquals(valueFileValidLength, valueFile.length());

    buffer.clear();
    ReadWriteForEncodingUtils.writeVarInt(1, buffer);
    ReadWriteIOUtils.writeVar("table1", buffer);
    ReadWriteIOUtils.write(10L, buffer);
    Files.write(
        valueFile.toPath(),
        Arrays.copyOf(buffer.array(), buffer.position()),
        StandardOpenOption.APPEND);
    writer = new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertEquals(keyFileValidLength, keyFile.length());
    Assert.assertEquals(valueFileValidLength, valueFile.length());
  }

  @Test
  public void testRecoverWriter2() throws IOException {
    File dir =
        StorageEngine.getDataRegionSystemDir(
            mockDataRegion.getDatabaseName(), mockDataRegion.getDataRegionIdString());
    dir.mkdirs();
    File keyFile1 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_KEY_FILENAME_PREFIX + "0");
    File valueFile1 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_VALUE_FILENAME_PREFIX + "0");
    File tempKeyFile2 =
        new File(
            dir,
            TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_KEY_FILENAME_PREFIX
                + "1"
                + AbstractTableSizeCacheWriter.TEMP_CACHE_FILE_SUBFIX);
    File tempValueFile2 =
        new File(
            dir,
            TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_VALUE_FILENAME_PREFIX
                + "1"
                + AbstractTableSizeCacheWriter.TEMP_CACHE_FILE_SUBFIX);

    keyFile1.createNewFile();
    valueFile1.createNewFile();
    tempKeyFile2.createNewFile();
    tempValueFile2.createNewFile();
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertTrue(keyFile1.exists());
    Assert.assertTrue(valueFile1.exists());
    Assert.assertFalse(tempKeyFile2.exists());
    Assert.assertFalse(tempValueFile2.exists());
    Assert.assertEquals(keyFile1, writer.getKeyFile());
    Assert.assertEquals(valueFile1, writer.getValueFile());
  }

  @Test
  public void testRecoverWriter3() throws IOException {
    File dir =
        StorageEngine.getDataRegionSystemDir(
            mockDataRegion.getDatabaseName(), mockDataRegion.getDataRegionIdString());
    dir.mkdirs();
    File keyFile1 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_KEY_FILENAME_PREFIX + "0");
    File valueFile1 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_VALUE_FILENAME_PREFIX + "0");
    File keyFile2 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_KEY_FILENAME_PREFIX + "1");
    File valueFile2 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_VALUE_FILENAME_PREFIX + "1");

    keyFile1.createNewFile();
    valueFile1.createNewFile();
    keyFile2.createNewFile();
    valueFile2.createNewFile();
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertFalse(keyFile1.exists());
    Assert.assertFalse(valueFile1.exists());
    Assert.assertTrue(keyFile2.exists());
    Assert.assertTrue(valueFile2.exists());
    Assert.assertEquals(keyFile2, writer.getKeyFile());
    Assert.assertEquals(valueFile2, writer.getValueFile());
  }

  @Test
  public void testRecoverWriter4() throws IOException {
    File dir =
        StorageEngine.getDataRegionSystemDir(
            mockDataRegion.getDatabaseName(), mockDataRegion.getDataRegionIdString());
    dir.mkdirs();
    File keyFile1 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_KEY_FILENAME_PREFIX + "0");
    File valueFile1 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_VALUE_FILENAME_PREFIX + "0");
    File keyFile2 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_KEY_FILENAME_PREFIX + "1");
    File tempValueFile2 =
        new File(
            dir,
            TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_VALUE_FILENAME_PREFIX
                + "1"
                + AbstractTableSizeCacheWriter.TEMP_CACHE_FILE_SUBFIX);
    File valueFile2 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_VALUE_FILENAME_PREFIX + "1");

    keyFile1.createNewFile();
    valueFile1.createNewFile();
    keyFile2.createNewFile();
    tempValueFile2.createNewFile();
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertFalse(keyFile1.exists());
    Assert.assertFalse(valueFile1.exists());
    Assert.assertFalse(tempValueFile2.exists());
    Assert.assertTrue(keyFile2.exists());
    Assert.assertTrue(valueFile2.exists());
    Assert.assertEquals(keyFile2, writer.getKeyFile());
    Assert.assertEquals(valueFile2, writer.getValueFile());
  }

  @Test
  public void testRecoverWriter5() throws IOException {
    File dir =
        StorageEngine.getDataRegionSystemDir(
            mockDataRegion.getDatabaseName(), mockDataRegion.getDataRegionIdString());
    dir.mkdirs();
    File keyFile1 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_KEY_FILENAME_PREFIX + "1");
    File valueFile1 =
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_VALUE_FILENAME_PREFIX + "1");
    File tempKeyFile2 =
        new File(
            dir,
            TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_KEY_FILENAME_PREFIX
                + "2"
                + TsFileTableDiskUsageCacheWriter.TEMP_CACHE_FILE_SUBFIX);
    File tempValueFile2 =
        new File(
            dir,
            TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_VALUE_FILENAME_PREFIX
                + "2"
                + AbstractTableSizeCacheWriter.TEMP_CACHE_FILE_SUBFIX);

    keyFile1.createNewFile();
    tempKeyFile2.createNewFile();
    tempValueFile2.createNewFile();
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertFalse(keyFile1.exists());
    Assert.assertFalse(valueFile1.exists());
    Assert.assertFalse(tempValueFile2.exists());
    Assert.assertFalse(tempKeyFile2.exists());
    Assert.assertEquals(
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_KEY_FILENAME_PREFIX + "0"),
        writer.getKeyFile());
    Assert.assertEquals(
        new File(dir, TsFileTableDiskUsageCacheWriter.TSFILE_CACHE_VALUE_FILENAME_PREFIX + "0"),
        writer.getValueFile());
  }

  private Map<String, Long> generateTableSizeMap(int tableNum) {
    Map<String, Long> map = new HashMap<>();
    for (int i = 1; i <= tableNum; i++) {
      map.put("table" + i, (long) i);
    }
    return map;
  }
}
