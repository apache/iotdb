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

package com.timecho.iotdb.dataregion.utils;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.DataRegionTableSizeQueryContext;

import com.timecho.iotdb.dataregion.compaction.AbstractCompactionTest;
import com.timecho.iotdb.dataregion.utils.tableDiskUsageIndex.object.ObjectTableSizeIndexReader;
import com.timecho.iotdb.dataregion.utils.tableDiskUsageIndex.object.ObjectTableSizeIndexWriter;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class ObjectTableSizeIndexWriterTest extends AbstractCompactionTest {

  private DataRegion mockDataRegion;
  private TsFileManager mockTsFileManager;

  @Before
  public void setUp()
      throws InterruptedException, IOException, MetadataException, WriteProcessException {
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
    ObjectTableSizeIndexWriter writer =
        new ObjectTableSizeIndexWriter(mockDataRegion.getDatabaseName(), 0);
    File oldCacheFile = writer.getFile();
    Assert.assertTrue(oldCacheFile.exists());
    Assert.assertEquals("TableObjectSizeFile_0", oldCacheFile.getName());
    writer.write("table1", 0, 1000L, 10);
    writer.write("table1", 0, -1000L, -10);

    writer.compact();

    File targetCacheFile = writer.getFile();
    Assert.assertFalse(oldCacheFile.exists());
    Assert.assertEquals("TableObjectSizeFile_1", targetCacheFile.getName());
    writer.close();

    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(true);
    ObjectTableSizeIndexReader reader =
        new ObjectTableSizeIndexReader(targetCacheFile, targetCacheFile.length());
    reader.loadObjectFileTableSize(context, System.nanoTime(), Long.MAX_VALUE);
    reader.close();
    Assert.assertEquals(0, context.getObjectFileNum());
    Assert.assertEquals(0L, context.getObjectFileSize());
  }

  @Test
  public void testCompactTargetFile1() throws IOException {
    ObjectTableSizeIndexWriter writer =
        new ObjectTableSizeIndexWriter(mockDataRegion.getDatabaseName(), 0);
    File oldCacheFile = writer.getFile();
    Assert.assertTrue(oldCacheFile.exists());
    Assert.assertEquals("TableObjectSizeFile_0", oldCacheFile.getName());
    writer.write("table1", 0, 1000L, 10);
    writer.write("table2", 0, 2000L, 1);
    writer.write("table3", 0, 3000L, 1);
    writer.write("table1", 0, -100L, -9);
    writer.write("table1", 1, 100L, 10);

    writer.compact();

    File targetCacheFile = writer.getFile();
    Assert.assertFalse(oldCacheFile.exists());
    Assert.assertEquals("TableObjectSizeFile_1", targetCacheFile.getName());
    writer.close();

    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(true);
    ObjectTableSizeIndexReader reader =
        new ObjectTableSizeIndexReader(targetCacheFile, targetCacheFile.length());
    reader.loadObjectFileTableSize(context, System.nanoTime(), Long.MAX_VALUE);
    reader.close();
    Assert.assertEquals(13, context.getObjectFileNum());
    Assert.assertEquals(6000L, context.getObjectFileSize());
  }

  @Test
  public void testReadPerformance() throws IOException {
    ObjectTableSizeIndexWriter writer =
        new ObjectTableSizeIndexWriter(mockDataRegion.getDatabaseName(), 0);
    File cacheFile = writer.getFile();
    Assert.assertTrue(cacheFile.exists());
    Assert.assertEquals("TableObjectSizeFile_0", cacheFile.getName());
    for (int i = 1; i <= 10; i++) {
      for (int j = 0; j < 10000; j++) {
        writer.write("table" + i, 0, 1000L, 1);
        writer.persistPendingObjectDeltas();
      }
    }
    writer.close();

    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(true);
    ObjectTableSizeIndexReader reader =
        new ObjectTableSizeIndexReader(cacheFile, cacheFile.length());
    long start = System.currentTimeMillis();
    Assert.assertTrue(reader.loadObjectFileTableSize(context, System.nanoTime(), Long.MAX_VALUE));
    System.out.println("cost: " + (System.currentTimeMillis() - start) + "ms");
    System.out.println("file length: " + cacheFile.length());
    reader.close();
    Assert.assertEquals(100000, context.getObjectFileNum());
  }

  @Test
  public void testRecoverWriter1() throws IOException {
    ObjectTableSizeIndexWriter writer =
        new ObjectTableSizeIndexWriter(mockDataRegion.getDatabaseName(), 0);
    File cacheFile = writer.getFile();
    Assert.assertTrue(cacheFile.exists());
    Assert.assertEquals("TableObjectSizeFile_0", cacheFile.getName());
    for (int i = 1; i <= 10; i++) {
      for (int j = 0; j < 10; j++) {
        writer.write("table" + i, 0, 1000L, 1);
        writer.persistPendingObjectDeltas();
      }
    }
    writer.close();
    long cacheFileValidLength = cacheFile.length();

    Files.write(cacheFile.toPath(), new byte[] {0, 0, 0, 0}, StandardOpenOption.APPEND);
    Assert.assertEquals(cacheFileValidLength + 4, cacheFile.length());

    writer = new ObjectTableSizeIndexWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertEquals(cacheFileValidLength, cacheFile.length());
  }

  @Test
  public void testRecoverWriter2() throws IOException {
    File dir =
        StorageEngine.getDataRegionSystemDir(
            mockDataRegion.getDatabaseName(), mockDataRegion.getDataRegionIdString());
    dir.mkdirs();
    File file1 = new File(dir, ObjectTableSizeIndexWriter.OBJECT_SIZE_INDEX_FILE_PREFIX + "1");
    File tempFile =
        new File(
            dir,
            ObjectTableSizeIndexWriter.OBJECT_SIZE_INDEX_FILE_PREFIX
                + "2"
                + ObjectTableSizeIndexWriter.TEMP_INDEX_FILE_SUBFIX);
    file1.createNewFile();
    tempFile.createNewFile();
    ObjectTableSizeIndexWriter writer =
        new ObjectTableSizeIndexWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertTrue(file1.exists());
    Assert.assertFalse(tempFile.exists());
  }

  @Test
  public void testRecoverWriter3() throws IOException {
    File dir =
        StorageEngine.getDataRegionSystemDir(
            mockDataRegion.getDatabaseName(), mockDataRegion.getDataRegionIdString());
    dir.mkdirs();
    File tempFile =
        new File(
            dir,
            ObjectTableSizeIndexWriter.OBJECT_SIZE_INDEX_FILE_PREFIX
                + "0"
                + ObjectTableSizeIndexWriter.TEMP_INDEX_FILE_SUBFIX);
    tempFile.createNewFile();
    ObjectTableSizeIndexWriter writer =
        new ObjectTableSizeIndexWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertTrue(
        new File(dir, ObjectTableSizeIndexWriter.OBJECT_SIZE_INDEX_FILE_PREFIX + "0").exists());
    Assert.assertFalse(tempFile.exists());
  }

  @Test
  public void testRecoverWriter4() throws IOException {
    File dir =
        StorageEngine.getDataRegionSystemDir(
            mockDataRegion.getDatabaseName(), mockDataRegion.getDataRegionIdString());
    dir.mkdirs();
    File file1 = new File(dir, ObjectTableSizeIndexWriter.OBJECT_SIZE_INDEX_FILE_PREFIX + "1");
    File file2 = new File(dir, ObjectTableSizeIndexWriter.OBJECT_SIZE_INDEX_FILE_PREFIX + "2");
    file2.createNewFile();
    ObjectTableSizeIndexWriter writer =
        new ObjectTableSizeIndexWriter(mockDataRegion.getDatabaseName(), 0);
    writer.close();
    Assert.assertTrue(file2.exists());
    Assert.assertFalse(file1.exists());
  }
}
