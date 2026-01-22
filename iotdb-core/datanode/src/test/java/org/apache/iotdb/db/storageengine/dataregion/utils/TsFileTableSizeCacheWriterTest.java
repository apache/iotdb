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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.tsfile.TsFileTableDiskUsageCacheWriter;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.tsfile.TsFileTableSizeCacheReader;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class TsFileTableSizeCacheWriterTest extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void testCompactEmptyTargetFile() throws IOException {
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn("root.test");
    Mockito.when(dataRegion.getDataRegionIdString()).thenReturn("0");
    StorageEngine.getInstance().setDataRegion(new DataRegionId(0), dataRegion);
    TsFileManager tsFileManager = new TsFileManager("root.test", "0", "");
    Mockito.when(dataRegion.getTsFileManager()).thenReturn(tsFileManager);
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(dataRegion.getDatabaseName(), 0);
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

    Assert.assertEquals("TableSizeKeyFile_1", writer.getKeyFile().getName());
    Assert.assertEquals("TableSizeValueFile_1", writer.getValueFile().getName());
    writer.close();
  }

  @Test
  public void testCompactTargetFile1() throws IOException {
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn("root.test");
    Mockito.when(dataRegion.getDataRegionIdString()).thenReturn("0");
    StorageEngine.getInstance().setDataRegion(new DataRegionId(0), dataRegion);
    TsFileManager tsFileManager = new TsFileManager("root.test", "0", "");
    Mockito.when(dataRegion.getTsFileManager()).thenReturn(tsFileManager);
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(dataRegion.getDatabaseName(), 0);
    File oldKeyFile = writer.getKeyFile();
    File oldValueFile = writer.getValueFile();
    Assert.assertEquals("TableSizeKeyFile_0", oldKeyFile.getName());
    Assert.assertEquals("TableSizeValueFile_0", oldValueFile.getName());

    TsFileResource resource1 = createEmptyFileAndResourceWithName("1-1-0-0.tsfile", 1, true);
    TsFileResource resource2 = createEmptyFileAndResourceWithName("2-2-0-0.tsfile", 1, true);
    TsFileResource resource3 = createEmptyFileAndResourceWithName("3-3-0-0.tsfile", 1, false);
    tsFileManager.add(resource1, true);
    tsFileManager.add(resource3, false);

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
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn("root.test");
    Mockito.when(dataRegion.getDataRegionIdString()).thenReturn("0");
    StorageEngine.getInstance().setDataRegion(new DataRegionId(0), dataRegion);
    TsFileManager tsFileManager = new TsFileManager("root.test", "0", "");
    Mockito.when(dataRegion.getTsFileManager()).thenReturn(tsFileManager);
    TsFileTableDiskUsageCacheWriter writer =
        new TsFileTableDiskUsageCacheWriter(dataRegion.getDatabaseName(), 0);
    File oldKeyFile = writer.getKeyFile();
    File oldValueFile = writer.getValueFile();
    Assert.assertEquals("TableSizeKeyFile_0", oldKeyFile.getName());
    Assert.assertEquals("TableSizeValueFile_0", oldValueFile.getName());

    TsFileResource resource1 = createEmptyFileAndResourceWithName("1-1-0-0.tsfile", 1, true);
    TsFileResource resource2 = createEmptyFileAndResourceWithName("2-2-0-0.tsfile", 1, true);
    TsFileResource resource3 = createEmptyFileAndResourceWithName("3-3-0-0.tsfile", 1, false);
    tsFileManager.add(resource1, true);
    tsFileManager.add(resource2, true);

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
}
