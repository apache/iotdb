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

package org.apache.iotdb.db.storageengine.rescon.memory;

import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionInfo;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SystemInfoTest {

  private SystemInfo systemInfo;
  private DataRegionInfo dataRegionInfo;
  private TsFileProcessor currentProcessor;
  private TsFileProcessor largerProcessor;

  @Before
  public void setUp() throws Exception {
    systemInfo = SystemInfo.getInstance();
    systemInfo.close();
    setMemoryThreshold("memorySizeForMemtable", 1000L);
    setMemoryThreshold("FLUSH_THRESHOLD", 500.0);
    setMemoryThreshold("REJECT_THRESHOLD", 800.0);

    DataRegion dataRegion = mock(DataRegion.class);
    when(dataRegion.getDatabaseName()).thenReturn("root.test");
    dataRegionInfo = new DataRegionInfo(dataRegion);
    currentProcessor = mock(TsFileProcessor.class);
    largerProcessor = mock(TsFileProcessor.class);
    when(currentProcessor.getWorkMemTable()).thenReturn(mock(IMemTable.class));
    when(currentProcessor.getWorkMemTableRamCost()).thenReturn(100L);
    when(largerProcessor.getWorkMemTableRamCost()).thenReturn(600L);
    dataRegionInfo.initTsFileProcessorInfo(currentProcessor);
    dataRegionInfo.initTsFileProcessorInfo(largerProcessor);
  }

  @After
  public void tearDown() {
    systemInfo.close();
    systemInfo.loadWriteMemory();
  }

  @Test
  public void testRejectedWriterCanFlushItsCurrentProcessor() throws Exception {
    dataRegionInfo.addStorageGroupMemCost(900);

    // The larger processor needs the region write lock held by the inserting thread. Admission
    // must let that thread finish and flush its current memtable instead of waiting for that lock.
    Assert.assertTrue(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));
    Assert.assertTrue(systemInfo.isRejected());
    verify(currentProcessor).setWorkMemTableShouldFlush();
    verify(largerProcessor).setWorkMemTableShouldFlush();
  }

  @Test
  public void testRejectedWriterCanFlushItsFirstTablet() throws Exception {
    when(currentProcessor.getWorkMemTableRamCost()).thenReturn(0L);
    dataRegionInfo.addStorageGroupMemCost(900);

    Assert.assertTrue(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));
    verify(currentProcessor).setWorkMemTableShouldFlush();
  }

  @Test
  public void testCurrentProcessorCannotExceedHardMemoryLimit() throws Exception {
    dataRegionInfo.addStorageGroupMemCost(1100);

    try {
      systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor);
      Assert.fail("Expected WriteProcessRejectException");
    } catch (WriteProcessRejectException expected) {
      Assert.assertTrue(systemInfo.isRejected());
    }
  }

  @Test
  public void testNormalFlushStillSelectsLargestMemtable() throws Exception {
    dataRegionInfo.addStorageGroupMemCost(700);

    Assert.assertTrue(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));
    Assert.assertFalse(systemInfo.isRejected());
    verify(largerProcessor).setWorkMemTableShouldFlush();
    verify(currentProcessor, never()).setWorkMemTableShouldFlush();
  }

  private void setMemoryThreshold(String fieldName, Object value) throws Exception {
    Field field = SystemInfo.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(systemInfo, value);
  }
}
