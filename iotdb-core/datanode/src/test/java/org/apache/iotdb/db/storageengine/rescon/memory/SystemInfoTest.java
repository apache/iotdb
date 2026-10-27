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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SystemInfoTest {

  private SystemInfo systemInfo;
  private DataRegionInfo dataRegionInfo;
  private TsFileProcessor currentProcessor;
  private TsFileProcessor largerProcessor;
  private ExecutorService originalFlushSubmitter;
  private ExecutorService flushSubmitter;

  @Before
  public void setUp() throws Exception {
    systemInfo = SystemInfo.getInstance();
    systemInfo.close();
    setMemoryThreshold("memorySizeForMemtable", 1000L);
    setMemoryThreshold("FLUSH_THRESHOLD", 500.0);
    setMemoryThreshold("REJECT_THRESHOLD", 800.0);
    Field field = SystemInfo.class.getDeclaredField("flushTaskSubmitThreadPool");
    field.setAccessible(true);
    originalFlushSubmitter = (ExecutorService) field.get(systemInfo);
    flushSubmitter = mock(ExecutorService.class);
    field.set(systemInfo, flushSubmitter);

    DataRegion dataRegion = mock(DataRegion.class);
    when(dataRegion.getDatabaseName()).thenReturn("root.test");
    dataRegionInfo = new DataRegionInfo(dataRegion);
    currentProcessor = mock(TsFileProcessor.class);
    largerProcessor = mock(TsFileProcessor.class);
    mockWorkMemTable(currentProcessor);
    mockWorkMemTable(largerProcessor);
    when(currentProcessor.getWorkMemTableRamCost()).thenReturn(100L);
    when(largerProcessor.getWorkMemTableRamCost()).thenReturn(600L);
    dataRegionInfo.initTsFileProcessorInfo(currentProcessor);
    dataRegionInfo.initTsFileProcessorInfo(largerProcessor);
  }

  @After
  public void tearDown() throws Exception {
    setMemoryThreshold("flushTaskSubmitThreadPool", originalFlushSubmitter);
    systemInfo.close();
    systemInfo.loadWriteMemory();
  }

  @Test
  public void testRejectedWriterCannotFlushSmallerCurrentProcessor() throws Exception {
    dataRegionInfo.addStorageGroupMemCost(900);

    Assert.assertFalse(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));
    Assert.assertTrue(systemInfo.isRejected());
    verify(currentProcessor, never()).setWorkMemTableShouldFlush();
    verify(largerProcessor).setWorkMemTableShouldFlush();
  }

  @Test
  public void testRejectedWriterFirstTabletCannotBypassSelection() throws Exception {
    when(currentProcessor.getWorkMemTableRamCost()).thenReturn(0L);
    dataRegionInfo.addStorageGroupMemCost(900);

    Assert.assertFalse(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));
    verify(currentProcessor, never()).setWorkMemTableShouldFlush();
    verify(largerProcessor).setWorkMemTableShouldFlush();
  }

  @Test
  public void testRepeatedReportsSubmitEachMemtableOnce() throws Exception {
    dataRegionInfo.addStorageGroupMemCost(900);

    for (int i = 0; i < 10; i++) {
      Assert.assertFalse(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));
    }
    Assert.assertTrue(systemInfo.isRejected());
    verify(currentProcessor, never()).setWorkMemTableShouldFlush();
    verify(largerProcessor).setWorkMemTableShouldFlush();
    verify(flushSubmitter).submit(any(Runnable.class));
  }

  @Test
  public void testRepeatedReportsForFirstTabletSubmitOnce() throws Exception {
    when(currentProcessor.getWorkMemTableRamCost()).thenReturn(0L);
    dataRegionInfo.addStorageGroupMemCost(900);

    for (int i = 0; i < 10; i++) {
      Assert.assertFalse(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));
    }
    verify(currentProcessor, never()).setWorkMemTableShouldFlush();
    verify(largerProcessor).setWorkMemTableShouldFlush();
    verify(flushSubmitter).submit(any(Runnable.class));
  }

  @Test
  public void testNewMemtableCanBeScheduledAfterPreviousFlush() throws Exception {
    when(currentProcessor.getWorkMemTableRamCost()).thenReturn(700L);
    when(largerProcessor.getWorkMemTableRamCost()).thenReturn(100L);
    dataRegionInfo.addStorageGroupMemCost(900);
    Assert.assertTrue(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));

    mockWorkMemTable(currentProcessor);
    Assert.assertTrue(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));
    verify(currentProcessor, times(2)).setWorkMemTableShouldFlush();
    verify(largerProcessor, never()).setWorkMemTableShouldFlush();
    verify(flushSubmitter, times(2)).submit(any(Runnable.class));
  }

  @Test
  public void testMarkedCurrentProcessorCannotReachHardMemoryLimit() throws Exception {
    when(currentProcessor.getWorkMemTableRamCost()).thenReturn(700L);
    when(largerProcessor.getWorkMemTableRamCost()).thenReturn(100L);
    dataRegionInfo.addStorageGroupMemCost(900);
    Assert.assertTrue(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));
    dataRegionInfo.addStorageGroupMemCost(100);

    try {
      systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor);
      Assert.fail("Expected WriteProcessRejectException");
    } catch (WriteProcessRejectException expected) {
      Assert.assertTrue(systemInfo.isRejected());
      verify(currentProcessor).setWorkMemTableShouldFlush();
      verify(flushSubmitter).submit(any(Runnable.class));
    }
  }

  @Test
  public void testNoCurrentMemtableCannotBypassRejection() throws Exception {
    when(currentProcessor.getWorkMemTable()).thenReturn(null);
    when(currentProcessor.getWorkMemTableRamCost()).thenReturn(0L);
    dataRegionInfo.addStorageGroupMemCost(900);

    Assert.assertFalse(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));
    Assert.assertTrue(systemInfo.isRejected());
    verify(currentProcessor, never()).setWorkMemTableShouldFlush();
    verify(largerProcessor).setWorkMemTableShouldFlush();
  }

  @Test
  public void testRejectionClearsAfterMemoryIsReleased() throws Exception {
    dataRegionInfo.addStorageGroupMemCost(900);
    Assert.assertFalse(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));

    dataRegionInfo.releaseStorageGroupMemCost(500);
    systemInfo.resetStorageGroupStatus(dataRegionInfo);
    Assert.assertFalse(systemInfo.isRejected());
    Assert.assertTrue(systemInfo.reportStorageGroupStatus(dataRegionInfo, currentProcessor));
    verify(flushSubmitter).submit(any(Runnable.class));
  }

  @Test
  public void testCurrentProcessorCannotExceedHardMemoryLimit() throws Exception {
    when(currentProcessor.getWorkMemTableRamCost()).thenReturn(700L);
    when(largerProcessor.getWorkMemTableRamCost()).thenReturn(100L);
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

  private void mockWorkMemTable(TsFileProcessor processor) {
    IMemTable memTable = mock(IMemTable.class);
    AtomicBoolean marked = new AtomicBoolean();
    when(memTable.shouldFlush()).thenAnswer(invocation -> marked.get());
    when(processor.getWorkMemTable()).thenReturn(memTable);
    doAnswer(
            invocation -> {
              marked.set(true);
              return null;
            })
        .when(processor)
        .setWorkMemTableShouldFlush();
  }
}
