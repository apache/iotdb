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
package org.apache.iotdb.db.storageengine;

import org.apache.iotdb.commons.concurrent.ExceptionalCountDownLatch;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.DirectBufferMemoryAllocationException;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRecoverManager;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(DataRegion.class)
public class StorageEngineTest {

  private StorageEngine storageEngine;

  @Before
  public void setUp() {
    storageEngine = StorageEngine.getInstance();
  }

  @After
  public void after() {
    storageEngine = null;
  }

  @Test
  public void testGetAllDataRegionIds() throws Exception {
    DataRegionId id1 = new DataRegionId(1);
    DataRegion rg1 = PowerMockito.mock(DataRegion.class);
    DataRegion rg2 = PowerMockito.mock(DataRegion.class);
    DataRegionId id2 = new DataRegionId(2);
    PowerMockito.when(rg1.getDataRegionIdString()).thenReturn("1");
    PowerMockito.when(rg2.getDataRegionIdString()).thenReturn("2");
    storageEngine.setDataRegion(id1, rg1);
    storageEngine.setDataRegion(id2, rg2);

    List<DataRegionId> actual = Lists.newArrayList(id1, id2);
    List<DataRegionId> expect = storageEngine.getAllDataRegionIds();

    Assert.assertEquals(expect.size(), actual.size());
    Assert.assertTrue(actual.containsAll(expect));
    rg1.syncDeleteDataFiles();
    rg2.syncDeleteDataFiles();
  }

  @Test
  public void testGetTimePartitionId() {
    long timePartitionInterval = TimePartitionUtils.getTimePartitionInterval();
    Assert.assertEquals(-2, TimePartitionUtils.getTimePartitionId(-timePartitionInterval - 1));
    Assert.assertEquals(-1, TimePartitionUtils.getTimePartitionId(-timePartitionInterval));
    Assert.assertEquals(-1, TimePartitionUtils.getTimePartitionId(-1));
    Assert.assertEquals(0, TimePartitionUtils.getTimePartitionId(0));
    Assert.assertEquals(0, TimePartitionUtils.getTimePartitionId(1));
    Assert.assertEquals(0, TimePartitionUtils.getTimePartitionId(timePartitionInterval / 2));
    Assert.assertEquals(1, TimePartitionUtils.getTimePartitionId(timePartitionInterval * 2 - 1));
    Assert.assertEquals(2, TimePartitionUtils.getTimePartitionId(timePartitionInterval * 2 + 1));
  }

  @Test
  public void testNotifyWALRecoverManagerWhenDirectBufferAllocationFailed() throws Exception {
    StorageEngine spyStorageEngine = PowerMockito.spy(storageEngine);
    DirectBufferMemoryAllocationException directBufferMemoryAllocationException =
        new DirectBufferMemoryAllocationException(2, 1);
    ExecutorService testThreadPool = Executors.newSingleThreadExecutor();
    setCachedThreadPool(spyStorageEngine, testThreadPool);
    PowerMockito.doReturn(
            Collections.singletonMap("root.sg", Lists.newArrayList(new DataRegionId(0))))
        .when(spyStorageEngine)
        .getLocalDataRegionInfo();
    PowerMockito.doThrow(directBufferMemoryAllocationException)
        .when(spyStorageEngine)
        .buildNewDataRegion(
            ArgumentMatchers.eq("root.sg"), ArgumentMatchers.any(DataRegionId.class));

    List<Future<Void>> futures = new ArrayList<>();
    Method asyncRecoverMethod = StorageEngine.class.getDeclaredMethod("asyncRecover", List.class);
    asyncRecoverMethod.setAccessible(true);
    try {
      asyncRecoverMethod.invoke(spyStorageEngine, futures);

      Assert.assertEquals(1, futures.size());
      try {
        futures.get(0).get();
        Assert.fail("Expected data region recovery to fail.");
      } catch (ExecutionException e) {
        Assert.assertSame(directBufferMemoryAllocationException, e.getCause());
      }

      ExceptionalCountDownLatch latch =
          WALRecoverManager.getInstance().getAllDataRegionScannedLatch();
      Assert.assertTrue(latch.hasException());
      Assert.assertEquals(
          directBufferMemoryAllocationException.getMessage(), latch.getExceptionMessage());
    } finally {
      WALRecoverManager.getInstance().clear();
      testThreadPool.shutdownNow();
      setCachedThreadPool(spyStorageEngine, null);
    }
  }

  @Test
  public void testNotifyWALRecoverManagerButContinueForOtherDataRegionException() throws Exception {
    StorageEngine spyStorageEngine = PowerMockito.spy(storageEngine);
    DataRegionException dataRegionException = new DataRegionException("other recovery failure");
    ExecutorService testThreadPool = Executors.newSingleThreadExecutor();
    setCachedThreadPool(spyStorageEngine, testThreadPool);
    PowerMockito.doReturn(
            Collections.singletonMap("root.sg", Lists.newArrayList(new DataRegionId(0))))
        .when(spyStorageEngine)
        .getLocalDataRegionInfo();
    PowerMockito.doThrow(dataRegionException)
        .when(spyStorageEngine)
        .buildNewDataRegion(
            ArgumentMatchers.eq("root.sg"), ArgumentMatchers.any(DataRegionId.class));

    List<Future<Void>> futures = new ArrayList<>();
    Method asyncRecoverMethod = StorageEngine.class.getDeclaredMethod("asyncRecover", List.class);
    asyncRecoverMethod.setAccessible(true);
    try {
      asyncRecoverMethod.invoke(spyStorageEngine, futures);

      Assert.assertEquals(1, futures.size());
      futures.get(0).get();

      ExceptionalCountDownLatch latch =
          WALRecoverManager.getInstance().getAllDataRegionScannedLatch();
      Assert.assertTrue(latch.hasException());
      Assert.assertEquals(dataRegionException.getMessage(), latch.getExceptionMessage());
    } finally {
      WALRecoverManager.getInstance().clear();
      testThreadPool.shutdownNow();
      setCachedThreadPool(spyStorageEngine, null);
    }
  }

  private void setCachedThreadPool(StorageEngine storageEngine, ExecutorService executorService)
      throws Exception {
    Field cachedThreadPoolField = StorageEngine.class.getDeclaredField("cachedThreadPool");
    cachedThreadPoolField.setAccessible(true);
    cachedThreadPoolField.set(storageEngine, executorService);
  }
}
