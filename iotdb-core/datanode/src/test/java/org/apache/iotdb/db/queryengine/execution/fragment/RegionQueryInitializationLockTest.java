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
package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceForRegionScan;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IFileScanHandle;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RegionQueryInitializationLockTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private final FileReaderManager manager = FileReaderManager.getInstance();
  private ExecutorService workers;
  private int originalTimeSlice;

  @Before
  public void setUp() throws Exception {
    manager.closeAndRemoveAllOpenedReaders();
    originalTimeSlice =
        CommonDescriptor.getInstance().getConfig().getDriverTaskExecutionTimeSliceInMs();
    CommonDescriptor.getInstance().getConfig().setDriverTaskExecutionTimeSliceInMs(10_000);
    workers = Executors.newFixedThreadPool(3);
  }

  @After
  public void tearDown() throws Exception {
    workers.shutdownNow();
    assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
    manager.closeAndRemoveAllOpenedReaders();
    CommonDescriptor.getInstance()
        .getConfig()
        .setDriverTaskExecutionTimeSliceInMs(originalTimeSlice);
  }

  private enum QueryMode {
    SHARED,
    BATCH,
    DEVICE_SCAN,
    SERIES_SCAN,
    FAKED_FAILURE,
    BATCH_FAILURE
  }

  private TsFileResource resource(int id) {
    TsFileResource resource = new TsFileResource(new File(folder.getRoot(), id + "-1-0-0.tsfile"));
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    return resource;
  }

  @Test
  public void sharedQueryReleasesDeletedFilesOutsideRegionLock() throws Exception {
    releasesOutsideRegionLock(QueryMode.SHARED);
  }

  @Test
  public void batchQueryReleasesDeletedFilesOutsideRegionLock() throws Exception {
    releasesOutsideRegionLock(QueryMode.BATCH);
  }

  @Test
  public void deviceScanReleasesDeletedFilesOutsideRegionLock() throws Exception {
    releasesOutsideRegionLock(QueryMode.DEVICE_SCAN);
  }

  @Test
  public void seriesScanReleasesDeletedFilesOutsideRegionLock() throws Exception {
    releasesOutsideRegionLock(QueryMode.SERIES_SCAN);
  }

  @Test
  public void fakedQueryRollsBackOutsideRegionLock() throws Exception {
    releasesOutsideRegionLock(QueryMode.FAKED_FAILURE);
  }

  @Test
  public void failedBatchReleasesAllReferencesOutsideRegionLock() throws Exception {
    releasesOutsideRegionLock(QueryMode.BATCH_FAILURE);
  }

  private void releasesOutsideRegionLock(QueryMode mode) throws Exception {
    DataRegion region = spy(new DataRegion("root.region_release", "1"));
    FakedFragmentInstanceContext context = new FakedFragmentInstanceContext(null, region);
    IFullPath path = mock(IFullPath.class);
    TsFileResource resource = resource(1);
    if (mode != QueryMode.FAKED_FAILURE) {
      resource.setStatusForTest(TsFileResourceStatus.DELETED);
    }
    TsFileResource failing = spy(resource(2));
    TsFileResource retained = resource(3);
    RuntimeException failure = new IllegalStateException("injected reference registration failure");
    doThrow(failure).when(failing).getTsFileID();
    ArrayList<TsFileResource> seq = new ArrayList<>(Collections.singletonList(resource));
    if (mode == QueryMode.FAKED_FAILURE || mode == QueryMode.BATCH_FAILURE) {
      seq.add(failing);
    }
    if (mode == QueryMode.BATCH_FAILURE) {
      seq.add(0, retained);
    }
    QueryDataSource dataSource = new QueryDataSource(seq, new ArrayList<>());
    doReturn(dataSource).when(region).query(anyList(), any(), any(), any(), any(), anyLong());
    IFileScanHandle handle = mock(IFileScanHandle.class);
    when(handle.getTsResource()).thenReturn(resource);
    when(handle.isClosed()).thenReturn(true);
    QueryDataSourceForRegionScan scanSource =
        new QueryDataSourceForRegionScan(
            new ArrayList<>(Collections.singletonList(handle)), new ArrayList<>());
    doReturn(scanSource)
        .when(region)
        .queryForDeviceRegionScan(any(), any(), any(), any(), anyLong());
    doReturn(scanSource)
        .when(region)
        .queryForSeriesRegionScan(anyList(), any(), any(), any(), anyLong());

    TsFileSequenceReader reader = mock(TsFileSequenceReader.class);
    manager.setReaderForTest(resource.getTsFileID(), true, reader);
    manager.increaseFileReaderReference(resource, true);
    CountDownLatch closing = new CountDownLatch(1);
    CountDownLatch finishClose = new CountDownLatch(1);
    AtomicReference<Thread> closeThread = new AtomicReference<>();
    AtomicReference<Thread> queryThread = new AtomicReference<>();
    CountDownLatch queryStarted = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              closeThread.set(Thread.currentThread());
              closing.countDown();
              assertTrue(finishClose.await(20, TimeUnit.SECONDS));
              return null;
            })
        .when(reader)
        .close();
    Future<?> release = workers.submit(() -> manager.decreaseFileReaderReference(resource, true));
    Future<?> query = null;
    try {
      assertTrue(closing.await(10, TimeUnit.SECONDS));
      query =
          workers.submit(
              () -> {
                queryThread.set(Thread.currentThread());
                queryStarted.countDown();
                switch (mode) {
                  case SHARED:
                    assertTrue(context.initQueryDataSource(Collections.singletonList(path)));
                    break;
                  case BATCH:
                    try (QueryDataSourceLease lease =
                        context.initBatchQueryDataSource(Collections.singletonList(path))) {
                      assertTrue(lease.getDataSource().getSeqResources().isEmpty());
                    }
                    break;
                  case DEVICE_SCAN:
                    assertTrue(context.initRegionScanQueryDataSource(Collections.emptyMap()));
                    break;
                  case SERIES_SCAN:
                    assertTrue(
                        context.initRegionScanQueryDataSource(Collections.singletonList(path)));
                    break;
                  case FAKED_FAILURE:
                    assertSame(
                        failure,
                        assertThrows(
                            RuntimeException.class, () -> context.initQueryDataSource(path)));
                    break;
                  case BATCH_FAILURE:
                    assertSame(
                        failure,
                        assertThrows(
                            RuntimeException.class,
                            () ->
                                context.initBatchQueryDataSource(Collections.singletonList(path))));
                    break;
                  default:
                    throw new AssertionError(mode);
                }
                return null;
              });
      assertTrue(queryStarted.await(10, TimeUnit.SECONDS));
      awaitEntryWait(queryThread.get(), closeThread.get());
      Future<?> writer =
          workers.submit(
              () -> {
                region.writeLock("deferred reference release test");
                region.writeUnlock();
              });
      writer.get(10, TimeUnit.SECONDS);
      assertFalse(query.isDone());
    } finally {
      finishClose.countDown();
      release.get(10, TimeUnit.SECONDS);
      if (query != null) {
        query.get(10, TimeUnit.SECONDS);
      }
    }
    verify(reader).close();
    if (mode == QueryMode.SHARED || mode == QueryMode.BATCH) {
      assertTrue(dataSource.getSeqResources().isEmpty());
    } else if (mode == QueryMode.DEVICE_SCAN || mode == QueryMode.SERIES_SCAN) {
      assertTrue(scanSource.getSeqFileScanHandles().isEmpty());
    }
    for (TsFileResource file : Arrays.asList(resource, failing, retained)) {
      assertTrue(file.tryWriteLock());
      file.writeUnlock();
    }
    assertTrue(manager.getClosedFileReaderMap().isEmpty());
    assertTrue(manager.getUnclosedFileReaderMap().isEmpty());
    if (mode == QueryMode.FAKED_FAILURE) {
      context.releaseSharedQueryDataSource();
    }
  }

  private static void awaitEntryWait(Thread waiter, Thread owner) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(waiter.getId());
      if (info != null
          && info.getThreadState() == Thread.State.BLOCKED
          && info.getLockOwnerId() == owner.getId()) {
        return;
      }
      Thread.sleep(1);
    }
    fail("query did not reach the entry held by the closing reader");
  }
}
