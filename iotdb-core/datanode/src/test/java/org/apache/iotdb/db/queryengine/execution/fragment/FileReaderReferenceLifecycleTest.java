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

import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FileReaderReferenceLifecycleTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private final FileReaderManager manager = FileReaderManager.getInstance();

  @After
  public void tearDown() throws Exception {
    manager.closeAndRemoveAllOpenedReaders();
  }

  private TsFileResource resource(int id) {
    return new TsFileResource(new File(folder.getRoot(), id + "-1-0-0.tsfile"));
  }

  @Test
  public void failedRegistrationIsRemovedFromFragmentReleaseSet() throws Exception {
    TsFileResource resource = spy(resource(1));
    Set<TsFileResource> closed = new HashSet<>();
    Set<TsFileResource> unclosed = new HashSet<>();
    // An unrelated query already owns a read lock. A failed registration must not repay that lock.
    resource.readLock();
    try {
      doThrow(new OutOfMemoryError("injected registration failure"))
          .doCallRealMethod()
          .when(resource)
          .getTsFileID();
      assertThrows(
          OutOfMemoryError.class,
          () -> FragmentInstanceContext.addFilePathToMap(resource, true, closed, unclosed));
      assertTrue(closed.isEmpty());
      closed.forEach(file -> manager.decreaseFileReaderReference(file, true));
      assertFalse(resource.tryWriteLock());
    } finally {
      resource.readUnlock();
    }
    assertTrue(resource.tryWriteLock());
    resource.writeUnlock();
  }

  private FakedFragmentInstanceContext context(TsFileResource... resources) throws Exception {
    DataRegion region = mock(DataRegion.class);
    when(region.tryReadLock(anyLong())).thenReturn(true);
    when(region.query(anyList(), any(), any(), any(), any(), anyLong()))
        .thenReturn(new QueryDataSource(Arrays.asList(resources), Collections.emptyList()));
    return new FakedFragmentInstanceContext(null, region);
  }

  @Test
  public void fakedContextReleasesTheRegistrationTimeSlotAfterSealing() throws Exception {
    TsFileResource resource = resource(1);
    resource.setStatus(TsFileResourceStatus.UNCLOSED);
    FakedFragmentInstanceContext context = context(resource);
    context.initQueryDataSource(mock(IFullPath.class));
    TsFileSequenceReader unclosed = mock(TsFileSequenceReader.class);
    manager.setReaderForTest(resource.getTsFileID(), false, unclosed);
    resource.setStatus(TsFileResourceStatus.NORMAL);
    manager.increaseFileReaderReference(resource, true);
    TsFileSequenceReader closed = mock(TsFileSequenceReader.class);
    manager.setReaderForTest(resource.getTsFileID(), true, closed);
    try {
      context.releaseSharedQueryDataSource();
      context.releaseSharedQueryDataSource();
      verify(unclosed).close();
      verify(closed, never()).close();
      assertFalse(manager.contains(resource, false));
      assertTrue(manager.contains(resource, true));
      assertFalse(resource.tryWriteLock());
    } finally {
      manager.decreaseFileReaderReference(resource, true);
    }
    verify(closed).close();
    assertTrue(resource.tryWriteLock());
    resource.writeUnlock();
  }

  @Test
  public void fakedContextRollsBackOnlySuccessfulRegistrations() throws Exception {
    TsFileResource first = resource(1);
    TsFileResource failing = spy(resource(2));
    FakedFragmentInstanceContext context = context(first, failing);
    failing.readLock();
    try {
      doThrow(new OutOfMemoryError("injected registration failure"))
          .doCallRealMethod()
          .when(failing)
          .getTsFileID();
      assertThrows(
          OutOfMemoryError.class, () -> context.initQueryDataSource(mock(IFullPath.class)));
      context.releaseSharedQueryDataSource();
      assertTrue(first.tryWriteLock());
      first.writeUnlock();
      assertFalse(failing.tryWriteLock());
    } finally {
      failing.readUnlock();
    }
    assertTrue(failing.tryWriteLock());
    failing.writeUnlock();
  }

  @Test
  public void fakedRollbackPreservesOriginalFailureAndReleasesEveryReference() throws Exception {
    TsFileResource first = resource(1);
    TsFileResource second = resource(2);
    TsFileResource failing = spy(resource(3));
    first.setStatus(TsFileResourceStatus.NORMAL);
    second.setStatus(TsFileResourceStatus.NORMAL);
    FakedFragmentInstanceContext context = context(first, second, failing);
    TsFileSequenceReader firstReader = mock(TsFileSequenceReader.class);
    TsFileSequenceReader secondReader = mock(TsFileSequenceReader.class);
    manager.setReaderForTest(first.getTsFileID(), true, firstReader);
    manager.setReaderForTest(second.getTsFileID(), true, secondReader);
    RuntimeException registrationFailure =
        new IllegalStateException("injected registration failure");
    RuntimeException closeFailure = new IllegalStateException("injected unchecked close failure");
    doThrow(registrationFailure).when(failing).getTsFileID();
    doThrow(closeFailure).when(firstReader).close();
    first.readLock();
    try {
      RuntimeException actual =
          assertThrows(
              RuntimeException.class, () -> context.initQueryDataSource(mock(IFullPath.class)));
      assertSame(registrationFailure, actual);
      assertEquals(1, actual.getSuppressed().length);
      assertSame(closeFailure, actual.getSuppressed()[0]);
      verify(firstReader).close();
      verify(secondReader).close();
      assertTrue(second.tryWriteLock());
      second.writeUnlock();
      context.releaseSharedQueryDataSource();
      assertFalse(first.tryWriteLock());
      assertTrue(manager.getClosedFileReaderMap().isEmpty());
    } finally {
      first.readUnlock();
    }
  }
}
