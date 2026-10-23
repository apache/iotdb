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

import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.IPageReader;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class QueryChunkReadersTest {
  @Test
  public void testLastPageReleasesOwnerOnce() throws Exception {
    IChunkReader owner = mock(IChunkReader.class);
    IPageReader first = mock(IPageReader.class);
    IPageReader second = mock(IPageReader.class);
    try (QueryChunkReaders readers = new QueryChunkReaders()) {
      readers.retain(owner, Arrays.asList(first, second));
      readers.release(first);
      verify(owner, never()).close();
      readers.release(second);
      readers.release(second);
      verify(owner, times(1)).close();
    }
    verify(owner, times(1)).close();
  }

  @Test
  public void testEarlyCloseReleasesRemainingAndEmptyOwners() throws Exception {
    IChunkReader firstOwner = mock(IChunkReader.class);
    IChunkReader emptyOwner = mock(IChunkReader.class);
    QueryChunkReaders readers = new QueryChunkReaders();
    readers.retain(firstOwner, Arrays.asList(mock(IPageReader.class), mock(IPageReader.class)));
    readers.retain(emptyOwner, Collections.emptyList());
    readers.close();
    readers.close();
    verify(firstOwner, times(1)).close();
    verify(emptyOwner, times(1)).close();
  }

  @Test
  public void testRetainAfterCloseCannotLeakOwner() throws Exception {
    IChunkReader owner = mock(IChunkReader.class);
    QueryChunkReaders readers = new QueryChunkReaders();
    readers.close();
    org.junit.Assert.assertThrows(
        java.io.IOException.class,
        () -> readers.retain(owner, Collections.singletonList(mock(IPageReader.class))));
    verify(owner, times(1)).close();
  }

  @Test
  public void testCloseAttemptsEveryOwnerAfterIOException() throws Exception {
    IChunkReader failed = mock(IChunkReader.class);
    IChunkReader healthy = mock(IChunkReader.class);
    doThrow(new java.io.IOException()).when(failed).close();
    QueryChunkReaders readers = new QueryChunkReaders();
    readers.retain(failed, Collections.singletonList(mock(IPageReader.class)));
    readers.retain(healthy, Collections.singletonList(mock(IPageReader.class)));
    org.junit.Assert.assertThrows(java.io.IOException.class, readers::close);
    readers.close();
    verify(failed, times(1)).close();
    verify(healthy, times(1)).close();
  }
}
