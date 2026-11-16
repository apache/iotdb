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

package org.apache.iotdb.session.pool;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SessionPoolCloseTest {

  private SessionPool pool;
  private Session session;
  private Session replacement;

  @Before
  public void setUp() {
    session = mock(Session.class);
    replacement = mock(Session.class);
    pool = spy(new SessionPool.Builder().maxSize(1).enableAutoFetch(false).build());
    doReturn(session, replacement).when(pool).constructNewSession();
  }

  @After
  public void tearDown() {
    pool.close();
  }

  @Test
  public void testOldResultSetCannotReleaseReborrowedSession() throws Exception {
    SessionDataSet firstDataSet = mock(SessionDataSet.class);
    SessionDataSet secondDataSet = mock(SessionDataSet.class);
    when(session.executeQueryStatement("select * from root.t"))
        .thenReturn(firstDataSet, secondDataSet);

    try (SessionDataSetWrapper first = pool.executeQueryStatement("select * from root.t")) {
      assertFalse(first.hasNext());
      try (SessionDataSetWrapper second = pool.executeQueryStatement("select * from root.t")) {
        first.close();
        assertEquals(1, pool.currentOccupiedSize());
        assertEquals(0, pool.currentAvailableSize());
      }
    }

    assertEquals(0, pool.currentOccupiedSize());
    assertEquals(1, pool.currentAvailableSize());
    verify(firstDataSet, times(1)).closeOperationHandle();
    verify(secondDataSet, times(1)).closeOperationHandle();
  }

  @Test
  public void testResultSetConnectionFailureReplacesSessionOnce() throws Exception {
    assertResultSetFailureReplacesSessionOnce(new IoTDBConnectionException("close failed"));
  }

  @Test
  public void testResultSetStatementFailureReplacesSessionOnce() throws Exception {
    assertResultSetFailureReplacesSessionOnce(new StatementExecutionException("close failed"));
  }

  @Test
  public void testFailedPhysicalCloseStillReplacesSession() throws Exception {
    doThrow(new IoTDBConnectionException("close failed")).when(session).close();
    assertResultSetFailureReplacesSessionOnce(new IoTDBConnectionException("close failed"));
  }

  @Test
  public void testConcurrentResultSetCloseReleasesSessionOnce() throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    when(session.executeQueryStatement("select * from root.t")).thenReturn(dataSet);
    CountDownLatch closeStarted = new CountDownLatch(1);
    CountDownLatch continueClose = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              closeStarted.countDown();
              assertTrue(continueClose.await(5, TimeUnit.SECONDS));
              return null;
            })
        .when(dataSet)
        .closeOperationHandle();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try (SessionDataSetWrapper result = pool.executeQueryStatement("select * from root.t")) {
      Future<?> firstClose = executor.submit(result::close);
      assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
      Future<?> secondClose = executor.submit(result::close);
      secondClose.get(5, TimeUnit.SECONDS);
      assertEquals(0, pool.currentOccupiedSize());
      assertEquals(0, pool.currentAvailableSize());
      continueClose.countDown();
      firstClose.get(5, TimeUnit.SECONDS);
    } finally {
      continueClose.countDown();
      executor.shutdownNow();
    }

    verify(dataSet, times(1)).closeOperationHandle();
    assertEquals(1, pool.currentAvailableSize());
  }

  @Test
  public void testReturningSessionAfterPoolCloseClosesConnection() throws Exception {
    doAnswer(
            invocation -> {
              pool.close();
              return null;
            })
        .when(session)
        .executeNonQueryStatement("flush");

    pool.executeNonQueryStatement("flush");

    assertEquals(0, pool.currentAvailableSize());
    verify(session, times(1)).close();
    verify(pool, times(1)).constructNewSession();
  }

  @Test
  public void testConnectionFailureAfterPoolCloseDoesNotCreateReplacement() throws Exception {
    IoTDBConnectionException failure = new IoTDBConnectionException("insert failed");
    doAnswer(
            invocation -> {
              pool.close();
              throw failure;
            })
        .when(session)
        .insertTablet(any(Tablet.class), anyBoolean());

    IoTDBConnectionException actual =
        assertThrows(IoTDBConnectionException.class, () -> pool.insertTablet(mock(Tablet.class)));

    assertSame(failure, actual.getCause());
    assertEquals(0, pool.currentAvailableSize());
    verify(pool, times(1)).constructNewSession();
    verify(session, times(1)).close();
  }

  private void assertResultSetFailureReplacesSessionOnce(Exception failure) throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    when(session.executeQueryStatement("select * from root.t")).thenReturn(dataSet);
    doThrow(failure).when(dataSet).closeOperationHandle();

    try (SessionDataSetWrapper result = pool.executeQueryStatement("select * from root.t")) {
      result.close();
      result.close();
    }

    verify(dataSet, times(1)).closeOperationHandle();
    assertEquals(0, pool.currentOccupiedSize());
    verify(session, times(1)).close();
    assertEquals(1, pool.currentAvailableSize());
    verify(pool, times(2)).constructNewSession();
    pool.executeNonQueryStatement("flush");
    verify(replacement).executeNonQueryStatement("flush");
    assertEquals(1, pool.currentAvailableSize());
  }
}
