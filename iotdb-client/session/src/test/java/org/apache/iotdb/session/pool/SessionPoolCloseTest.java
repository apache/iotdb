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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SessionPoolCloseTest {

  private SessionPool pool;
  private Session session;
  private Session replacement;
  private Session.Builder builder;

  @Before
  public void setUp() {
    session = mock(Session.class);
    replacement = mock(Session.class);
    builder = mock(Session.Builder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(session, replacement);
    when(session.getSqlDialect()).thenReturn("table");
    when(session.getDatabase()).thenReturn("db");
    when(replacement.getSqlDialect()).thenReturn("table");
    when(replacement.getDatabase()).thenReturn("db");
    pool = spy(new SessionPool.Builder().maxSize(1).enableAutoFetch(false).build());
    pool.sqlDialect = "table";
    pool.database = "db";
    doReturn(builder).when(pool).createSessionBuilder();
  }

  @After
  public void tearDown() {
    pool.close();
  }

  @Test
  public void testTableSessionCloseIsIdempotent() throws Exception {
    ITableSession borrowed = pool.getPooledTableSession();
    borrowed.close();
    borrowed.close();

    assertEquals(1, pool.currentAvailableSize());
    verify(pool, times(1)).putBack(session);
    verify(session, never()).close();
  }

  @Test
  public void testConcurrentCloseAndConnectionFailureReplaceSessionOnce() throws Exception {
    ITableSession borrowed = pool.getPooledTableSession();
    IoTDBConnectionException failure = new IoTDBConnectionException("statement failed");
    CountDownLatch operationStarted = new CountDownLatch(1);
    CountDownLatch continueOperation = new CountDownLatch(1);
    CountDownLatch closeStarted = new CountDownLatch(1);
    AtomicReference<Thread> closeThread = new AtomicReference<>();
    doAnswer(
            invocation -> {
              operationStarted.countDown();
              continueOperation.await();
              throw failure;
            })
        .when(session)
        .executeNonQueryStatement("flush");

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<IoTDBConnectionException> operation =
          executor.submit(
              () -> {
                try {
                  borrowed.executeNonQueryStatement("flush");
                  return null;
                } catch (IoTDBConnectionException e) {
                  return e;
                }
              });
      assertTrue(operationStarted.await(5, TimeUnit.SECONDS));
      Future<?> closing =
          executor.submit(
              () -> {
                closeThread.set(Thread.currentThread());
                closeStarted.countDown();
                borrowed.close();
                return null;
              });
      assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!closing.isDone()
          && closeThread.get().getState() != Thread.State.BLOCKED
          && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertFalse("close should wait for the in-flight operation", closing.isDone());
      assertEquals(Thread.State.BLOCKED, closeThread.get().getState());
      continueOperation.countDown();

      assertSame(failure, operation.get(5, TimeUnit.SECONDS));
      closing.get(5, TimeUnit.SECONDS);
      assertReplacementAvailable();
    } finally {
      continueOperation.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testTableSessionRestoresStateBeforeReturning() throws Exception {
    ITableSession borrowed = pool.getPooledTableSession();
    when(session.getSqlDialect()).thenReturn("tree");
    when(session.getDatabase()).thenReturn("other");

    borrowed.close();
    borrowed.close();

    InOrder order = inOrder(session, pool);
    order.verify(session).executeNonQueryStatement("set sql_dialect=table");
    order.verify(session).executeNonQueryStatement("use db");
    order.verify(pool).putBack(session);
    assertEquals(1, pool.currentAvailableSize());
  }

  @Test
  public void testCloseAfterInsertConnectionFailure() throws Exception {
    IoTDBConnectionException failure = new IoTDBConnectionException("insert failed");
    doThrow(failure).when(session).insertRelationalTablet(any(Tablet.class), isNull());
    assertCloseAfterConnectionFailure(failure, s -> s.insert(mock(Tablet.class)));
  }

  @Test
  public void testCloseAfterQueryConnectionFailure() throws Exception {
    IoTDBConnectionException failure = new IoTDBConnectionException("query failed");
    doThrow(failure).when(session).executeQueryStatement("select * from t");
    assertCloseAfterConnectionFailure(failure, s -> s.executeQueryStatement("select * from t"));
  }

  @Test
  public void testCloseAfterTimedQueryConnectionFailure() throws Exception {
    IoTDBConnectionException failure = new IoTDBConnectionException("query failed");
    doThrow(failure).when(session).executeQueryStatement("select * from t", 1000);
    assertCloseAfterConnectionFailure(
        failure, s -> s.executeQueryStatement("select * from t", 1000));
  }

  @Test
  public void testCloseAfterNonQueryConnectionFailure() throws Exception {
    IoTDBConnectionException failure = new IoTDBConnectionException("statement failed");
    doThrow(failure).when(session).executeNonQueryStatement("flush");
    assertCloseAfterConnectionFailure(failure, s -> s.executeNonQueryStatement("flush"));
  }

  @Test
  public void testDialectResetStatementFailureReplacesSession() throws Exception {
    ITableSession borrowed = pool.getPooledTableSession();
    when(session.getSqlDialect()).thenReturn("tree");
    doThrow(new StatementExecutionException("reset failed"))
        .when(session)
        .executeNonQueryStatement("set sql_dialect=table");

    borrowed.close();
    borrowed.close();

    assertReplacementAvailable();
  }

  @Test
  public void testDatabaseResetStatementFailureReplacesSession() throws Exception {
    ITableSession borrowed = pool.getPooledTableSession();
    when(session.getDatabase()).thenReturn("other");
    doThrow(new StatementExecutionException("reset failed"))
        .when(session)
        .executeNonQueryStatement("use db");

    borrowed.close();
    borrowed.close();

    assertReplacementAvailable();
  }

  @Test
  public void testDialectResetConnectionFailureReplacesSession() throws Exception {
    ITableSession borrowed = pool.getPooledTableSession();
    when(session.getSqlDialect()).thenReturn("tree");
    IoTDBConnectionException failure = new IoTDBConnectionException("reset failed");
    doThrow(failure).when(session).executeNonQueryStatement("set sql_dialect=table");

    assertSame(failure, assertThrows(IoTDBConnectionException.class, borrowed::close));
    borrowed.close();

    assertReplacementAvailable();
  }

  @Test
  public void testDatabaseResetConnectionFailureReplacesSession() throws Exception {
    ITableSession borrowed = pool.getPooledTableSession();
    when(session.getDatabase()).thenReturn("other");
    IoTDBConnectionException failure = new IoTDBConnectionException("reset failed");
    doThrow(failure).when(session).executeNonQueryStatement("use db");

    assertSame(failure, assertThrows(IoTDBConnectionException.class, borrowed::close));
    borrowed.close();

    assertReplacementAvailable();
  }

  @Test
  public void testFailedPhysicalCloseStillReplacesSession() throws Exception {
    doThrow(new IoTDBConnectionException("close failed")).when(session).close();
    IoTDBConnectionException failure = new IoTDBConnectionException("statement failed");
    doThrow(failure).when(session).executeNonQueryStatement("flush");

    assertCloseAfterConnectionFailure(failure, s -> s.executeNonQueryStatement("flush"));
  }

  @Test
  public void testOldResultSetCannotReleaseReborrowedSession() throws Exception {
    SessionDataSet firstDataSet = mock(SessionDataSet.class);
    SessionDataSet secondDataSet = mock(SessionDataSet.class);
    when(session.executeQueryStatement("select * from t")).thenReturn(firstDataSet, secondDataSet);

    try (SessionDataSetWrapper first = pool.executeQueryStatement("select * from t")) {
      assertFalse(first.hasNext());
      try (SessionDataSetWrapper second = pool.executeQueryStatement("select * from t")) {
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
    SessionDataSet dataSet = mock(SessionDataSet.class);
    when(session.executeQueryStatement("select * from t")).thenReturn(dataSet);
    doThrow(new IoTDBConnectionException("close failed")).when(dataSet).closeOperationHandle();

    try (SessionDataSetWrapper result = pool.executeQueryStatement("select * from t")) {
      result.close();
      result.close();
    }

    verify(dataSet, times(1)).closeOperationHandle();
    assertEquals(0, pool.currentOccupiedSize());
    assertReplacementAvailable();
  }

  @Test
  public void testResultSetStatementFailureReplacesSessionOnce() throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    when(session.executeQueryStatement("select * from t")).thenReturn(dataSet);
    doThrow(new StatementExecutionException("close failed")).when(dataSet).closeOperationHandle();

    try (SessionDataSetWrapper result = pool.executeQueryStatement("select * from t")) {
      result.close();
      result.close();
    }

    verify(dataSet, times(1)).closeOperationHandle();
    assertEquals(0, pool.currentOccupiedSize());
    assertReplacementAvailable();
  }

  @Test
  public void testReturningTableSessionAfterPoolCloseClosesConnection() throws Exception {
    ITableSession borrowed = pool.getPooledTableSession();
    pool.close();

    borrowed.close();
    borrowed.close();

    assertEquals(0, pool.currentAvailableSize());
    verify(session, times(1)).close();
  }

  @Test
  public void testConnectionFailureAfterPoolCloseDoesNotCreateReplacement() throws Exception {
    ITableSession borrowed = pool.getPooledTableSession();
    IoTDBConnectionException failure = new IoTDBConnectionException("statement failed");
    doThrow(failure).when(session).executeNonQueryStatement("flush");
    pool.close();

    assertSame(
        failure,
        assertThrows(
            IoTDBConnectionException.class, () -> borrowed.executeNonQueryStatement("flush")));
    borrowed.close();

    assertEquals(0, pool.currentAvailableSize());
    verify(builder, times(1)).build();
    verify(session, times(1)).close();
  }

  private void assertCloseAfterConnectionFailure(
      IoTDBConnectionException failure, TableOperation operation) throws Exception {
    ITableSession borrowed = pool.getPooledTableSession();
    assertSame(
        failure,
        assertThrows(
            IoTDBConnectionException.class,
            () -> {
              try (ITableSession resource = borrowed) {
                operation.execute(resource);
              }
            }));
    borrowed.close();

    assertEquals(0, failure.getSuppressed().length);
    assertReplacementAvailable();
  }

  private void assertReplacementAvailable() throws Exception {
    verify(session, times(1)).close();
    assertEquals(1, pool.currentAvailableSize());
    verify(builder, times(2)).build();
    try (ITableSession recovered = pool.getPooledTableSession()) {
      recovered.executeNonQueryStatement("select 1");
      verify(replacement).executeNonQueryStatement("select 1");
    }
    assertEquals(1, pool.currentAvailableSize());
  }

  @FunctionalInterface
  private interface TableOperation {
    void execute(ITableSession session) throws Exception;
  }
}
