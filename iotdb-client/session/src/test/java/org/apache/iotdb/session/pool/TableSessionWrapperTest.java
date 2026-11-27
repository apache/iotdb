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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TestInsertStage;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.write.record.Tablet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TableSessionWrapperTest {
  @Test
  public void testDefaultAndExplicitStagesReturnConnectionOnlyOnce() throws Exception {
    Session delegate = mock(Session.class);
    SessionPool pool = mock(SessionPool.class);
    Tablet tablet = mock(Tablet.class);
    ITableSession session = new TableSessionWrapper(delegate, pool);
    session.testInsert(tablet);
    session.testInsert(tablet, TestInsertStage.UNTIL_PLANNED);
    session.close();
    session.close();
    verify(delegate).testInsertRelationalTablet(tablet, TestInsertStage.UNTIL_RPC_PROCESSED);
    verify(delegate).testInsertRelationalTablet(tablet, TestInsertStage.UNTIL_PLANNED);
    verify(delegate, never()).insertRelationalTablet(any());
    verify(pool).putBack(delegate);
    verify(pool, never()).cleanSessionAndMayThrowConnectionException(any());
  }

  @Test
  public void testConnectionFailureDiscardsSessionWithoutMaskingTheErrorOnClose() throws Exception {
    Session delegate = mock(Session.class);
    SessionPool pool = mock(SessionPool.class);
    Tablet tablet = mock(Tablet.class);
    IoTDBConnectionException failure = new IoTDBConnectionException("disconnected");
    doThrow(failure)
        .when(delegate)
        .testInsertRelationalTablet(tablet, TestInsertStage.UNTIL_PLANNED);
    IoTDBConnectionException actual =
        assertThrows(
            IoTDBConnectionException.class,
            () -> {
              try (ITableSession session = new TableSessionWrapper(delegate, pool)) {
                session.testInsert(tablet, TestInsertStage.UNTIL_PLANNED);
              }
            });
    assertSame(failure, actual);
    assertEquals(0, actual.getSuppressed().length);
    verify(pool).cleanSessionAndMayThrowConnectionException(delegate);
    verify(pool, never()).putBack(any());
  }

  @Test
  public void testStatementFailurePreservesConnectionForReuse() throws Exception {
    Session delegate = mock(Session.class);
    SessionPool pool = mock(SessionPool.class);
    Tablet tablet = mock(Tablet.class);
    StatementExecutionException failure = new StatementExecutionException("missing partition");
    doThrow(failure)
        .when(delegate)
        .testInsertRelationalTablet(tablet, TestInsertStage.UNTIL_ANALYZED);
    try (ITableSession session = new TableSessionWrapper(delegate, pool)) {
      assertSame(
          failure,
          assertThrows(
              StatementExecutionException.class,
              () -> session.testInsert(tablet, TestInsertStage.UNTIL_ANALYZED)));
      session.testInsert(tablet);
    }
    verify(delegate).testInsertRelationalTablet(tablet, TestInsertStage.UNTIL_RPC_PROCESSED);
    verify(pool, never()).cleanSessionAndMayThrowConnectionException(any());
    verify(pool).putBack(delegate);
  }
}
