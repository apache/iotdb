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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.isession.NumericRowPreparedStatement;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSExecutePreparedNumericRowReq;
import org.apache.iotdb.service.rpc.thrift.TSPrepareNumericRowResp;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionConnection;
import org.apache.iotdb.session.i18n.SessionMessages;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.powermock.reflect.Whitebox;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class PreparedNumericRowPoolTest {
  private Session session;
  private SessionPool pool;
  private TableSessionWrapper wrapper;
  private IClientRPCService.Iface client;

  @Before
  public void setUp() throws Exception {
    session = new Session.Builder().enableRedirection(false).enableAutoFetch(false).build();
    client = mock(IClientRPCService.Iface.class);
    SessionConnection connection = new SessionConnection("table");
    Whitebox.setInternalState(connection, "client", client);
    Whitebox.setInternalState(connection, "sessionId", 11L);
    Whitebox.setInternalState(session, "defaultSessionConnection", connection);
    pool = mock(SessionPool.class);
    pool.sqlDialect = session.getSqlDialect();
    wrapper = new TableSessionWrapper(session, pool);
    when(client.prepareNumericRow(any()))
        .thenReturn(new TSPrepareNumericRowResp(success()).setPreparedId(101))
        .thenReturn(new TSPrepareNumericRowResp(success()).setPreparedId(102));
    when(client.executePreparedNumericRow(any())).thenReturn(success());
    when(client.closePreparedNumericRow(anyLong(), anyLong())).thenReturn(success());
  }

  @Test
  public void testClosedWrapperCannotUseReborrowedSession() throws Exception {
    NumericRowPreparedStatement oldPrepared = prepare(wrapper);
    wrapper.close();
    // A new borrower owns the same physical connection; the previous wrapper must stay closed.
    try (TableSessionWrapper next = new TableSessionWrapper(session, pool);
        NumericRowPreparedStatement nextPrepared = prepare(next)) {
      clearInvocations(client);
      IoTDBConnectionException error =
          assertThrows(IoTDBConnectionException.class, () -> prepare(wrapper));
      assertEquals(SessionMessages.EXCEPTION_TABLE_SESSION_IS_CLOSED_77FE359B, error.getMessage());
      assertThrows(
          IoTDBConnectionException.class,
          () ->
              wrapper.executePreparedInsertNumericRow(
                  oldPrepared, 1L, new long[] {42}, new String[] {"a"}));
      assertThrows(
          IoTDBConnectionException.class,
          () ->
              wrapper.executePreparedInsertNumericRow(
                  oldPrepared, 1L, new long[] {42}, new String[] {"a"}, new BitMap(1)));
      verifyZeroInteractions(client);
      verify(pool, never()).cleanSessionAndMayThrowConnectionException(any());

      next.executePreparedInsertNumericRow(nextPrepared, 2L, new long[] {43}, new String[] {"a"});
      ArgumentCaptor<TSExecutePreparedNumericRowReq> request =
          ArgumentCaptor.forClass(TSExecutePreparedNumericRowReq.class);
      verify(client).executePreparedNumericRow(request.capture());
      assertEquals(102L, request.getValue().getPreparedId());
    }
    wrapper.close();
    verify(pool, times(2)).putBack(session);
  }

  @Test
  public void testPrepareTransportFailureDiscardsSession() throws Exception {
    TException transportFailure = new TException("transport closed");
    when(client.prepareNumericRow(any())).thenThrow(transportFailure);
    IoTDBConnectionException error =
        assertThrows(
            IoTDBConnectionException.class,
            () -> {
              try (TableSessionWrapper borrowed = wrapper) {
                prepare(borrowed);
              }
            });
    assertSame(transportFailure, error.getCause());
    // Automatic close must neither mask the transport failure nor return the broken session.
    assertEquals(0, error.getSuppressed().length);
    assertThrows(IoTDBConnectionException.class, () -> prepare(wrapper));
    wrapper.close();
    verify(pool).cleanSessionAndMayThrowConnectionException(session);
    verify(pool, never()).putBack(any());
    verify(client).prepareNumericRow(any());
  }

  @Test
  public void testExecuteTransportFailureDiscardsSession() throws Exception {
    NumericRowPreparedStatement prepared = prepare(wrapper);
    TException transportFailure = new TException("transport closed");
    when(client.executePreparedNumericRow(any())).thenThrow(transportFailure);
    IoTDBConnectionException error =
        assertThrows(
            IoTDBConnectionException.class,
            () -> {
              try (TableSessionWrapper borrowed = wrapper) {
                borrowed.executePreparedInsertNumericRow(
                    prepared, 1L, new long[] {42}, new String[] {"a"});
              }
            });
    assertSame(transportFailure, error.getCause());
    assertEquals(0, error.getSuppressed().length);
    assertThrows(
        IoTDBConnectionException.class,
        () ->
            wrapper.executePreparedInsertNumericRow(
                prepared, 2L, new long[] {43}, new String[] {"a"}));
    verify(pool).cleanSessionAndMayThrowConnectionException(session);
    verify(pool, never()).putBack(any());
    verify(client).executePreparedNumericRow(any());
  }

  @Test
  public void testPrepareStatementErrorPreservesHealthySession() throws Exception {
    when(client.prepareNumericRow(any()))
        .thenReturn(
            new TSPrepareNumericRowResp(
                new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                    .setMessage("invalid prepared metadata")));
    try (TableSessionWrapper borrowed = wrapper) {
      assertThrows(StatementExecutionException.class, () -> prepare(borrowed));
    }
    verify(pool, never()).cleanSessionAndMayThrowConnectionException(any());
    verify(pool).putBack(session);
  }

  @Test
  public void testCloseReturnsSessionOnlyOnce() throws Exception {
    wrapper.close();
    wrapper.close();
    verify(pool).putBack(session);
    verify(pool, never()).cleanSessionAndMayThrowConnectionException(any());
    verifyZeroInteractions(client);
  }

  private static NumericRowPreparedStatement prepare(TableSessionWrapper borrowed)
      throws Exception {
    return borrowed.prepareInsertNumericRow(
        "t",
        List.of("tag", "v"),
        List.of(TSDataType.STRING, TSDataType.INT64),
        List.of(ColumnCategory.TAG, ColumnCategory.FIELD));
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
