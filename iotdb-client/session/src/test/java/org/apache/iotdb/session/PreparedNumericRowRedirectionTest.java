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
package org.apache.iotdb.session;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.isession.NumericRowPreparedStatement;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSExecutePreparedNumericRowReq;
import org.apache.iotdb.service.rpc.thrift.TSPrepareNumericRowResp;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.powermock.reflect.Whitebox;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PreparedNumericRowRedirectionTest {
  private final TEndPoint leader = new TEndPoint("127.0.0.1", 6668);
  private Session session;
  private TableSession tableSession;
  private SessionConnection initial;
  private SessionConnection destination;
  private IClientRPCService.Iface initialClient;
  private IClientRPCService.Iface destinationClient;

  @Before
  public void setUp() throws Exception {
    session = new Session.Builder().enableRedirection(true).enableAutoFetch(false).build();
    initialClient = mock(IClientRPCService.Iface.class);
    destinationClient = mock(IClientRPCService.Iface.class);
    initial = connection(initialClient, 11);
    destination = connection(destinationClient, 22);
    session.setDefaultSessionConnection(initial);
    session.tableModelDeviceIdToEndpoint = new HashMap<>();
    session.endPointToSessionConnection = new HashMap<>();
    session.endPointToSessionConnection.put(leader, destination);
    tableSession = new TableSession(session);
    when(initialClient.prepareNumericRow(any()))
        .thenReturn(new TSPrepareNumericRowResp(success()).setPreparedId(101));
    when(destinationClient.prepareNumericRow(any()))
        .thenReturn(new TSPrepareNumericRowResp(success()).setPreparedId(202));
    when(initialClient.executePreparedNumericRow(any()))
        .thenReturn(
            new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
                .setRedirectNode(leader));
    when(destinationClient.executePreparedNumericRow(any())).thenReturn(success());
    when(initialClient.closePreparedNumericRow(anyLong(), anyLong())).thenReturn(success());
    when(destinationClient.closePreparedNumericRow(anyLong(), anyLong())).thenReturn(success());
  }

  // Recommendations acknowledge this row; later rows prepare once on the leader with intact nulls.
  @Test
  public void testRedirectPreparesPerSessionWithoutReplaying() throws Exception {
    NumericRowPreparedStatement prepared = prepare();
    write(prepared, 1, "a");
    verify(destinationClient, never()).executePreparedNumericRow(any());
    write(prepared, 2, "a");
    write(prepared, 3, "a");
    // A different device has no route yet, so it still uses the original handle.
    write(prepared, 4, "b");
    verify(initialClient, times(2)).executePreparedNumericRow(any());
    verify(destinationClient, times(1)).prepareNumericRow(any());
    ArgumentCaptor<TSExecutePreparedNumericRowReq> requests =
        ArgumentCaptor.forClass(TSExecutePreparedNumericRowReq.class);
    verify(destinationClient, times(2)).executePreparedNumericRow(requests.capture());
    for (TSExecutePreparedNumericRowReq request : requests.getAllValues()) {
      assertEquals(22, request.getSessionId());
      assertEquals(202, request.getPreparedId());
      assertEquals(2, request.bufferForNullBitmap().get());
      assertEquals(8, request.bufferForValues().remaining());
      assertEquals(42, request.bufferForValues().getLong());
      assertEquals("a", ReadWriteIOUtils.readString(request.bufferForTagValues()));
    }
    prepared.close();
    prepared.close();
    verify(initialClient).closePreparedNumericRow(11, 101);
    verify(destinationClient).closePreparedNumericRow(22, 202);
  }

  // Disabling redirection ignores recommendations; USE must not prepare an old row in a new DB.
  @Test
  public void testDisabledRedirectionAndDatabaseChange() throws Exception {
    try (NumericRowPreparedStatement prepared = prepare()) {
      session.setEnableRedirection(false);
      write(prepared, 1, "a");
      write(prepared, 2, "a");
      assertTrue(session.tableModelDeviceIdToEndpoint.isEmpty());
      session.setEnableRedirection(true);
      write(prepared, 3, "a");
      session.changeDatabase("another_database");
      write(prepared, 4, "a");
      verify(initialClient, times(4)).executePreparedNumericRow(any());
      verify(destinationClient, never()).prepareNumericRow(any());
    }
  }

  // Reconnecting a destination requires a new ID; reconnecting the owner invalidates the statement.
  @Test
  public void testReconnectedSessionsNeverReuseOldIds() throws Exception {
    NumericRowPreparedStatement prepared = prepare();
    write(prepared, 1, "a");
    write(prepared, 2, "a");
    Whitebox.setInternalState(destination, "sessionId", 23L);
    when(destinationClient.prepareNumericRow(any()))
        .thenReturn(new TSPrepareNumericRowResp(success()).setPreparedId(303));
    write(prepared, 3, "a");
    assertEquals(Long.valueOf(303), prepared.getPreparedId(destination, 23));
    verify(destinationClient, times(2)).prepareNumericRow(any());
    Whitebox.setInternalState(initial, "sessionId", 12L);
    assertThrows(IoTDBConnectionException.class, () -> write(prepared, 4, "a"));
    prepared.close();
    verify(initialClient, never()).closePreparedNumericRow(anyLong(), anyLong());
    verify(destinationClient).closePreparedNumericRow(23, 303);
    verify(destinationClient, never()).closePreparedNumericRow(22, 202);
  }

  // A failed redirected execution is propagated once, and all handles close on a server error.
  @Test
  public void testFailureDoesNotReplayOrReviveStatement() throws Exception {
    NumericRowPreparedStatement prepared = prepare();
    write(prepared, 1, "a");
    when(destinationClient.executePreparedNumericRow(any()))
        .thenThrow(new TException("lost response"));
    assertThrows(IoTDBConnectionException.class, () -> write(prepared, 2, "a"));
    verify(initialClient).executePreparedNumericRow(any());
    verify(destinationClient).executePreparedNumericRow(any());
    assertFalse(prepared.isClosed());
    doReturn(
            new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                .setMessage("schema error"))
        .when(destinationClient)
        .executePreparedNumericRow(any());
    assertThrows(StatementExecutionException.class, () -> write(prepared, 3, "a"));
    assertTrue(prepared.isClosed());
    verify(initialClient).closePreparedNumericRow(11, 101);
    verify(destinationClient).closePreparedNumericRow(22, 202);
    assertThrows(IoTDBConnectionException.class, () -> write(prepared, 4, "b"));
    verify(initialClient).executePreparedNumericRow(any());
  }

  // A preexisting device route is used immediately, including a device with a null TAG.
  @Test
  public void testExistingRouteAndNullTag() throws Exception {
    session.tableModelDeviceIdToEndpoint.put(
        IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"t", null}), leader);
    try (NumericRowPreparedStatement prepared = prepare()) {
      write(prepared, 1, null);
      verify(initialClient, never()).executePreparedNumericRow(any());
      verify(destinationClient).executePreparedNumericRow(any());
    }
  }

  private NumericRowPreparedStatement prepare() throws Exception {
    return tableSession.prepareInsertNumericRow(
        "t",
        List.of("tag", "v", "n"),
        List.of(TSDataType.STRING, TSDataType.INT64, TSDataType.INT64),
        List.of(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD));
  }

  private void write(NumericRowPreparedStatement prepared, long time, String tag) throws Exception {
    BitMap nulls = new BitMap(2);
    nulls.mark(1);
    tableSession.executePreparedInsertNumericRow(
        prepared, time, new long[] {42, 99}, new String[] {tag}, nulls);
  }

  private static SessionConnection connection(IClientRPCService.Iface client, long sessionId) {
    SessionConnection connection = new SessionConnection("table");
    Whitebox.setInternalState(connection, "client", client);
    Whitebox.setInternalState(connection, "sessionId", sessionId);
    return connection;
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
