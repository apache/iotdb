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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.RpcThrottlingException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.PreparedNumericRowInfo;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertNumericRowStatement;
import org.apache.iotdb.db.storageengine.rescon.quotas.DataNodeThrottleQuotaManager;
import org.apache.iotdb.db.storageengine.rescon.quotas.OperationQuota;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecutePreparedNumericRowReq;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@PrepareForTest({
  ClientRPCServiceImpl.class,
  DataNodeThrottleQuotaManager.class,
  PreparedNumericRowInfo.class
})
public class ClientRPCServiceImplPreparedNumericRowTest {
  private ClientRPCServiceImpl service;
  private int originalDataNodeId;
  private Coordinator coordinator;
  private Coordinator originalCoordinator;
  private SessionManager originalSessionManager;
  private IClientSession session;
  private PreparedNumericRowInfo prepared;
  private InsertNumericRowStatement statement;
  private DataNodeThrottleQuotaManager quotaManager;
  private OperationQuota quota;
  private TSExecutePreparedNumericRowReq request;
  private TSStatus success;

  @Before
  public void setUp() throws Exception {
    // Coordinator initialization requires a DataNode ID even when query execution is mocked.
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    // Exercise the actual RPC method while isolating query execution and per-user quota state.
    service = mock(ClientRPCServiceImpl.class, CALLS_REAL_METHODS);
    originalCoordinator = Whitebox.getInternalState(ClientRPCServiceImpl.class, "COORDINATOR");
    originalSessionManager =
        Whitebox.getInternalState(ClientRPCServiceImpl.class, "SESSION_MANAGER");
    coordinator = mock(Coordinator.class);
    SessionManager sessions = mock(SessionManager.class);
    Whitebox.setInternalState(ClientRPCServiceImpl.class, "COORDINATOR", coordinator);
    Whitebox.setInternalState(ClientRPCServiceImpl.class, "SESSION_MANAGER", sessions);
    session = mock(IClientSession.class);
    prepared = PowerMockito.mock(PreparedNumericRowInfo.class);
    statement = new InsertNumericRowStatement();
    quotaManager = mock(DataNodeThrottleQuotaManager.class);
    quota = mock(OperationQuota.class);
    PowerMockito.mockStatic(DataNodeThrottleQuotaManager.class);
    when(DataNodeThrottleQuotaManager.getInstance()).thenReturn(quotaManager);
    when(sessions.getCurrSessionAndUpdateIdleTime()).thenReturn(session);
    when(sessions.checkLogin(session)).thenReturn(true);
    when(session.getId()).thenReturn(11L);
    when(session.getUsername()).thenReturn("quota-user");
    when(session.getPreparedNumericRow(101L)).thenReturn(prepared);
    when(prepared.bind(anyLong(), any(), any(), any())).thenReturn(statement);
    when(quotaManager.checkQuota("quota-user", statement)).thenReturn(quota);
    success = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    when(coordinator.executeForTableModel(
            same(statement), any(), same(session), anyLong(), any(), eq(""), any(), anyLong()))
        .thenReturn(new ExecutionResult(new QueryId("prepared_quota"), success));
    request =
        new TSExecutePreparedNumericRowReq(
            11L, 101L, 1L, ByteBuffer.allocate(0), ByteBuffer.allocate(0));
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
    Whitebox.setInternalState(ClientRPCServiceImpl.class, "COORDINATOR", originalCoordinator);
    Whitebox.setInternalState(
        ClientRPCServiceImpl.class, "SESSION_MANAGER", originalSessionManager);
  }

  @Test
  public void testChecksQuotaBeforeExecutionAndClosesOnSuccess() throws Exception {
    assertSame(success, service.executePreparedNumericRow(request));
    InOrder order = inOrder(quotaManager, coordinator, quota);
    order.verify(quotaManager).checkQuota("quota-user", statement);
    order
        .verify(coordinator)
        .executeForTableModel(
            same(statement), any(), same(session), anyLong(), any(), eq(""), any(), anyLong());
    order.verify(quota).close();
    verify(session, never()).removePreparedNumericRow(anyLong());
  }

  @Test
  public void testQuotaRejectionDoesNotExecuteOrInvalidatePreparedRow() throws Exception {
    int code = TSStatusCode.NUM_WRITE_REQUESTS_EXCEEDED.getStatusCode();
    when(quotaManager.checkQuota("quota-user", statement))
        .thenThrow(new RpcThrottlingException("write request quota exceeded", code));
    TSStatus status = service.executePreparedNumericRow(request);
    assertEquals(code, status.getCode());
    // Standard RPC handling adds an operation prefix while retaining the quota failure detail.
    assertTrue(status.getMessage().endsWith("write request quota exceeded"));
    verifyZeroInteractions(coordinator, quota);
    verify(session, never()).removePreparedNumericRow(anyLong());
  }

  @Test
  public void testExecutionExceptionClosesQuota() throws Exception {
    when(coordinator.executeForTableModel(
            same(statement), any(), same(session), anyLong(), any(), eq(""), any(), anyLong()))
        .thenThrow(new IllegalStateException("executor failure"));
    TSStatus status = service.executePreparedNumericRow(request);
    assertNotEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verify(quota).close();
    verify(session, never()).removePreparedNumericRow(anyLong());
  }

  @Test
  public void testSchemaFailureClosesQuotaAndRemovesPreparedRow() throws Exception {
    TSStatus schemaFailure = new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode());
    when(coordinator.executeForTableModel(
            same(statement), any(), same(session), anyLong(), any(), eq(""), any(), anyLong()))
        .thenAnswer(
            invocation -> {
              statement.markTableSchemaValidationFailed();
              return new ExecutionResult(new QueryId("prepared_quota"), schemaFailure);
            });
    assertSame(schemaFailure, service.executePreparedNumericRow(request));
    verify(quotaManager).checkQuota("quota-user", statement);
    verify(quota).close();
    verify(session).removePreparedNumericRow(101L);
  }

  @Test
  public void testInvalidPayloadDoesNotConsumeQuota() throws Exception {
    when(prepared.bind(anyLong(), any(), any(), any()))
        .thenThrow(new SemanticException("invalid numeric payload"));
    TSStatus status = service.executePreparedNumericRow(request);
    assertNotEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verifyZeroInteractions(quotaManager, coordinator, quota);
    verify(session, never()).removePreparedNumericRow(anyLong());
  }
}
