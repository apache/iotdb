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

package org.apache.iotdb.db.protocol.session;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.net.Socket;
import java.util.Map;
import java.util.Optional;

public class QueryOwnershipTest {

  private static final long STATEMENT_ID = 1L;
  private static final long QUERY_ID = 2L;

  private static int previousDataNodeId;

  @BeforeClass
  public static void setUp() {
    // the coordinator builds its query id generator from the data node id of this node
    previousDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @AfterClass
  public static void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
  }

  @Test
  public void testQueryIdsAreBoundToTheSessionThatSubmittedTheQuery() {
    ClientSession owner = createSession("user");
    owner.addStatementId(STATEMENT_ID);
    owner.addQueryId(STATEMENT_ID, QUERY_ID);

    ClientSession anotherSession = createSession("user");
    anotherSession.addStatementId(STATEMENT_ID);

    Assert.assertTrue(owner.containsQueryId(STATEMENT_ID, QUERY_ID));
    // clients that do not send a statement id together with the query id are still served
    Assert.assertTrue(owner.containsQueryId(null, QUERY_ID));
    Assert.assertFalse(anotherSession.containsQueryId(STATEMENT_ID, QUERY_ID));
    Assert.assertFalse(anotherSession.containsQueryId(null, QUERY_ID));
    Assert.assertFalse(owner.containsQueryId(STATEMENT_ID + 1, QUERY_ID));
  }

  @Test
  public void testFetchResultsRejectsQueryOfAnotherSession() throws Exception {
    ClientSession anotherSession = createSession("user");
    anotherSession.addStatementId(STATEMENT_ID);
    anotherSession.setLogin(true);

    Map<Long, IQueryExecution> queryExecutionMap = getQueryExecutionMap();
    queryExecutionMap.put(QUERY_ID, mockQueryExecution());
    try {
      withCurrentSession(
          anotherSession,
          () -> {
            ClientRPCServiceImpl service = new ClientRPCServiceImpl();
            TSFetchResultsReq request = createFetchResultsReq(anotherSession);
            Assert.assertEquals(
                TSStatusCode.NO_PERMISSION.getStatusCode(),
                service.fetchResults(request).getStatus().getCode());
            Assert.assertEquals(
                TSStatusCode.NO_PERMISSION.getStatusCode(),
                service.fetchResultsV2(request).getStatus().getCode());
          });
      // the rejected requests must not release the query of the session that submitted it
      Assert.assertTrue(queryExecutionMap.containsKey(QUERY_ID));
    } finally {
      queryExecutionMap.remove(QUERY_ID);
    }
  }

  @Test
  public void testFetchResultsOfOwnQueryIsStillServed() throws Exception {
    ClientSession owner = createSession("user");
    owner.addStatementId(STATEMENT_ID);
    owner.addQueryId(STATEMENT_ID, QUERY_ID);
    owner.setLogin(true);

    IQueryExecution queryExecution = mockQueryExecution();
    Map<Long, IQueryExecution> queryExecutionMap = getQueryExecutionMap();
    queryExecutionMap.put(QUERY_ID, queryExecution);
    try {
      withCurrentSession(
          owner,
          () ->
              Assert.assertEquals(
                  TSStatusCode.SUCCESS_STATUS.getStatusCode(),
                  new ClientRPCServiceImpl()
                      .fetchResultsV2(createFetchResultsReq(owner))
                      .getStatus()
                      .getCode()));
      // a fully consumed query is released, and the client closes it afterwards
      Assert.assertFalse(queryExecutionMap.containsKey(QUERY_ID));
      Assert.assertFalse(owner.containsQueryId(STATEMENT_ID, QUERY_ID));
    } finally {
      queryExecutionMap.remove(QUERY_ID);
    }
  }

  @Test
  public void testFetchResultsOfQueryWithMoreDataKeepsQueryAndSessionBinding() throws Exception {
    ClientSession owner = createSession("user");
    owner.addStatementId(STATEMENT_ID);
    owner.addQueryId(STATEMENT_ID, QUERY_ID);
    owner.setLogin(true);

    Map<Long, IQueryExecution> queryExecutionMap = getQueryExecutionMap();
    queryExecutionMap.put(QUERY_ID, mockQueryExecution(true));
    try {
      withCurrentSession(
          owner,
          () -> {
            TSFetchResultsResp response =
                new ClientRPCServiceImpl().fetchResultsV2(createFetchResultsReq(owner));
            Assert.assertEquals(
                TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
            Assert.assertTrue(response.isMoreData());
          });
      // the result set is not consumed yet, so the query has to stay fetchable by its owner
      Assert.assertTrue(queryExecutionMap.containsKey(QUERY_ID));
      Assert.assertTrue(owner.containsQueryId(STATEMENT_ID, QUERY_ID));
    } finally {
      queryExecutionMap.remove(QUERY_ID);
    }
  }

  @Test
  public void testCloseOperationRejectsQueryOfAnotherSession() throws Exception {
    ClientSession anotherSession = createSession("user");
    anotherSession.addStatementId(STATEMENT_ID);
    anotherSession.setLogin(true);

    Map<Long, IQueryExecution> queryExecutionMap = getQueryExecutionMap();
    queryExecutionMap.put(QUERY_ID, mockQueryExecution());
    try {
      withCurrentSession(
          anotherSession,
          () ->
              Assert.assertEquals(
                  TSStatusCode.NO_PERMISSION.getStatusCode(),
                  new ClientRPCServiceImpl().closeOperation(createCloseOperationReq()).getCode()));
      // the rejected request must not release the query of the session that submitted it
      Assert.assertTrue(queryExecutionMap.containsKey(QUERY_ID));
    } finally {
      queryExecutionMap.remove(QUERY_ID);
    }
  }

  @Test
  public void testCloseOperationReleasesQueryOfOwnSession() throws Exception {
    ClientSession owner = createSession("user");
    owner.addStatementId(STATEMENT_ID);
    owner.addQueryId(STATEMENT_ID, QUERY_ID);
    owner.setLogin(true);

    Map<Long, IQueryExecution> queryExecutionMap = getQueryExecutionMap();
    queryExecutionMap.put(QUERY_ID, mockQueryExecution());
    try {
      withCurrentSession(
          owner,
          () ->
              Assert.assertEquals(
                  TSStatusCode.SUCCESS_STATUS.getStatusCode(),
                  new ClientRPCServiceImpl().closeOperation(createCloseOperationReq()).getCode()));
      Assert.assertFalse(queryExecutionMap.containsKey(QUERY_ID));
      Assert.assertFalse(owner.containsQueryId(STATEMENT_ID, QUERY_ID));
    } finally {
      queryExecutionMap.remove(QUERY_ID);
    }
  }

  @Test
  public void testCloseOperationOfQueryThatIsNoLongerRunningStaysANoOp() {
    // a client that consumed a result set completely sends closeOperation after the query has
    // already been released, and that request has to succeed as it always did
    ClientSession session = createSession("user");
    session.addStatementId(STATEMENT_ID);
    session.setLogin(true);

    withCurrentSession(
        session,
        () ->
            Assert.assertEquals(
                TSStatusCode.SUCCESS_STATUS.getStatusCode(),
                new ClientRPCServiceImpl().closeOperation(createCloseOperationReq()).getCode()));
  }

  private IQueryExecution mockQueryExecution() throws Exception {
    return mockQueryExecution(false);
  }

  /**
   * A mocked execution. {@code hasNextResult} says whether the mocked query still holds data after
   * the batch that is about to be read, which decides if a single fetch consumes the whole result
   * set and therefore releases the query.
   */
  private IQueryExecution mockQueryExecution(boolean hasNextResult) throws Exception {
    IQueryExecution queryExecution = Mockito.mock(IQueryExecution.class);
    Mockito.when(queryExecution.getQueryId()).thenReturn("query");
    // the mock carries no buffered batch, only the "is there anything left" flag
    Mockito.when(queryExecution.getByteBufferBatchResult()).thenReturn(Optional.empty());
    Mockito.when(queryExecution.hasNextResult()).thenReturn(hasNextResult);
    return queryExecution;
  }

  private TSFetchResultsReq createFetchResultsReq(ClientSession session) {
    return new TSFetchResultsReq(session.getId(), "select 1", 1024, QUERY_ID, true)
        .setStatementId(STATEMENT_ID);
  }

  private TSCloseOperationReq createCloseOperationReq() {
    return new TSCloseOperationReq().setStatementId(STATEMENT_ID).setQueryId(QUERY_ID);
  }

  private void withCurrentSession(ClientSession session, Runnable body) {
    SessionManager sessionManager = SessionManager.getInstance();
    IClientSession previousSession = sessionManager.getCurrSession();
    sessionManager.setCurrSession(session);
    try {
      body.run();
    } finally {
      sessionManager.restoreSession(previousSession, session);
    }
  }

  private ClientSession createSession(String username) {
    ClientSession session = new ClientSession(Mockito.mock(Socket.class));
    session.setUsername(username);
    return session;
  }

  @SuppressWarnings("unchecked")
  private Map<Long, IQueryExecution> getQueryExecutionMap() throws Exception {
    Field field = Coordinator.class.getDeclaredField("queryExecutionMap");
    field.setAccessible(true);
    return (Map<Long, IQueryExecution>) field.get(Coordinator.getInstance());
  }
}
