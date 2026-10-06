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
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.net.ServerSocket;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A broken connection must fail with {@link IoTDBConnectionException}, not a NullPointerException,
 * so that SessionPool evicts the session instead of handing it out again.
 *
 * <p>Setup: a {@link SessionConnection} whose Thrift transport was closed (as {@code reconnect()}
 * does) and whose reconnect targets are unreachable. Every RPC fails with "Cannot write to null
 * outputStream", every reconnect fails, and {@code callWithRetryAndReconnect} runs out of retries.
 */
public class BrokenSessionConnectionTest {

  private static final String SQL = "select count(*) from root.sg.d1";

  /** Nothing listens here. Connecting is refused right away. */
  private TEndPoint deadEndPoint;

  private TTransport closedTransport;

  @Before
  public void setUp() throws Exception {
    // Open a real transport against a short-lived server, then close both. The transport ends up
    // in the same state as after SessionConnection.reconnect() closed it: its outputStream is null.
    try (ServerSocket server = new ServerSocket(0)) {
      deadEndPoint = new TEndPoint("127.0.0.1", server.getLocalPort());
      closedTransport =
          DeepCopyRpcTransportFactory.getInstance(
                  SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
                  SessionConfig.DEFAULT_MAX_FRAME_SIZE)
              .getTransport(deadEndPoint.getIp(), deadEndPoint.getPort(), 1000);
      closedTransport.open();
    }
    closedTransport.close();
  }

  @After
  public void tearDown() {
    if (closedTransport != null) {
      closedTransport.close();
    }
  }

  /**
   * This just checks if the test setup works as expected, meaning we have a TTransport with a null
   * outputStream.
   */
  @Test
  public void closedTransportFailsWithNullOutputStream() throws Exception {
    // Sanity check: the client fails with the same error as in the bug report.
    IClientRPCService.Iface client = newClient(closedTransport);
    try {
      client.executeQueryStatementV2(new TSExecuteStatementReq(0, SQL, 0));
      fail("expected TTransportException");
    } catch (TTransportException e) {
      assertEquals("Cannot write to null outputStream", e.getMessage());
    }
  }

  /**
   * Retries run out and {@code executeQueryStatement} should throw {@link IoTDBConnectionException}
   * with the last TException as the cause. On unfixed code it throws a NullPointerException from
   * {@code execResp.getStatus()}.
   */
  @Test
  public void exhaustedRetriesThrowConnectionExceptionInsteadOfNpe() throws Exception {
    SessionConnection connection = newBrokenConnection(newSession());

    try {
      connection.executeQueryStatement(SQL, 1000);
      fail("expected IoTDBConnectionException");
    } catch (IoTDBConnectionException e) {
      assertTrue(
          "cause should be the retained TException, was " + e.getCause(),
          e.getCause() instanceof TTransportException);
    } catch (NullPointerException e) {
      throw new AssertionError("exhausted retries surfaced as NPE, root cause was lost", e);
    }
  }

  /** The same bug on a TSStatus-returning path: the null status NPEs in RpcUtils.verifySuccess. */
  @Test
  public void exhaustedRetriesOnStatusPathThrowConnectionException() throws Exception {
    SessionConnection connection = newBrokenConnection(newSession());

    try {
      connection.setStorageGroup("root.sg");
      fail("expected IoTDBConnectionException");
    } catch (IoTDBConnectionException e) {
      // expected
    } catch (NullPointerException e) {
      throw new AssertionError("exhausted retries surfaced as NPE, root cause was lost", e);
    }
  }

  /**
   * A session whose connection fails after all retries now surfaces {@link
   * IoTDBConnectionException}, so SessionPool's existing eviction path removes it. Before, the NPE
   * took the {@code RuntimeException -> putBack} branch and the session was queued again.
   */
  @Test
  public void brokenConnectionLeadsToSessionEviction() throws Exception {
    Session brokenSession = newSession();
    Whitebox.setInternalState(
        brokenSession, "defaultSessionConnection", newBrokenConnection(brokenSession));

    SessionPool pool =
        new SessionPool.Builder()
            .nodeUrls(
                Collections.singletonList(deadEndPoint.getIp() + ":" + deadEndPoint.getPort()))
            .user("root")
            .password("root")
            .maxSize(1)
            .waitToGetSessionTimeoutInMs(1000)
            .enableAutoFetch(false)
            .build();
    try {
      ConcurrentLinkedDeque<ISession> queue = Whitebox.getInternalState(pool, "queue");
      queue.add(brokenSession);
      Whitebox.setInternalState(pool, "size", 1);

      // Each call fails, but with a connection error, and the broken session must not be reused.
      for (int call = 1; call <= 3; call++) {
        try {
          pool.executeQueryStatement(SQL);
          fail("call " + call + ": expected IoTDBConnectionException");
        } catch (IoTDBConnectionException e) {
          // expected: the server is unreachable
        } catch (NullPointerException e) {
          throw new AssertionError(
              "call "
                  + call
                  + ": the pool reused the broken session"
                  + (queue.contains(brokenSession) ? " (still queued)" : ""),
              e);
        }
        assertFalse(
            "call " + call + ": broken session was put back into the pool",
            queue.contains(brokenSession));
      }
    } finally {
      pool.close();
    }
  }

  private Session newSession() {
    return new Session.Builder()
        .nodeUrls(Collections.singletonList(deadEndPoint.getIp() + ":" + deadEndPoint.getPort()))
        .username("root")
        .password("root")
        .enableAutoFetch(false)
        .build();
  }

  /**
   * A connection whose transport was closed and whose reconnect targets are unreachable. Retry
   * interval is 1 ms to keep the test fast (the default is 500 ms with 11 attempts).
   */
  private SessionConnection newBrokenConnection(Session session) {
    SessionConnection connection = new SessionConnection(Session.TREE);
    Whitebox.setInternalState(connection, "session", session);
    Whitebox.setInternalState(connection, "transport", closedTransport);
    Whitebox.setInternalState(connection, "client", newClient(closedTransport));
    Whitebox.setInternalState(connection, "endPoint", deadEndPoint);
    Supplier<List<TEndPoint>> availableNodes = () -> Collections.singletonList(deadEndPoint);
    Whitebox.setInternalState(connection, "availableNodes", availableNodes);
    Whitebox.setInternalState(connection, "retryIntervalInMs", 1L);
    return connection;
  }

  private static IClientRPCService.Iface newClient(TTransport transport) {
    return RpcUtils.newSynchronizedClient(
        new IClientRPCService.Client(new TBinaryProtocol(transport)));
  }
}
