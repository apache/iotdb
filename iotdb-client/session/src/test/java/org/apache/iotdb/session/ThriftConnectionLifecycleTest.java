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
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.time.ZoneId;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ThriftConnectionLifecycleTest {
  @Test
  public void testAbortWhileTcpConnectIsBlocked() throws Exception {
    assertTcpConnectAborted(false);
  }

  @Test
  public void testAbortBeforeTcpConnectCannotReopenSocket() throws Exception {
    assertTcpConnectAborted(true);
  }

  private void assertTcpConnectAborted(boolean beforeConnect) throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch proceed = new CountDownLatch(beforeConnect ? 1 : 0);
    CountDownLatch closed = new CountDownLatch(1);
    Socket socket = mock(Socket.class);
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(socket)
        .close();
    doAnswer(
            invocation -> {
              entered.countDown();
              assertTrue(closed.await(5, TimeUnit.SECONDS));
              throw new SocketException("Connection cancelled by test");
            })
        .when(socket)
        .connect(any(SocketAddress.class), anyInt());
    TSocket raw = new TSocket("127.0.0.1", 1, 0);
    raw.close();
    Whitebox.setInternalState(raw, "socket_", socket);
    TTransport prepared =
        new TElasticFramedTransport(
            raw,
            SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
            SessionConfig.DEFAULT_MAX_FRAME_SIZE,
            true) {
          @Override
          public void open() throws TTransportException {
            if (beforeConnect) {
              entered.countDown();
              try {
                assertTrue(proceed.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TTransportException(e);
              }
            }
            super.open();
          }
        };
    ThriftConnection connection = connectionReturning(prepared, null, null);
    ExecutorService tasks = Executors.newCachedThreadPool();
    try {
      Future<?> initialization =
          tasks.submit(
              () -> {
                initialize(connection);
                return null;
              });
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      tasks.submit(connection::abort).get(5, TimeUnit.SECONDS);
      assertTrue(closed.await(5, TimeUnit.SECONDS));
      if (beforeConnect) {
        // TSocket.close() would null this field and allow open() to create another socket.
        assertSame(socket, raw.getSocket());
      }
      proceed.countDown();
      assertConnectionFailure(initialization);
      verify(socket).connect(any(SocketAddress.class), anyInt());
    } finally {
      closed.countDown();
      proceed.countDown();
      tasks.shutdownNow();
      assertTrue(tasks.awaitTermination(5, TimeUnit.SECONDS));
      connection.close();
    }
  }

  @Test
  public void testTransportCreatedAfterClosureIsDisposedWithoutOpening() throws Exception {
    CountDownLatch creating = new CountDownLatch(1);
    CountDownLatch publish = new CountDownLatch(1);
    TTransport prepared = mock(TTransport.class);
    ThriftConnection connection = connectionReturning(prepared, creating, publish);
    ExecutorService tasks = Executors.newCachedThreadPool();
    try {
      Future<?> initialization =
          tasks.submit(
              () -> {
                initialize(connection);
                return null;
              });
      assertTrue(creating.await(5, TimeUnit.SECONDS));
      tasks.submit(connection::close).get(5, TimeUnit.SECONDS);
      publish.countDown();
      assertConnectionFailure(initialization);
      verify(prepared).close();
      verify(prepared, never()).open();
    } finally {
      publish.countDown();
      tasks.shutdownNow();
      assertTrue(tasks.awaitTermination(5, TimeUnit.SECONDS));
      connection.close();
    }
  }

  private ThriftConnection connectionReturning(
      TTransport prepared, CountDownLatch creating, CountDownLatch publish) {
    return new ThriftConnection(
        new TEndPoint("127.0.0.1", 1),
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        0) {
      @Override
      protected TTransport createTransport(
          boolean ssl,
          String trustStore,
          String trustStorePwd,
          String keyStore,
          String keyStorePwd,
          String protocol)
          throws TTransportException {
        if (creating != null) {
          creating.countDown();
          try {
            assertTrue(publish.await(5, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TTransportException(e);
          }
        }
        return prepared;
      }
    };
  }

  private void initialize(ThriftConnection connection) throws IoTDBConnectionException {
    connection.init(
        false, null, null, null, null, "TLS", "root", "root", false, ZoneId.of("UTC"), "V_1_0");
  }

  private void assertConnectionFailure(Future<?> initialization) throws Exception {
    try {
      initialization.get(5, TimeUnit.SECONDS);
      fail("A cancelled initialization must not succeed");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof IoTDBConnectionException);
    }
  }
}
