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
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.RpcServiceConstants;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.net.ServerSocket;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalSessionProtocolTest {

  private final AtomicInteger openSessionCalls = new AtomicInteger();
  private TestServer server;

  @Before
  public void setUp() throws Exception {
    server = new TestServer(openSessionCalls);
  }

  @After
  public void tearDown() throws Exception {
    server.close();
  }

  @Test
  public void testReconnectRetainsMultiplexedClientServiceName() throws Exception {
    final INodeSupplier nodeSupplier =
        new DummyNodesSupplier(Collections.singletonList(server.endpoint));
    try (InternalSession session =
        new Session.Builder()
            .host(server.endpoint.getIp())
            .port(server.endpoint.getPort())
            .username("root")
            .buildInternal(nodeSupplier)) {
      session.open();

      final SessionConnection connection =
          Whitebox.getInternalState(session, "defaultSessionConnection");
      assertTrue(Whitebox.invokeMethod(connection, "reconnect"));
      assertEquals(2, openSessionCalls.get());
    }
  }

  private static final class TestServer implements AutoCloseable {
    private final TEndPoint endpoint;
    private final TServerSocket socket;
    private final TThreadPoolServer server;
    private final ExecutorService workers = Executors.newCachedThreadPool();
    private final Thread thread;

    private TestServer(final AtomicInteger openSessionCalls) throws Exception {
      final ServerSocket listener = new ServerSocket(0);
      endpoint = new TEndPoint("127.0.0.1", listener.getLocalPort());
      socket = new TServerSocket(listener);

      final IClientRPCService.Iface handler = mock(IClientRPCService.Iface.class);
      when(handler.openSession(any(TSOpenSessionReq.class)))
          .thenAnswer(
              invocation -> {
                openSessionCalls.incrementAndGet();
                return new TSOpenSessionResp(
                        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
                        TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3)
                    .setSessionId(1);
              });
      when(handler.closeSession(any(TSCloseSessionReq.class)))
          .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

      final TMultiplexedProcessor processor = new TMultiplexedProcessor();
      processor.registerProcessor(
          RpcServiceConstants.INTERNAL_CLIENT_RPC_SERVICE,
          new IClientRPCService.Processor<>(handler));

      server =
          new TThreadPoolServer(
              new TThreadPoolServer.Args(socket)
                  .processor(processor)
                  .protocolFactory(new TBinaryProtocol.Factory())
                  .transportFactory(DeepCopyRpcTransportFactory.INSTANCE)
                  .executorService(workers));
      final CountDownLatch ready = new CountDownLatch(1);
      thread =
          new Thread(
              () -> {
                ready.countDown();
                server.serve();
              },
              "internal-session-protocol-test-server");
      thread.setDaemon(true);
      thread.start();
      assertTrue(ready.await(5, TimeUnit.SECONDS));
    }

    @Override
    public void close() throws Exception {
      server.stop();
      socket.close();
      workers.shutdownNow();
      thread.join(5_000);
    }
  }
}
