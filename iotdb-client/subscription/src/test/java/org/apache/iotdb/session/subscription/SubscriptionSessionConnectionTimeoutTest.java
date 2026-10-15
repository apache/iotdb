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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TElasticFramedTransport;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeCloseReq;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.thrift.TException;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SubscriptionSessionConnectionTimeoutTest {

  private static final int READ_TIMEOUT_MS = 200;

  @Test(timeout = 10_000)
  public void testPipeSubscribeReadTimeout() throws Exception {
    final CountDownLatch pipeSubscribeStarted = new CountDownLatch(1);
    final CountDownLatch releasePipeSubscribe = new CountDownLatch(1);
    final ServerSocket socket = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
    final TServerSocket serverSocket = new TServerSocket(socket);
    final IClientRPCService.Iface service =
        (IClientRPCService.Iface)
            Proxy.newProxyInstance(
                IClientRPCService.Iface.class.getClassLoader(),
                new Class<?>[] {IClientRPCService.Iface.class},
                (proxy, method, args) -> {
                  switch (method.getName()) {
                    case "openSession":
                      return new TSOpenSessionResp(
                              successStatus(), TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3)
                          .setSessionId(1L);
                    case "requestStatementId":
                      return 1L;
                    case "pipeSubscribe":
                      pipeSubscribeStarted.countDown();
                      releasePipeSubscribe.await(5, TimeUnit.SECONDS);
                      return new TPipeSubscribeResp(successStatus(), (byte) 1, (short) 6);
                    case "closeSession":
                      return successStatus();
                    default:
                      return defaultValue(method.getReturnType());
                  }
                });
    final TSimpleServer server =
        new TSimpleServer(
            new TSimpleServer.Args(serverSocket)
                .processor(new IClientRPCService.Processor<>(service))
                .transportFactory(new TElasticFramedTransport.Factory()));
    final Thread serverThread = new Thread(server::serve, "subscription-timeout-test-server");
    serverThread.setDaemon(true);
    serverThread.start();

    final SubscriptionSessionWrapper session =
        new SubscriptionSessionWrapper(
            new SubscriptionTreeSessionBuilder()
                .host("127.0.0.1")
                .port(socket.getLocalPort())
                .connectionTimeoutInMs(0));
    SubscriptionSessionConnection connection = null;
    try {
      session.open();
      connection = session.getSessionConnection();
      Assert.assertTrue(connection.setTimeout(READ_TIMEOUT_MS));

      final long startNanos = System.nanoTime();
      try {
        connection.pipeSubscribe(PipeSubscribeCloseReq.toTPipeSubscribeReq());
        Assert.fail("Expected the unresponsive pipeSubscribe RPC to time out");
      } catch (final TException expected) {
        final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        Assert.assertTrue("RPC did not honor the read timeout: " + elapsedMs, elapsedMs < 3_000);
      }
      Assert.assertTrue(pipeSubscribeStarted.await(1, TimeUnit.SECONDS));
    } finally {
      if (connection != null) {
        connection.forceClose();
      }
      session.close();
      releasePipeSubscribe.countDown();
      server.stop();
      serverThread.join(2_000);
    }
  }

  private static TSStatus successStatus() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static Object defaultValue(final Class<?> returnType) {
    if (!returnType.isPrimitive() || returnType == Void.TYPE) {
      return null;
    }
    if (returnType == Boolean.TYPE) {
      return false;
    }
    if (returnType == Character.TYPE) {
      return '\0';
    }
    if (returnType == Byte.TYPE) {
      return (byte) 0;
    }
    if (returnType == Short.TYPE) {
      return (short) 0;
    }
    if (returnType == Integer.TYPE) {
      return 0;
    }
    if (returnType == Long.TYPE) {
      return 0L;
    }
    if (returnType == Float.TYPE) {
      return 0.0F;
    }
    return 0.0D;
  }
}
