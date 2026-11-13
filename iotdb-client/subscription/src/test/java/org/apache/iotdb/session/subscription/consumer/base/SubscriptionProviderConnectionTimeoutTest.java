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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeNonCriticalException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestType;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHandshakeResp;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.SessionConnection;
import org.apache.iotdb.session.subscription.SubscriptionSessionConnection;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;

import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SubscriptionProviderConnectionTimeoutTest {

  private static final int READ_TIMEOUT_MS = 200;
  private static final int CONNECTION_TIMEOUT_MS = 5_000;

  @Test(timeout = 10_000)
  public void testCommitReadTimeoutClosesProviderTransport() throws Exception {
    final CountDownLatch commitStarted = new CountDownLatch(1);
    final CountDownLatch releaseCommit = new CountDownLatch(1);
    final AtomicInteger pipeSubscribeCount = new AtomicInteger();
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
                    case "getProperties":
                      return new ServerProperties(
                          "test", Arrays.asList("max_time", "min_time"), "ms", 1);
                    case "pipeSubscribe":
                      if (pipeSubscribeCount.getAndIncrement() == 0) {
                        return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
                            successStatus(), 1, "consumer", "group");
                      }
                      commitStarted.countDown();
                      releaseCommit.await(5, TimeUnit.SECONDS);
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
                .transportFactory(new org.apache.iotdb.rpc.TElasticFramedTransport.Factory()));
    final Thread serverThread = new Thread(server::serve, "subscription-provider-timeout-server");
    serverThread.setDaemon(true);
    serverThread.start();

    final TestSubscriptionProvider provider = new TestSubscriptionProvider(socket.getLocalPort());
    try {
      provider.handshake();
      final TTransport transport = getTransport(provider.getSessionConnection());
      Assert.assertTrue(transport.isOpen());
      Assert.assertTrue(provider.getSessionConnection().setTimeout(READ_TIMEOUT_MS));

      Assert.assertThrows(
          SubscriptionConnectionException.class,
          () ->
              provider.commit(
                  Collections.singletonList(
                      new SubscriptionCommitContext(1, 0, "topic", "group", 1L)),
                  false));

      Assert.assertTrue(commitStarted.await(1, TimeUnit.SECONDS));
      Assert.assertFalse(provider.isAvailable());
      Assert.assertFalse(transport.isOpen());
    } finally {
      provider.close();
      releaseCommit.countDown();
      server.stop();
      serverThread.join(2_000);
    }
  }

  @Test
  public void testMissingBrokerPollErrorMarksProviderUnavailable() throws Exception {
    assertPollError(
        "Subscription: no broker bound to consumer group [group]", true /* broker missing */);
  }

  @Test
  public void testMissingFileBrokerPollErrorMarksProviderUnavailable() throws Exception {
    assertPollError(
        "Subscription: pipe broker bound to consumer group [group] does not exist", true);
  }

  @Test
  public void testMissingTabletBrokerPollErrorMarksProviderUnavailable() throws Exception {
    assertPollError("Subscription: broker bound to consumer group [group] does not exist", true);
  }

  @Test
  public void testMissingTabletBrokerPollErrorFromChineseServerMarksProviderUnavailable()
      throws Exception {
    assertPollError("订阅：绑定到消费者组 [group] 的 Broker 不存在", true);
  }

  @Test
  public void testOtherPollErrorRemainsNonCritical() throws Exception {
    assertPollError("other poll error", false /* broker missing */);
  }

  @Test
  public void testNullPollErrorRemainsNonCritical() throws Exception {
    assertPollError(null, false /* broker missing */);
  }

  private void assertPollError(final String message, final boolean brokerMissing) throws Exception {
    final AtomicInteger subscriptionCloseCount = new AtomicInteger();
    final AtomicInteger sessionCloseCount = new AtomicInteger();
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
                    case "getProperties":
                      return new ServerProperties(
                          "test", Arrays.asList("max_time", "min_time"), "ms", 1);
                    case "pipeSubscribe":
                      final TPipeSubscribeReq request = (TPipeSubscribeReq) args[0];
                      if (request.getType() == PipeSubscribeRequestType.HANDSHAKE.getType()) {
                        return PipeSubscribeHandshakeResp.toTPipeSubscribeResp(
                            successStatus(), 1, "consumer", "group");
                      }
                      if (request.getType() != PipeSubscribeRequestType.POLL.getType()) {
                        if (request.getType() == PipeSubscribeRequestType.CLOSE.getType()) {
                          subscriptionCloseCount.incrementAndGet();
                        }
                        return new TPipeSubscribeResp(successStatus(), (byte) 1, (short) 6);
                      }
                      return new TPipeSubscribeResp(
                          new TSStatus(TSStatusCode.SUBSCRIPTION_POLL_ERROR.getStatusCode())
                              .setMessage(message),
                          (byte) 1,
                          (short) 6);
                    case "closeSession":
                      sessionCloseCount.incrementAndGet();
                      return successStatus();
                    default:
                      return defaultValue(method.getReturnType());
                  }
                });
    final TSimpleServer server =
        new TSimpleServer(
            new TSimpleServer.Args(serverSocket)
                .processor(new IClientRPCService.Processor<>(service))
                .transportFactory(new org.apache.iotdb.rpc.TElasticFramedTransport.Factory()));
    final Thread serverThread =
        new Thread(server::serve, "subscription-provider-poll-error-server");
    serverThread.setDaemon(true);
    serverThread.start();

    final TestSubscriptionProvider provider = new TestSubscriptionProvider(socket.getLocalPort());
    try {
      provider.handshake();
      final TTransport transport = getTransport(provider.getSessionConnection());
      final SubscriptionCommitContext context =
          new SubscriptionCommitContext(1, 0, "topic", "group", 1L);

      if (brokerMissing) {
        Assert.assertThrows(
            SubscriptionConnectionException.class,
            () -> provider.poll(Collections.singleton("topic"), 1_000L));
        Assert.assertFalse(provider.isAvailable());
        Assert.assertThrows(
            SubscriptionConnectionException.class, () -> provider.pollFile(context, 0L, 1_000L));
        Assert.assertThrows(
            SubscriptionConnectionException.class, () -> provider.pollTablets(context, 0, 1_000L));
        provider.closeSession();
        Assert.assertFalse(transport.isOpen());
        Assert.assertEquals(0, subscriptionCloseCount.get());
        Assert.assertEquals(0, sessionCloseCount.get());
      } else {
        Assert.assertThrows(
            SubscriptionRuntimeNonCriticalException.class,
            () -> provider.poll(Collections.singleton("topic"), 1_000L));
        Assert.assertTrue(provider.isAvailable());
        Assert.assertThrows(
            SubscriptionRuntimeNonCriticalException.class,
            () -> provider.pollFile(context, 0L, 1_000L));
        Assert.assertThrows(
            SubscriptionRuntimeNonCriticalException.class,
            () -> provider.pollTablets(context, 0, 1_000L));
      }
    } finally {
      provider.close();
      server.stop();
      serverThread.join(2_000);
    }
  }

  private static TTransport getTransport(final SubscriptionSessionConnection connection)
      throws Exception {
    final Field field = SessionConnection.class.getDeclaredField("transport");
    field.setAccessible(true);
    return (TTransport) field.get(connection);
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

  private static class TestSubscriptionProvider extends AbstractSubscriptionProvider {

    private TestSubscriptionProvider(final int port) {
      super(
          new TEndPoint("127.0.0.1", port),
          "user",
          "password",
          null,
          "consumer",
          "group",
          null,
          null,
          1_048_576,
          30_000L,
          CONNECTION_TIMEOUT_MS);
    }

    @Override
    protected AbstractSessionBuilder constructSubscriptionSessionBuilder(
        final String host,
        final int port,
        final String username,
        final String password,
        final String encryptedPassword,
        final int thriftMaxFrameSize,
        final int connectionTimeoutInMs) {
      final boolean useEncryptedPassword = Objects.nonNull(encryptedPassword);
      return new SubscriptionTreeSessionBuilder()
          .host(host)
          .port(port)
          .username(username)
          .password(useEncryptedPassword ? encryptedPassword : password)
          .useEncryptedPassword(useEncryptedPassword)
          .thriftMaxFrameSize(thriftMaxFrameSize)
          .connectionTimeoutInMs(connectionTimeoutInMs);
    }
  }
}
