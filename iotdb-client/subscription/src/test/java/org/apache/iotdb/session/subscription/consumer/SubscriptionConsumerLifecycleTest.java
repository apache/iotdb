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

package org.apache.iotdb.session.subscription.consumer;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.SubscriptionSessionConnection;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionConsumerLifecycleTest {

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 6667;
  private static final String CONSUMER_ID = "test_consumer";
  private static final String CONSUMER_GROUP_ID = "test_consumer_group";
  private static final long LONG_INTERVAL_MS = 86_400_000L;
  private static final int THRIFT_MAX_FRAME_SIZE = 1024;
  private static final int CONNECTION_TIMEOUT_MS = 1000;

  @Test
  public void testPushConsumerIsOpenBeforeProviderHandshake() {
    final TestPushConsumer consumer = new TestPushConsumer();

    try {
      consumer.open();
      Assert.fail("Expected provider creation failure.");
    } catch (final SubscriptionException e) {
      Assert.assertTrue(e instanceof SubscriptionConnectionException);
      Assert.assertEquals(1, consumer.closedStatesDuringHandshake.size());
      Assert.assertFalse(consumer.closedStatesDuringHandshake.get(0));
      Assert.assertTrue(consumer.isClosed());
    }
  }

  @Test
  public void testPushConsumerIsClosedBeforeProviderClose() throws Exception {
    final TestPushConsumer consumer = new TestPushConsumer();
    final List<Boolean> closedStatesDuringClose = new ArrayList<>();

    prepareOpenedConsumerWithProvider(
        consumer, SubscriptionPushConsumer.class, closedStatesDuringClose);
    consumer.close();

    Assert.assertEquals(1, closedStatesDuringClose.size());
    Assert.assertTrue(closedStatesDuringClose.get(0));
  }

  @Test
  public void testPullConsumerIsOpenBeforeProviderHandshake() {
    final TestPullConsumer consumer = new TestPullConsumer();

    try {
      consumer.open();
      Assert.fail("Expected provider creation failure.");
    } catch (final SubscriptionException e) {
      Assert.assertTrue(e instanceof SubscriptionConnectionException);
      Assert.assertEquals(1, consumer.closedStatesDuringHandshake.size());
      Assert.assertFalse(consumer.closedStatesDuringHandshake.get(0));
      Assert.assertTrue(consumer.isClosed());
    }
  }

  @Test
  public void testPullConsumerIsClosedBeforeProviderClose() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer();
    final List<Boolean> closedStatesDuringClose = new ArrayList<>();

    prepareOpenedConsumerWithProvider(
        consumer, SubscriptionPullConsumer.class, closedStatesDuringClose);
    consumer.close();

    Assert.assertEquals(1, closedStatesDuringClose.size());
    Assert.assertTrue(closedStatesDuringClose.get(0));
  }

  private static void prepareOpenedConsumerWithProvider(
      final SubscriptionConsumer consumer,
      final Class<?> concreteConsumerClass,
      final List<Boolean> closedStatesDuringClose)
      throws Exception {
    setAtomicBooleanField(concreteConsumerClass, consumer, "isClosed", false);
    setAtomicBooleanField(SubscriptionConsumer.class, consumer, "isClosed", false);

    final SubscriptionProvider provider =
        new SubscriptionProvider(
            new TEndPoint(HOST, PORT),
            "root",
            "root",
            null,
            CONSUMER_ID,
            CONSUMER_GROUP_ID,
            THRIFT_MAX_FRAME_SIZE,
            LONG_INTERVAL_MS,
            CONNECTION_TIMEOUT_MS);
    setAtomicBooleanField(SubscriptionProvider.class, provider, "isClosed", false);

    final SubscriptionSessionConnection connection =
        Mockito.mock(SubscriptionSessionConnection.class);
    Mockito.when(connection.pipeSubscribe(Mockito.any(TPipeSubscribeReq.class)))
        .thenAnswer(
            invocation -> {
              closedStatesDuringClose.add(consumer.isClosed());
              return successResponse();
            });
    setField(Session.class, provider, "defaultSessionConnection", connection);

    getProviders(consumer).addProvider(0, provider);
  }

  private static TPipeSubscribeResp successResponse() {
    return new TPipeSubscribeResp().setStatus(new TSStatus(200));
  }

  private static SubscriptionProviders getProviders(final SubscriptionConsumer consumer)
      throws Exception {
    final Field field = SubscriptionConsumer.class.getDeclaredField("providers");
    field.setAccessible(true);
    return (SubscriptionProviders) field.get(consumer);
  }

  private static void setAtomicBooleanField(
      final Class<?> clazz, final Object target, final String fieldName, final boolean value)
      throws Exception {
    final Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    ((AtomicBoolean) field.get(target)).set(value);
  }

  private static void setField(
      final Class<?> clazz, final Object target, final String fieldName, final Object value)
      throws Exception {
    final Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static class TestPushConsumer extends SubscriptionPushConsumer {

    private final List<Boolean> closedStatesDuringHandshake = new ArrayList<>();

    private TestPushConsumer() {
      super(
          new SubscriptionPushConsumer.Builder()
              .host(HOST)
              .port(PORT)
              .consumerId(CONSUMER_ID)
              .consumerGroupId(CONSUMER_GROUP_ID)
              .heartbeatIntervalMs(LONG_INTERVAL_MS)
              .endpointsSyncIntervalMs(LONG_INTERVAL_MS)
              .autoPollIntervalMs(LONG_INTERVAL_MS));
    }

    @Override
    SubscriptionProvider constructProviderAndHandshake(final TEndPoint endPoint)
        throws SubscriptionException {
      closedStatesDuringHandshake.add(isClosed());
      throw new SubscriptionException("intentional provider creation failure");
    }
  }

  private static class TestPullConsumer extends SubscriptionPullConsumer {

    private final List<Boolean> closedStatesDuringHandshake = new ArrayList<>();

    private TestPullConsumer() {
      super(
          new SubscriptionPullConsumer.Builder()
              .host(HOST)
              .port(PORT)
              .consumerId(CONSUMER_ID)
              .consumerGroupId(CONSUMER_GROUP_ID)
              .heartbeatIntervalMs(LONG_INTERVAL_MS)
              .endpointsSyncIntervalMs(LONG_INTERVAL_MS)
              .autoCommit(false));
    }

    @Override
    SubscriptionProvider constructProviderAndHandshake(final TEndPoint endPoint)
        throws SubscriptionException {
      closedStatesDuringHandshake.add(isClosed());
      throw new SubscriptionException("intentional provider creation failure");
    }
  }
}
