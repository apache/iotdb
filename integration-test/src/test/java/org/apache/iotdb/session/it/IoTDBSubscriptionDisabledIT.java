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

package org.apache.iotdb.session.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeCriticalException;
import org.apache.iotdb.session.subscription.SubscriptionTreeSession;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSubscriptionDisabledIT {

  private static final String DISABLED_MESSAGE = "Subscription is not enabled.";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTopicAndSubscriptionStatementsAreRejected() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("path", "root.**");

    try (final SubscriptionTreeSession session = subscriptionSession()) {
      session.open();
      final List<ThrowingRunnable> statements =
          Arrays.asList(
              () -> session.createTopic("disabled_topic"),
              () -> session.createTopicIfNotExists("disabled_topic"),
              () -> session.alterTopic("disabled_topic", properties),
              () -> session.dropTopic("disabled_topic"),
              () -> session.dropTopicIfExists("disabled_topic"),
              session::getTopics,
              () -> session.getTopic("disabled_topic"),
              session::getSubscriptions,
              () -> session.getSubscriptions("disabled_topic"),
              () -> session.dropSubscription("disabled_subscription"),
              () -> session.dropSubscriptionIfExists("disabled_subscription"));

      for (final ThrowingRunnable statement : statements) {
        assertSubscriptionDisabled(statement);
      }
    }
  }

  @Test
  public void testConsumerHandshakeIsRejected() {
    try (final SubscriptionTreePullConsumer consumer =
        new SubscriptionTreePullConsumer.Builder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .consumerId("disabled_consumer")
            .consumerGroupId("disabled_consumer_group")
            .autoCommit(false)
            .buildPullConsumer()) {
      final SubscriptionConnectionException exception =
          Assert.assertThrows(SubscriptionConnectionException.class, consumer::open);
      Assert.assertTrue(hasCause(exception, SubscriptionRuntimeCriticalException.class));
      Assert.assertTrue(hasMessage(exception, DISABLED_MESSAGE));
    }
  }

  private static SubscriptionTreeSession subscriptionSession() {
    return new SubscriptionTreeSession(
        EnvFactory.getEnv().getIP(), Integer.parseInt(EnvFactory.getEnv().getPort()));
  }

  private static void assertSubscriptionDisabled(final ThrowingRunnable statement) {
    final StatementExecutionException exception =
        Assert.assertThrows(StatementExecutionException.class, statement);
    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_NOT_ENABLED_ERROR.getStatusCode(), exception.getStatusCode());
    Assert.assertTrue(exception.getMessage().contains(DISABLED_MESSAGE));
  }

  private static boolean hasCause(
      final Throwable throwable, final Class<? extends Throwable> expectedType) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (expectedType.isInstance(current)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasMessage(final Throwable throwable, final String expectedMessage) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current.getMessage() != null && current.getMessage().contains(expectedMessage)) {
        return true;
      }
    }
    return false;
  }
}
