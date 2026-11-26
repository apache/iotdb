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

package org.apache.iotdb.subscription.it.consensus.local.tablemodel;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.ManualIT;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionSessionWrapper;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.model.Subscription;
import org.apache.iotdb.subscription.it.consensus.local.AbstractSubscriptionConsensusLocalIT;

import org.apache.thrift.transport.TTransport;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ManualIT.class})
public class IoTDBConsensusSubscriptionConsumerDisconnectTableIT
    extends AbstractSubscriptionConsensusLocalIT {

  private static final long HEARTBEAT_INTERVAL_MS = 1_000L;
  private static final int INITIAL_ROW_COUNT = 20;
  private static final int ROW_COUNT_DURING_DISCONNECT = 200;

  @Test
  public void testOtherConsumersContinueAfterConsumerHardDisconnect() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers keepOneIds =
        ConsensusSubscriptionTableITSupport.newIdentifiers("hard_disconnect_keep_one");
    final ConsensusSubscriptionTableITSupport.TestIdentifiers keepTwoIds =
        ConsensusSubscriptionTableITSupport.newIdentifiers("hard_disconnect_keep_two");
    final ConsensusSubscriptionTableITSupport.TestIdentifiers stoppedIds =
        ConsensusSubscriptionTableITSupport.newIdentifiers("hard_disconnect_stopped");
    final String database = keepOneIds.getDatabase();
    final String table = "t1";
    final Set<String> topics = new LinkedHashSet<>();
    topics.add(keepOneIds.getTopic());
    topics.add(keepTwoIds.getTopic());
    topics.add(stoppedIds.getTopic());

    SubscriptionTablePullConsumer keepOneConsumer = null;
    SubscriptionTablePullConsumer keepTwoConsumer = null;
    SubscriptionTablePullConsumer stoppedConsumer = null;
    final ExecutorService writer = Executors.newSingleThreadExecutor();
    final CountDownLatch disconnectPointReached = new CountDownLatch(1);
    final CountDownLatch disconnectCompleted = new CountDownLatch(1);
    Future<Set<String>> writeFuture = null;

    try {
      ConsensusSubscriptionTableITSupport.bootstrapDatabaseAndTable(
          database, table, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
      for (final String topic : topics) {
        ConsensusSubscriptionTableITSupport.createConsensusTopic(topic, database, table);
      }

      keepOneConsumer = createConsumer(keepOneIds.getConsumerId(), keepOneIds.getConsumerGroupId());
      keepTwoConsumer = createConsumer(keepTwoIds.getConsumerId(), keepTwoIds.getConsumerGroupId());
      stoppedConsumer = createConsumer(stoppedIds.getConsumerId(), stoppedIds.getConsumerGroupId());
      keepOneConsumer.subscribe(keepOneIds.getTopic());
      keepTwoConsumer.subscribe(keepTwoIds.getTopic());
      stoppedConsumer.subscribe(stoppedIds.getTopic());

      awaitSubscriptionPresent(
          keepOneIds.getTopic(), keepOneIds.getConsumerGroupId(), keepOneIds.getConsumerId());
      awaitSubscriptionPresent(
          keepTwoIds.getTopic(), keepTwoIds.getConsumerGroupId(), keepTwoIds.getConsumerId());
      awaitSubscriptionPresent(
          stoppedIds.getTopic(), stoppedIds.getConsumerGroupId(), stoppedIds.getConsumerId());

      final Set<String> initialRows =
          ConsensusSubscriptionTableITSupport.insertRows(
              database, table, 100L, INITIAL_ROW_COUNT, true);
      assertRowsConsumed(keepOneConsumer, initialRows);
      assertRowsConsumed(keepTwoConsumer, initialRows);
      assertRowsConsumed(stoppedConsumer, initialRows);

      writeFuture =
          writer.submit(
              () ->
                  insertRowsAcrossDisconnect(
                      database, table, 1_000L, disconnectPointReached, disconnectCompleted));
      Assert.assertTrue(
          "Timed out waiting for the batch writer to reach the disconnect point",
          disconnectPointReached.await(30, TimeUnit.SECONDS));

      disconnectWithoutClose(stoppedConsumer);
      disconnectCompleted.countDown();
      final Set<String> rowsWrittenAcrossDisconnect = writeFuture.get(2, TimeUnit.MINUTES);

      awaitSubscriptionRemoved(stoppedIds.getTopic());
      awaitSubscriptionPresent(
          keepOneIds.getTopic(), keepOneIds.getConsumerGroupId(), keepOneIds.getConsumerId());
      awaitSubscriptionPresent(
          keepTwoIds.getTopic(), keepTwoIds.getConsumerGroupId(), keepTwoIds.getConsumerId());

      assertRowsConsumed(keepOneConsumer, rowsWrittenAcrossDisconnect);
      assertRowsConsumed(keepTwoConsumer, rowsWrittenAcrossDisconnect);
    } finally {
      disconnectCompleted.countDown();
      if (writeFuture != null && !writeFuture.isDone()) {
        writeFuture.cancel(true);
      }
      writer.shutdownNow();
      writer.awaitTermination(10, TimeUnit.SECONDS);
      closeQuietly(stoppedConsumer);
      closeQuietly(keepTwoConsumer);
      closeQuietly(keepOneConsumer);
      ConsensusSubscriptionTableITSupport.cleanup(null, topics, database);
    }
  }

  private static SubscriptionTablePullConsumer createConsumer(
      final String consumerId, final String consumerGroupId) throws Exception {
    final SubscriptionTablePullConsumer consumer =
        (SubscriptionTablePullConsumer)
            new SubscriptionTablePullConsumerBuilder()
                .host(EnvFactory.getEnv().getIP())
                .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
                .consumerId(consumerId)
                .consumerGroupId(consumerGroupId)
                .heartbeatIntervalMs(HEARTBEAT_INTERVAL_MS)
                .endpointsSyncIntervalMs(5_000L)
                .autoCommit(false)
                .build();
    consumer.open();
    return consumer;
  }

  private static Set<String> insertRowsAcrossDisconnect(
      final String database,
      final String table,
      final long startTimestamp,
      final CountDownLatch disconnectPointReached,
      final CountDownLatch disconnectCompleted)
      throws Exception {
    final Set<String> rowKeys = new LinkedHashSet<>();
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + database);
      for (int row = 0; row < ROW_COUNT_DURING_DISCONNECT; row++) {
        if (row == ROW_COUNT_DURING_DISCONNECT / 4) {
          disconnectPointReached.countDown();
          Assert.assertTrue(
              "Timed out waiting for the consumer hard disconnect",
              disconnectCompleted.await(30, TimeUnit.SECONDS));
        }
        final long timestamp = startTimestamp + row;
        session.executeNonQueryStatement(
            String.format(
                Locale.ROOT,
                "insert into %s(tag1, s1, time) values ('batch', %d, %d)",
                table,
                timestamp * 10L,
                timestamp));
        rowKeys.add(ConsensusSubscriptionTableITSupport.rowKey(database, table, timestamp));
      }
      session.executeNonQueryStatement("flush");
    }
    return rowKeys;
  }

  private static void disconnectWithoutClose(final SubscriptionTablePullConsumer consumer)
      throws Exception {
    final AtomicBoolean isClosed = (AtomicBoolean) getField(consumer, "isClosed");
    isClosed.set(true);

    // Let the scheduled workers observe isClosed before severing the only provider connection.
    ConsensusSubscriptionTableITSupport.pause(HEARTBEAT_INTERVAL_MS * 2L);

    final Object providers = getField(consumer, "providers");
    final Map<?, ?> providerMap = (Map<?, ?>) getField(providers, "subscriptionProviders");
    final List<?> providerSnapshot = new ArrayList<>(providerMap.values());
    Assert.assertFalse("Expected at least one subscription provider", providerSnapshot.isEmpty());
    for (final Object provider : providerSnapshot) {
      final SubscriptionSessionWrapper session =
          (SubscriptionSessionWrapper) getField(provider, "session");
      final Object connection = session.getSessionConnection();
      final TTransport transport = (TTransport) getField(connection, "transport");
      Assert.assertTrue("Expected an open subscription transport", transport.isOpen());
      transport.close();
    }
  }

  private static void awaitSubscriptionPresent(
      final String topicName, final String consumerGroupId, final String consumerId)
      throws Exception {
    try (final ISubscriptionTableSession session = createSubscriptionSession()) {
      Awaitility.await()
          .pollInSameThread()
          .pollInterval(Duration.ofMillis(500))
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                final Set<Subscription> subscriptions = session.getSubscriptions(topicName);
                Assert.assertEquals(subscriptions.toString(), 1, subscriptions.size());
                final Subscription subscription = subscriptions.iterator().next();
                Assert.assertEquals(consumerGroupId, subscription.getConsumerGroupId());
                Assert.assertTrue(
                    subscription.toString(), subscription.getConsumerIds().contains(consumerId));
              });
    }
  }

  private static void awaitSubscriptionRemoved(final String topicName) throws Exception {
    try (final ISubscriptionTableSession session = createSubscriptionSession()) {
      Awaitility.await()
          .pollInSameThread()
          .pollInterval(Duration.ofMillis(500))
          .atMost(Duration.ofSeconds(45))
          .untilAsserted(
              () -> {
                final Set<Subscription> subscriptions = session.getSubscriptions(topicName);
                Assert.assertTrue(subscriptions.toString(), subscriptions.isEmpty());
              });
    }
  }

  private static ISubscriptionTableSession createSubscriptionSession() throws Exception {
    final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .build();
    session.open();
    return session;
  }

  private static void assertRowsConsumed(
      final SubscriptionTablePullConsumer consumer, final Set<String> expectedRows)
      throws Exception {
    final ConsensusSubscriptionTableITSupport.ConsumedRecords consumed =
        ConsensusSubscriptionTableITSupport.pollAndCommitUntilContains(consumer, expectedRows, 60);
    ConsensusSubscriptionTableITSupport.assertExactRowKeys(expectedRows, consumed);
  }

  private static Object getField(final Object target, final String fieldName) throws Exception {
    Class<?> currentClass = target.getClass();
    while (currentClass != null) {
      try {
        final Field field = currentClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
      } catch (final NoSuchFieldException ignored) {
        currentClass = currentClass.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }

  private static void closeQuietly(final SubscriptionTablePullConsumer consumer) {
    if (consumer != null) {
      try {
        consumer.close();
      } catch (final Exception ignored) {
        // ignored on cleanup
      }
    }
  }
}
