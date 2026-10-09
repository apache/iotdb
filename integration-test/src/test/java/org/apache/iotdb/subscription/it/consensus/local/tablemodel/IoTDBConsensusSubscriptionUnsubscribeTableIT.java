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

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.model.Subscription;
import org.apache.iotdb.subscription.it.AbstractSubscriptionIT;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(IoTDBTestRunner.class)
@Category({TableClusterIT.class})
public class IoTDBConsensusSubscriptionUnsubscribeTableIT extends AbstractSubscriptionIT {

  private static final long MAX_UNSUBSCRIBE_MS = 30_000L;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setAutoCreateSchemaEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    EnvFactory.getEnv().initClusterEnvironment(3, 1);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    super.tearDown();
  }

  @Test
  public void testLastConsumerUnsubscribeDuringWritesAndImmediateResubscribe() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
        ConsensusSubscriptionTableITSupport.newIdentifiers("unsubscribe_during_writes");
    final String database = ids.getDatabase();
    final String table = "table_0";
    final String firstConsumerId = ids.consumer("first");
    final String lastConsumerId = ids.consumer("last");
    final ExecutorService writer = Executors.newSingleThreadExecutor();
    final ExecutorService unsubscribeExecutor =
        Executors.newSingleThreadExecutor(
            runnable -> {
              final Thread thread = new Thread(runnable, "subscription-unsubscribe-it");
              thread.setDaemon(true);
              return thread;
            });
    final CountDownLatch writesStarted = new CountDownLatch(1);
    final AtomicBoolean keepWriting = new AtomicBoolean(true);
    SubscriptionTablePullConsumer consumer1 = null;
    SubscriptionTablePullConsumer consumer2 = null;
    Future<?> writeFuture = null;

    try {
      ConsensusSubscriptionTableITSupport.bootstrapDatabaseAndTable(
          database, table, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
      ConsensusSubscriptionTableITSupport.createConsensusTopic(ids.getTopic(), database, table);
      consumer1 =
          ConsensusSubscriptionTableITSupport.createConsumer(
              firstConsumerId, ids.getConsumerGroupId());
      consumer2 =
          ConsensusSubscriptionTableITSupport.createConsumer(
              lastConsumerId, ids.getConsumerGroupId());
      consumer1.subscribe(ids.getTopic());
      consumer2.subscribe(ids.getTopic());
      awaitConsumerIds(ids.getTopic(), ids.getConsumerGroupId(), firstConsumerId, lastConsumerId);

      writeFuture =
          writer.submit(
              (Callable<Void>)
                  () -> {
                    writeWhileSubscribed(database, table, keepWriting, writesStarted);
                    return null;
                  });
      Assert.assertTrue("Writer did not start", writesStarted.await(30, TimeUnit.SECONDS));
      if (writeFuture.isDone()) {
        writeFuture.get();
      }
      Assert.assertFalse("Writer finished before unsubscribe", writeFuture.isDone());
      consumer1.poll(Duration.ofMillis(100));

      unsubscribeWithin(consumer1, ids.getTopic(), unsubscribeExecutor);
      awaitConsumerIds(ids.getTopic(), ids.getConsumerGroupId(), lastConsumerId);
      Assert.assertFalse("Writer finished before last unsubscribe", writeFuture.isDone());
      unsubscribeWithin(consumer2, ids.getTopic(), unsubscribeExecutor);
      awaitConsumerIds(ids.getTopic(), ids.getConsumerGroupId());

      // A previous queue may still be closing. The new queue must retain its task registration.
      consumer1.subscribe(ids.getTopic());
      awaitConsumerIds(ids.getTopic(), ids.getConsumerGroupId(), firstConsumerId);

      keepWriting.set(false);
      writeFuture.get(30, TimeUnit.SECONDS);
      final Set<String> expectedRows =
          ConsensusSubscriptionTableITSupport.insertRows(database, table, 1_000_000_000L, 20, true);
      final ConsensusSubscriptionTableITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionTableITSupport.pollAndCommitUntilContains(
              consumer1, expectedRows, 60);
      Assert.assertTrue(consumed.toString(), consumed.getRowKeys().containsAll(expectedRows));
    } finally {
      keepWriting.set(false);
      if (writeFuture != null && !writeFuture.isDone()) {
        writeFuture.cancel(true);
      }
      writer.shutdownNow();
      writer.awaitTermination(10, TimeUnit.SECONDS);
      unsubscribeExecutor.shutdownNow();
      closeQuietly(consumer2);
      closeQuietly(consumer1);
      ConsensusSubscriptionTableITSupport.cleanup(null, ids.getTopic(), database);
    }
  }

  private static void writeWhileSubscribed(
      final String database,
      final String table,
      final AtomicBoolean keepWriting,
      final CountDownLatch writesStarted)
      throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + database);
      for (long row = 0; keepWriting.get(); row++) {
        final long timestamp = 1_000L + row;
        session.executeNonQueryStatement(
            String.format(
                Locale.ROOT,
                "insert into %s(tag1, s1, time) values ('writing', %d, %d)",
                table,
                timestamp,
                timestamp));
        if (row == 20) {
          writesStarted.countDown();
        }
        Thread.sleep(10L);
      }
      session.executeNonQueryStatement("flush");
    } finally {
      writesStarted.countDown();
    }
  }

  private static void unsubscribeWithin(
      final SubscriptionTablePullConsumer consumer,
      final String topicName,
      final ExecutorService executor)
      throws Exception {
    final Future<?> future = executor.submit(() -> consumer.unsubscribe(topicName));
    try {
      future.get(MAX_UNSUBSCRIBE_MS, TimeUnit.MILLISECONDS);
    } finally {
      future.cancel(true);
    }
  }

  private static void closeQuietly(final SubscriptionTablePullConsumer consumer) {
    if (consumer != null) {
      try {
        consumer.close();
      } catch (final Exception ignored) {
        // Best effort cleanup after a failed assertion.
      }
    }
  }

  private static void awaitConsumerIds(
      final String topicName, final String consumerGroupId, final String... expectedConsumerIds)
      throws Exception {
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .build()) {
      session.open();
      Awaitility.await()
          .pollInSameThread()
          .pollInterval(Duration.ofMillis(500))
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                final Set<Subscription> subscriptions = session.getSubscriptions(topicName);
                if (expectedConsumerIds.length == 0) {
                  Assert.assertTrue(subscriptions.toString(), subscriptions.isEmpty());
                } else {
                  Assert.assertEquals(subscriptions.toString(), 1, subscriptions.size());
                  final Subscription subscription = subscriptions.iterator().next();
                  Assert.assertEquals(consumerGroupId, subscription.getConsumerGroupId());
                  final String consumerIds = subscription.getConsumerIds();
                  Assert.assertTrue(
                      subscription.toString(),
                      consumerIds.startsWith("[") && consumerIds.endsWith("]"));
                  final String idList = consumerIds.substring(1, consumerIds.length() - 1);
                  Assert.assertEquals(
                      subscription.toString(),
                      new HashSet<>(Arrays.asList(expectedConsumerIds)),
                      new HashSet<>(Arrays.asList(idList.split(",\\s*"))));
                }
              });
    }
  }
}
