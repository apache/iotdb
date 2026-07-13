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
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.subscription.it.AbstractSubscriptionIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({TableClusterIT.class})
public class IoTDBConsensusSubscriptionColumnFilterClusterIT extends AbstractSubscriptionIT {

  private static final long OWNER_LEASE_DURATION_MS = 60_000L;

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
        .setDataReplicationFactor(2)
        .setAutoCreateSchemaEnabled(true)
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setSubscriptionOwnerLeaseDurationMsMin(1000);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    super.tearDown();
  }

  @Test
  public void testAlterColumnFilterRebindsAfterOwnerTransferOnThreeDataNodes() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
        ConsensusSubscriptionTableITSupport.newIdentifiers("cluster_owner_rebind");
    final String database = ids.getDatabase();
    final String table = "t1";
    final String schema = "tag1 STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD, s3 BOOLEAN FIELD";
    SubscriptionTablePullConsumer ownerConsumer = null;

    try {
      ConsensusSubscriptionTableITSupport.createDatabaseAndTable(database, table, schema);
      ConsensusSubscriptionTableITSupport.insertRows(database, table, 0L, 1, true, true, true);
      createOwnedConsensusTopic(ids.getTopic(), database, table, "column_name = \"s1\"");

      ownerConsumer = createOwnerConsumer(ids.consumer("owner1"), ids.consumerGroup("owner"), 1L);
      ownerConsumer.subscribe(ids.getTopic());

      final Set<String> rowsBeforeTransfer =
          ConsensusSubscriptionTableITSupport.insertRows(
              database, table, 100L, 3, true, true, true);
      final ConsensusSubscriptionTableITSupport.ConsumedRecords beforeTransfer =
          ConsensusSubscriptionTableITSupport.pollAndCommitUntilAtLeast(
              ownerConsumer, rowsBeforeTransfer.size(), 60);

      ConsensusSubscriptionTableITSupport.assertExactRowKeys(rowsBeforeTransfer, beforeTransfer);
      Assert.assertEquals(
          Collections.singleton(
              ConsensusSubscriptionTableITSupport.normalizeColumnSignature("tag1", "s1")),
          beforeTransfer.getSeenColumnSignatures());

      ownerConsumer.unsubscribe(ids.getTopic());
      ownerConsumer.close();
      ownerConsumer = null;

      addColumn(database, table, "s4 INT32 FIELD");
      transferOwner(ids.getTopic(), "owner2", 2L);
      ConsensusSubscriptionTableITSupport.alterConsensusTopicColumnFilter(
          ids.getTopic(), "category = \"FIELD\"");

      ownerConsumer = createOwnerConsumer(ids.consumer("owner2"), ids.consumerGroup("owner"), 2L);
      ownerConsumer.subscribe(ids.getTopic());

      final Set<String> rowsAfterTransfer = insertRowsWithS4(database, table, 200L, 3);
      final ConsensusSubscriptionTableITSupport.ConsumedRecords afterTransfer =
          ConsensusSubscriptionTableITSupport.pollAndCommitUntilContains(
              ownerConsumer, rowsAfterTransfer, 80);

      Assert.assertTrue(
          "Missing post-transfer rows. Consumed records: " + afterTransfer,
          afterTransfer.getRowKeys().containsAll(rowsAfterTransfer));
      Assert.assertTrue(
          afterTransfer
              .getSeenColumnSignatures()
              .contains(
                  ConsensusSubscriptionTableITSupport.normalizeColumnSignature(
                      "tag1", "s1", "s2", "s3", "s4")));
    } finally {
      ConsensusSubscriptionTableITSupport.cleanup(ownerConsumer, ids.getTopic(), database);
    }
  }

  private static void createOwnedConsensusTopic(
      final String topicName, final String database, final String table, final String columnFilter)
      throws Exception {
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .build()) {
      session.open();
      session.dropTopicIfExists(topicName);

      final Properties config = new Properties();
      config.put(TopicConstant.MODE_KEY, TopicConstant.MODE_CONSENSUS_VALUE);
      config.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE);
      config.put(TopicConstant.DATABASE_KEY, database);
      config.put(TopicConstant.TABLE_KEY, table);
      config.put(TopicConstant.COLUMN_FILTER_KEY, columnFilter);
      config.put(TopicConstant.OWNER_ID_KEY, "owner1");
      config.put(TopicConstant.OWNER_EPOCH_KEY, "1");
      config.put(
          TopicConstant.OWNER_LEASE_DURATION_MS_KEY, String.valueOf(OWNER_LEASE_DURATION_MS));
      session.createTopic(topicName, config);
    }
  }

  private static SubscriptionTablePullConsumer createOwnerConsumer(
      final String consumerId, final String consumerGroupId, final long ownerEpoch)
      throws Exception {
    final SubscriptionTablePullConsumer consumer =
        (SubscriptionTablePullConsumer)
            new SubscriptionTablePullConsumerBuilder()
                .host(EnvFactory.getEnv().getIP())
                .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
                .consumerId(consumerId)
                .consumerGroupId(consumerGroupId)
                .ownerId(ownerEpoch == 1L ? "owner1" : "owner2")
                .ownerEpoch(ownerEpoch)
                .heartbeatIntervalMs(1000)
                .endpointsSyncIntervalMs(5000)
                .autoCommit(false)
                .build();
    consumer.open();
    return consumer;
  }

  private static void transferOwner(
      final String topicName, final String ownerId, final long ownerEpoch) throws Exception {
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .build()) {
      session.open();
      session.alterTopicOwner(topicName, ownerId, ownerEpoch, OWNER_LEASE_DURATION_MS);
    }
  }

  private static void addColumn(final String database, final String table, final String column)
      throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + database);
      session.executeNonQueryStatement("alter table " + table + " add column " + column);
    }
  }

  private static Set<String> insertRowsWithS4(
      final String database, final String table, final long startTimestamp, final int rowCount)
      throws Exception {
    final Set<String> rowKeys = new LinkedHashSet<>();
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + database);
      for (int row = 0; row < rowCount; row++) {
        final long timestamp = startTimestamp + row;
        session.executeNonQueryStatement(
            String.format(
                Locale.ROOT,
                "insert into %s(tag1, s1, s2, s3, s4, time) "
                    + "values ('tag_%d', %d, %.1f, %s, %d, %d)",
                table,
                timestamp,
                timestamp * 10L,
                timestamp + 0.5d,
                timestamp % 2 == 0 ? "true" : "false",
                timestamp,
                timestamp));
        rowKeys.add(ConsensusSubscriptionTableITSupport.rowKey(database, table, timestamp));
      }
      session.executeNonQueryStatement("flush");
    }
    return rowKeys;
  }
}
