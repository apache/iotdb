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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionRecordHandler;
import org.apache.iotdb.subscription.it.consensus.local.AbstractSubscriptionConsensusLocalIT;

import org.apache.tsfile.read.query.dataset.ResultSet;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConsensusSubscriptionRecoveryTableIT
    extends AbstractSubscriptionConsensusLocalIT {

  @Override
  @Before
  public void setUp() throws Exception {
    // Persist each acknowledged batch before restarting the servers.
    final Properties properties = new Properties();
    properties.setProperty("subscription_consensus_commit_persist_interval", "1");
    properties.setProperty("subscription_consensus_commit_fsync_enabled", "true");
    ((MppCommonConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig())
        .updateProperties(properties);
    super.setUp();
  }

  @Test
  public void testRebuiltConsumersResumeAfterFullClusterRestart() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
        ConsensusSubscriptionTableITSupport.newIdentifiers("table_full_cluster_restart");
    final String tableName = "t1";
    final Set<String> expectedRows = new LinkedHashSet<>();
    final Set<String> consumedRows = new LinkedHashSet<>();
    SubscriptionTablePullConsumer firstConsumer = null;
    SubscriptionTablePullConsumer secondConsumer = null;

    try {
      ConsensusSubscriptionTableITSupport.bootstrapDatabaseAndTable(
          ids.getDatabase(), tableName, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
      ConsensusSubscriptionTableITSupport.createConsensusTopic(ids.getTopic(), ".*", ".*");
      firstConsumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.consumer("first"), ids.getConsumerGroupId());
      secondConsumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.consumer("second"), ids.getConsumerGroupId());
      firstConsumer.subscribe(ids.getTopic());
      secondConsumer.subscribe(ids.getTopic());

      final Set<String> committedRows =
          ConsensusSubscriptionTableITSupport.insertRows(
              ids.getDatabase(), tableName, 1_000L, 16, true);
      expectedRows.addAll(committedRows);
      assertConsumedRows(expectedRows, consumedRows, firstConsumer, secondConsumer);

      final Set<String> pendingRows =
          ConsensusSubscriptionTableITSupport.insertRows(
              ids.getDatabase(), tableName, 2_000L, 64, true);

      EnvFactory.getEnv().shutdownAllDataNodes();
      EnvFactory.getEnv().shutdownAllConfigNodes();
      // Close while the servers are down so the old clients cannot remove persisted subscriptions.
      firstConsumer.close();
      firstConsumer = null;
      secondConsumer.close();
      secondConsumer = null;

      EnvFactory.getEnv().startAllConfigNodes();
      EnvFactory.getEnv().startAllDataNodes();
      ((AbstractEnv) EnvFactory.getEnv()).checkClusterStatusWithoutUnknown();

      firstConsumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.consumer("first"), ids.getConsumerGroupId());
      secondConsumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.consumer("second"), ids.getConsumerGroupId());
      firstConsumer.subscribe(ids.getTopic());
      secondConsumer.subscribe(ids.getTopic());

      // Do not seek, recreate the topic, or insert new rows to trigger subscription setup.
      // Track unique rows as a receiver would, allowing redelivery around the restart.
      expectedRows.addAll(pendingRows);
      assertConsumedRows(expectedRows, consumedRows, firstConsumer, secondConsumer);

      final Set<String> newRows =
          ConsensusSubscriptionTableITSupport.insertRows(
              ids.getDatabase(), tableName, 3_000L, 16, true);
      expectedRows.addAll(newRows);
      assertConsumedRows(expectedRows, consumedRows, firstConsumer, secondConsumer);
    } finally {
      ConsensusSubscriptionTableITSupport.cleanup(firstConsumer, ids.getTopic(), ids.getDatabase());
      ConsensusSubscriptionTableITSupport.cleanup(
          secondConsumer, ids.getTopic(), ids.getDatabase());
    }
  }

  private static void assertConsumedRows(
      final Set<String> expectedRows,
      final Set<String> consumedRows,
      final SubscriptionTablePullConsumer... consumers) {
    Awaitility.await()
        .pollInSameThread()
        .atMost(Duration.ofMinutes(2))
        .untilAsserted(
            () -> {
              for (final SubscriptionTablePullConsumer consumer : consumers) {
                final List<SubscriptionMessage> messages = consumer.poll(Duration.ofSeconds(1));
                for (final SubscriptionMessage message : messages) {
                  for (final ResultSet resultSet : message.getResultSets()) {
                    final SubscriptionRecordHandler.SubscriptionResultSet records =
                        (SubscriptionRecordHandler.SubscriptionResultSet) resultSet;
                    while (records.hasNext()) {
                      consumedRows.add(
                          ConsensusSubscriptionTableITSupport.rowKey(
                              records.getDatabaseName(),
                              records.getTableName(),
                              records.nextRecord().getTimestamp()));
                    }
                  }
                }
                consumer.commitSync(messages);
              }
              Assert.assertEquals(expectedRows, consumedRows);
            });
  }
}
