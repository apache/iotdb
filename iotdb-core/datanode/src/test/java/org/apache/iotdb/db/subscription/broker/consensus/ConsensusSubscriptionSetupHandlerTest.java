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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConsensusSubscriptionSetupHandlerTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSingleTopicSetupFailurePropagates() {
    SubscriptionException failure = null;
    try {
      ConsensusSubscriptionSetupHandler.setupConsensusTopics(
          "consumerGroup",
          Collections.singleton("topic"),
          topicName -> true,
          topicName -> {
            throw new IllegalStateException("setup failed");
          },
          ignored -> {});
    } catch (final SubscriptionException e) {
      failure = e;
    }

    assertNotNull(failure);
    assertTrue(failure.getMessage().contains("topic"));
    assertTrue(failure.getMessage().contains("consumerGroup"));
    assertTrue(failure.getCause() instanceof IllegalStateException);
  }

  @Test
  public void testTopicModeLookupFailurePropagates() {
    SubscriptionException failure = null;
    try {
      ConsensusSubscriptionSetupHandler.setupConsensusTopics(
          "consumerGroup",
          Collections.singleton("topic"),
          topicName -> {
            throw new IllegalStateException("topic metadata missing");
          },
          topicName -> {},
          ignored -> {});
    } catch (final SubscriptionException e) {
      failure = e;
    }

    assertNotNull(failure);
    assertTrue(failure.getCause() instanceof IllegalStateException);
  }

  @Test
  public void testMultiTopicSetupFailureRollsBackAllAttemptedTopics() {
    final Set<String> attemptedTopicNames = new LinkedHashSet<>();
    final Set<String> rolledBackTopicNames = new LinkedHashSet<>();
    final Set<String> topicNames = new LinkedHashSet<>(Arrays.asList("first", "second", "third"));
    SubscriptionException failure = null;

    try {
      ConsensusSubscriptionSetupHandler.setupConsensusTopics(
          "consumerGroup",
          topicNames,
          topicName -> true,
          topicName -> failOnSecondTopic(topicName, attemptedTopicNames),
          attemptedTopics -> rolledBackTopicNames.addAll(attemptedTopics));
    } catch (final SubscriptionException e) {
      failure = e;
    }

    assertNotNull(failure);
    assertEquals(new LinkedHashSet<>(Arrays.asList("first", "second")), attemptedTopicNames);
    assertEquals(attemptedTopicNames, rolledBackTopicNames);
  }

  @Test
  public void testFallbackLookupDoesNotQueryConfigNodeWithoutLocalPersistence() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("noLocalPersistence");
    try {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(systemDir.getAbsolutePath());
      final AtomicInteger queryCount = new AtomicInteger();
      final ConsensusSubscriptionCommitManager commitManager =
          new ConsensusSubscriptionCommitManager(
              (consumerGroupId, topicName, regionId) -> {
                queryCount.incrementAndGet();
                return ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent();
              });

      assertNull(
          ConsensusSubscriptionSetupHandler.resolveFallbackCommittedRegionProgress(
              commitManager, "consumerGroup", "topic", new DataRegionId(1)));
      assertEquals(0, queryCount.get());
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testAuditDatabaseNeverMatchesTopic() {
    final Map<String, String> tableTopicAttributes = new HashMap<>();
    tableTopicAttributes.put(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    tableTopicAttributes.put(TopicConstant.DATABASE_KEY, ".*");
    final TopicConfig tableTopicConfig = new TopicConfig(tableTopicAttributes);

    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDatabase(tableTopicConfig, "__audit"));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDatabase(tableTopicConfig, "root.__audit"));
    assertTrue(ConsensusSubscriptionSetupHandler.matchesTopicDatabase(tableTopicConfig, "user_db"));

    final TopicConfig treeTopicConfig = new TopicConfig(Collections.emptyMap());
    assertFalse(ConsensusSubscriptionSetupHandler.matchesTopicDatabase(treeTopicConfig, "__audit"));
    assertTrue(ConsensusSubscriptionSetupHandler.matchesTopicDatabase(treeTopicConfig, "user_db"));
  }

  @Test
  public void testTopicDataRegionModelIsolation() {
    final TopicConfig treeTopicConfig = new TopicConfig(Collections.emptyMap());
    final Map<String, String> tableTopicAttributes = new HashMap<>();
    tableTopicAttributes.put(
        SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    tableTopicAttributes.put(TopicConstant.DATABASE_KEY, "table_db");
    final TopicConfig tableTopicConfig = new TopicConfig(tableTopicAttributes);

    assertTrue(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "root.tree_db", treeTopicConfig, false));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "table_db", treeTopicConfig, false));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "table_db", treeTopicConfig, true));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "root.table_db", tableTopicConfig, true));
    assertTrue(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "table_db", tableTopicConfig, true));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "other_table_db", tableTopicConfig, true));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "table_db", tableTopicConfig, false));
    assertFalse(
        ConsensusSubscriptionSetupHandler.matchesTopicDataRegion(
            "root.table_db", tableTopicConfig, false));
  }

  private static void failOnSecondTopic(
      final String topicName, final Set<String> attemptedTopicNames) {
    attemptedTopicNames.add(topicName);
    if ("second".equals(topicName)) {
      throw new IllegalStateException("setup failed");
    }
  }
}
