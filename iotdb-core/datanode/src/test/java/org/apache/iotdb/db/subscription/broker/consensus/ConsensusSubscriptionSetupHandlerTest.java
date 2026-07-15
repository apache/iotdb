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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
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

  private static void failOnSecondTopic(
      final String topicName, final Set<String> attemptedTopicNames) {
    attemptedTopicNames.add(topicName);
    if ("second".equals(topicName)) {
      throw new IllegalStateException("setup failed");
    }
  }
}
