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

package org.apache.iotdb.subscription.it.local;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionOwnerFencedException;
import org.apache.iotdb.session.subscription.SubscriptionTreeSession;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.model.Topic;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionRecordHandler;
import org.apache.iotdb.subscription.it.AbstractSubscriptionIT;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;

import org.apache.tsfile.read.query.dataset.ResultSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionTopicOwnerIT extends AbstractSubscriptionIT {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        // Lower the owner-lease floor so the drain test can use a short lease and stay fast.
        .setSubscriptionOwnerLeaseDurationMsMin(1000);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    super.tearDown();
  }

  @Test
  public void testTopicOwnerFencingRejectsStaleOwnerAndAllowsCurrentOwner() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    final String topicName = "topic_owner_fencing";

    try (final SubscriptionTreeSession session = new SubscriptionTreeSession(host, port)) {
      session.open();
      final Properties properties = new Properties();
      properties.put(TopicConstant.PATH_KEY, "root.topic_owner.**");
      properties.put(TopicConstant.START_TIME_KEY, "0");
      properties.put(TopicConstant.OWNER_ID_KEY, "owner2");
      properties.put(TopicConstant.OWNER_EPOCH_KEY, "6");
      session.createTopic(topicName, properties);
    }

    try {
      try (final SubscriptionTreePullConsumer staleOwnerConsumer =
          new SubscriptionTreePullConsumer.Builder()
              .host(host)
              .port(port)
              .consumerId("stale_owner")
              .consumerGroupId("topic_owner_group")
              .ownerId("owner1")
              .ownerEpoch(5L)
              .autoCommit(false)
              .buildPullConsumer()) {
        staleOwnerConsumer.open();
        Assert.assertThrows(
            SubscriptionOwnerFencedException.class, () -> staleOwnerConsumer.subscribe(topicName));
      }

      try (final SubscriptionTreePullConsumer currentOwnerConsumer =
          new SubscriptionTreePullConsumer.Builder()
              .host(host)
              .port(port)
              .consumerId("current_owner")
              .consumerGroupId("topic_owner_group")
              .ownerId("owner2")
              .ownerEpoch(6L)
              .autoCommit(false)
              .buildPullConsumer()) {
        currentOwnerConsumer.open();
        currentOwnerConsumer.subscribe(topicName);

        insertData();

        final AtomicReference<List<SubscriptionMessage>> polledMessages =
            new AtomicReference<>(Collections.emptyList());
        IoTDBSubscriptionITConstant.AWAIT.untilAsserted(
            () -> {
              final List<SubscriptionMessage> messages =
                  currentOwnerConsumer.poll(Duration.ofMillis(1000));
              polledMessages.set(messages);
              Assert.assertFalse(messages.isEmpty());
              Assert.assertTrue(countRows(messages) > 0);
            });

        currentOwnerConsumer.commitSync(polledMessages.get());
        currentOwnerConsumer.unsubscribe(topicName);
      }
    } finally {
      try (final SubscriptionTreeSession session = new SubscriptionTreeSession(host, port)) {
        session.open();
        session.dropTopicIfExists(topicName);
      }
    }
  }

  @Test
  public void testAlterTopicOwnerWaitsForLeaseDrainBeforeInstallingNewOwner() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    final String topicName = "topic_owner_lease_drain";
    final long ownerLeaseDurationMs = 2500L;

    try (final SubscriptionTreeSession session = new SubscriptionTreeSession(host, port)) {
      session.open();
      final Properties properties = new Properties();
      properties.put(TopicConstant.PATH_KEY, "root.topic_owner.**");
      properties.put(TopicConstant.START_TIME_KEY, "0");
      properties.put(TopicConstant.OWNER_ID_KEY, "owner1");
      properties.put(TopicConstant.OWNER_EPOCH_KEY, "5");
      properties.put(
          TopicConstant.OWNER_LEASE_DURATION_MS_KEY, String.valueOf(ownerLeaseDurationMs));
      session.createTopic(topicName, properties);

      try {
        // Transferring to a different owner must wait for the old owner's lease to drain on every
        // DataNode before the new owner is installed. ConfigNode stops renewing and then waits, on
        // its own clock, for the lease duration plus one renewal (heartbeat) interval. The extra
        // heartbeat interval is necessary because a renewal may have been in flight to a DataNode
        // right before renewal was stopped, extending that DataNode's lease by up to the full
        // duration starting as late as ~one heartbeat after the block; waiting (duration + one
        // interval) on the local clock — rather than comparing against a DataNode's absolute
        // expire time, which would couple the two clocks — guarantees every lease has drained. The
        // call therefore blocks for at least the lease duration. This is the admission gate that
        // prevents cross-DataNode double-active consuming.
        final long alterStartTimeMs = System.currentTimeMillis();
        session.alterTopicOwner(topicName, "owner2", 6L);
        final long alterElapsedTimeMs = System.currentTimeMillis() - alterStartTimeMs;
        Assert.assertTrue(
            "alterTopicOwner should block at least the lease drain duration, but only took "
                + alterElapsedTimeMs
                + "ms",
            alterElapsedTimeMs >= ownerLeaseDurationMs);

        final String topicAttributes = getTopicAttributes(session, topicName);
        Assert.assertTrue(topicAttributes.contains("owner-id=owner2"));
        Assert.assertTrue(topicAttributes.contains("owner-epoch=6"));
      } finally {
        session.dropTopicIfExists(topicName);
      }
    }
  }

  private void insertData() throws Exception {
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 10; i++) {
        session.executeNonQueryStatement(
            String.format("insert into root.topic_owner.d1(time, s1) values (%s, %s)", i, i));
      }
      session.executeNonQueryStatement("flush");
    }
  }

  private static int countRows(final List<SubscriptionMessage> messages) throws Exception {
    int rowCount = 0;
    for (final SubscriptionMessage message : messages) {
      for (final ResultSet resultSet : message.getResultSets()) {
        while (((SubscriptionRecordHandler.SubscriptionResultSet) resultSet).hasNext()) {
          resultSet.next();
          rowCount++;
        }
      }
    }
    return rowCount;
  }

  private static String getTopicAttributes(
      final SubscriptionTreeSession session, final String topicName) throws Exception {
    for (final Topic topic : session.getTopics()) {
      if (topicName.equals(topic.getTopicName())) {
        return topic.getTopicAttributes();
      }
    }
    Assert.fail("Topic " + topicName + " should exist.");
    return "";
  }
}
