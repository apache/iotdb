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
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionMessageIT extends AbstractSubscriptionLocalIT {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Ignore
  @Test
  public void testPullConsumerCommitAfterRemoveUserData() throws Exception {
    final String topicName = "topic_remove_user_data";
    insertHistoricalData(0, 100);
    createTopic(topicName);

    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(host)
            .port(port)
            .consumerId("c_remove_user_data")
            .consumerGroupId("cg_remove_user_data")
            .autoCommit(false)
            .buildPullConsumer()) {
      consumer.open();
      consumer.subscribe(topicName);

      final List<SubscriptionMessage> messages = pollMessages(consumer);
      Assert.assertFalse(messages.isEmpty());

      for (final SubscriptionMessage message : messages) {
        Assert.assertNotNull(message.getCommitContext());
        Assert.assertFalse(message.getResultSets().isEmpty());

        message.removeUserData();

        Assert.assertNotNull(message.getCommitContext());
        Assert.assertThrows(SubscriptionRuntimeException.class, message::getResultSets);
        Assert.assertThrows(SubscriptionRuntimeException.class, message::getRecordTabletIterator);
        message.removeUserData();
      }

      consumer.commitSync(messages);
      consumer.unsubscribe(topicName);
    }
  }

  @Ignore
  @Test
  public void testPullConsumerAutoCommitStoresCommitContextsOnly() throws Exception {
    final String topicName = "topic_auto_commit_context_only";
    insertHistoricalData(100, 200);
    createTopic(topicName);

    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(host)
            .port(port)
            .consumerId("c_auto_commit")
            .consumerGroupId("cg_auto_commit")
            .autoCommit(true)
            .autoCommitIntervalMs(60_000L)
            .buildPullConsumer()) {
      consumer.open();
      consumer.subscribe(topicName);

      final List<SubscriptionMessage> messages = pollMessages(consumer);
      Assert.assertFalse(messages.isEmpty());
      messages.forEach(SubscriptionMessage::removeUserData);

      final SortedMap<Long, Set<SubscriptionCommitContext>> uncommittedCommitContexts =
          getUncommittedCommitContexts(consumer);
      Assert.assertFalse(uncommittedCommitContexts.isEmpty());

      final Object storedObject =
          uncommittedCommitContexts.values().iterator().next().iterator().next();
      Assert.assertTrue(storedObject instanceof SubscriptionCommitContext);
      Assert.assertFalse(storedObject instanceof SubscriptionMessage);

      consumer.unsubscribe(topicName);
    }
  }

  private void insertHistoricalData(final int start, final int end) {
    try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = start; i < end; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void createTopic(final String topicName) {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      session.createTopic(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private List<SubscriptionMessage> pollMessages(final SubscriptionPullConsumer consumer)
      throws Exception {
    for (int i = 0; i < 10; ++i) {
      final List<SubscriptionMessage> messages =
          consumer.poll(Duration.ofMillis(IoTDBSubscriptionITConstant.POLL_TIMEOUT_MS));
      if (!messages.isEmpty()) {
        return messages;
      }
      LockSupport.parkNanos(IoTDBSubscriptionITConstant.SLEEP_NS);
    }
    fail("Failed to poll subscription messages within the expected timeout.");
    throw new IllegalStateException("unreachable");
  }

  @SuppressWarnings("unchecked")
  private SortedMap<Long, Set<SubscriptionCommitContext>> getUncommittedCommitContexts(
      final SubscriptionPullConsumer consumer) throws Exception {
    final Field field =
        SubscriptionPullConsumer.class.getDeclaredField("uncommittedCommitContexts");
    field.setAccessible(true);
    return (SortedMap<Long, Set<SubscriptionCommitContext>>) field.get(consumer);
  }
}
