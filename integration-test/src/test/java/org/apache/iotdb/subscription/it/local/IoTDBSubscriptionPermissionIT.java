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

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.subscription.SubscriptionTreeSession;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePushConsumer;
import org.apache.iotdb.session.subscription.model.Subscription;
import org.apache.iotdb.session.subscription.model.Topic;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionPermissionIT extends AbstractSubscriptionLocalIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionPermissionIT.class);

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testMetaAccessControl() {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    final String username = "thulab";
    final String password = "passwd";

    // create user
    createUser(EnvFactory.getEnv(), username, password);

    // root user
    try (final SubscriptionTreeSession session = new SubscriptionTreeSession(host, port)) {
      session.open();
      // create topic
      final String topicName = "topic_root";
      session.createTopic(topicName);
      Assert.assertTrue(session.getTopic(topicName).isPresent());
      Assert.assertEquals(topicName, session.getTopic(topicName).get().getTopicName());
      // show topic
      final Optional<Topic> topic = session.getTopic(topicName);
      Assert.assertTrue(topic.isPresent());
      Assert.assertEquals(topicName, topic.get().getTopicName());
      // drop topic
      session.dropTopic(topicName);
      // show subscription
      final Set<Subscription> subscriptions = session.getSubscriptions(topicName);
      Assert.assertTrue(subscriptions.isEmpty());
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // normal user
    try (final SubscriptionTreeSession session =
        new SubscriptionTreeSession(
            host, port, username, password, SessionConfig.DEFAULT_MAX_FRAME_SIZE)) {
      session.open();
      // create topic
      String topicName = "topic_thulab";
      session.createTopic(topicName);
      fail();
    } catch (final Exception e) {

    }

    // normal user
    try (final SubscriptionTreeSession session =
        new SubscriptionTreeSession(
            host, port, username, password, SessionConfig.DEFAULT_MAX_FRAME_SIZE)) {
      session.open();
      // show topics
      session.getTopics();
      fail();
    } catch (final Exception e) {

    }

    // normal user
    try (final SubscriptionTreeSession session =
        new SubscriptionTreeSession(
            host, port, username, password, SessionConfig.DEFAULT_MAX_FRAME_SIZE)) {
      session.open();
      // show subscriptions
      session.getSubscriptions();
      fail();
    } catch (final Exception e) {

    }
  }

  @Test
  public void testRuntimeAccessControl() {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    final String topicName = "topic1";

    // create user
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        EnvFactory.getEnv(),
        Arrays.asList(
            "create role `admin`",
            "grant MAINTAIN on root.** to role `admin`",
            "create user `thulab` 'passwd'",
            "grant role `admin` to `thulab`",
            "create user `hacker` 'qwerty123'",
            "grant role `admin` to `hacker`"))) {
      return;
    }

    // root user
    try (final SubscriptionTreeSession session = new SubscriptionTreeSession(host, port)) {
      session.open();
      // create topic
      session.createTopic(topicName);
      Assert.assertTrue(session.getTopic(topicName).isPresent());
      Assert.assertEquals(topicName, session.getTopic(topicName).get().getTopicName());
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    final AtomicInteger rowCount = new AtomicInteger();
    try (final SubscriptionTreePushConsumer consumer1 =
            new SubscriptionTreePushConsumer.Builder()
                .host(host)
                .port(port)
                .username("thulab")
                .password("passwd")
                .consumerId("thulab_consumer_1")
                .consumerGroupId("thulab_consumer_group")
                .ackStrategy(AckStrategy.AFTER_CONSUME)
                .consumeListener(
                    message -> {
                      for (final SubscriptionSessionDataSet dataSet :
                          message.getSessionDataSetsHandler()) {
                        while (dataSet.hasNext()) {
                          dataSet.next();
                          rowCount.addAndGet(1);
                        }
                      }
                      return ConsumeResult.SUCCESS;
                    })
                .buildPushConsumer();
        final SubscriptionTreePushConsumer consumer2 =
            new SubscriptionTreePushConsumer.Builder()
                .host(host)
                .port(port)
                .username("thulab")
                .password("passwd")
                .consumerId("thulab_consumer_2")
                .consumerGroupId("thulab_consumer_group")
                .ackStrategy(AckStrategy.AFTER_CONSUME)
                .consumeListener(
                    message -> {
                      for (final SubscriptionSessionDataSet dataSet :
                          message.getSessionDataSetsHandler()) {
                        while (dataSet.hasNext()) {
                          dataSet.next();
                          rowCount.addAndGet(1);
                        }
                      }
                      return ConsumeResult.SUCCESS;
                    })
                .buildPushConsumer();
        final SubscriptionTreePushConsumer consumer3 =
            new SubscriptionTreePushConsumer.Builder()
                .host(host)
                .port(port)
                .username("hacker")
                .password("qwerty123")
                .consumerId("hacker_consumer")
                .consumerGroupId("thulab_consumer_group")
                .ackStrategy(AckStrategy.AFTER_CONSUME)
                .consumeListener(
                    message -> {
                      for (final SubscriptionSessionDataSet dataSet :
                          message.getSessionDataSetsHandler()) {
                        while (dataSet.hasNext()) {
                          dataSet.next();
                          rowCount.addAndGet(1);
                        }
                      }
                      return ConsumeResult.SUCCESS;
                    })
                .buildPushConsumer()) {

      consumer1.open();
      consumer1.subscribe(topicName);

      consumer2.open();
      consumer2.subscribe(topicName);

      consumer3.open();
      consumer3.subscribe(topicName);
      fail();
    } catch (final Exception e) {
    }
  }
}
