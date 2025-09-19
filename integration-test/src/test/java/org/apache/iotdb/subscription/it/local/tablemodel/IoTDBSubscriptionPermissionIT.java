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

package org.apache.iotdb.subscription.it.local.tablemodel;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePushConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePushConsumerBuilder;
import org.apache.iotdb.session.subscription.model.Subscription;
import org.apache.iotdb.session.subscription.model.Topic;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.subscription.it.local.AbstractSubscriptionLocalIT;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTableNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.assertTableTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionPermissionIT extends AbstractSubscriptionLocalIT {

  @Override
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    super.setUp();
  }

  @Test
  public void testMetaAccessControl() {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    final String username = "thulab";
    final String password = "passwd123456";

    // create user
    createUser(EnvFactory.getEnv(), username, password);

    // root user
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
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
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            .build()) {
      // create topic
      final String topicName = "topic_thulab";
      session.createTopic(topicName);
      fail();
    } catch (final Exception ignored) {

    }

    // normal user
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            .build()) {
      // show topics
      session.getTopics();
      fail();
    } catch (final Exception ignored) {

    }

    // normal user
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            .build()) {
      // show subscriptions
      session.getSubscriptions();
      fail();
    } catch (final Exception ignored) {

    }
  }

  /**
   * Tests runtime access control in the same consumer group.
   *
   * <p>In IoTDB subscriptions, all consumers in one group must use identical credentials when
   * subscribing to the same topic. This test creates a topic and three consumers:
   *
   * <p>
   *
   * <ul>
   *   <li>consumer1 and consumer2 use "thulab:passwd".
   *   <li>consumer3 uses "hacker:qwerty123".
   * </ul>
   *
   * <p>Since consumer3 uses different credentials, it should be rejected.
   */
  @Test
  public void testRuntimeAccessControl() {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    final String topicName = "topic1";

    // create user
    TestUtils.executeNonQueries(
        EnvFactory.getEnv(),
        Arrays.asList("create user `thulab` 'passwd123456'", "create user `hacker` 'qwerty123456'"),
        null);

    // root user
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      // create topic
      session.createTopic(topicName);
      Assert.assertTrue(session.getTopic(topicName).isPresent());
      Assert.assertEquals(topicName, session.getTopic(topicName).get().getTopicName());
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    final AtomicInteger rowCount = new AtomicInteger();
    try (final ISubscriptionTablePushConsumer consumer1 =
            new SubscriptionTablePushConsumerBuilder()
                .host(host)
                .port(port)
                .username("thulab")
                .password("passwd123456")
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
                .build();
        final ISubscriptionTablePushConsumer consumer2 =
            new SubscriptionTablePushConsumerBuilder()
                .host(host)
                .port(port)
                .username("thulab")
                .password("passwd123456")
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
                .build();
        final ISubscriptionTablePushConsumer consumer3 =
            new SubscriptionTablePushConsumerBuilder()
                .host(host)
                .port(port)
                .username("hacker")
                .password("qwerty123456")
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
                .build()) {

      consumer1.open();
      consumer1.subscribe(topicName);
      consumer2.open();
      consumer2.subscribe(topicName);
      consumer3.open();
      consumer3.subscribe(topicName);

      fail();
    } catch (final Exception ignored) {
    }
  }

  /**
   * Tests strict runtime access control in the same consumer group.
   *
   * <p>In IoTDB subscriptions, all consumers in one group must use identical credentials. This test
   * creates two consumers with "thulab:passwd" and one with "hacker:qwerty123". Since the latter
   * does not match the required credentials, it should be rejected.
   */
  @Test
  public void testStrictRuntimeAccessControl() {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // create user
    TestUtils.executeNonQueries(
        EnvFactory.getEnv(),
        Arrays.asList("create user `thulab` 'passwd123456'", "create user `hacker` 'qwerty123456'"),
        null);

    final AtomicInteger rowCount = new AtomicInteger();
    try (final ISubscriptionTablePushConsumer consumer1 =
            new SubscriptionTablePushConsumerBuilder()
                .host(host)
                .port(port)
                .username("thulab")
                .password("passwd123456")
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
                .build();
        final ISubscriptionTablePushConsumer consumer2 =
            new SubscriptionTablePushConsumerBuilder()
                .host(host)
                .port(port)
                .username("thulab")
                .password("passwd123456")
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
                .build();
        final ISubscriptionTablePushConsumer consumer3 =
            new SubscriptionTablePushConsumerBuilder()
                .host(host)
                .port(port)
                .username("hacker")
                .password("qwerty123456")
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
                .build()) {

      consumer1.open();
      consumer2.open();
      consumer3.open();

      fail();
    } catch (final Exception ignored) {
    }
  }

  @Test
  public void testTablePermission() {
    createUser(EnvFactory.getEnv(), "test", "test123123456");

    assertTableNonQueryTestFail(
        EnvFactory.getEnv(),
        "create topic topic1",
        "803: Access Denied: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456",
        null);
    assertTableTestFail(
        EnvFactory.getEnv(),
        "show topics",
        "803: Access Denied: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456",
        null);
    assertTableTestFail(
        EnvFactory.getEnv(),
        "show subscriptions",
        "803: Access Denied: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456",
        null);
    assertTableNonQueryTestFail(
        EnvFactory.getEnv(),
        "drop topic topic1",
        "803: Access Denied: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456",
        null);
  }
}
