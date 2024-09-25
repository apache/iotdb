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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionIdempotentIT extends AbstractSubscriptionLocalIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionIdempotentIT.class);

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testSubscribeOrUnsubscribeNonExistedTopicTest() {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // Subscribe non-existed topic
    final String topicName = "topic1";
    try (final SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(host)
            .port(port)
            .consumerId("c1")
            .consumerGroupId("cg1")
            .autoCommit(false)
            .buildPullConsumer()) {
      consumer.open();
      consumer.subscribe(topicName);
      fail();
    } catch (final Exception ignored) {
    } finally {
      LOGGER.info("consumer exiting...");
    }

    // Unsubscribe non-existed topic
    try (final SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(host)
            .port(port)
            .consumerId("c1")
            .consumerGroupId("cg1")
            .autoCommit(false)
            .buildPullConsumer()) {
      consumer.open();
      consumer.unsubscribe(topicName);
      fail();
    } catch (final Exception ignored) {
    } finally {
      LOGGER.info("consumer exiting...");
    }
  }

  @Test
  public void testSubscribeExistedSubscribedTopicTest() {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // Create topic
    final String topicName = "topic2";
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      session.createTopic(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    try (final SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(host)
            .port(port)
            .consumerId("c1")
            .consumerGroupId("cg1")
            .autoCommit(false)
            .buildPullConsumer()) {
      consumer.open();
      consumer.subscribe(topicName);
      // Subscribe existed subscribed topic
      consumer.subscribe(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } finally {
      LOGGER.info("consumer exiting...");
    }
  }

  @Test
  public void testUnsubscribeExistedNonSubscribedTopicTest() {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // Create topic
    final String topicName = "topic3";
    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      session.createTopic(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    try (final SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(host)
            .port(port)
            .consumerId("c1")
            .consumerGroupId("cg1")
            .autoCommit(false)
            .buildPullConsumer()) {
      consumer.open();
      // Unsubscribe existed non-subscribed topic
      consumer.unsubscribe(topicName);
    } catch (final Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } finally {
      LOGGER.info("consumer exiting...");
    }
  }
}
