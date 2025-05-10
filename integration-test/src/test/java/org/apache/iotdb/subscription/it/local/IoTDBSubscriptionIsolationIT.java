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
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.ISubscriptionTreeSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumerBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionIsolationIT extends AbstractSubscriptionLocalIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionIsolationIT.class);

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testTopicIsolation() throws Exception {
    final String treeTopicName = "treeTopic";
    final String tableTopicName = "tableTopic";

    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // create tree topic
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder().host(host).port(port).build()) {
      session.open();
      session.createTopic(treeTopicName);
    }

    // create table topic
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      session.createTopic(tableTopicName);
    }

    // show tree topic on tree session
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder().host(host).port(port).build()) {
      session.open();
      Assert.assertEquals(1, session.getTopics().size());
      Assert.assertTrue(session.getTopic(treeTopicName).isPresent());
      Assert.assertFalse(session.getTopic(tableTopicName).isPresent());
    }

    // show table topic on table session
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      Assert.assertEquals(1, session.getTopics().size());
      Assert.assertTrue(session.getTopic(tableTopicName).isPresent());
      Assert.assertFalse(session.getTopic(treeTopicName).isPresent());
    }

    // drop table topic on tree session
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder().host(host).port(port).build()) {
      session.open();
      try {
        session.dropTopic(tableTopicName);
      } catch (final Exception ignored) {
      }
    }

    // drop tree topic on table session
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      try {
        session.dropTopic(treeTopicName);
      } catch (final Exception ignored) {
      }
    }

    // drop tree topic on tree session
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder().host(host).port(port).build()) {
      session.open();
      session.dropTopic(treeTopicName);
    }

    // drop table topic on table session
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      session.dropTopic(tableTopicName);
    }
  }

  @Test
  public void testSubscriptionIsolation() throws Exception {
    final String treeTopicName = "treeTopic";
    final String tableTopicName = "tableTopic";

    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // create tree topic
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder().host(host).port(port).build()) {
      session.open();
      session.createTopic(treeTopicName);
    }

    // create table topic
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      session.createTopic(tableTopicName);
    }

    // subscribe table topic on tree consumer
    try (final ISubscriptionTreePullConsumer consumer =
        new SubscriptionTreePullConsumerBuilder().host(host).port(port).build()) {
      consumer.open();
      try {
        consumer.subscribe(tableTopicName);
      } catch (final Exception ignored) {
      }
    }

    // subscribe tree topic on table consumer
    try (final ISubscriptionTablePullConsumer consumer =
        new SubscriptionTablePullConsumerBuilder().host(host).port(port).build()) {
      consumer.open();
      try {
        consumer.subscribe(treeTopicName);
      } catch (final Exception ignored) {
      }
    }

    // subscribe tree topic on tree consumer
    try (final ISubscriptionTreePullConsumer consumer =
        new SubscriptionTreePullConsumerBuilder().host(host).port(port).build()) {
      consumer.open();
      consumer.subscribe(treeTopicName);
    }

    // subscribe table topic on table consumer
    try (final ISubscriptionTablePullConsumer consumer =
        new SubscriptionTablePullConsumerBuilder().host(host).port(port).build()) {
      consumer.open();
      consumer.subscribe(tableTopicName);
    }
  }
}
