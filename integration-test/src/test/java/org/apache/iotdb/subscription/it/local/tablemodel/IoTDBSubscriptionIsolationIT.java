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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.ISubscriptionTreeSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumerBuilder;
import org.apache.iotdb.subscription.it.local.AbstractSubscriptionLocalIT;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionIsolationIT extends AbstractSubscriptionLocalIT {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testTopicIsolation() throws Exception {
    final String topicName = "topic";

    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    final Properties treeTopicProperties = new Properties();
    treeTopicProperties.setProperty(TopicConstant.PATH_KEY, "root.tree.**");
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder().host(host).port(port).build()) {
      session.open();
      session.createTopic(topicName, treeTopicProperties);

      final Properties alteredProperties = new Properties();
      alteredProperties.setProperty(TopicConstant.PATH_KEY, "root.tree_altered.**");
      session.alterTopic(topicName, alteredProperties);
    }

    final Properties tableTopicProperties = new Properties();
    tableTopicProperties.setProperty(TopicConstant.DATABASE_KEY, "table_db");
    tableTopicProperties.setProperty(TopicConstant.TABLE_KEY, "table_name");
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      session.createTopic(topicName, tableTopicProperties);

      final Properties alteredProperties = new Properties();
      alteredProperties.setProperty(TopicConstant.DATABASE_KEY, "table_db_altered");
      session.alterTopic(topicName, alteredProperties);
    }

    // show topic on tree session
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder().host(host).port(port).build()) {
      session.open();
      Assert.assertEquals(1, session.getTopics().size());
      Assert.assertTrue(session.getTopic(topicName).isPresent());
      Assert.assertTrue(
          session.getTopic(topicName).get().getTopicAttributes().contains("root.tree_altered.**"));
      Assert.assertFalse(
          session.getTopic(topicName).get().getTopicAttributes().contains("table_db_altered"));
    }

    // show topic on table session
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      Assert.assertEquals(1, session.getTopics().size());
      Assert.assertTrue(session.getTopic(topicName).isPresent());
      Assert.assertTrue(
          session.getTopic(topicName).get().getTopicAttributes().contains("table_db_altered"));
      Assert.assertFalse(
          session.getTopic(topicName).get().getTopicAttributes().contains("root.tree_altered.**"));
    }

    // Dropping the tree-model topic must not affect the same-named table-model topic.
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder().host(host).port(port).build()) {
      session.open();
      session.dropTopic(topicName);
      Assert.assertFalse(session.getTopic(topicName).isPresent());
    }

    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      Assert.assertTrue(session.getTopic(topicName).isPresent());
      session.dropTopic(topicName);
      Assert.assertFalse(session.getTopic(topicName).isPresent());
    }
  }

  @Test
  public void testSubscriptionIsolation() throws Exception {
    final String topicName = "topic";

    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // create tree topic
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder().host(host).port(port).build()) {
      session.open();
      session.createTopic(topicName);
    }

    // create table topic
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      session.createTopic(topicName);
    }

    final ISubscriptionTreePullConsumer treeConsumer =
        new SubscriptionTreePullConsumerBuilder()
            .host(host)
            .port(port)
            .consumerId("tree_consumer")
            .consumerGroupId("tree_consumer_group")
            .build();
    treeConsumer.open();
    treeConsumer.subscribe(topicName);

    final ISubscriptionTablePullConsumer tableConsumer =
        new SubscriptionTablePullConsumerBuilder()
            .host(host)
            .port(port)
            .consumerId("table_consumer")
            .consumerGroupId("table_consumer_group")
            .build();
    tableConsumer.open();
    tableConsumer.subscribe(topicName);

    // show subscription on tree session
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder().host(host).port(port).build()) {
      session.open();
      Assert.assertEquals(1, session.getSubscriptions().size());
      Assert.assertEquals(1, session.getSubscriptions(topicName).size());
      Assert.assertEquals(
          "tree_consumer_group",
          session.getSubscriptions(topicName).iterator().next().getConsumerGroupId());
    }

    // show subscription on table session
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      Assert.assertEquals(1, session.getSubscriptions().size());
      Assert.assertEquals(1, session.getSubscriptions(topicName).size());
      Assert.assertEquals(
          "table_consumer_group",
          session.getSubscriptions(topicName).iterator().next().getConsumerGroupId());
    }

    // Unsubscribing the tree-model topic must not affect the same-named table-model subscription.
    treeConsumer.unsubscribe(topicName);
    try (final ISubscriptionTreeSession session =
        new SubscriptionTreeSessionBuilder().host(host).port(port).build()) {
      session.open();
      Assert.assertEquals(0, session.getSubscriptions(topicName).size());
    }
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      Assert.assertEquals(1, session.getSubscriptions(topicName).size());
    }

    tableConsumer.unsubscribe(topicName);
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      Assert.assertEquals(0, session.getSubscriptions(topicName).size());
    }

    treeConsumer.close();
    tableConsumer.close();
  }
}
