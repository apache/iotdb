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
import org.apache.iotdb.session.subscription.model.Topic;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionTopicIT extends AbstractSubscriptionLocalIT {

  @Test
  public void testBasicCreateTopic() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      // create topic
      String topicName = "topic1";
      session.createTopic(topicName);
      Assert.assertTrue(session.getTopic(topicName).isPresent());
      Assert.assertEquals(topicName, session.getTopic(topicName).get().getTopicName());

      // create topic
      topicName = "topic2";
      Properties properties = new Properties();
      properties.put("path", "root.**");
      properties.put("start-time", "2023-01-01");
      properties.put("end-time", "2023-12-31");
      properties.put("format", "TsFileHandler");
      session.createTopic(topicName, properties);
      Optional<Topic> topic = session.getTopic(topicName);
      Assert.assertTrue(topic.isPresent());
      Assert.assertEquals(topicName, topic.get().getTopicName());
      // verify topic parameters
      Assert.assertTrue(topic.get().getTopicAttributes().contains("path=root.**"));
      Assert.assertTrue(topic.get().getTopicAttributes().contains("start-time=2023-01-01"));
      Assert.assertTrue(topic.get().getTopicAttributes().contains("end-time=2023-12-31"));
      Assert.assertTrue(topic.get().getTopicAttributes().contains("format=TsFileHandler"));

    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testBasicCreateTopicIfNotExists() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      // create topic if not exits
      String topicName = "topic3";
      session.createTopicIfNotExists(topicName);
      Optional<Topic> topic = session.getTopic(topicName);
      Assert.assertTrue(topic.isPresent());
      Assert.assertEquals(topicName, topic.get().getTopicName());

      // create topic if not exits
      session.createTopicIfNotExists(topicName);
      topic = session.getTopic(topicName);
      Assert.assertTrue(topic.isPresent());
      Assert.assertEquals(topicName, topic.get().getTopicName());

      // create topic if not exits
      topicName = "topic4";
      Properties properties = new Properties();
      properties.put("path", "root.**");
      properties.put("start-time", "2023-01-01");
      properties.put("end-time", "2023-12-31");
      properties.put("format", "TsFileHandler");
      session.createTopicIfNotExists(topicName, properties);
      topic = session.getTopic(topicName);
      Assert.assertTrue(topic.isPresent());
      Assert.assertEquals(topicName, topic.get().getTopicName());
      // verify topic parameters
      Assert.assertTrue(topic.get().getTopicAttributes().contains("path=root.**"));
      Assert.assertTrue(topic.get().getTopicAttributes().contains("start-time=2023-01-01"));
      Assert.assertTrue(topic.get().getTopicAttributes().contains("end-time=2023-12-31"));
      Assert.assertTrue(topic.get().getTopicAttributes().contains("format=TsFileHandler"));

      // create topic if not exits
      properties.put("start-time", "2023-01-02");
      session.createTopicIfNotExists(topicName, properties);
      topic = session.getTopic(topicName);
      Assert.assertTrue(topic.isPresent());
      Assert.assertEquals(topicName, topic.get().getTopicName());
      // Verify Topic Parameters
      Assert.assertTrue(topic.get().getTopicAttributes().contains("path=root.**"));
      Assert.assertTrue(topic.get().getTopicAttributes().contains("start-time=2023-01-01"));
      Assert.assertFalse(topic.get().getTopicAttributes().contains("start-time=2023-01-02"));
      Assert.assertTrue(topic.get().getTopicAttributes().contains("end-time=2023-12-31"));
      Assert.assertTrue(topic.get().getTopicAttributes().contains("format=TsFileHandler"));
    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testBasicDropTopic() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      // create topic
      String topicName = "topic5";
      session.createTopic(topicName);

      // drop topic
      session.dropTopic(topicName);
      Assert.assertFalse(session.getTopic(topicName).isPresent());

    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testBasicDropTopicIfExists() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    try (final SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      // create topic
      String topicName = "topic6";
      session.createTopic(topicName);

      // drop topic if exists
      session.dropTopicIfExists(topicName);
      Assert.assertFalse(session.getTopic(topicName).isPresent());

      // drop topic if exists
      session.dropTopicIfExists(topicName);
      Assert.assertFalse(session.getTopic(topicName).isPresent());

    } catch (final Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
