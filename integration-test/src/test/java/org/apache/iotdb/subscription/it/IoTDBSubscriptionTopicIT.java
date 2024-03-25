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

package org.apache.iotdb.subscription.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ISessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionMessage;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.SubscriptionSession;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSubscriptionTopicIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSubscriptionTopicIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTopicPathSubscription() {
    // insert some history data
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
      }
      for (int i = 100; i < 200; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d2(time, s) values (%s, 1)", i));
      }
      for (int i = 200; i < 300; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d3(time, t) values (%s, 1)", i));
      }
      for (int i = 300; i < 400; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.t1(time, s1) values (%s, 1)", i));
      }
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // create topic
    String host = EnvFactory.getEnv().getIP();
    int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      Properties config = new Properties();
      config.put(TopicConstant.PATH_KEY, "root.db.*.s");
      session.createTopic("topic1", config);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // subscription
    int count = 0;
    try (SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(host)
            .port(port)
            .consumerId("c1")
            .consumerGroupId("cg1")
            .buildPullConsumer()) {
      consumer.subscribe("topic1");
      while (true) {
        Thread.sleep(1000); // wait some time
        List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(100));
        if (messages.isEmpty()) {
          break;
        }
        for (SubscriptionMessage message : messages) {
          ISessionDataSet dataSet = message.getPayload();
          List<String> columnNameList = dataSet.getColumnNames();
          Assert.assertEquals(3, columnNameList.size());
          Assert.assertTrue(columnNameList.contains("root.db.d1.s"));
          Assert.assertTrue(columnNameList.contains("root.db.d2.s"));
          while (dataSet.hasNext()) {
            dataSet.next();
            count += 1;
          }
        }
        consumer.commitSync(messages);
        consumer.unsubscribe("topic1");
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    Assert.assertEquals(200, count);
  }

  @Test
  public void testTopicTimeSubscription() {
    // insert some history data
    long currentTime = System.currentTimeMillis();
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
      }
      for (int i = 0; i < 100; ++i) {
        session.executeNonQueryStatement(
            String.format("insert into root.db.d2(time, s) values (%s, 1)", currentTime + i));
      }
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // create topic
    String host = EnvFactory.getEnv().getIP();
    int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      Properties config = new Properties();
      config.put(TopicConstant.START_TIME_KEY, currentTime);
      session.createTopic("topic1", config);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // subscription
    int count = 0;
    try (SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(host)
            .port(port)
            .consumerId("c1")
            .consumerGroupId("cg1")
            .buildPullConsumer()) {
      consumer.subscribe("topic1");
      while (true) {
        Thread.sleep(1000); // wait some time
        List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(100));
        if (messages.isEmpty()) {
          break;
        }
        for (SubscriptionMessage message : messages) {
          ISessionDataSet dataSet = message.getPayload();
          while (dataSet.hasNext()) {
            dataSet.next();
            count += 1;
          }
        }
        consumer.commitSync(messages);
        consumer.unsubscribe("topic1");
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    Assert.assertEquals(100, count);
  }

  @Test
  public void testTopicProcessorSubscription() {
    // insert some history data
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "insert into root.db.d1 (time, at1) values (1000, 1), (1500, 2), (2000, 3), (2500, 4), (3000, 5)");
      session.executeNonQueryStatement("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // create topic
    String host = EnvFactory.getEnv().getIP();
    int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (SubscriptionSession session = new SubscriptionSession(host, port)) {
      session.open();
      Properties config = new Properties();
      config.put("processor", "tumbling-time-sampling-processor");
      config.put("processor.tumbling-time.interval-seconds", "1");
      config.put("processor.down-sampling.split-file", "true");
      session.createTopic("topic1", config);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // subscription
    int count = 0;
    try (SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .consumerId("c1")
            .consumerGroupId("cg1")
            .buildPullConsumer()) {
      consumer.subscribe("topic1");
      while (true) {
        Thread.sleep(1000); // wait some time
        List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(100));
        if (messages.isEmpty()) {
          break;
        }
        for (SubscriptionMessage message : messages) {
          ISessionDataSet dataSet = message.getPayload();
          while (dataSet.hasNext()) {
            dataSet.next();
            count += 1;
          }
        }
        consumer.commitSync(messages);
        consumer.unsubscribe("topic1");
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    Assert.assertEquals(3, count);
  }
}
