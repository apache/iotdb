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

package org.apache.iotdb;

import org.apache.iotdb.isession.ISessionDataSet;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.SubscriptionMessage;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.SubscriptionSession;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SubscriptionSessionExample {

  private static Session session;

  private static final String LOCAL_HOST = "127.0.0.1";

  public static void main(String[] args) throws Exception {
    session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .build();
    session.open(false);

    // insert some history data
    long currentTime = System.currentTimeMillis();
    for (int i = 0; i < 100; ++i) {
      session.executeNonQueryStatement(
          String.format("insert into root.db.d1(time, s1, s2) values (%s, 1, 2)", i));
    }
    for (int i = 0; i < 100; ++i) {
      session.executeNonQueryStatement(
          String.format("insert into root.db.d2(time, s3, s4) values (%s, 3, 4)", currentTime + i));
    }
    for (int i = 0; i < 100; ++i) {
      session.executeNonQueryStatement(
          String.format("insert into root.sg.d2(time, s) values (%s, 5)", currentTime + 2 * i));
    }
    session.executeNonQueryStatement("flush");

    // create topic
    try (SubscriptionSession subscriptionSession = new SubscriptionSession(LOCAL_HOST, 6667)) {
      subscriptionSession.open();
      subscriptionSession.createTopic("topic1");
      subscriptionSession.createTopic("topic2");
      subscriptionSession.getTopics();
    }

    // subscription: property-style ctor
    Properties config = new Properties();
    config.put(ConsumerConstant.CONSUMER_ID_KEY, "c1");
    config.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
    try (SubscriptionPullConsumer consumer = new SubscriptionPullConsumer(config)) {
      consumer.open();
      consumer.subscribe("topic1");
      while (true) {
        Thread.sleep(1000); // wait some time
        List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
        if (messages.isEmpty()) {
          break;
        }
        for (SubscriptionMessage message : messages) {
          ISessionDataSet dataSet = message.getPayload();
          System.out.println(dataSet.getColumnNames());
          System.out.println(dataSet.getColumnTypes());
          while (dataSet.hasNext()) {
            System.out.println(dataSet.next());
          }
        }
        // auto commit
        consumer.unsubscribe("topic1");
      }
    }

    // subscription: builder-style ctor
    try (SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            // TODO: Currently, specific parameters must be set before general parameters.
            .autoCommit(false)
            .consumerId("c2")
            .consumerGroupId("cg2")
            .buildPullConsumer()) {
      consumer.open();
      consumer.subscribe("topic2");
      while (true) {
        Thread.sleep(1000); // wait some time
        List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
        if (messages.isEmpty()) {
          break;
        }
        for (SubscriptionMessage message : messages) {
          ISessionDataSet dataSet = message.getPayload();
          System.out.println(dataSet.getColumnNames());
          System.out.println(dataSet.getColumnTypes());
          while (dataSet.hasNext()) {
            System.out.println(dataSet.next());
          }
        }
        consumer.commitSync(messages);
        consumer.unsubscribe("topic2");
      }
    }

    // query
    SessionDataSet dataSet = session.executeQueryStatement("select ** from root.**");
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }
    session.close();
  }
}
