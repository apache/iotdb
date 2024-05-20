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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.SubscriptionMessage;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.SubscriptionSessionDataSets;

import java.time.Duration;
import java.util.Collections;
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

    // Insert some historical data
    long currentTime = System.currentTimeMillis();
    for (int i = 0; i < 100; ++i) {
      session.executeNonQueryStatement(
          String.format("insert into root.db.d1(time, s1, s2) values (%s, 1, 2)", i));
      session.executeNonQueryStatement(
          String.format("insert into root.db.d2(time, s3, s4) values (%s, 3, 4)", currentTime + i));
      session.executeNonQueryStatement(
          String.format("insert into root.sg.d3(time, s5) values (%s, 5)", currentTime + 2 * i));
    }
    session.executeNonQueryStatement("flush");

    // Create topic
    final String topic1 = "topic1";
    final String topic2 = "`topic2`";
    try (SubscriptionSession subscriptionSession = new SubscriptionSession(LOCAL_HOST, 6667)) {
      subscriptionSession.open();
      subscriptionSession.createTopic(topic1);
      subscriptionSession.createTopic(topic2);
    }

    // Subscription: property-style ctor
    Properties config = new Properties();
    config.put(ConsumerConstant.CONSUMER_ID_KEY, "c1");
    config.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
    SubscriptionPullConsumer consumer1 = new SubscriptionPullConsumer(config);
    consumer1.open();
    consumer1.subscribe(topic1);
    while (true) {
      Thread.sleep(1000); // Wait for some time
      List<SubscriptionMessage> messages = consumer1.poll(Duration.ofMillis(10000));
      if (messages.isEmpty()) {
        break;
      }
      for (SubscriptionMessage message : messages) {
        SubscriptionSessionDataSets payload = (SubscriptionSessionDataSets) message.getPayload();
        for (SubscriptionSessionDataSet dataSet : payload) {
          System.out.println(dataSet.getColumnNames());
          System.out.println(dataSet.getColumnTypes());
          while (dataSet.hasNext()) {
            System.out.println(dataSet.next());
          }
        }
      }
      // Auto commit
    }

    // Show topics and subscriptions
    try (SubscriptionSession subscriptionSession = new SubscriptionSession(LOCAL_HOST, 6667)) {
      subscriptionSession.open();
      subscriptionSession.getTopics().forEach((System.out::println));
      subscriptionSession.getSubscriptions().forEach((System.out::println));
    }

    consumer1.unsubscribe(topic1);
    consumer1.close();

    // Subscription: builder-style ctor
    try (SubscriptionPullConsumer consumer2 =
        new SubscriptionPullConsumer.Builder()
            .consumerId("c2")
            .consumerGroupId("cg2")
            .autoCommit(false)
            .buildPullConsumer()) {
      consumer2.open();
      consumer2.subscribe(topic2);
      while (true) {
        Thread.sleep(1000); // wait some time
        List<SubscriptionMessage> messages =
            consumer2.poll(Collections.singleton(topic2), Duration.ofMillis(10000));
        if (messages.isEmpty()) {
          break;
        }
        for (SubscriptionMessage message : messages) {
          SubscriptionSessionDataSets payload = (SubscriptionSessionDataSets) message.getPayload();
          for (SubscriptionSessionDataSet dataSet : payload) {
            System.out.println(dataSet.getColumnNames());
            System.out.println(dataSet.getColumnTypes());
            while (dataSet.hasNext()) {
              System.out.println(dataSet.next());
            }
          }
        }
        consumer2.commitSync(messages);
      }
      consumer2.unsubscribe(topic2);
    }

    // Query
    SessionDataSet dataSet = session.executeQueryStatement("select ** from root.**");
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }
    session.close();
  }
}
