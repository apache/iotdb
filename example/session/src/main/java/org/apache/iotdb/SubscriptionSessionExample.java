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
import org.apache.iotdb.session.subscription.PollMessage;
import org.apache.iotdb.session.subscription.PollMessages;
import org.apache.iotdb.session.subscription.SubscriptionPullConsumer;

import java.time.Duration;
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

    // subscription
    Properties config = new Properties();
    config.put(ConsumerConstant.CONSUMER_ID_KEY, "c1");
    config.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
    try (SubscriptionPullConsumer consumer = new SubscriptionPullConsumer(config)) {
      consumer.createTopic("topic1");
      consumer.subscribe("topic1");
      Thread.sleep(1000); // wait some time
      PollMessages pollMessages = consumer.poll(Duration.ofMillis(100));
      for (PollMessage message : pollMessages) {
        message.debug();
        message.commit();
      }
      consumer.unsubscribe("topic1");
    }
  }

  public static void query(String[] args) throws Exception {
    session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .build();
    session.open(false);
    SessionDataSet dataSet = session.executeQueryStatement("select ** from root.**");
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }
    session.close();
  }
}
