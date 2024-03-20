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

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.subscription.payload.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    int count = 0;
    session.executeNonQueryStatement("create topic topic1 with ('start-time'='now')");

    // insert some history data
    long currentTime = System.currentTimeMillis();
    for (int i = 0; i < 100; ++i) {
      session.executeNonQueryStatement(
          String.format("insert into root.db.d1(time, s) values (%s, 1)", i));
    }
    for (int i = 0; i < 100; ++i) {
      session.executeNonQueryStatement(
          String.format("insert into root.db.d2(time, s) values (%s, 1)", currentTime + i));
    }
    session.executeNonQueryStatement("flush");

    // subscription
    Map<String, String> consumerAttributes = new HashMap<>();
    consumerAttributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "cg1");
    consumerAttributes.put(ConsumerConstant.CONSUMER_ID_KEY, "c1");

    session.createConsumer(new ConsumerConfig(consumerAttributes));
    session.subscribe(Collections.singleton("topic1"));

    List<EnrichedTablets> enrichedTabletsList;
    while (true) {
      Thread.sleep(1000); // wait some time
      enrichedTabletsList = session.poll(Collections.singleton("topic1"));
      if (enrichedTabletsList.isEmpty()) {
        break;
      }
      Map<String, List<String>> topicNameToSubscriptionCommitIds = new HashMap<>();
      for (EnrichedTablets enrichedTablets : enrichedTabletsList) {
        for (Tablet tablet : enrichedTablets.getTablets()) {
          count += tablet.rowSize;
        }
        topicNameToSubscriptionCommitIds
            .computeIfAbsent(enrichedTablets.getTopicName(), (topicName) -> new ArrayList<>())
            .add(enrichedTablets.getSubscriptionCommitId());
      }
      session.commit(topicNameToSubscriptionCommitIds);
    }
    session.unsubscribe(Collections.singleton("topic1"));
    session.dropConsumer();
    session.close();

    System.out.println(count);
  }
}
