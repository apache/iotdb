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

import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class PullConsumeLatency {
  private static final String HOST = "127.0.0.1";
  private static final int SRC_PORT = 6667;

  public static void main(String[] args) throws Exception {
    final String topicName = "topic1";
    try (final SubscriptionSession session = new SubscriptionSession(HOST, SRC_PORT)) {
      session.open();
      final Properties properties = new Properties();
      properties.setProperty(TopicConstant.PATH_KEY, "root.test.g_0.*.s_75");
      properties.setProperty(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
      session.createTopic(topicName, properties);
    }
    final SubscriptionPullConsumer consumer =
        new SubscriptionPullConsumer.Builder()
            .host(HOST)
            .port(SRC_PORT)
            .consumerId("pull_consumer")
            .consumerGroupId("dataset")
            .autoCommit(false)
            .buildPullConsumer();
    consumer.open();
    consumer.subscribe(topicName);
    long currentTime = System.currentTimeMillis();
    final long endTime = currentTime + Duration.ofSeconds(1200).toMillis();
    while (System.currentTimeMillis() < endTime) {
      final List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(3000));
      for (final SubscriptionMessage message : messages) {
        System.out.println(message.getTsFileHandler().getFile().getName());
        //        for (final Iterator<Tablet> it =
        // message.getSessionDataSetsHandler().tabletIterator();
        //            it.hasNext(); ) {
        //          it.next();
        //        }
        System.out.println(
            String.format(String.valueOf(new Date()))
                + " | "
                + (System.currentTimeMillis() - currentTime));
        currentTime = System.currentTimeMillis();
      }
      consumer.commitSync(messages);
    }
    consumer.close();
  }
}
