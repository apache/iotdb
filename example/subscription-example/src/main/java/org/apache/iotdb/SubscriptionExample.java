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

import org.apache.iotdb.subscription.SubscriptionFactory;
import org.apache.iotdb.subscription.api.SubscriptionConfiguration;
import org.apache.iotdb.subscription.api.consumer.push.IPushConsumer;
import org.apache.iotdb.subscription.api.strategy.disorder.IntolerableStrategy;
import org.apache.iotdb.subscription.api.strategy.topic.SingleTopicStrategy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SubscriptionExample {
  public static void main(String[] args) {
    System.out.println("Start to PushConsumer for Subscription, 10 MINUTES");
    startPushConsumer();
  }

  private static void startPushConsumer() {
    try (IPushConsumer pushConsumer =
        (new SubscriptionFactory())
            .createPushConsumer(
                new SubscriptionConfiguration.Builder()
                    .host("localhost")
                    .port(6667)
                    .localHost("localhost")
                    .localPort(9997)
                    .username("root")
                    .password("root")
                    .topicStrategy(new SingleTopicStrategy("root.sg1"))
                    .disorderHandlingStrategy(new IntolerableStrategy())
                    .build())) {
      pushConsumer.registerDataArrivalListener(
          (data) -> {
            System.out.println("Start Data arrived----------------------------------");
            data.forEach(
                (item) -> {
                  System.out.printf(
                      "Time: %s, paths: %s, values: ",
                      item.getTime(), String.join("、", item.getColumnNames()));
                  if (item.getColumnNames().size() > 0) {
                    for (int i = 0; i < item.getColumnNames().size(); i++) {
                      System.out.printf("%s, ", item.Data().getInt(i));
                    }
                  }
                  System.out.println();
                });
            System.out.println("End Data arrived----------------------------------");
          });
      pushConsumer.openSubscription();
      pushConsumer.start();
      System.out.println("PushConsumer started");
      CountDownLatch latch = new CountDownLatch(1);
      latch.await(10, TimeUnit.MINUTES);
      pushConsumer.stop();
      pushConsumer.closeSubscription();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
