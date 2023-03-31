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

import org.apache.iotdb.subscription.SubscriptionFactory;
import org.apache.iotdb.subscription.api.SubscriptionConfiguration;
import org.apache.iotdb.subscription.api.consumer.push.IPushConsumer;
import org.apache.iotdb.subscription.api.strategy.disorder.IntolerableStrategy;
import org.apache.iotdb.subscription.api.strategy.topic.SingleTopicStrategy;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SubscroptionTest {
  @Test
  public void testPush() {
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
            System.out.println(data);
          });
      pushConsumer.start();
      CountDownLatch latch = new CountDownLatch(1);
      latch.await(3, TimeUnit.SECONDS);
      pushConsumer.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
