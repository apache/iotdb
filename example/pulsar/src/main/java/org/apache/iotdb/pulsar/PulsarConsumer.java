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
package org.apache.iotdb.pulsar;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public class PulsarConsumer {
  private static final String SERVICE_URL = "pulsar://localhost:6650";
  // Specify the number of consumers
  private static final int CONSUMER_NUM = 5;
  private List<Consumer<?>> consumerList;

  public PulsarConsumer(List<Consumer<?>> consumerList) {
    this.consumerList = consumerList;
  }

  public void consumeInParallel() {
    ExecutorService executor = Executors.newFixedThreadPool(CONSUMER_NUM);
    for (int i = 0; i < consumerList.size(); i++) {
      PulsarConsumerThread consumerExecutor = new PulsarConsumerThread(consumerList.get(i));
      executor.submit(consumerExecutor);
    }
  }

  public static void main(String[] args) throws PulsarClientException {
    PulsarClient client = PulsarClient.builder()
        .serviceUrl(SERVICE_URL)
        .build();

    List<Consumer<?>> consumerList = new ArrayList<>();
    for (int i = 0; i < CONSUMER_NUM; i++) {
      // In shared subscription mode, multiple consumers can attach to the same subscription
      // and message are delivered in a round robin distribution across consumers.
      Consumer<byte[]> consumer = client.newConsumer()
          .topic(Constant.TOPIC_NAME)
          .subscriptionName("my-subscription")
          .subscriptionType(SubscriptionType.Shared)
          .subscribe();
      consumerList.add(consumer);
    }
    PulsarConsumer pulsarConsumer = new PulsarConsumer(consumerList);
    pulsarConsumer.consumeInParallel();
  }
}
