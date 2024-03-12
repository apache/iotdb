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

package org.apache.iotdb.db.subscription.meta;

import org.apache.iotdb.rpc.subscription.payload.request.ConsumerConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConsumerGroupMeta {

  private String consumerGroupID;
  private final Map<String, Set<String>> topicNameToSubscribedConsumers = new HashMap<>();
  private final Map<String, ConsumerConfig> consumers = new HashMap<>();

  public ConsumerGroupMeta(ConsumerConfig consumerConfig) {
    this.consumerGroupID = consumerConfig.getConsumerGroupID();
    this.consumers.put(consumerConfig.getConsumerClientID(), consumerConfig);
  }

  public void addConsumer(ConsumerConfig consumerConfig) {
    consumers.put(consumerConfig.getConsumerClientID(), consumerConfig);
  }

  public void removeConsumer(String consumerClientID) {
    consumers.remove(consumerClientID);
    topicNameToSubscribedConsumers.forEach(
        (topicName, subscribedConsumers) -> subscribedConsumers.remove(consumerClientID));
  }

  /** @return true if new subscription is created */
  public boolean subscribe(String consumerClientID, String topicName) {
    Set<String> subscribedConsumers =
        topicNameToSubscribedConsumers.getOrDefault(topicName, new HashSet<>());
    boolean isNewSubscription = subscribedConsumers.isEmpty();
    subscribedConsumers.add(consumerClientID);
    topicNameToSubscribedConsumers.put(topicName, subscribedConsumers);
    return isNewSubscription;
  }

  public void unsubscribe(String consumerClientID, String topicName) {
    Set<String> subscribedConsumers =
        topicNameToSubscribedConsumers.getOrDefault(topicName, new HashSet<>());
    subscribedConsumers.remove(consumerClientID);
    if (subscribedConsumers.isEmpty()) {
      topicNameToSubscribedConsumers.remove(topicName);
    } else {
      topicNameToSubscribedConsumers.put(topicName, subscribedConsumers);
    }
  }

  public boolean isEmpty() {
    return consumers.isEmpty();
  }

  public Set<String> subscribedTopics(String consumerClientID) {
    Set<String> topicNames = new HashSet<>();
    topicNameToSubscribedConsumers.forEach(
        (topicName, subscribedConsumers) -> {
          if (subscribedConsumers.contains(consumerClientID)) {
            topicNames.add(topicName);
          }
        });
    return topicNames;
  }

  public boolean isConsumerExisted(String consumerClientID) {
    return consumers.containsKey(consumerClientID);
  }
}
