/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.db.subscription.broker.SubscriptionBroker;
import org.apache.iotdb.rpc.subscription.payload.request.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SubscriptionBrokerAgent {

  private Map<String, SubscriptionBroker> subscriptionBrokerMap;

  public Iterable<EnrichedTablets> poll(ConsumerConfig consumerConfig) {
    SubscriptionBroker broker = subscriptionBrokerMap.get(consumerConfig.getConsumerGroupID());
    if (Objects.isNull(broker)) {
      // TODO: handle error
      return Collections.emptyList();
    }
    return broker.poll(consumerConfig);
  }

  public void commit(
      ConsumerConfig consumerConfig, List<Pair<String, Integer>> committerKeyAndCommitIds) {
    SubscriptionBroker broker = subscriptionBrokerMap.get(consumerConfig.getConsumerGroupID());
    if (Objects.isNull(broker)) {
      // TODO: handle error
      return;
    }
    broker.commit(committerKeyAndCommitIds);
  }
}
