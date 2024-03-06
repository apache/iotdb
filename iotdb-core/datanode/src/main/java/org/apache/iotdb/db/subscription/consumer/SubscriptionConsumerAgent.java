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

package org.apache.iotdb.db.subscription.consumer;

import org.apache.iotdb.rpc.subscription.payload.request.ConsumerConfig;

import java.util.List;

public class SubscriptionConsumerAgent {

  public void createConsumer(ConsumerConfig consumerConfig) {}

  private void createConsumerGroup() {}

  public void dropConsumer(ConsumerConfig consumerConfig) {}

  private void dropConsumerGroup() {}

  public void subscribe(List<String> topicNames) {
    // TODO: handle nonexistence of topics
  }

  public void unsubscribe(List<String> topicNames) {
    // TODO: handle nonexistence of topics
  }

  //////////////////////////// singleton ////////////////////////////

  private static class SubscriptionConsumerAgentHolder {

    private static final SubscriptionConsumerAgent INSTANCE = new SubscriptionConsumerAgent();

    private SubscriptionConsumerAgentHolder() {
      // empty constructor
    }
  }

  public static SubscriptionConsumerAgent getInstance() {
    return SubscriptionConsumerAgent.SubscriptionConsumerAgentHolder.INSTANCE;
  }

  private SubscriptionConsumerAgent() {
    // empty constructor
  }
}
