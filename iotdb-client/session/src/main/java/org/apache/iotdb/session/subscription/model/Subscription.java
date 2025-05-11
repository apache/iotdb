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

package org.apache.iotdb.session.subscription.model;

public class Subscription {

  private final String subscriptionId;
  private final String topicName;
  private final String consumerGroupId;
  private final String consumerIds;

  public Subscription(
      final String subscriptionId,
      final String topicName,
      final String consumerGroupId,
      final String consumerIds) {
    this.subscriptionId = subscriptionId;
    this.topicName = topicName;
    this.consumerGroupId = consumerGroupId;
    this.consumerIds = consumerIds;
  }

  public String getSubscriptionId() {
    return subscriptionId;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public String getConsumerIds() {
    return consumerIds;
  }

  @Override
  public String toString() {
    return "Subscription{subscriptionId="
        + subscriptionId
        + ", topicName="
        + topicName
        + ", consumerGroupId="
        + consumerGroupId
        + ", consumerIds="
        + consumerIds
        + "}";
  }
}
