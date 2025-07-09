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

package org.apache.iotdb.commons.subscription.meta.subscription;

import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** SubscriptionMeta is created for show subscription and is not stored in meta keeper. */
public class SubscriptionMeta {

  private TopicMeta topicMeta;
  private String consumerGroupId;
  private Set<String> consumerIds;
  private Long creationTime;

  private SubscriptionMeta() {
    // Empty constructor
  }

  public SubscriptionMeta(
      TopicMeta topicMeta, String consumerGroupId, Set<String> consumerIds, Long creationTime) {
    this.topicMeta = topicMeta;
    this.consumerGroupId = consumerGroupId;
    this.consumerIds = consumerIds;
    this.creationTime = creationTime;
  }

  public TopicMeta getTopicMeta() {
    return topicMeta;
  }

  public String getConsumerGroupId() {
    return consumerGroupId;
  }

  public Set<String> getConsumerIds() {
    return consumerIds;
  }

  public Optional<Long> getCreationTime() {
    return Objects.nonNull(creationTime) ? Optional.of(creationTime) : Optional.empty();
  }

  public String getSubscriptionId() {
    final StringBuilder subscriptionId =
        new StringBuilder(topicMeta.getTopicName() + "_" + consumerGroupId);
    getCreationTime().ifPresent(creationTime -> subscriptionId.append("_").append(creationTime));
    return subscriptionId.toString();
  }
}
