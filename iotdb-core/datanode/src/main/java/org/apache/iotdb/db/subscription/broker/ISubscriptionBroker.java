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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public interface ISubscriptionBroker {

  List<SubscriptionEvent> poll(String consumerId, Set<String> topicNames, long maxBytes);

  default List<SubscriptionEvent> poll(
      final String consumerId,
      final Set<String> topicNames,
      final long maxBytes,
      final Map<String, TopicProgress> progressByTopic) {
    return poll(consumerId, topicNames, maxBytes);
  }

  List<SubscriptionEvent> pollTablets(
      String consumerId, SubscriptionCommitContext commitContext, int offset);

  List<SubscriptionCommitContext> commit(
      String consumerId, List<SubscriptionCommitContext> commitContexts, boolean nack);

  default List<SubscriptionCommitContext> selectAcceptedCommitContexts(
      final List<SubscriptionCommitContext> commitContexts) {
    if (Objects.isNull(commitContexts) || commitContexts.isEmpty()) {
      return Collections.emptyList();
    }

    final List<SubscriptionCommitContext> acceptedCommitContexts = new ArrayList<>();
    for (final SubscriptionCommitContext commitContext : commitContexts) {
      if (acceptsCommitContext(commitContext)) {
        acceptedCommitContexts.add(commitContext);
      }
    }
    return acceptedCommitContexts;
  }

  default boolean acceptsCommitContext(final SubscriptionCommitContext commitContext) {
    return Objects.nonNull(commitContext) && acceptsTopic(commitContext.getTopicName());
  }

  boolean acceptsTopic(String topicName);

  int refreshInFlightEventLeases(String consumerId, List<SubscriptionCommitContext> commitContexts);

  boolean isCommitContextOutdated(SubscriptionCommitContext commitContext);

  boolean executePrefetch(String topicName);

  int getEventCount(String topicName);

  int getQueueCount();

  default Map<String, Long> getLagSummary() {
    return Collections.emptyMap();
  }

  void unbind(String topicName);

  void removeQueue(String topicName);

  boolean isEmpty();

  boolean hasQueue(String topicName);
}
