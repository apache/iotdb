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

package org.apache.iotdb.confignode.manager.subscription;

import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class SubscriptionReadyDataNodeTracker {

  private record SubscriptionKey(String topicName, String consumerGroupId, boolean tableModel) {}

  private final Map<SubscriptionKey, Set<Integer>> readyDataNodeIds = new HashMap<>();

  public synchronized void replaceFromFullPush(
      final Collection<ConsumerGroupMeta> consumerGroupMetas,
      final Map<Integer, TPushConsumerGroupMetaResp> responses) {
    readyDataNodeIds.clear();
    for (final ConsumerGroupMeta consumerGroupMeta : consumerGroupMetas) {
      for (final String topicName : consumerGroupMeta.getSubscribedTopicNames()) {
        final SubscriptionKey key =
            new SubscriptionKey(
                topicName,
                consumerGroupMeta.getConsumerGroupId(),
                consumerGroupMeta.visibleUnder(true));
        for (final Map.Entry<Integer, TPushConsumerGroupMetaResp> response : responses.entrySet()) {
          if (isSuccessful(response.getValue())) {
            readyDataNodeIds
                .computeIfAbsent(key, ignored -> new HashSet<>())
                .add(response.getKey());
          }
        }
      }
    }
  }

  public synchronized void recordSinglePush(
      final ConsumerGroupMeta existingMeta,
      final ConsumerGroupMeta updatedMeta,
      final Map<Integer, TPushConsumerGroupMetaResp> responses) {
    final Set<String> affectedTopics = new HashSet<>();
    if (existingMeta != null) {
      affectedTopics.addAll(existingMeta.getSubscribedTopicNames());
    }
    if (updatedMeta != null) {
      affectedTopics.addAll(updatedMeta.getSubscribedTopicNames());
    }
    if (affectedTopics.isEmpty()) {
      return;
    }

    final String consumerGroupId =
        updatedMeta == null ? existingMeta.getConsumerGroupId() : updatedMeta.getConsumerGroupId();
    final boolean tableModel =
        updatedMeta == null ? existingMeta.visibleUnder(true) : updatedMeta.visibleUnder(true);
    for (final String topicName : affectedTopics) {
      final SubscriptionKey key = new SubscriptionKey(topicName, consumerGroupId, tableModel);
      final Set<Integer> readyIds = readyDataNodeIds.get(key);
      if (readyIds != null) {
        readyIds.removeAll(responses.keySet());
        if (readyIds.isEmpty()) {
          readyDataNodeIds.remove(key);
        }
      }
    }

    if (updatedMeta == null) {
      return;
    }
    for (final Map.Entry<Integer, TPushConsumerGroupMetaResp> response : responses.entrySet()) {
      if (!isSuccessful(response.getValue())) {
        continue;
      }
      for (final String topicName : updatedMeta.getSubscribedTopicNames()) {
        readyDataNodeIds
            .computeIfAbsent(
                new SubscriptionKey(topicName, consumerGroupId, tableModel),
                ignored -> new HashSet<>())
            .add(response.getKey());
      }
    }
  }

  public synchronized List<Integer> getReadyDataNodeIds(
      final String topicName,
      final String consumerGroupId,
      final boolean tableModel,
      final Set<Integer> registeredDataNodeIds) {
    final Set<Integer> readyIds =
        new TreeSet<>(
            readyDataNodeIds.getOrDefault(
                new SubscriptionKey(topicName, consumerGroupId, tableModel), Set.of()));
    readyIds.retainAll(registeredDataNodeIds);
    return new ArrayList<>(readyIds);
  }

  private static boolean isSuccessful(final TPushConsumerGroupMetaResp response) {
    return response != null
        && response.isSetStatus()
        && response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }
}
