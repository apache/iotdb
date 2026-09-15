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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.columnfilter.ColumnFilterMatcher;
import org.apache.iotdb.db.subscription.tagfilter.TagFilterEvaluationException;
import org.apache.iotdb.db.subscription.tagfilter.TagFilterMatcher;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Immutable topic-filter state used by every event in one subscription batch. */
final class SubscriptionFilterSnapshot {

  private static final int MAX_CAPTURE_ATTEMPTS = 3;

  private final boolean tableModel;
  private final TopicConfig topicConfig;
  private final Map<String, String> filteringAttributes;
  private final TagFilterMatcher tagFilterMatcher;
  private final ColumnFilterMatcher columnFilterMatcher;

  private SubscriptionFilterSnapshot(
      final boolean tableModel,
      final TopicConfig topicConfig,
      final TagFilterMatcher tagFilterMatcher,
      final ColumnFilterMatcher columnFilterMatcher) {
    this.tableModel = tableModel;
    this.topicConfig = topicConfig;
    this.filteringAttributes = filteringAttributes(topicConfig);
    this.tagFilterMatcher = tagFilterMatcher;
    this.columnFilterMatcher = columnFilterMatcher;
  }

  static SubscriptionFilterSnapshot capture(final SubscriptionPrefetchingQueue queue) {
    final boolean tableModel =
        SubscriptionAgent.consumer().isTableModel(queue.getConsumerGroupId());
    for (int attempt = 0; attempt < MAX_CAPTURE_ATTEMPTS; attempt++) {
      final TopicConfig topicConfig = getCurrentTopicConfig(queue, tableModel);
      if (Objects.isNull(topicConfig)) {
        throw new TagFilterEvaluationException(
            DataNodeMiscMessages
                .EXCEPTION_TOPIC_CONFIGURATION_IS_NOT_AVAILABLE_FOR_TAG_FILTER_1E023E3A);
      }

      final TagFilterMatcher tagFilterMatcher =
          SubscriptionAgent.broker().getTagFilterMatcher(queue.getTopicName(), tableModel);
      tagFilterMatcher.throwIfFailure();
      final ColumnFilterMatcher columnFilterMatcher =
          SubscriptionAgent.broker().getColumnFilterMatcher(queue.getTopicName(), tableModel);

      final TopicConfig currentTopicConfig = getCurrentTopicConfig(queue, tableModel);
      if (Objects.nonNull(currentTopicConfig)
          && filteringAttributes(topicConfig).equals(filteringAttributes(currentTopicConfig))
          && tagFilterMatcher
              == SubscriptionAgent.broker().getTagFilterMatcher(queue.getTopicName(), tableModel)
          && columnFilterMatcher
              == SubscriptionAgent.broker()
                  .getColumnFilterMatcher(queue.getTopicName(), tableModel)) {
        return new SubscriptionFilterSnapshot(
            tableModel,
            new TopicConfig(new HashMap<>(topicConfig.getAttribute())),
            tagFilterMatcher,
            columnFilterMatcher);
      }
    }

    throw new TagFilterEvaluationException(
        DataNodeMiscMessages
            .EXCEPTION_TOPIC_CONFIGURATION_CHANGED_WHILE_CAPTURING_TAG_FILTER_SNAPSHOT_984CEB1B);
  }

  boolean isCurrent(final SubscriptionPrefetchingQueue queue) {
    if (!tableModel) {
      return true;
    }
    final TopicConfig currentTopicConfig = getCurrentTopicConfig(queue, true);
    return Objects.nonNull(currentTopicConfig)
        && filteringAttributes.equals(filteringAttributes(currentTopicConfig))
        && tagFilterMatcher
            == SubscriptionAgent.broker().getTagFilterMatcher(queue.getTopicName(), true)
        && columnFilterMatcher
            == SubscriptionAgent.broker().getColumnFilterMatcher(queue.getTopicName(), true);
  }

  TopicConfig getTopicConfig() {
    return topicConfig;
  }

  TagFilterMatcher getTagFilterMatcher() {
    return tagFilterMatcher;
  }

  ColumnFilterMatcher getColumnFilterMatcher() {
    return columnFilterMatcher;
  }

  boolean hasNonTrivialFilter() {
    return tableModel && (!tagFilterMatcher.isMatchAll() || !columnFilterMatcher.isMatchAll());
  }

  private static TopicConfig getCurrentTopicConfig(
      final SubscriptionPrefetchingQueue queue, final boolean tableModel) {
    return SubscriptionAgent.topic()
        .getTopicConfigs(Collections.singleton(queue.getTopicName()), tableModel)
        .get(queue.getTopicName());
  }

  private static Map<String, String> filteringAttributes(final TopicConfig topicConfig) {
    if (Objects.isNull(topicConfig) || Objects.isNull(topicConfig.getAttribute())) {
      return Collections.emptyMap();
    }
    final Map<String, String> result = new HashMap<>();
    topicConfig
        .getAttribute()
        .forEach(
            (key, value) -> {
              if (Objects.isNull(key)) {
                return;
              }
              final String normalizedKey = key.trim().toLowerCase(Locale.ROOT);
              if (!isOwnerAttribute(normalizedKey)) {
                result.put(normalizedKey, value);
              }
            });
    return result;
  }

  private static boolean isOwnerAttribute(final String key) {
    return TopicConstant.OWNER_ID_KEY.equals(key)
        || TopicConstant.OWNER_EPOCH_KEY.equals(key)
        || TopicConstant.MAX_OWNER_EPOCH_KEY.equals(key)
        || TopicConstant.OWNER_LEASE_DURATION_MS_KEY.equals(key);
  }
}
