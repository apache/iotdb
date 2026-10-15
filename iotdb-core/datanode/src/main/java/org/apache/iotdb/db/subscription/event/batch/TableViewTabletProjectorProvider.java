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

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.columnfilter.TreeViewTabletProjector;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

/**
 * Resolves and caches the tree-view projector for one tablet batch. Keeping the provider scoped to
 * the batch preserves its schema snapshot and the initialization checks of subsequent batches.
 */
class TableViewTabletProjectorProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPipeTabletEventBatch.class);

  private final SubscriptionPrefetchingTabletQueue prefetchingQueue;
  private volatile boolean initialized;
  private volatile TreeViewTabletProjector treeViewTabletProjector;

  TableViewTabletProjectorProvider(final SubscriptionPrefetchingTabletQueue prefetchingQueue) {
    this.prefetchingQueue = prefetchingQueue;
  }

  boolean prepareForEmission() {
    if (initialized) {
      return true;
    }

    synchronized (this) {
      if (initialized) {
        return true;
      }

      final TopicConfig topicConfig =
          SubscriptionAgent.topic()
              .getTopicConfigs(
                  Collections.singleton(prefetchingQueue.getTopicName()),
                  SubscriptionAgent.consumer().isTableModel(prefetchingQueue.getConsumerGroupId()))
              .get(prefetchingQueue.getTopicName());
      if (Objects.isNull(topicConfig)) {
        return false;
      }
      if (!topicConfig.isTableTopic()) {
        initialized = true;
        return true;
      }

      final String database =
          topicConfig.getStringOrDefault(
              TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE);
      final String tableName =
          topicConfig.getStringOrDefault(
              TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE);
      if (isDefaultTopicPattern(database, TopicConstant.DATABASE_DEFAULT_VALUE)
          || isDefaultTopicPattern(tableName, TopicConstant.TABLE_DEFAULT_VALUE)
          || !isLiteralTopicPattern(database)
          || !isLiteralTopicPattern(tableName)) {
        initialized = true;
        return true;
      }

      if (!isTreeCapturedByTopic(topicConfig) && topicConfig.isColumnFilterTrivial()) {
        initialized = true;
        return true;
      }

      final TsTable table = DataNodeTableCache.getInstance().getTable(database, tableName, false);
      if (Objects.isNull(table)) {
        LOGGER.debug(
            DataNodePipeMessages
                .PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_POSTPONE_EMITTING_SUBSCRIPTION_TABLET_BATCH_FOR_TOPIC_ARG_BECAUSE_TABLE_SCHEMA_ARG_ARG_IS_NOT_AVAILABLE_LOCALLY_996C618D,
            prefetchingQueue.getTopicName(),
            database,
            tableName);
        return false;
      }
      if (TreeViewSchema.isTreeViewTable(table)) {
        treeViewTabletProjector = new TreeViewTabletProjector(database, table);
      }

      initialized = true;
      return true;
    }
  }

  private static boolean isDefaultTopicPattern(final String pattern, final String defaultPattern) {
    return Objects.isNull(pattern) || defaultPattern.equals(pattern.trim());
  }

  private static boolean isLiteralTopicPattern(final String pattern) {
    final String regexMetaCharacters = ".*+?[](){}\\|^$";
    return Objects.nonNull(pattern)
        && pattern.chars().noneMatch(c -> regexMetaCharacters.indexOf((char) c) >= 0);
  }

  private static boolean isTreeCapturedByTopic(final TopicConfig topicConfig) {
    return topicConfig.getBooleanOrDefault(
        Arrays.asList(
            PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY,
            PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY),
        false);
  }

  TreeViewTabletProjector getTreeViewTabletProjector() {
    return treeViewTabletProjector;
  }
}
