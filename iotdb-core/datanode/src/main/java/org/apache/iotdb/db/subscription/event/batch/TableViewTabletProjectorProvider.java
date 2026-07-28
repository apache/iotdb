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

import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.ITableCache;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.columnfilter.TreeViewTabletProjector;
import org.apache.iotdb.db.subscription.columnfilter.WritableViewTabletProjector;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;

/**
 * Shares table-view projectors across tablet batches and refreshes them only when the associated
 * cached table changes. Exact database/table topics identify at most one logical view, so a single
 * projector is sufficient for each prefetching queue.
 */
class TableViewTabletProjectorProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TableViewTabletProjectorProvider.class);

  private final SubscriptionPrefetchingTabletQueue prefetchingQueue;

  private volatile boolean initialized;
  private volatile boolean schemaDependent;
  private volatile long tableCacheVersion = Long.MIN_VALUE;
  private volatile TsTable cachedTable;
  private volatile TreeViewTabletProjector treeViewTabletProjector;
  private volatile WritableViewTabletProjector writableViewTabletProjector;

  TableViewTabletProjectorProvider(final SubscriptionPrefetchingTabletQueue prefetchingQueue) {
    this.prefetchingQueue = prefetchingQueue;
  }

  boolean prepareForEmission() {
    final ITableCache tableCache = DataNodeTableCache.getInstance();
    if (initialized && (!schemaDependent || tableCacheVersion == tableCache.getInstanceVersion())) {
      return true;
    }

    synchronized (this) {
      if (initialized
          && (!schemaDependent || tableCacheVersion == tableCache.getInstanceVersion())) {
        return true;
      }

      final TopicConfig topicConfig =
          SubscriptionAgent.topic()
              .getTopicConfigs(Collections.singleton(prefetchingQueue.getTopicName()))
              .get(prefetchingQueue.getTopicName());
      if (Objects.isNull(topicConfig)) {
        return false;
      }
      if (!topicConfig.isTableTopic()) {
        markSchemaIndependent();
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
        markSchemaIndependent();
        return true;
      }

      while (true) {
        final long versionBeforeGet = tableCache.getInstanceVersion();
        final TsTable table = tableCache.getTable(database, tableName, false);
        final long versionAfterGet = tableCache.getInstanceVersion();
        if (versionBeforeGet != versionAfterGet) {
          continue;
        }
        if (Objects.isNull(table)) {
          LOGGER.debug(
              DataNodePipeMessages
                  .PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_POSTPONE_EMITTING_SUBSCRIPTION_TABLET_BATCH_FOR_TOPIC_ARG_BECAUSE_TABLE_SCHEMA_ARG_ARG_IS_NOT_AVAILABLE_LOCALLY_996C618D,
              prefetchingQueue.getTopicName(),
              database,
              tableName);
          return false;
        }

        if (!initialized || table != cachedTable) {
          treeViewTabletProjector =
              TreeViewSchema.isTreeViewTable(table)
                  ? new TreeViewTabletProjector(database, table)
                  : null;
          writableViewTabletProjector =
              table instanceof WritableView
                  ? new WritableViewTabletProjector(database, (WritableView) table)
                  : null;
          cachedTable = table;
        }
        schemaDependent = true;
        tableCacheVersion = versionAfterGet;
        initialized = true;
        return true;
      }
    }
  }

  TreeViewTabletProjector getTreeViewTabletProjector() {
    return treeViewTabletProjector;
  }

  WritableViewTabletProjector getWritableViewTabletProjector() {
    return writableViewTabletProjector;
  }

  private void markSchemaIndependent() {
    cachedTable = null;
    treeViewTabletProjector = null;
    writableViewTabletProjector = null;
    schemaDependent = false;
    initialized = true;
  }

  private static boolean isDefaultTopicPattern(final String pattern, final String defaultPattern) {
    return Objects.isNull(pattern) || defaultPattern.equals(pattern.trim());
  }

  private static boolean isLiteralTopicPattern(final String pattern) {
    final String regexMetaCharacters = ".*+?[](){}\\|^$";
    return Objects.nonNull(pattern)
        && pattern.chars().noneMatch(c -> regexMetaCharacters.indexOf((char) c) >= 0);
  }
}
