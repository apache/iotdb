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

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.columnfilter.TreeViewTabletProjector;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Resolves and caches the tree-view projector for one tablet batch. Keeping the provider scoped to
 * the batch preserves its schema snapshot and the initialization checks of subsequent batches.
 */
class TableViewTabletProjectorProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TableViewTabletProjectorProvider.class);

  private final SubscriptionPrefetchingTabletQueue prefetchingQueue;
  private volatile SubscriptionTreeViewProjector treeViewProjector;

  TableViewTabletProjectorProvider(final SubscriptionPrefetchingTabletQueue prefetchingQueue) {
    this.prefetchingQueue = prefetchingQueue;
  }

  boolean prepareForEmission() {
    final TopicConfig topicConfig =
        SubscriptionAgent.topic()
            .getTopicConfig(
                prefetchingQueue.getTopicName(),
                SubscriptionAgent.consumer().isTableModel(prefetchingQueue.getConsumerGroupId()));
    return Objects.nonNull(topicConfig) && prepareForEmission(topicConfig);
  }

  /** Uses the same frozen topic configuration as the batch's column and TAG filters. */
  synchronized boolean prepareForEmission(final TopicConfig topicConfig) {
    if (Objects.isNull(treeViewProjector)) {
      treeViewProjector = new SubscriptionTreeViewProjector(new TopicConfig(topicConfig));
    }
    final boolean prepared = treeViewProjector.prepare();
    if (!prepared) {
      LOGGER.debug(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_POSTPONE_EMITTING_SUBSCRIPTION_TABLET_BATCH_FOR_TOPIC_ARG_BECAUSE_TABLE_SCHEMA_ARG_ARG_IS_NOT_AVAILABLE_LOCALLY_996C618D,
          prefetchingQueue.getTopicName(),
          topicConfig.getStringOrDefault(
              TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE),
          topicConfig.getStringOrDefault(
              TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE));
    }
    return prepared;
  }

  synchronized TreeViewTabletProjector getTreeViewTabletProjector() {
    return Objects.nonNull(treeViewProjector)
        ? treeViewProjector.getTreeViewTabletProjector()
        : null;
  }
}
