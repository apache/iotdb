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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles the setup and teardown of consensus-based subscription queues on DataNode. When a
 * real-time subscription is detected, this handler finds the local IoTConsensus data regions,
 * creates the appropriate converter, and binds prefetching queues to the subscription broker.
 */
public class ConsensusSubscriptionSetupHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusSubscriptionSetupHandler.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private ConsensusSubscriptionSetupHandler() {
    // utility class
  }

  /**
   * Ensures that the IoTConsensus new-peer and peer-removed callbacks are set, so that when a new
   * DataRegion is created, all active consensus subscriptions are automatically bound to the new
   * region, and when a DataRegion is removed, all subscription queues are properly cleaned up.
   */
  public static void ensureNewRegionListenerRegistered() {
    if (IoTConsensus.onNewPeerCreated == null) {
      IoTConsensus.onNewPeerCreated = ConsensusSubscriptionSetupHandler::onNewRegionCreated;
      LOGGER.info(
          "Set IoTConsensus.onNewPeerCreated callback for consensus subscription auto-binding");
    }
    if (IoTConsensus.onPeerRemoved == null) {
      IoTConsensus.onPeerRemoved = ConsensusSubscriptionSetupHandler::onRegionRemoved;
      LOGGER.info("Set IoTConsensus.onPeerRemoved callback for consensus subscription cleanup");
    }
  }

  /**
   * Callback invoked when a new DataRegion (IoTConsensusServerImpl) is created locally. Queries
   * existing subscription metadata to find all active consensus subscriptions and binds prefetching
   * queues to the new region.
   */
  private static void onNewRegionCreated(
      final ConsensusGroupId groupId, final IoTConsensusServerImpl serverImpl) {
    if (!(groupId instanceof DataRegionId)) {
      return;
    }

    // Query existing metadata keepers for all active subscriptions
    final Map<String, java.util.Set<String>> allSubscriptions =
        SubscriptionAgent.consumer().getAllSubscriptions();
    if (allSubscriptions.isEmpty()) {
      return;
    }

    final ConsensusSubscriptionCommitManager commitManager =
        ConsensusSubscriptionCommitManager.getInstance();

    LOGGER.info(
        "New DataRegion {} created, checking {} consumer group(s) for auto-binding, "
            + "currentSearchIndex={}",
        groupId,
        allSubscriptions.size(),
        serverImpl.getSearchIndex());

    for (final Map.Entry<String, java.util.Set<String>> groupEntry : allSubscriptions.entrySet()) {
      final String consumerGroupId = groupEntry.getKey();
      for (final String topicName : groupEntry.getValue()) {
        if (!isConsensusBasedTopic(topicName)) {
          continue;
        }
        try {
          final Map<String, TopicConfig> topicConfigs =
              SubscriptionAgent.topic().getTopicConfigs(java.util.Collections.singleton(topicName));
          final TopicConfig topicConfig = topicConfigs.get(topicName);
          if (topicConfig == null) {
            continue;
          }

          // Resolve the new DataRegion's actual database name
          final DataRegion dataRegion =
              StorageEngine.getInstance().getDataRegion((DataRegionId) groupId);
          if (dataRegion == null) {
            continue;
          }
          final String dbRaw = dataRegion.getDatabaseName();
          final String dbTableModel = dbRaw.startsWith("root.") ? dbRaw.substring(5) : dbRaw;

          // For table topics, skip if this region's database doesn't match the topic filter
          if (topicConfig.isTableTopic()) {
            final String topicDb =
                topicConfig.getStringOrDefault(
                    TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE);
            if (topicDb != null
                && !topicDb.isEmpty()
                && !TopicConstant.DATABASE_DEFAULT_VALUE.equals(topicDb)
                && !topicDb.equalsIgnoreCase(dbTableModel)) {
              continue;
            }
          }

          final String actualDbName = topicConfig.isTableTopic() ? dbTableModel : null;
          final ConsensusLogToTabletConverter converter = buildConverter(topicConfig, actualDbName);

          // Use persisted committedSearchIndex for restart recovery; fall back to WAL tail
          // for brand-new regions that have no prior subscription progress.
          final long persistedIndex =
              commitManager.getCommittedSearchIndex(consumerGroupId, topicName, groupId.toString());
          final long startSearchIndex =
              (persistedIndex > 0) ? persistedIndex + 1 : serverImpl.getSearchIndex() + 1;

          LOGGER.info(
              "Auto-binding consensus queue for topic [{}] in group [{}] to new region {} "
                  + "(database={}, startSearchIndex={}, persistedIndex={})",
              topicName,
              consumerGroupId,
              groupId,
              dbTableModel,
              startSearchIndex,
              persistedIndex);

          SubscriptionAgent.broker()
              .bindConsensusPrefetchingQueue(
                  consumerGroupId,
                  topicName,
                  groupId.toString(),
                  serverImpl,
                  converter,
                  commitManager,
                  startSearchIndex);
        } catch (final Exception e) {
          LOGGER.error(
              "Failed to auto-bind topic [{}] in group [{}] to new region {}",
              topicName,
              consumerGroupId,
              groupId,
              e);
        }
      }
    }
  }

  /**
   * Callback invoked before a DataRegion (IoTConsensusServerImpl) is deleted locally. Unbinds and
   * cleans up all subscription prefetching queues associated with the removed region across all
   * consumer groups.
   */
  private static void onRegionRemoved(final ConsensusGroupId groupId) {
    if (!(groupId instanceof DataRegionId)) {
      return;
    }
    final String regionIdStr = groupId.toString();
    LOGGER.info(
        "DataRegion {} being removed, unbinding all consensus subscription queues", regionIdStr);
    try {
      SubscriptionAgent.broker().unbindByRegion(regionIdStr);
    } catch (final Exception e) {
      LOGGER.error(
          "Failed to unbind consensus subscription queues for removed region {}", regionIdStr, e);
    }
  }

  public static boolean isConsensusBasedTopic(final String topicName) {
    try {
      final String topicMode = SubscriptionAgent.topic().getTopicMode(topicName);
      final String topicFormat = SubscriptionAgent.topic().getTopicFormat(topicName);
      final boolean result =
          TopicConstant.MODE_LIVE_VALUE.equalsIgnoreCase(topicMode)
              && !TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE.equalsIgnoreCase(topicFormat);
      LOGGER.debug(
          "isConsensusBasedTopic check for topic [{}]: mode={}, format={}, result={}",
          topicName,
          topicMode,
          topicFormat,
          result);
      return result;
    } catch (final Exception e) {
      LOGGER.warn(
          "Failed to check if topic [{}] is consensus-based, defaulting to false", topicName, e);
      return false;
    }
  }

  public static void setupConsensusSubscriptions(
      final String consumerGroupId, final Set<String> topicNames) {
    final IConsensus dataRegionConsensus = DataRegionConsensusImpl.getInstance();
    if (!(dataRegionConsensus instanceof IoTConsensus)) {
      LOGGER.warn(
          "Data region consensus is not IoTConsensus (actual: {}), "
              + "cannot set up consensus-based subscription for consumer group [{}]",
          dataRegionConsensus.getClass().getSimpleName(),
          consumerGroupId);
      return;
    }

    // Ensure the new-region listener is registered (idempotent)
    ensureNewRegionListenerRegistered();

    final IoTConsensus ioTConsensus = (IoTConsensus) dataRegionConsensus;
    final ConsensusSubscriptionCommitManager commitManager =
        ConsensusSubscriptionCommitManager.getInstance();

    LOGGER.info(
        "Setting up consensus subscriptions for consumer group [{}], topics={}, "
            + "total consensus groups={}",
        consumerGroupId,
        topicNames,
        ioTConsensus.getAllConsensusGroupIds().size());

    for (final String topicName : topicNames) {
      if (!isConsensusBasedTopic(topicName)) {
        continue;
      }

      try {
        setupConsensusQueueForTopic(consumerGroupId, topicName, ioTConsensus, commitManager);
      } catch (final Exception e) {
        LOGGER.error(
            "Failed to set up consensus subscription for topic [{}] in consumer group [{}]",
            topicName,
            consumerGroupId,
            e);
      }
    }
  }

  /**
   * Set up consensus queue for a single topic. Discovers all local data region consensus groups and
   * binds a ConsensusReqReader-based prefetching queue to every matching region.
   *
   * <p>For table-model topics, only regions whose database matches the topic's {@code DATABASE_KEY}
   * filter are bound. For tree-model topics, all local data regions are bound. Additionally, the
   * {@link #onNewRegionCreated} callback ensures that regions created after this method runs are
   * also automatically bound.
   */
  private static void setupConsensusQueueForTopic(
      final String consumerGroupId,
      final String topicName,
      final IoTConsensus ioTConsensus,
      final ConsensusSubscriptionCommitManager commitManager) {

    // Get topic config for building the converter
    final Map<String, TopicConfig> topicConfigs =
        SubscriptionAgent.topic().getTopicConfigs(java.util.Collections.singleton(topicName));
    final TopicConfig topicConfig = topicConfigs.get(topicName);
    if (topicConfig == null) {
      LOGGER.warn(
          "Topic config not found for topic [{}], cannot set up consensus queue", topicName);
      return;
    }

    // Build the converter based on topic config (path pattern, time range, tree/table model)
    LOGGER.info(
        "Setting up consensus queue for topic [{}]: isTableTopic={}, config={}",
        topicName,
        topicConfig.isTableTopic(),
        topicConfig.getAttribute());

    // For table topics, extract the database filter from topic config
    final String topicDatabaseFilter =
        topicConfig.isTableTopic()
            ? topicConfig.getStringOrDefault(
                TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE)
            : null;

    final List<ConsensusGroupId> allGroupIds = ioTConsensus.getAllConsensusGroupIds();
    LOGGER.info(
        "Discovered {} consensus group(s) for topic [{}] in consumer group [{}]: {}",
        allGroupIds.size(),
        topicName,
        consumerGroupId,
        allGroupIds);
    boolean bound = false;

    for (final ConsensusGroupId groupId : allGroupIds) {
      if (!(groupId instanceof DataRegionId)) {
        continue;
      }

      final IoTConsensusServerImpl serverImpl = ioTConsensus.getImpl(groupId);
      if (serverImpl == null) {
        continue;
      }

      // Resolve the DataRegion's actual database name
      final DataRegion dataRegion =
          StorageEngine.getInstance().getDataRegion((DataRegionId) groupId);
      if (dataRegion == null) {
        continue;
      }
      final String dbRaw = dataRegion.getDatabaseName();
      final String dbTableModel = dbRaw.startsWith("root.") ? dbRaw.substring(5) : dbRaw;

      if (topicDatabaseFilter != null
          && !topicDatabaseFilter.isEmpty()
          && !TopicConstant.DATABASE_DEFAULT_VALUE.equals(topicDatabaseFilter)
          && !topicDatabaseFilter.equalsIgnoreCase(dbTableModel)) {
        LOGGER.info(
            "Skipping region {} (database={}) for table topic [{}] (DATABASE_KEY={})",
            groupId,
            dbTableModel,
            topicName,
            topicDatabaseFilter);
        continue;
      }

      final String actualDbName = topicConfig.isTableTopic() ? dbTableModel : null;
      final ConsensusLogToTabletConverter converter = buildConverter(topicConfig, actualDbName);

      // Use persisted committedSearchIndex for restart recovery; fall back to WAL tail
      // for brand-new regions that have no prior subscription progress.
      final long persistedIndex =
          commitManager.getCommittedSearchIndex(consumerGroupId, topicName, groupId.toString());
      final long startSearchIndex =
          (persistedIndex > 0) ? persistedIndex + 1 : serverImpl.getSearchIndex() + 1;

      LOGGER.info(
          "Binding consensus prefetching queue for topic [{}] in consumer group [{}] "
              + "to data region consensus group [{}] (database={}, startSearchIndex={}, "
              + "persistedIndex={})",
          topicName,
          consumerGroupId,
          groupId,
          dbTableModel,
          startSearchIndex,
          persistedIndex);

      SubscriptionAgent.broker()
          .bindConsensusPrefetchingQueue(
              consumerGroupId,
              topicName,
              groupId.toString(),
              serverImpl,
              converter,
              commitManager,
              startSearchIndex);

      bound = true;
    }

    if (!bound) {
      LOGGER.warn(
          "No local IoTConsensus data region found for topic [{}] in consumer group [{}]. "
              + "Consensus subscription will be set up when a matching data region becomes available.",
          topicName,
          consumerGroupId);
    }
  }

  private static ConsensusLogToTabletConverter buildConverter(
      final TopicConfig topicConfig, final String actualDatabaseName) {
    // Determine tree or table model
    final boolean isTableTopic = topicConfig.isTableTopic();

    TreePattern treePattern = null;
    TablePattern tablePattern = null;

    if (isTableTopic) {
      // Table model: database + table name pattern
      final String database =
          topicConfig.getStringOrDefault(
              TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE);
      final String table =
          topicConfig.getStringOrDefault(
              TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE);
      tablePattern = new TablePattern(true, database, table);
    } else {
      // Tree model: path or pattern
      if (topicConfig.getAttribute().containsKey(TopicConstant.PATTERN_KEY)) {
        final String pattern = topicConfig.getAttribute().get(TopicConstant.PATTERN_KEY);
        treePattern = new PrefixTreePattern(pattern);
      } else {
        final String path =
            topicConfig.getStringOrDefault(
                TopicConstant.PATH_KEY, TopicConstant.PATH_DEFAULT_VALUE);
        treePattern = new IoTDBTreePattern(path);
      }
    }

    return new ConsensusLogToTabletConverter(treePattern, tablePattern, actualDatabaseName);
  }

  public static void teardownConsensusSubscriptions(
      final String consumerGroupId, final Set<String> topicNames) {
    for (final String topicName : topicNames) {
      try {
        SubscriptionAgent.broker().unbindConsensusPrefetchingQueue(consumerGroupId, topicName);

        // Clean up commit state for all regions of this topic
        ConsensusSubscriptionCommitManager.getInstance()
            .removeAllStatesForTopic(consumerGroupId, topicName);

        LOGGER.info(
            "Tore down consensus subscription for topic [{}] in consumer group [{}]",
            topicName,
            consumerGroupId);
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to tear down consensus subscription for topic [{}] in consumer group [{}]",
            topicName,
            consumerGroupId,
            e);
      }
    }
  }

  public static void handleNewSubscriptions(
      final String consumerGroupId, final Set<String> newTopicNames) {
    if (newTopicNames == null || newTopicNames.isEmpty()) {
      return;
    }

    LOGGER.info(
        "Checking new subscriptions in consumer group [{}] for consensus-based topics: {}",
        consumerGroupId,
        newTopicNames);

    setupConsensusSubscriptions(consumerGroupId, newTopicNames);
  }
}
