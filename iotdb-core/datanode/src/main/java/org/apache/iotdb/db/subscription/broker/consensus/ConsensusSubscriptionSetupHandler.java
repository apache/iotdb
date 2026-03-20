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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles the setup and teardown of consensus-based subscription queues on DataNode. When a
 * real-time subscription is detected, this handler finds the local IoTConsensus data regions,
 * creates the appropriate converter, and binds prefetching queues to the subscription broker.
 */
public class ConsensusSubscriptionSetupHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusSubscriptionSetupHandler.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  /** Last-known preferred writer node ID per region, used to detect routing changes. */
  private static final ConcurrentHashMap<TConsensusGroupId, Integer> lastKnownPreferredWriter =
      new ConcurrentHashMap<>();

  /**
   * Per-region current epoch value. Uses the routing-broadcast timestamp from ConfigNode, ensuring
   * all DataNodes derive the same epoch for the same routing change without local persistence.
   */
  private static final ConcurrentHashMap<TConsensusGroupId, Long> regionEpoch =
      new ConcurrentHashMap<>();

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

          // Recover from persisted global consensus progress when available. The queue will
          // translate (epoch, syncIndex) back to the local WAL searchIndex on first poll.
          final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState commitState =
              commitManager.getOrCreateState(consumerGroupId, topicName, groupId);
          final boolean hasLocalPersistedState =
              commitManager.hasPersistedState(consumerGroupId, topicName, groupId);
          final long committedEpoch = hasLocalPersistedState ? commitState.getCommittedEpoch() : 0L;
          final long committedSyncIndex =
              hasLocalPersistedState ? commitState.getCommittedSyncIndex() : -1L;
          final long tailStartSearchIndex = serverImpl.getSearchIndex() + 1;
          final long initialEpoch =
              regionEpoch.getOrDefault(groupId.convertToTConsensusGroupId(), 0L);
          final boolean initialActive =
              lastKnownPreferredWriter.getOrDefault(groupId.convertToTConsensusGroupId(), -1)
                  == IOTDB_CONFIG.getDataNodeId();

          LOGGER.info(
              "Auto-binding consensus queue for topic [{}] in group [{}] to new region {} "
                  + "(database={}, tailStartSearchIndex={}, hasLocalPersistedState={}, "
                  + "committedEpoch={}, committedSyncIndex={}, initialEpoch={}, initialActive={})",
              topicName,
              consumerGroupId,
              groupId,
              dbTableModel,
              tailStartSearchIndex,
              hasLocalPersistedState,
              committedEpoch,
              committedSyncIndex,
              initialEpoch,
              initialActive);

          SubscriptionAgent.broker()
              .bindConsensusPrefetchingQueue(
                  consumerGroupId,
                  topicName,
                  groupId,
                  serverImpl,
                  converter,
                  commitManager,
                  committedEpoch,
                  committedSyncIndex,
                  tailStartSearchIndex,
                  initialEpoch,
                  initialActive);
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
    LOGGER.info(
        "DataRegion {} being removed, unbinding all consensus subscription queues", groupId);
    try {
      SubscriptionAgent.broker().unbindByRegion(groupId);
    } catch (final Exception e) {
      LOGGER.error(
          "Failed to unbind consensus subscription queues for removed region {}", groupId, e);
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
    final int myNodeId = IOTDB_CONFIG.getDataNodeId();

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

      // Recover from persisted global consensus progress when available. The queue will
      // translate (epoch, syncIndex) back to the local WAL searchIndex on first poll.
      final ConsensusSubscriptionCommitManager.ConsensusSubscriptionCommitState commitState =
          commitManager.getOrCreateState(consumerGroupId, topicName, groupId);
      final boolean hasLocalPersistedState =
          commitManager.hasPersistedState(consumerGroupId, topicName, groupId);
      final long committedEpoch = hasLocalPersistedState ? commitState.getCommittedEpoch() : 0L;
      final long committedSyncIndex =
          hasLocalPersistedState ? commitState.getCommittedSyncIndex() : -1L;
      final long tailStartSearchIndex = serverImpl.getSearchIndex() + 1;
      final long initialEpoch = regionEpoch.getOrDefault(groupId.convertToTConsensusGroupId(), 0L);
      final boolean initialActive =
          lastKnownPreferredWriter.getOrDefault(groupId.convertToTConsensusGroupId(), -1)
              == myNodeId;

      LOGGER.info(
          "Binding consensus prefetching queue for topic [{}] in consumer group [{}] "
              + "to data region consensus group [{}] (database={}, tailStartSearchIndex={}, "
              + "hasLocalPersistedState={}, committedEpoch={}, committedSyncIndex={}, "
              + "initialEpoch={}, initialActive={})",
          topicName,
          consumerGroupId,
          groupId,
          dbTableModel,
          tailStartSearchIndex,
          hasLocalPersistedState,
          committedEpoch,
          committedSyncIndex,
          initialEpoch,
          initialActive);

      SubscriptionAgent.broker()
          .bindConsensusPrefetchingQueue(
              consumerGroupId,
              topicName,
              groupId,
              serverImpl,
              converter,
              commitManager,
              committedEpoch,
              committedSyncIndex,
              tailStartSearchIndex,
              initialEpoch,
              initialActive);

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

  public static void onRegionRouteChanged(
      final Map<TConsensusGroupId, TRegionReplicaSet> newMap, final long routingTimestamp) {
    if (!SubscriptionConfig.getInstance().isSubscriptionConsensusEpochOrderingEnabled()) {
      return;
    }

    final int myNodeId = IOTDB_CONFIG.getDataNodeId();

    for (final Map.Entry<TConsensusGroupId, TRegionReplicaSet> newEntry : newMap.entrySet()) {
      final TConsensusGroupId groupId = newEntry.getKey();
      final TRegionReplicaSet newReplicaSet = newEntry.getValue();

      final int newPreferredNodeId = getPreferredNodeId(newReplicaSet);
      final Integer oldPreferredBoxed = lastKnownPreferredWriter.put(groupId, newPreferredNodeId);
      final int oldPreferredNodeId = (oldPreferredBoxed != null) ? oldPreferredBoxed : -1;

      if (oldPreferredNodeId == newPreferredNodeId) {
        continue; // no leader change for this region
      }

      final ConsensusGroupId regionId =
          ConsensusGroupId.Factory.createFromTConsensusGroupId(groupId);
      final long oldEpoch = regionEpoch.getOrDefault(groupId, 0L);
      final long newEpoch = routingTimestamp;
      regionEpoch.put(groupId, newEpoch);

      LOGGER.info(
          "ConsensusSubscriptionSetupHandler: region {} preferred writer changed {} -> {}, "
              + "epoch {} -> {}",
          regionId,
          oldPreferredNodeId,
          newPreferredNodeId,
          oldEpoch,
          newEpoch);

      if (oldPreferredNodeId == myNodeId) {
        // This node was the old preferred writer: inject epoch sentinel, then update epoch.
        // Order matters: sentinel marks the end of oldEpoch; subsequent in-flight writes
        // that slip past the sentinel will carry newEpoch, avoiding a stale-epoch tail that
        // would cause the consumer-side EpochOrderingProcessor to enter unnecessary BUFFERING.
        try {
          SubscriptionAgent.broker().onOldLeaderRegionChanged(regionId, oldEpoch);
          SubscriptionAgent.broker().onNewLeaderRegionChanged(regionId, newEpoch);
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to inject epoch sentinel / update epoch for region {} (oldLeader={})",
              regionId,
              myNodeId,
              e);
        }
        // Deactivate queues on old leader: stop serving subscription data
        SubscriptionAgent.broker().setActiveForRegion(regionId, false);
        // Notify LogDispatcher to send SYNC_COMPLETE marker to Followers so they can
        // release buffered events of the completed epoch without waiting for timeout.
        try {
          final IConsensus consensus = DataRegionConsensusImpl.getInstance();
          if (consensus instanceof IoTConsensus) {
            final IoTConsensusServerImpl serverImpl = ((IoTConsensus) consensus).getImpl(regionId);
            if (serverImpl != null) {
              serverImpl.setCurrentEpochWithSyncComplete(newEpoch);
            }
          }
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to send SYNC_COMPLETE for region {} (oldLeader={})", regionId, myNodeId, e);
        }
      }

      if (newPreferredNodeId == myNodeId) {
        // This node is the new preferred writer: update epoch on queues and consensus server
        try {
          SubscriptionAgent.broker().onNewLeaderRegionChanged(regionId, newEpoch);
        } catch (final Exception e) {
          LOGGER.warn("Failed to set epoch for region {} (newLeader={})", regionId, myNodeId, e);
        }
        // Activate queues on new leader: start serving subscription data
        SubscriptionAgent.broker().setActiveForRegion(regionId, true);
        try {
          final IConsensus consensus = DataRegionConsensusImpl.getInstance();
          if (consensus instanceof IoTConsensus) {
            final IoTConsensusServerImpl serverImpl = ((IoTConsensus) consensus).getImpl(regionId);
            if (serverImpl != null) {
              serverImpl.setCurrentEpoch(newEpoch);
            }
          }
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to set consensus epoch for region {} (newLeader={})", regionId, myNodeId, e);
        }
      }
    }
  }

  private static int getPreferredNodeId(final TRegionReplicaSet replicaSet) {
    final List<TDataNodeLocation> locations = replicaSet.getDataNodeLocations();
    if (locations == null || locations.isEmpty()) {
      return -1;
    }
    return locations.get(0).getDataNodeId();
  }
}
