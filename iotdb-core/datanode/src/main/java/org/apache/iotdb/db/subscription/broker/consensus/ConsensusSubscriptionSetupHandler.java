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
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Handles setup and teardown of consensus-based subscription queues on DataNode.
 *
 * <p>For each consensus-mode topic subscribed by a consumer group, this handler discovers matching
 * local IoTConsensus DataRegions, builds the appropriate log-to-tablet converter, and binds one
 * queue per region to the consensus subscription broker.
 */
public class ConsensusSubscriptionSetupHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusSubscriptionSetupHandler.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  /** Last-known preferred writer node ID per region, used to detect routing changes. */
  private static final ConcurrentHashMap<TConsensusGroupId, Integer> lastKnownPreferredWriter =
      new ConcurrentHashMap<>();

  /**
   * Per-region routing runtime version. Uses the routing-broadcast timestamp from ConfigNode so all
   * DataNodes derive the same ordering version for the same routing change without local
   * persistence.
   */
  private static final ConcurrentHashMap<TConsensusGroupId, Long> regionRuntimeVersion =
      new ConcurrentHashMap<>();

  /** Per-region active writer node IDs for subscription runtime control. */
  private static final ConcurrentHashMap<TConsensusGroupId, Set<Integer>>
      regionActiveWriterNodeIds = new ConcurrentHashMap<>();

  static RegionProgress resolveFallbackCommittedRegionProgress(
      final ConsensusSubscriptionCommitManager commitManager,
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId groupId) {
    commitManager.getOrCreateState(consumerGroupId, topicName, groupId);
    final RegionProgress committedRegionProgress =
        commitManager.getCommittedRegionProgress(consumerGroupId, topicName, groupId);
    return committedRegionProgress != null
            && !committedRegionProgress.getWriterPositions().isEmpty()
        ? committedRegionProgress
        : null;
  }

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
          DataNodePipeMessages
              .PIPE_LOG_SET_IOTCONSENSUS_ONNEWPEERCREATED_CALLBACK_FOR_CONSENSUS_0766CE68);
    }
    if (IoTConsensus.onPeerRemoved == null) {
      IoTConsensus.onPeerRemoved = ConsensusSubscriptionSetupHandler::onRegionRemoved;
      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_SET_IOTCONSENSUS_ONPEERREMOVED_CALLBACK_FOR_CONSENSUS_SUBSCRIPTION_21D4D6AC);
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
        DataNodePipeMessages
            .PIPE_LOG_NEW_DATAREGION_CREATED_CHECKING_CONSUMER_GROUP_S_FOR_AUTO_787C16E9,
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

          // For table topics, skip if this region's database doesn't match the topic filter.
          if (!matchesTopicDatabase(topicConfig, dbTableModel)) {
            continue;
          }

          final String actualDbName = topicConfig.isTableTopic() ? dbTableModel : null;
          final ConsensusLogToTabletConverter converter = buildConverter(topicConfig, actualDbName);
          final SubscriptionWalRetentionPolicy retentionPolicy =
              buildSubscriptionWalRetentionPolicy(topicName, topicConfig, serverImpl);

          // Recover from persisted per-writer region progress when available. The queue will
          // resolve a replay start from that progress on first poll via the region-level locator.
          final RegionProgress committedRegionProgress =
              resolveFallbackCommittedRegionProgress(
                  commitManager, consumerGroupId, topicName, groupId);
          final boolean hasLocalPersistedState =
              commitManager.hasPersistedState(consumerGroupId, topicName, groupId);
          final long tailStartSearchIndex = serverImpl.getSearchIndex() + 1;
          final long initialRuntimeVersion =
              regionRuntimeVersion.getOrDefault(groupId.convertToTConsensusGroupId(), 0L);
          final boolean initialActive =
              lastKnownPreferredWriter.getOrDefault(groupId.convertToTConsensusGroupId(), -1)
                  == IOTDB_CONFIG.getDataNodeId();
          final Set<Integer> initialActiveWriterNodeIds =
              regionActiveWriterNodeIds.getOrDefault(
                  groupId.convertToTConsensusGroupId(),
                  initialActive
                      ? Collections.singleton(IOTDB_CONFIG.getDataNodeId())
                      : Collections.emptySet());
          final ConsensusRegionRuntimeState initialRuntimeState =
              new ConsensusRegionRuntimeState(
                  initialRuntimeVersion,
                  lastKnownPreferredWriter.getOrDefault(groupId.convertToTConsensusGroupId(), -1),
                  initialActive,
                  initialActiveWriterNodeIds);

          LOGGER.info(
              DataNodePipeMessages
                  .PIPE_LOG_AUTO_BINDING_CONSENSUS_QUEUE_FOR_TOPIC_IN_GROUP_TO_NEW_REGION_86F21649,
              topicName,
              consumerGroupId,
              groupId,
              dbTableModel,
              tailStartSearchIndex,
              hasLocalPersistedState,
              committedRegionProgress,
              initialRuntimeVersion,
              initialActive);

          SubscriptionAgent.broker()
              .bindConsensusPrefetchingQueue(
                  consumerGroupId,
                  topicName,
                  topicConfig.getOrderMode(),
                  groupId,
                  serverImpl,
                  retentionPolicy,
                  converter,
                  commitManager,
                  committedRegionProgress,
                  tailStartSearchIndex,
                  initialRuntimeVersion,
                  initialActive);
          SubscriptionAgent.broker().applyRuntimeStateForRegion(groupId, initialRuntimeState);
        } catch (final Exception e) {
          LOGGER.error(
              DataNodePipeMessages
                  .PIPE_LOG_FAILED_TO_AUTO_BIND_TOPIC_IN_GROUP_TO_NEW_REGION_5BFD0E7D,
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
    lastKnownPreferredWriter.remove(groupId.convertToTConsensusGroupId());
    regionRuntimeVersion.remove(groupId.convertToTConsensusGroupId());
    regionActiveWriterNodeIds.remove(groupId.convertToTConsensusGroupId());
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_DATAREGION_BEING_REMOVED_UNBINDING_ALL_CONSENSUS_SUBSCRIPTION_848A29F0,
        groupId);
    try {
      SubscriptionAgent.broker().unbindByRegion(groupId);
    } catch (final Exception e) {
      LOGGER.error(
          DataNodePipeMessages
              .PIPE_LOG_FAILED_TO_UNBIND_CONSENSUS_SUBSCRIPTION_QUEUES_FOR_REMOVED_7086F70A,
          groupId,
          e);
    }
  }

  public static boolean isConsensusBasedTopic(final String topicName) {
    try {
      final String topicMode = SubscriptionAgent.topic().getTopicMode(topicName);
      final boolean result = TopicConstant.MODE_CONSENSUS_VALUE.equalsIgnoreCase(topicMode);
      LOGGER.debug(
          DataNodePipeMessages.PIPE_LOG_ISCONSENSUSBASEDTOPIC_CHECK_FOR_TOPIC_MODE_RESULT_19EFA0F9,
          topicName,
          topicMode,
          result);
      return result;
    } catch (final Exception e) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_FAILED_TO_CHECK_IF_TOPIC_IS_CONSENSUS_BASED_DEFAULTING_TO_ECCE1509,
          topicName,
          e);
      return false;
    }
  }

  public static void setupConsensusSubscriptions(
      final String consumerGroupId, final Set<String> topicNames) {
    final IConsensus dataRegionConsensus = DataRegionConsensusImpl.getInstance();
    if (!(dataRegionConsensus instanceof IoTConsensus)) {
      final String configuredProtocol = IOTDB_CONFIG.getDataRegionConsensusProtocolClass();
      final String runtimeConsensusImplementation =
          Objects.nonNull(dataRegionConsensus) ? dataRegionConsensus.getClass().getName() : "null";
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_SKIPPING_SETUP_OF_CONSENSUS_BASED_SUBSCRIPTIONS_FOR_CONSUMER_A7B2C812,
          consumerGroupId,
          ConsensusFactory.IOT_CONSENSUS,
          configuredProtocol,
          runtimeConsensusImplementation);
      return;
    }

    // Ensure the new-region listener is registered (idempotent)
    ensureNewRegionListenerRegistered();

    final IoTConsensus ioTConsensus = (IoTConsensus) dataRegionConsensus;
    final ConsensusSubscriptionCommitManager commitManager =
        ConsensusSubscriptionCommitManager.getInstance();

    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_SETTING_UP_CONSENSUS_SUBSCRIPTIONS_FOR_CONSUMER_GROUP_TOPICS_204374A2,
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
            DataNodePipeMessages
                .PIPE_LOG_FAILED_TO_SET_UP_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_CONSUMER_1A30001B,
            topicName,
            consumerGroupId,
            e);
      }
    }
  }

  /**
   * Sets up consensus queues for a single topic.
   *
   * <p>This method discovers local DataRegion consensus groups that match the topic filter and
   * binds one consensus subscription queue to each matching region.
   *
   * <p>For table-model topics, only regions whose database matches the topic's {@code DATABASE_KEY}
   * filter are bound. For tree-model topics, all local data regions are candidates. Additionally,
   * the {@link #onNewRegionCreated} callback ensures that regions created after this method runs
   * are also automatically bound.
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
          DataNodePipeMessages
              .PIPE_LOG_TOPIC_CONFIG_NOT_FOUND_FOR_TOPIC_CANNOT_SET_UP_CONSENSUS_A93339CE,
          topicName);
      return;
    }

    // Build the converter from the currently supported topic filters.
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_SETTING_UP_CONSENSUS_QUEUE_FOR_TOPIC_ISTABLETOPIC_ORDERMODE_4F1CDC66,
        topicName,
        topicConfig.isTableTopic(),
        topicConfig.getOrderMode(),
        topicConfig.getAttribute());

    final List<ConsensusGroupId> allGroupIds = ioTConsensus.getAllConsensusGroupIds();
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_DISCOVERED_CONSENSUS_GROUP_S_FOR_TOPIC_IN_CONSUMER_GROUP_012EE420,
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

      if (!matchesTopicDatabase(topicConfig, dbTableModel)) {
        LOGGER.info(
            DataNodePipeMessages
                .PIPE_LOG_SKIPPING_REGION_DATABASE_FOR_TABLE_TOPIC_DATABASE_KEY_2DA27A84,
            groupId,
            dbTableModel,
            topicName,
            topicConfig.getStringOrDefault(
                TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE));
        continue;
      }

      final String actualDbName = topicConfig.isTableTopic() ? dbTableModel : null;
      final ConsensusLogToTabletConverter converter = buildConverter(topicConfig, actualDbName);
      final SubscriptionWalRetentionPolicy retentionPolicy =
          buildSubscriptionWalRetentionPolicy(topicName, topicConfig, serverImpl);

      // Recover from persisted per-writer region progress when available. The queue will resolve a
      // replay start from that progress on first poll via the region-level locator.
      final RegionProgress committedRegionProgress =
          resolveFallbackCommittedRegionProgress(
              commitManager, consumerGroupId, topicName, groupId);
      final boolean hasLocalPersistedState =
          commitManager.hasPersistedState(consumerGroupId, topicName, groupId);
      final long tailStartSearchIndex = serverImpl.getSearchIndex() + 1;
      final long initialRuntimeVersion =
          regionRuntimeVersion.getOrDefault(groupId.convertToTConsensusGroupId(), 0L);
      final boolean initialActive =
          lastKnownPreferredWriter.getOrDefault(groupId.convertToTConsensusGroupId(), -1)
              == myNodeId;
      final Set<Integer> initialActiveWriterNodeIds =
          regionActiveWriterNodeIds.getOrDefault(
              groupId.convertToTConsensusGroupId(),
              initialActive
                  ? Collections.singleton(IOTDB_CONFIG.getDataNodeId())
                  : Collections.emptySet());
      final ConsensusRegionRuntimeState initialRuntimeState =
          new ConsensusRegionRuntimeState(
              initialRuntimeVersion,
              lastKnownPreferredWriter.getOrDefault(groupId.convertToTConsensusGroupId(), -1),
              initialActive,
              initialActiveWriterNodeIds);

      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_BINDING_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_IN_CONSUMER_45239EEA,
          topicName,
          consumerGroupId,
          groupId,
          dbTableModel,
          tailStartSearchIndex,
          hasLocalPersistedState,
          committedRegionProgress,
          initialRuntimeVersion,
          initialActive);

      SubscriptionAgent.broker()
          .bindConsensusPrefetchingQueue(
              consumerGroupId,
              topicName,
              topicConfig.getOrderMode(),
              groupId,
              serverImpl,
              retentionPolicy,
              converter,
              commitManager,
              committedRegionProgress,
              tailStartSearchIndex,
              initialRuntimeVersion,
              initialActive);

      SubscriptionAgent.broker().applyRuntimeStateForRegion(groupId, initialRuntimeState);

      bound = true;
    }

    if (!bound) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_NO_LOCAL_IOTCONSENSUS_DATA_REGION_FOUND_FOR_TOPIC_IN_CONSUMER_6FD0600E,
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
      final String column =
          topicConfig.getStringOrDefault(
              TopicConstant.COLUMN_KEY, TopicConstant.COLUMN_DEFAULT_VALUE);
      tablePattern = buildTablePattern(topicConfig);
      final Pattern columnPattern =
          TopicConstant.COLUMN_DEFAULT_VALUE.equals(column) ? null : Pattern.compile(column);
      return new ConsensusLogToTabletConverter(
          null, tablePattern, columnPattern, actualDatabaseName);
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

    return new ConsensusLogToTabletConverter(treePattern, tablePattern, null, actualDatabaseName);
  }

  private static boolean matchesTopicDatabase(
      final TopicConfig topicConfig, final String actualDatabaseName) {
    return !topicConfig.isTableTopic()
        || buildTablePattern(topicConfig).matchesDatabase(actualDatabaseName);
  }

  private static TablePattern buildTablePattern(final TopicConfig topicConfig) {
    return new TablePattern(
        true,
        topicConfig.getStringOrDefault(
            TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE),
        topicConfig.getStringOrDefault(TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE));
  }

  private static SubscriptionWalRetentionPolicy buildSubscriptionWalRetentionPolicy(
      final String topicName,
      final TopicConfig topicConfig,
      final IoTConsensusServerImpl serverImpl) {
    return new SubscriptionWalRetentionPolicy(
        topicName,
        resolveRetentionValue(
            topicConfig,
            TopicConstant.RETENTION_BYTES_KEY,
            serverImpl.getConfig().getReplication().getSubscriptionWalRetentionSizeInBytes()),
        resolveRetentionValue(
            topicConfig,
            TopicConstant.RETENTION_MS_KEY,
            serverImpl.getConfig().getReplication().getSubscriptionWalRetentionTimeMs()));
  }

  private static long resolveRetentionValue(
      final TopicConfig topicConfig, final String key, final long defaultValue) {
    if (!topicConfig.hasAttribute(key)) {
      return normalizeRetentionValue(defaultValue);
    }
    final long parsedValue = Long.parseLong(topicConfig.getAttribute().get(key));
    if (parsedValue == 0 || parsedValue < SubscriptionWalRetentionPolicy.UNBOUNDED) {
      throw new IllegalArgumentException(
          String.format(
              DataNodePipeMessages.PIPE_EXCEPTION_ILLEGAL_S_S_72D743AA,
              key,
              topicConfig.getAttribute().get(key)));
    }
    return normalizeRetentionValue(parsedValue);
  }

  private static long normalizeRetentionValue(final long retentionValue) {
    return retentionValue <= 0 ? SubscriptionWalRetentionPolicy.UNBOUNDED : retentionValue;
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
            DataNodePipeMessages
                .PIPE_LOG_TORE_DOWN_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_CONSUMER_GROUP_80B84227,
            topicName,
            consumerGroupId);
      } catch (final Exception e) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_FAILED_TO_TEAR_DOWN_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_F59E8B7C,
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
        DataNodePipeMessages
            .PIPE_LOG_CHECKING_NEW_SUBSCRIPTIONS_IN_CONSUMER_GROUP_FOR_CONSENSUS_4A56D78A,
        consumerGroupId,
        newTopicNames);

    setupConsensusSubscriptions(consumerGroupId, newTopicNames);
  }

  public static void applyRuntimeState(
      final TConsensusGroupId groupId, final ConsensusRegionRuntimeState runtimeState) {
    final int newPreferredNodeId = runtimeState.getPreferredWriterNodeId();
    final Integer oldPreferredBoxed = lastKnownPreferredWriter.put(groupId, newPreferredNodeId);
    final int oldPreferredNodeId = (oldPreferredBoxed != null) ? oldPreferredBoxed : -1;
    final ConsensusGroupId regionId = ConsensusGroupId.Factory.createFromTConsensusGroupId(groupId);
    final long oldRuntimeVersion = regionRuntimeVersion.getOrDefault(groupId, 0L);
    if (runtimeState.getRuntimeVersion() < oldRuntimeVersion) {
      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_IGNORE_STALE_RUNTIME_STATE_6C36B250,
          regionId,
          runtimeState.getRuntimeVersion(),
          oldRuntimeVersion,
          runtimeState);
      return;
    }
    regionRuntimeVersion.put(groupId, runtimeState.getRuntimeVersion());
    regionActiveWriterNodeIds.put(groupId, runtimeState.getActiveWriterNodeIds());
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_APPLYING_RUNTIME_STATE_1FB8937E,
        regionId,
        oldPreferredNodeId,
        newPreferredNodeId,
        oldRuntimeVersion,
        runtimeState.getRuntimeVersion(),
        runtimeState);
    SubscriptionAgent.broker().applyRuntimeStateForRegion(regionId, runtimeState);
  }

  public static void onRegionRouteChanged(
      final Map<TConsensusGroupId, TRegionReplicaSet> newMap, final long routingTimestamp) {
    final int myNodeId = IOTDB_CONFIG.getDataNodeId();

    for (final Map.Entry<TConsensusGroupId, TRegionReplicaSet> newEntry : newMap.entrySet()) {
      final TConsensusGroupId groupId = newEntry.getKey();
      final TRegionReplicaSet newReplicaSet = newEntry.getValue();

      final int newPreferredNodeId = getPreferredNodeId(newReplicaSet);
      final Integer oldPreferredBoxed = lastKnownPreferredWriter.put(groupId, newPreferredNodeId);
      final int oldPreferredNodeId = (oldPreferredBoxed != null) ? oldPreferredBoxed : -1;

      if (oldPreferredNodeId == newPreferredNodeId) {
        continue;
      }

      final ConsensusGroupId regionId =
          ConsensusGroupId.Factory.createFromTConsensusGroupId(groupId);
      final long oldRuntimeVersion = regionRuntimeVersion.getOrDefault(groupId, 0L);
      final long newRuntimeVersion = Math.max(routingTimestamp, oldRuntimeVersion);
      regionRuntimeVersion.put(groupId, newRuntimeVersion);

      final LinkedHashSet<Integer> activeWriterNodeIds =
          new LinkedHashSet<>(
              regionActiveWriterNodeIds.getOrDefault(groupId, Collections.emptySet()));
      activeWriterNodeIds.add(newPreferredNodeId);
      final Set<Integer> runtimeActiveWriterNodeIds =
          Collections.unmodifiableSet(activeWriterNodeIds);
      regionActiveWriterNodeIds.put(groupId, runtimeActiveWriterNodeIds);

      final ConsensusRegionRuntimeState runtimeState =
          new ConsensusRegionRuntimeState(
              newRuntimeVersion,
              newPreferredNodeId,
              newPreferredNodeId == myNodeId,
              runtimeActiveWriterNodeIds);

      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_REGION_PREFERRED_WRITER_46C1A894,
          regionId,
          oldPreferredNodeId,
          newPreferredNodeId,
          oldRuntimeVersion,
          newRuntimeVersion,
          runtimeState);

      SubscriptionAgent.broker().applyRuntimeStateForRegion(regionId, runtimeState);
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
