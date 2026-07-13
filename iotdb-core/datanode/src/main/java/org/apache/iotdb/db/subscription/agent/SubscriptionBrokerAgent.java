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

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.subscription.broker.ConsensusSubscriptionBroker;
import org.apache.iotdb.db.subscription.broker.ISubscriptionBroker;
import org.apache.iotdb.db.subscription.broker.SubscriptionBroker;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusLogToTabletConverter;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusRegionRuntimeState;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusSubscriptionCommitManager;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusSubscriptionSetupHandler;
import org.apache.iotdb.db.subscription.columnfilter.ColumnFilterBinder;
import org.apache.iotdb.db.subscription.columnfilter.ColumnFilterMatcher;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.resource.SubscriptionDataNodeResourceManager;
import org.apache.iotdb.db.subscription.task.execution.ConsensusSubscriptionPrefetchExecutorManager;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionSinkSubtask;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.request.SubscriptionSeekReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;

public class SubscriptionBrokerAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionBrokerAgent.class);

  private static final ColumnFilterMatcher EMPTY_COLUMN_FILTER_MATCHER =
      ColumnFilterMatcher.ofSelectedColumnNames(Collections.emptySet());

  /** Subscription brokers grouped by consumer group. */
  private final Map<String, List<ISubscriptionBroker>> consumerGroupIdToBrokers =
      new ConcurrentHashMap<>();

  private final Cache<Integer> prefetchingQueueCount =
      new Cache<>(this::getPrefetchingQueueCountInternal);

  private final Map<String, ColumnFilterMatcher> topicNameToColumnFilterMatcher =
      new ConcurrentHashMap<>();
  private final ColumnFilterBinder columnFilterBinder = new ColumnFilterBinder();

  //////////////////////////// provided for subscription agent ////////////////////////////

  public List<SubscriptionEvent> poll(
      final ConsumerConfig consumerConfig, final Set<String> topicNames, final long maxBytes) {
    return poll(consumerConfig, topicNames, maxBytes, Collections.emptyMap());
  }

  public List<SubscriptionEvent> poll(
      final ConsumerConfig consumerConfig,
      final Set<String> topicNames,
      final long maxBytes,
      final Map<String, TopicProgress> progressByTopic) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final String consumerId = consumerConfig.getConsumerId();
    final List<String> unsupportedConsensusTopics = getUnsupportedConsensusTopics(topicNames);
    if (!unsupportedConsensusTopics.isEmpty()) {
      final String errorMessage =
          buildUnsupportedConsensusRuntimeMessage(
              consumerGroupId, unsupportedConsensusTopics, "poll");
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }

    final List<SubscriptionEvent> allEvents = new ArrayList<>();
    long remainingBytes = maxBytes;

    final List<ISubscriptionBroker> brokers = getBrokers(consumerGroupId);
    for (final ISubscriptionBroker broker : brokers) {
      if (remainingBytes <= 0) {
        break;
      }
      final List<SubscriptionEvent> events =
          broker.poll(consumerId, topicNames, remainingBytes, progressByTopic);
      allEvents.addAll(events);
      for (final SubscriptionEvent event : events) {
        try {
          remainingBytes -= event.getCurrentResponseSize();
        } catch (final IOException ignored) {
          // best effort
        }
      }
    }

    if (allEvents.isEmpty() && brokers.isEmpty()) {
      final String errorMessage =
          String.format("Subscription: no broker bound to consumer group [%s]", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }

    return allEvents;
  }

  public List<SubscriptionEvent> pollTsFile(
      final ConsumerConfig consumerConfig,
      final SubscriptionCommitContext commitContext,
      final long writingOffset) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    // TsFile polling can only be called by pipe-based subscriptions
    final SubscriptionBroker pipeBroker = getPipeBroker(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      final String errorMessage =
          String.format(
              "Subscription: pipe broker bound to consumer group [%s] does not exist",
              consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    final String consumerId = consumerConfig.getConsumerId();
    return pipeBroker.pollTsFile(consumerId, commitContext, writingOffset);
  }

  public List<SubscriptionEvent> pollTablets(
      final ConsumerConfig consumerConfig,
      final SubscriptionCommitContext commitContext,
      final int offset) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final String consumerId = consumerConfig.getConsumerId();
    return getBrokerForTopicOrThrow(consumerGroupId, commitContext.getTopicName())
        .pollTablets(consumerId, commitContext, offset);
  }

  /**
   * @return list of successful commit contexts
   */
  public List<SubscriptionCommitContext> commit(
      final ConsumerConfig consumerConfig,
      final List<SubscriptionCommitContext> commitContexts,
      final boolean nack) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final String consumerId = consumerConfig.getConsumerId();
    final List<SubscriptionCommitContext> allSuccessful = new ArrayList<>();

    final List<ISubscriptionBroker> brokers = getBrokers(consumerGroupId);
    if (brokers.isEmpty()) {
      final String errorMessage =
          String.format("Subscription: no broker bound to consumer group [%s]", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }

    for (final ISubscriptionBroker broker : brokers) {
      final List<SubscriptionCommitContext> acceptedContexts =
          broker.selectAcceptedCommitContexts(commitContexts);
      if (!acceptedContexts.isEmpty()) {
        allSuccessful.addAll(broker.commit(consumerId, acceptedContexts, nack));
      }
    }

    return allSuccessful;
  }

  public int refreshInFlightEventLeases(
      final ConsumerConfig consumerConfig,
      final List<SubscriptionCommitContext> processorBufferedCommitContexts) {
    if (Objects.isNull(processorBufferedCommitContexts)
        || processorBufferedCommitContexts.isEmpty()) {
      return 0;
    }

    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final String consumerId = consumerConfig.getConsumerId();
    int refreshedCount = 0;
    for (final ISubscriptionBroker broker : getBrokers(consumerGroupId)) {
      final List<SubscriptionCommitContext> acceptedContexts =
          broker.selectAcceptedCommitContexts(processorBufferedCommitContexts);
      if (!acceptedContexts.isEmpty()) {
        refreshedCount += broker.refreshInFlightEventLeases(consumerId, acceptedContexts);
      }
    }
    return refreshedCount;
  }

  public Map<String, TopicProgress> getConsensusCommittedProgressByTopic(
      final ConsumerConfig consumerConfig,
      final List<SubscriptionCommitContext> acceptedCommitContexts,
      final boolean nack) {
    if (nack || acceptedCommitContexts.isEmpty()) {
      return Collections.emptyMap();
    }

    final Map<String, Set<String>> regionIdsByTopic = new LinkedHashMap<>();
    for (final SubscriptionCommitContext commitContext : acceptedCommitContexts) {
      if (Objects.isNull(commitContext) || Objects.isNull(commitContext.getTopicName())) {
        continue;
      }
      final String topicName = commitContext.getTopicName();
      if (!ConsensusSubscriptionSetupHandler.isConsensusBasedTopic(topicName)) {
        continue;
      }
      final String regionId = commitContext.getRegionId();
      if (Objects.isNull(regionId) || regionId.isEmpty()) {
        continue;
      }
      regionIdsByTopic.computeIfAbsent(topicName, ignored -> new LinkedHashSet<>()).add(regionId);
    }

    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final Map<String, TopicProgress> result = new LinkedHashMap<>();
    for (final Map.Entry<String, Set<String>> entry : regionIdsByTopic.entrySet()) {
      final String topicName = entry.getKey();
      final Map<String, RegionProgress> regionProgressById = new LinkedHashMap<>();
      for (final String regionId : entry.getValue()) {
        putRegionProgress(
            regionProgressById,
            regionId,
            getConsensusCommittedRegionProgress(consumerGroupId, topicName, regionId));
      }
      if (!regionProgressById.isEmpty()) {
        result.put(topicName, new TopicProgress(regionProgressById));
      }
    }
    return result;
  }

  private RegionProgress getConsensusCommittedRegionProgress(
      final String consumerGroupId, final String topicName, final String regionId) {
    try {
      return ConsensusSubscriptionCommitManager.getInstance()
          .getCommittedRegionProgress(
              consumerGroupId, topicName, ConsensusGroupId.Factory.createFromString(regionId));
    } catch (final IllegalArgumentException e) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_FAILED_TO_PARSE_CONSENSUS_REGION_ID_FOR_COMMITTED_9F1A50EB,
          regionId,
          topicName,
          consumerGroupId,
          e);
      return null;
    }
  }

  private void putRegionProgress(
      final Map<String, RegionProgress> regionProgressById,
      final String regionId,
      final RegionProgress regionProgress) {
    if (Objects.isNull(regionId)
        || regionId.isEmpty()
        || Objects.isNull(regionProgress)
        || regionProgress.getWriterPositions().isEmpty()) {
      return;
    }
    regionProgressById.put(regionId, regionProgress);
  }

  public void seek(
      final ConsumerConfig consumerConfig, final String topicName, final short seekType) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();

    if (seekType != SubscriptionSeekReq.SEEK_TO_BEGINNING
        && seekType != SubscriptionSeekReq.SEEK_TO_END) {
      final String errorMessage =
          String.format(
              "Subscription: consensus seek only supports beginning/end or topic progress, "
                  + "consumerGroup=%s, topic=%s, seekType=%s",
              consumerGroupId, topicName, seekType);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }

    final ConsensusSubscriptionBroker consensusBroker =
        getConsensusBrokerForSeekOrNoOp(consumerGroupId, topicName, "seek");
    if (Objects.nonNull(consensusBroker)) {
      consensusBroker.seek(topicName, seekType);
    }
  }

  public void seekToTopicProgress(
      final ConsumerConfig consumerConfig,
      final String topicName,
      final TopicProgress topicProgress) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();

    final ConsensusSubscriptionBroker consensusBroker =
        getConsensusBrokerForSeekOrNoOp(consumerGroupId, topicName, "seek(topicProgress)");
    if (Objects.nonNull(consensusBroker)) {
      consensusBroker.seek(topicName, topicProgress);
    }
  }

  public void seekAfterTopicProgress(
      final ConsumerConfig consumerConfig,
      final String topicName,
      final TopicProgress topicProgress) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();

    final ConsensusSubscriptionBroker consensusBroker =
        getConsensusBrokerForSeekOrNoOp(consumerGroupId, topicName, "seekAfter(topicProgress)");
    if (Objects.nonNull(consensusBroker)) {
      consensusBroker.seekAfter(topicName, topicProgress);
    }
  }

  private ConsensusSubscriptionBroker getConsensusBrokerForSeekOrNoOp(
      final String consumerGroupId, final String topicName, final String operation) {
    if (!ConsensusSubscriptionSetupHandler.isConsensusBasedTopic(topicName)) {
      final String errorMessage =
          String.format(
              "Subscription: %s is only supported for consensus-based subscriptions, "
                  + "consumerGroup=%s, topic=%s",
              operation, consumerGroupId, topicName);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }

    if (!(DataRegionConsensusImpl.getInstance() instanceof IoTConsensus)) {
      final String errorMessage =
          buildUnsupportedConsensusRuntimeMessage(consumerGroupId, topicName, operation);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }

    ensureConsensusSeekRuntimeAvailable(consumerGroupId, topicName, operation);

    final ConsensusSubscriptionBroker consensusBroker = getConsensusBroker(consumerGroupId);
    if (Objects.nonNull(consensusBroker) && consensusBroker.hasQueue(topicName)) {
      return consensusBroker;
    }

    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_SUBSCRIPTION_CONSENSUS_IS_A_NO_OP_ON_THIS_DATANODE_BECAUSE_28F7E92B,
        operation,
        consumerGroupId,
        topicName);
    return null;
  }

  private void ensureConsensusSeekRuntimeAvailable(
      final String consumerGroupId, final String topicName, final String operation) {
    if (!ConsensusSubscriptionPrefetchExecutorManager.getInstance().isStarted()
        || SubscriptionAgent.runtime().isShutdown()) {
      final String errorMessage =
          String.format(
              "Subscription: consensus %s is unavailable because subscription runtime is stopped, "
                  + "consumerGroup=%s, topic=%s",
              operation, consumerGroupId, topicName);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
  }

  private List<String> getUnsupportedConsensusTopics(final Set<String> topicNames) {
    if (DataRegionConsensusImpl.getInstance() instanceof IoTConsensus) {
      return Collections.emptyList();
    }

    final List<String> unsupportedConsensusTopics = new ArrayList<>();
    for (final String topicName : topicNames) {
      if (ConsensusSubscriptionSetupHandler.isConsensusBasedTopic(topicName)) {
        unsupportedConsensusTopics.add(topicName);
      }
    }
    return unsupportedConsensusTopics;
  }

  private String buildUnsupportedConsensusRuntimeMessage(
      final String consumerGroupId, final String topicName, final String operation) {
    return buildUnsupportedConsensusRuntimeMessage(
        consumerGroupId, Collections.singletonList(topicName), operation);
  }

  private String buildUnsupportedConsensusRuntimeMessage(
      final String consumerGroupId, final List<String> topicNames, final String operation) {
    final IConsensus dataRegionConsensus = DataRegionConsensusImpl.getInstance();
    final String configuredProtocol =
        IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass();
    final String runtimeConsensusImplementation =
        Objects.nonNull(dataRegionConsensus) ? dataRegionConsensus.getClass().getName() : "null";
    return String.format(
        "Subscription: cannot %s consensus-based topic(s) %s in consumer group [%s] because "
            + "mode=consensus only supports data_region_consensus_protocol_class=%s, but current "
            + "configured value is %s (runtime consensus implementation: %s)",
        operation,
        topicNames,
        consumerGroupId,
        ConsensusFactory.IOT_CONSENSUS,
        configuredProtocol,
        runtimeConsensusImplementation);
  }

  public boolean isCommitContextOutdated(final SubscriptionCommitContext commitContext) {
    final ISubscriptionBroker broker =
        getBrokerForTopic(commitContext.getConsumerGroupId(), commitContext.getTopicName());
    return Objects.isNull(broker) || broker.isCommitContextOutdated(commitContext);
  }

  private ISubscriptionBroker getBrokerForTopic(
      final String consumerGroupId, final String topicName) {
    for (final ISubscriptionBroker broker : getBrokers(consumerGroupId)) {
      if (broker.acceptsTopic(topicName)) {
        return broker;
      }
    }
    return null;
  }

  private ISubscriptionBroker getBrokerForTopicOrThrow(
      final String consumerGroupId, final String topicName) {
    final ISubscriptionBroker broker = getBrokerForTopic(consumerGroupId, topicName);
    if (Objects.nonNull(broker)) {
      return broker;
    }
    final String errorMessage =
        String.format(DataNodeMiscMessages.SUBSCRIPTION_BROKER_NOT_EXIST_FMT, consumerGroupId);
    LOGGER.warn(errorMessage);
    throw new SubscriptionException(errorMessage);
  }

  public List<String> fetchTopicNamesToUnsubscribe(
      final ConsumerConfig consumerConfig, final Set<String> topicNames) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();

    // Consensus-based subscription topics are unbounded streams, so they do not trigger
    // auto-unsubscribe.
    final ConsensusSubscriptionBroker consensusBroker = getConsensusBroker(consumerGroupId);
    final Set<String> pipeOnlyTopicNames;
    if (Objects.nonNull(consensusBroker)) {
      pipeOnlyTopicNames = new java.util.HashSet<>(topicNames);
      pipeOnlyTopicNames.removeIf(consensusBroker::hasQueue);
    } else {
      pipeOnlyTopicNames = topicNames;
    }

    if (pipeOnlyTopicNames.isEmpty()) {
      return Collections.emptyList();
    }

    final SubscriptionBroker pipeBroker = getPipeBroker(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      return Collections.emptyList();
    }
    return pipeBroker.fetchTopicNamesToUnsubscribe(pipeOnlyTopicNames);
  }

  private List<ISubscriptionBroker> getBrokers(final String consumerGroupId) {
    return consumerGroupIdToBrokers.getOrDefault(consumerGroupId, Collections.emptyList());
  }

  private SubscriptionBroker getPipeBroker(final String consumerGroupId) {
    return getBroker(consumerGroupId, SubscriptionBroker.class);
  }

  private ConsensusSubscriptionBroker getConsensusBroker(final String consumerGroupId) {
    return getBroker(consumerGroupId, ConsensusSubscriptionBroker.class);
  }

  private <T extends ISubscriptionBroker> T getBroker(
      final String consumerGroupId, final Class<T> brokerClass) {
    return getBroker(getBrokers(consumerGroupId), brokerClass);
  }

  private <T extends ISubscriptionBroker> T getBroker(
      final Collection<ISubscriptionBroker> brokers, final Class<T> brokerClass) {
    for (final ISubscriptionBroker broker : brokers) {
      if (brokerClass.isInstance(broker)) {
        return brokerClass.cast(broker);
      }
    }
    return null;
  }

  private <T extends ISubscriptionBroker> List<T> getBrokers(final Class<T> brokerClass) {
    final List<T> result = new ArrayList<>();
    for (final List<ISubscriptionBroker> brokers : consumerGroupIdToBrokers.values()) {
      final T broker = getBroker(brokers, brokerClass);
      if (Objects.nonNull(broker)) {
        result.add(broker);
      }
    }
    return result;
  }

  private <T extends ISubscriptionBroker> T getOrCreateBroker(
      final String consumerGroupId, final Class<T> brokerClass, final Function<String, T> factory) {
    final List<ISubscriptionBroker> brokers =
        consumerGroupIdToBrokers.computeIfAbsent(
            consumerGroupId, id -> new CopyOnWriteArrayList<>());
    final T existingBroker = getBroker(brokers, brokerClass);
    if (Objects.nonNull(existingBroker)) {
      return existingBroker;
    }

    synchronized (brokers) {
      final T recheckedBroker = getBroker(brokers, brokerClass);
      if (Objects.nonNull(recheckedBroker)) {
        return recheckedBroker;
      }
      final T createdBroker = factory.apply(consumerGroupId);
      brokers.add(createdBroker);
      return createdBroker;
    }
  }

  private void warnBrokerNotEmpty(final String consumerGroupId, final ISubscriptionBroker broker) {
    if (broker instanceof ConsensusSubscriptionBroker) {
      LOGGER.warn(DataNodeMiscMessages.SUBSCRIPTION_CONSENSUS_BROKER_NOT_EMPTY, consumerGroupId);
    } else {
      LOGGER.warn(DataNodeMiscMessages.SUBSCRIPTION_PIPE_BROKER_NOT_EMPTY, consumerGroupId);
    }
  }

  private void logBrokerDropped(final String consumerGroupId, final ISubscriptionBroker broker) {
    if (broker instanceof ConsensusSubscriptionBroker) {
      LOGGER.info(DataNodeMiscMessages.SUBSCRIPTION_DROP_CONSENSUS_BROKER, consumerGroupId);
    } else {
      LOGGER.info(DataNodeMiscMessages.SUBSCRIPTION_DROP_BROKER, consumerGroupId);
    }
  }

  /////////////////////////////// broker ///////////////////////////////

  public boolean isBrokerExist(final String consumerGroupId) {
    return !getBrokers(consumerGroupId).isEmpty();
  }

  public void createPipeBrokerIfNotExist(final String consumerGroupId) {
    getOrCreateBroker(consumerGroupId, SubscriptionBroker.class, SubscriptionBroker::new);
    LOGGER.info(DataNodeMiscMessages.SUBSCRIPTION_CREATE_BROKER, consumerGroupId);
  }

  /**
   * @return {@code true} if drop broker success, {@code false} otherwise
   */
  public boolean dropBroker(final String consumerGroupId) {
    final List<ISubscriptionBroker> brokers = consumerGroupIdToBrokers.get(consumerGroupId);
    if (Objects.isNull(brokers) || brokers.isEmpty()) {
      return true;
    }

    boolean dropped = false;
    synchronized (brokers) {
      for (final ISubscriptionBroker broker : new ArrayList<>(brokers)) {
        if (!broker.isEmpty()) {
          warnBrokerNotEmpty(consumerGroupId, broker);
          continue;
        }
        brokers.remove(broker);
        dropped = true;
        logBrokerDropped(consumerGroupId, broker);
      }
      if (brokers.isEmpty()) {
        consumerGroupIdToBrokers.remove(consumerGroupId, brokers);
      }
    }

    return dropped;
  }

  /////////////////////////////// prefetching queue ///////////////////////////////

  public void bindPrefetchingQueue(final SubscriptionSinkSubtask subtask) {
    final String consumerGroupId = subtask.getConsumerGroupId();
    getOrCreateBroker(
            consumerGroupId,
            SubscriptionBroker.class,
            id -> {
              LOGGER.info(
                  DataNodeMiscMessages.SUBSCRIPTION_CREATE_PIPE_BROKER_FOR_BINDING,
                  consumerGroupId);
              return new SubscriptionBroker(consumerGroupId);
            })
        .bindPrefetchingQueue(subtask.getTopicName(), subtask.getInputPendingQueue());
    prefetchingQueueCount.invalidate();
  }

  public void bindConsensusPrefetchingQueue(
      final String consumerGroupId,
      final String topicName,
      final String orderMode,
      final ConsensusGroupId consensusGroupId,
      final IoTConsensusServerImpl serverImpl,
      final SubscriptionWalRetentionPolicy retentionPolicy,
      final ConsensusLogToTabletConverter converter,
      final ConsensusSubscriptionCommitManager commitManager,
      final RegionProgress fallbackCommittedRegionProgress,
      final long tailStartSearchIndex,
      final long initialRuntimeVersion,
      final boolean initialActive) {
    getOrCreateBroker(
            consumerGroupId,
            ConsensusSubscriptionBroker.class,
            id -> {
              LOGGER.info(
                  DataNodeMiscMessages.SUBSCRIPTION_CREATE_CONSENSUS_BROKER_FOR_BINDING,
                  consumerGroupId);
              return new ConsensusSubscriptionBroker(consumerGroupId);
            })
        .bindConsensusPrefetchingQueue(
            topicName,
            orderMode,
            consensusGroupId,
            serverImpl,
            retentionPolicy,
            converter,
            commitManager,
            fallbackCommittedRegionProgress,
            tailStartSearchIndex,
            initialRuntimeVersion,
            initialActive);
    prefetchingQueueCount.invalidate();
  }

  public void refreshConsensusQueueOrderMode(final String topicName, final String orderMode) {
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_SUBSCRIPTIONBROKERAGENT_REFRESHING_CONSENSUS_QUEUE_ORDER_1886704D,
        topicName,
        orderMode);
    for (final ConsensusSubscriptionBroker broker : getBrokers(ConsensusSubscriptionBroker.class)) {
      broker.refreshConsensusQueueOrderMode(topicName, orderMode);
    }
  }

  public void refreshColumnFilter(final String topicName, final TopicConfig topicConfig) {
    final ColumnFilterMatcher matcher;
    try {
      final Map<String, Map<String, TsTable>> bindingTables =
          getColumnFilterBindingTables(topicConfig);
      if (Objects.isNull(bindingTables)) {
        topicNameToColumnFilterMatcher.remove(topicName);
        LOGGER.info(
            DataNodeMiscMessages.SUBSCRIPTION_COLUMN_FILTER_SCHEMA_NOT_AVAILABLE, topicName);
        return;
      }
      matcher =
          ColumnFilterMatcher.fromBoundColumnFilter(
              columnFilterBinder.bind(topicConfig, bindingTables));
    } catch (final Exception e) {
      LOGGER.warn(DataNodeMiscMessages.SUBSCRIPTION_REFRESH_COLUMN_FILTER_FAILED, topicName, e);
      topicNameToColumnFilterMatcher.put(topicName, EMPTY_COLUMN_FILTER_MATCHER);
      return;
    }
    topicNameToColumnFilterMatcher.put(topicName, matcher);
    LOGGER.info(DataNodeMiscMessages.SUBSCRIPTION_REFRESH_COLUMN_FILTER_SUCCESS, topicName);
  }

  public ColumnFilterMatcher getColumnFilterMatcher(final String topicName) {
    final ColumnFilterMatcher matcher = topicNameToColumnFilterMatcher.get(topicName);
    if (Objects.nonNull(matcher)) {
      return matcher;
    }

    final TopicConfig topicConfig =
        SubscriptionAgent.topic().getTopicConfigs(Collections.singleton(topicName)).get(topicName);
    if (Objects.isNull(topicConfig)) {
      return ColumnFilterMatcher.matchAll();
    }

    try {
      refreshColumnFilter(topicName, topicConfig);
      return topicNameToColumnFilterMatcher.getOrDefault(
          topicName, getDefaultColumnFilterMatcher(topicConfig));
    } catch (final Exception e) {
      LOGGER.warn(
          DataNodeMiscMessages.SUBSCRIPTION_LAZY_REFRESH_COLUMN_FILTER_FAILED, topicName, e);
      return EMPTY_COLUMN_FILTER_MATCHER;
    }
  }

  private static ColumnFilterMatcher getDefaultColumnFilterMatcher(final TopicConfig topicConfig) {
    return Objects.nonNull(topicConfig)
            && topicConfig.isTableTopic()
            && !topicConfig.isColumnFilterTrivial()
        ? EMPTY_COLUMN_FILTER_MATCHER
        : ColumnFilterMatcher.matchAll();
  }

  private static Map<String, Map<String, TsTable>> getColumnFilterBindingTables(
      final TopicConfig topicConfig) {
    if (Objects.isNull(topicConfig)
        || !topicConfig.isTableTopic()
        || topicConfig.isColumnFilterTrivial()) {
      return Collections.emptyMap();
    }

    final String database =
        topicConfig.getStringOrDefault(
            TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE);
    final String tableName =
        topicConfig.getStringOrDefault(TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE);
    if (isDefaultTopicPattern(database, TopicConstant.DATABASE_DEFAULT_VALUE)
        || isDefaultTopicPattern(tableName, TopicConstant.TABLE_DEFAULT_VALUE)
        || !isLiteralTopicPattern(database)
        || !isLiteralTopicPattern(tableName)) {
      return DataNodeTableCache.getInstance().getTableSnapshot();
    }

    final TsTable table = DataNodeTableCache.getInstance().getTable(database, tableName, false);
    return Objects.isNull(table)
        ? null
        : Collections.singletonMap(database, Collections.singletonMap(tableName, table));
  }

  private static boolean isLiteralTopicPattern(final String pattern) {
    final String regexMetaCharacters = ".*+?[](){}\\|^$";
    return Objects.nonNull(pattern)
        && pattern.chars().noneMatch(c -> regexMetaCharacters.indexOf((char) c) >= 0);
  }

  private static boolean isDefaultTopicPattern(final String pattern, final String defaultPattern) {
    return Objects.isNull(pattern) || defaultPattern.equals(pattern.trim());
  }

  public void dropColumnFilter(final String topicName) {
    topicNameToColumnFilterMatcher.remove(topicName);
    LOGGER.info(DataNodeMiscMessages.SUBSCRIPTION_DROP_COLUMN_FILTER, topicName);
  }

  public void unbindConsensusPrefetchingQueue(
      final String consumerGroupId, final String topicName) {
    final ConsensusSubscriptionBroker broker = getConsensusBroker(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_CONSENSUS_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_E46FCDD9,
          consumerGroupId);
      return;
    }
    broker.unbindConsensusPrefetchingQueue(topicName);
    prefetchingQueueCount.invalidate();
  }

  public void unbindByRegion(final ConsensusGroupId regionId) {
    int totalClosed = 0;
    for (final ConsensusSubscriptionBroker broker : getBrokers(ConsensusSubscriptionBroker.class)) {
      totalClosed += broker.unbindByRegion(regionId);
    }
    if (totalClosed > 0) {
      prefetchingQueueCount.invalidate();
      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_UNBOUND_CONSENSUS_PREFETCHING_QUEUE_S_FOR_REMOVED_AC018742,
          totalClosed,
          regionId);
    }
  }

  /**
   * Activates or deactivates all consensus prefetching queues bound to {@code regionId} across all
   * consumer groups. Called on leader migration to ensure only the preferred writer serves
   * subscription data.
   */
  public void setActiveForRegion(final ConsensusGroupId regionId, final boolean active) {
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_SUBSCRIPTIONBROKERAGENT_SETACTIVEFORREGION_REGIONID_ACTIVE_4AC3A2CB,
        regionId,
        active);
    for (final ConsensusSubscriptionBroker broker : getBrokers(ConsensusSubscriptionBroker.class)) {
      broker.setActiveForRegion(regionId, active);
    }
  }

  public void setActiveWritersForRegion(
      final ConsensusGroupId regionId, final Set<Integer> activeWriterNodeIds) {
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_SUBSCRIPTIONBROKERAGENT_SETACTIVEWRITERSFORREGION_REGIONID_48B39B3E,
        regionId,
        activeWriterNodeIds);
    for (final ConsensusSubscriptionBroker broker : getBrokers(ConsensusSubscriptionBroker.class)) {
      broker.setActiveWritersForRegion(regionId, activeWriterNodeIds);
    }
  }

  public void applyRuntimeStateForRegion(
      final ConsensusGroupId regionId, final ConsensusRegionRuntimeState runtimeState) {
    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_SUBSCRIPTIONBROKERAGENT_APPLYRUNTIMESTATEFORREGION_REGIONID_6D8C37A1,
        regionId,
        runtimeState);
    for (final ConsensusSubscriptionBroker broker : getBrokers(ConsensusSubscriptionBroker.class)) {
      broker.applyRuntimeStateForRegion(regionId, runtimeState);
    }
  }

  public void abortConsensusPendingSeeksForRuntimeStop() {
    for (final ConsensusSubscriptionBroker broker : getBrokers(ConsensusSubscriptionBroker.class)) {
      broker.abortPendingSeeksForRuntimeStop();
    }
  }

  public void updateCompletedTopicNames(final String consumerGroupId, final String topicName) {
    final SubscriptionBroker pipeBroker = getPipeBroker(consumerGroupId);
    if (Objects.isNull(pipeBroker)) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_PIPE_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_E9B60B22,
          consumerGroupId);
      return;
    }
    pipeBroker.updateCompletedTopicNames(topicName);
  }

  public void unbindPrefetchingQueue(final String consumerGroupId, final String topicName) {
    final ISubscriptionBroker broker = getBrokerForTopic(consumerGroupId, topicName);
    if (Objects.isNull(broker)) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_EXIST_74CAD5BE,
          consumerGroupId);
      return;
    }
    broker.unbind(topicName);
    prefetchingQueueCount.invalidate();
  }

  public void removePrefetchingQueue(final String consumerGroupId, final String topicName) {
    final ISubscriptionBroker broker = getBrokerForTopic(consumerGroupId, topicName);
    if (Objects.isNull(broker)) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_EXIST_74CAD5BE,
          consumerGroupId);
      return;
    }
    broker.removeQueue(topicName);
    prefetchingQueueCount.invalidate();
  }

  public boolean executePrefetch(final String consumerGroupId, final String topicName) {
    final ISubscriptionBroker broker = getBrokerForTopic(consumerGroupId, topicName);
    if (Objects.isNull(broker)) {
      SubscriptionDataNodeResourceManager.log()
          .schedule(SubscriptionBrokerAgent.class, consumerGroupId, topicName)
          .ifPresent(
              l ->
                  l.warn(
                      DataNodePipeMessages
                          .PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_EXIST_74CAD5BE,
                      consumerGroupId));
      return false;
    }
    return broker.executePrefetch(topicName);
  }

  public int getPipeEventCount(final String consumerGroupId, final String topicName) {
    final ISubscriptionBroker broker = getBrokerForTopic(consumerGroupId, topicName);
    if (Objects.isNull(broker)) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_EXIST_74CAD5BE,
          consumerGroupId);
      return 0;
    }
    return broker.getEventCount(topicName);
  }

  public int getPrefetchingQueueCount() {
    return prefetchingQueueCount.get();
  }

  public Map<String, Long> getConsensusLagSummary() {
    final Map<String, Long> result = new ConcurrentHashMap<>();
    for (final Map.Entry<String, List<ISubscriptionBroker>> entry :
        consumerGroupIdToBrokers.entrySet()) {
      for (final ISubscriptionBroker broker : entry.getValue()) {
        for (final Map.Entry<String, Long> lag : broker.getLagSummary().entrySet()) {
          result.put(entry.getKey() + "/" + lag.getKey(), lag.getValue());
        }
      }
    }
    return result;
  }

  private int getPrefetchingQueueCountInternal() {
    int count = 0;
    for (final List<ISubscriptionBroker> brokers : consumerGroupIdToBrokers.values()) {
      for (final ISubscriptionBroker broker : brokers) {
        count += broker.getQueueCount();
      }
    }
    return count;
  }

  /////////////////////////////// Commit Progress ///////////////////////////////

  public Map<String, ByteBuffer> collectAllRegionCommitProgress(final int dataNodeId) {
    return ConsensusSubscriptionCommitManager.getInstance().collectAllRegionProgress(dataNodeId);
  }

  /**
   * Receives a committed progress broadcast from another DataNode (Leader → Follower). Delegates to
   * CommitManager to update local progress state.
   */
  public void receiveSubscriptionProgress(
      final String consumerGroupId,
      final String topicName,
      final String regionId,
      final long physicalTime,
      final int writerNodeId,
      final long localSeq) {
    ConsensusSubscriptionCommitManager.getInstance()
        .receiveProgressBroadcast(
            consumerGroupId, topicName, regionId, physicalTime, writerNodeId, localSeq);
  }

  /////////////////////////////// Cache ///////////////////////////////

  /**
   * A simple generic cache that computes and stores a value on demand.
   *
   * <p>Both {@code value} and {@code valid} are volatile to ensure visibility across threads. The
   * {@code get()} method uses a local snapshot of {@code valid} to avoid double-read reordering.
   * Concurrent recomputation by multiple threads is benign (idempotent supplier).
   *
   * @param <T> the type of the cached value
   */
  private static class Cache<T> {

    private volatile T value;
    private volatile boolean valid = false;
    private final Supplier<T> supplier;

    /**
     * Construct a cache with a supplier that knows how to compute the value.
     *
     * @param supplier a Supplier that computes the value when needed
     */
    private Cache(final Supplier<T> supplier) {
      this.supplier = supplier;
    }

    /** Invalidate the cache. The next call to get() will recompute the value. */
    private void invalidate() {
      valid = false;
    }

    /**
     * Return the cached value, recomputing it if the cache is invalid.
     *
     * @return the current value, recomputed if necessary
     */
    private T get() {
      if (!valid) {
        final T computed = supplier.get();
        value = computed;
        valid = true;
        return computed;
      }
      return value;
    }
  }
}
