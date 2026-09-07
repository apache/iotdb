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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMetaKeeper;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaRespExceptionMessage;
import org.apache.iotdb.mpp.rpc.thrift.TTopicOwnerLeaseEntry;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SubscriptionTopicAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionTopicAgent.class);

  private final TopicMetaKeeper topicMetaKeeper;

  public SubscriptionTopicAgent() {
    this.topicMetaKeeper = new TopicMetaKeeper();
  }

  ////////////////////////// TopicMeta Lock Control //////////////////////////

  protected void acquireReadLock() {
    topicMetaKeeper.acquireReadLock();
  }

  protected void releaseReadLock() {
    topicMetaKeeper.releaseReadLock();
  }

  protected void acquireWriteLock() {
    topicMetaKeeper.acquireWriteLock();
  }

  protected void releaseWriteLock() {
    topicMetaKeeper.releaseWriteLock();
  }

  ////////////////////////// Topic Management Entry //////////////////////////

  public TPushTopicMetaRespExceptionMessage handleSingleTopicMetaChanges(
      final TopicMeta topicMetaFromCoordinator) {
    acquireWriteLock();
    try {
      handleSingleTopicMetaChangesInternal(topicMetaFromCoordinator);
      return null;
    } catch (final Exception e) {
      final String topicName = topicMetaFromCoordinator.getTopicName();
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_EXCEPTION_OCCURRED_WHEN_HANDLING_SINGLE_TOPIC_META_CHANGES_43434FC4,
          topicName,
          e);
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to handle single topic meta changes for topic %s, because %s",
              topicName, e);
      return new TPushTopicMetaRespExceptionMessage(
          topicName, exceptionMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleSingleTopicMetaChangesInternal(final TopicMeta metaFromCoordinator) {
    final String topicName = metaFromCoordinator.getTopicName();
    final boolean isTableModel = metaFromCoordinator.visibleUnderTableModel();
    final TopicMeta oldMeta = topicMetaKeeper.getTopicMeta(topicName, isTableModel);
    TopicMeta.validateOwnerProgression(oldMeta, metaFromCoordinator);
    topicMetaKeeper.removeTopicMeta(topicName, isTableModel);
    topicMetaKeeper.addTopicMeta(topicName, metaFromCoordinator);
    if (shouldRefreshColumnFilter(oldMeta, metaFromCoordinator)) {
      SubscriptionAgent.broker().refreshColumnFilter(topicName, metaFromCoordinator.getConfig());
    } else if (!metaFromCoordinator.getConfig().isTableTopic()
        && !topicMetaKeeper.containsTopicMeta(topicName, true)) {
      // ConfigNode rejects column-filter on tree topics. Drop defensively in case stale or replayed
      // topic metadata reaches this DataNode after a table-topic to tree-topic transition.
      SubscriptionAgent.broker().dropColumnFilter(topicName);
    }
    SubscriptionAgent.broker()
        .refreshConsensusQueueOrderMode(
            topicName, isTableModel, metaFromCoordinator.getConfig().getOrderMode());
  }

  static boolean shouldRefreshColumnFilter(final TopicMeta oldMeta, final TopicMeta newMeta) {
    if (Objects.isNull(newMeta) || !newMeta.getConfig().isTableTopic()) {
      return false;
    }
    if (Objects.isNull(oldMeta) || !oldMeta.getConfig().isTableTopic()) {
      return true;
    }

    final TopicConfig oldConfig = oldMeta.getConfig();
    final TopicConfig newConfig = newMeta.getConfig();
    return !Objects.equals(
            normalizeColumnFilterBindingValue(oldConfig.getColumnFilter()),
            normalizeColumnFilterBindingValue(newConfig.getColumnFilter()))
        || !Objects.equals(
            normalizeColumnFilterBindingValue(
                getAttributeIgnoreCase(
                    oldConfig, TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE)),
            normalizeColumnFilterBindingValue(
                getAttributeIgnoreCase(
                    newConfig, TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE)))
        || !Objects.equals(
            normalizeColumnFilterBindingValue(
                getAttributeIgnoreCase(
                    oldConfig, TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE)),
            normalizeColumnFilterBindingValue(
                getAttributeIgnoreCase(
                    newConfig, TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE)));
  }

  private static String getAttributeIgnoreCase(
      final TopicConfig topicConfig, final String key, final String defaultValue) {
    return topicConfig.getAttribute().entrySet().stream()
        .filter(entry -> key.equalsIgnoreCase(entry.getKey()))
        .map(Map.Entry::getValue)
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(defaultValue);
  }

  private static String normalizeColumnFilterBindingValue(final String value) {
    return Objects.nonNull(value) ? value.trim().toLowerCase(Locale.ROOT) : "";
  }

  public TPushTopicMetaRespExceptionMessage handleTopicMetaChanges(
      final List<TopicMeta> topicMetasFromCoordinator) {
    acquireWriteLock();
    try {
      for (final TopicMeta topicMetaFromCoordinator : topicMetasFromCoordinator) {
        try {
          handleSingleTopicMetaChangesInternal(topicMetaFromCoordinator);
        } catch (final Exception e) {
          final String topicName = topicMetaFromCoordinator.getTopicName();
          LOGGER.warn(
              DataNodePipeMessages
                  .PIPE_LOG_EXCEPTION_OCCURRED_WHEN_HANDLING_SINGLE_TOPIC_META_CHANGES_43434FC4,
              topicName,
              e);
          final String exceptionMessage =
              String.format(
                  "Subscription: Failed to handle single topic meta changes for topic %s, because %s",
                  topicName, e);
          return new TPushTopicMetaRespExceptionMessage(
              topicName, exceptionMessage, System.currentTimeMillis());
        }
      }
      return null;
    } finally {
      releaseWriteLock();
    }
  }

  public TPushTopicMetaRespExceptionMessage handleDropTopic(final String topicName) {
    return handleDropTopic(topicName, null);
  }

  public TPushTopicMetaRespExceptionMessage handleDropTopic(
      final String topicName, final boolean isTableModel) {
    return handleDropTopic(topicName, Boolean.valueOf(isTableModel));
  }

  private TPushTopicMetaRespExceptionMessage handleDropTopic(
      final String topicName, final Boolean isTableModel) {
    acquireWriteLock();
    try {
      handleDropTopicInternal(topicName, isTableModel);
      return null;
    } catch (final Exception e) {
      LOGGER.warn(DataNodeMiscMessages.EXCEPTION_DROPPING_TOPIC, topicName, e);
      final String exceptionMessage =
          String.format("Subscription: Failed to drop topic %s, because %s", topicName, e);
      return new TPushTopicMetaRespExceptionMessage(
          topicName, exceptionMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleDropTopicInternal(final String topicName, final Boolean isTableModel) {
    final TopicMeta topicMeta =
        Objects.isNull(isTableModel)
            ? topicMetaKeeper.getTopicMeta(topicName)
            : topicMetaKeeper.getTopicMeta(topicName, isTableModel);
    if (Objects.isNull(isTableModel)) {
      topicMetaKeeper.removeTopicMeta(topicName);
    } else {
      topicMetaKeeper.removeTopicMeta(topicName, isTableModel);
    }
    if (Objects.nonNull(topicMeta) && topicMeta.visibleUnderTableModel()) {
      SubscriptionAgent.broker().dropColumnFilter(topicName);
    }
  }

  public boolean isTopicExisted(final String topicName) {
    acquireReadLock();
    try {
      return topicMetaKeeper.containsTopicMeta(topicName);
    } finally {
      releaseReadLock();
    }
  }

  public String getTopicFormat(final String topicName) {
    return getTopicFormat(topicName, false);
  }

  public String getTopicFormat(final String topicName, final boolean isTableModel) {
    acquireReadLock();
    try {
      return topicMetaKeeper.containsTopicMeta(topicName, isTableModel)
          ? topicMetaKeeper
              .getTopicMeta(topicName, isTableModel)
              .getConfig()
              .getStringOrDefault(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_DEFAULT_VALUE)
          : null;
    } finally {
      releaseReadLock();
    }
  }

  public String getTopicMode(final String topicName) {
    return getTopicMode(topicName, false);
  }

  public String getTopicMode(final String topicName, final boolean isTableModel) {
    acquireReadLock();
    try {
      return topicMetaKeeper.containsTopicMeta(topicName, isTableModel)
          ? topicMetaKeeper.getTopicMeta(topicName, isTableModel).getConfig().getMode()
          : null;
    } finally {
      releaseReadLock();
    }
  }

  public String getTopicOrderMode(final String topicName) {
    return getTopicOrderMode(topicName, false);
  }

  public String getTopicOrderMode(final String topicName, final boolean isTableModel) {
    acquireReadLock();
    try {
      return topicMetaKeeper.getTopicMeta(topicName, isTableModel).getConfig().getOrderMode();
    } finally {
      releaseReadLock();
    }
  }

  public Map<String, TopicConfig> getTopicConfigs(final Set<String> topicNames) {
    return getTopicConfigs(topicNames, false);
  }

  public Map<String, TopicConfig> getTopicConfigs(
      final Set<String> topicNames, final boolean isTableModel) {
    acquireReadLock();
    try {
      return topicNames.stream()
          .filter(topicName -> topicMetaKeeper.containsTopicMeta(topicName, isTableModel))
          .collect(
              Collectors.toMap(
                  topicName -> topicName,
                  topicName -> topicMetaKeeper.getTopicMeta(topicName, isTableModel).getConfig()));
    } finally {
      releaseReadLock();
    }
  }

  public TSStatus checkTopicOwner(final ConsumerConfig consumerConfig, final String topicName) {
    acquireReadLock();
    try {
      final TopicMeta topicMeta =
          topicMetaKeeper.getTopicMeta(topicName, isTableModel(consumerConfig));
      if (Objects.isNull(topicMeta) || !topicMeta.isOwnerFencingEnabled()) {
        return RpcUtils.SUCCESS_STATUS;
      }

      final String requestOwnerId = consumerConfig.getOwnerId();
      if (Objects.isNull(requestOwnerId)) {
        return RpcUtils.getStatus(
            TSStatusCode.SUBSCRIPTION_OWNER_REQUIRED,
            String.format(
                "Subscription: topic %s enables owner fencing, but consumer %s does not carry owner-id.",
                topicName, consumerConfig));
      }

      final Long requestOwnerEpoch = consumerConfig.getOwnerEpoch();
      if (Objects.isNull(requestOwnerEpoch)) {
        return RpcUtils.getStatus(
            TSStatusCode.SUBSCRIPTION_OWNER_EPOCH_REQUIRED,
            String.format(
                "Subscription: topic %s enables owner fencing, but consumer %s does not carry owner-epoch.",
                topicName, consumerConfig));
      }

      if (Objects.nonNull(topicMeta.getOwnerLeaseExpireTimeMs())
          && System.currentTimeMillis() > topicMeta.getOwnerLeaseExpireTimeMs()) {
        return RpcUtils.getStatus(
            TSStatusCode.SUBSCRIPTION_OWNER_LEASE_EXPIRED,
            String.format(
                "Subscription: owner lease for topic %s has expired, owner-id: %s, owner-epoch: %s.",
                topicName, topicMeta.getOwnerId(), topicMeta.getOwnerEpoch()));
      }

      if (!topicMeta.matchesOwner(requestOwnerId, requestOwnerEpoch)) {
        return RpcUtils.getStatus(
            TSStatusCode.SUBSCRIPTION_OWNER_FENCED,
            String.format(
                "Subscription: consumer owner is fenced for topic %s, request owner-id: %s,"
                    + " request owner-epoch: %s, current owner-id: %s, current owner-epoch: %s.",
                topicName,
                requestOwnerId,
                requestOwnerEpoch,
                topicMeta.getOwnerId(),
                topicMeta.getOwnerEpoch()));
      }

      return RpcUtils.SUCCESS_STATUS;
    } finally {
      releaseReadLock();
    }
  }

  public TSStatus checkTopicOwners(
      final ConsumerConfig consumerConfig, final Iterable<String> topicNames) {
    for (final String topicName : topicNames) {
      final TSStatus status = checkTopicOwner(consumerConfig, topicName);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  /**
   * Check that the authenticated session can read all data covered by the requested topics.
   * ConsumerConfig is client-controlled and therefore must not be used as the authorization
   * identity.
   */
  public TSStatus checkTopicReadPermissions(
      final String username,
      final ConsumerConfig consumerConfig,
      final Iterable<String> topicNames) {
    if (Objects.isNull(username)) {
      return RpcUtils.getStatus(TSStatusCode.NO_PERMISSION);
    }

    acquireReadLock();
    try {
      for (final String topicName : topicNames) {
        final TopicMeta topicMeta =
            topicMetaKeeper.getTopicMeta(topicName, isTableModel(consumerConfig));
        if (Objects.isNull(topicMeta)) {
          continue;
        }

        final TSStatus status =
            topicMeta.getConfig().isTableTopic()
                ? checkTableTopicReadPermission(username, topicMeta)
                : checkTreeTopicReadPermission(username, topicMeta);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return status;
        }
      }
      return RpcUtils.SUCCESS_STATUS;
    } finally {
      releaseReadLock();
    }
  }

  private TSStatus checkTreeTopicReadPermission(final String username, final TopicMeta topicMeta) {
    final TopicConfig topicConfig = topicMeta.getConfig();
    final TreePattern treePattern =
        topicConfig.getAttribute().containsKey(TopicConstant.PATTERN_KEY)
            ? new PrefixTreePattern(topicConfig.getAttribute().get(TopicConstant.PATTERN_KEY))
            : new IoTDBTreePattern(
                topicConfig.getStringOrDefault(
                    TopicConstant.PATH_KEY, TopicConstant.PATH_DEFAULT_VALUE));
    for (final PartialPath path : treePattern.getBaseInclusionPaths()) {
      if (!AuthorityChecker.checkFullPathOrPatternPermission(
          username, path, PrivilegeType.READ_DATA)) {
        return AuthorityChecker.getTSStatus(false, path, PrivilegeType.READ_DATA);
      }
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  private TSStatus checkTableTopicReadPermission(final String username, final TopicMeta topicMeta) {
    if (AuthorityChecker.SUPER_USER.equals(username)) {
      return RpcUtils.SUCCESS_STATUS;
    }
    final TopicConfig topicConfig = topicMeta.getConfig();
    final String database =
        topicConfig.getStringOrDefault(
            TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE);
    final String table =
        topicConfig.getStringOrDefault(TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE);

    // A database-level SELECT grant covers all tables in one database. For a topic whose
    // database/table is a regular expression, only an any-scope SELECT grant is broad enough to
    // cover every object matched by the topic.
    final boolean databasePattern = isRegexPattern(database);
    final boolean tablePattern = isRegexPattern(table);
    final boolean allowed =
        (databasePattern
            ? AuthorityChecker.checkDBPermission(
                username, AuthorityChecker.ANY_SCOPE, PrivilegeType.SELECT)
            : AuthorityChecker.checkDBPermission(username, database, PrivilegeType.SELECT)
                || (!tablePattern
                    && AuthorityChecker.checkTablePermission(
                        username, database, table, PrivilegeType.SELECT)));
    return allowed
        ? RpcUtils.SUCCESS_STATUS
        : AuthorityChecker.getTSStatus(false, PrivilegeType.SELECT, database, table);
  }

  private static boolean isRegexPattern(final String value) {
    return value.indexOf('.') >= 0
        || value.indexOf('*') >= 0
        || value.indexOf('+') >= 0
        || value.indexOf('?') >= 0
        || value.indexOf('[') >= 0
        || value.indexOf(']') >= 0
        || value.indexOf('(') >= 0
        || value.indexOf(')') >= 0
        || value.indexOf('{') >= 0
        || value.indexOf('}') >= 0
        || value.indexOf('|') >= 0
        || value.indexOf('^') >= 0
        || value.indexOf('$') >= 0
        || value.indexOf('\\') >= 0;
  }

  /**
   * Apply owner lease renewals pushed by ConfigNode via the dedicated subscription owner heartbeat.
   * The pushed remaining duration is converted to a DataNode-local expire time on the local clock,
   * so no absolute timestamp is compared across nodes. Owner identity/epoch changes are delivered
   * via the topic-meta push path; here we only refresh the lease for the matching current owner.
   */
  public void handleTopicOwnerLeases(final List<TTopicOwnerLeaseEntry> ownerLeases) {
    if (Objects.isNull(ownerLeases) || ownerLeases.isEmpty()) {
      return;
    }
    acquireWriteLock();
    try {
      for (final TTopicOwnerLeaseEntry lease : ownerLeases) {
        final TopicMeta topicMeta =
            lease.isSetIsTableModel()
                ? topicMetaKeeper.getTopicMeta(lease.getTopicName(), lease.isIsTableModel())
                : topicMetaKeeper.getTopicMeta(lease.getTopicName());
        if (Objects.isNull(topicMeta)) {
          continue;
        }
        topicMeta.applyOwnerLeaseFromHeartbeat(
            lease.getOwnerId(), lease.getOwnerEpoch(), lease.getLeaseRemainingMs());
      }
    } finally {
      releaseWriteLock();
    }
  }

  private static boolean isTableModel(final ConsumerConfig consumerConfig) {
    return SystemConstant.SQL_DIALECT_TABLE_VALUE.equalsIgnoreCase(consumerConfig.getSqlDialect());
  }
}
