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

package org.apache.iotdb.commons.subscription.meta.topic;

import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.datastructure.visibility.Visibility;
import org.apache.iotdb.commons.pipe.datastructure.visibility.VisibilityUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.i18n.SubscriptionMessages;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TopicMeta {

  private String topicName;
  private long creationTime; // unit in ms
  private TopicConfig config;

  private String ownerId;
  private long ownerEpoch;
  // Highest owner epoch ever granted for this topic; retained across clearOwner. Mirrors the
  // MAX_OWNER_EPOCH_KEY attribute (which carries it through serialization). Used to guarantee owner
  // epoch is globally monotonic and never reused.
  private long maxOwnerEpoch;
  private long ownerLastTransferTimeMs;
  private Long ownerLeaseDurationMs;
  private Long ownerLeaseExpireTimeMs;

  // TODO: remove this variable later
  private Set<String> subscribedConsumerGroupIds; // unused now

  private TopicMeta() {
    this.config = new TopicConfig(new HashMap<>());
    this.ownerEpoch = -1L;
    this.maxOwnerEpoch = -1L;
    this.ownerLastTransferTimeMs = -1L;

    this.subscribedConsumerGroupIds = new HashSet<>();
  }

  public TopicMeta(
      final String topicName, final long creationTime, final Map<String, String> topicAttributes) {
    this.topicName = topicName;
    this.creationTime = creationTime;
    this.config = new TopicConfig(topicAttributes);
    this.ownerEpoch = -1L;
    this.maxOwnerEpoch = -1L;
    this.ownerLastTransferTimeMs = -1L;
    initOwnerFromTopicAttributes(topicAttributes);

    this.subscribedConsumerGroupIds = new HashSet<>();
  }

  public TopicMeta deepCopy() {
    final TopicMeta copied = new TopicMeta();
    copied.topicName = topicName;
    copied.creationTime = creationTime;
    copied.config = new TopicConfig(new HashMap<>(config.getAttribute()));
    copied.ownerId = ownerId;
    copied.ownerEpoch = ownerEpoch;
    copied.maxOwnerEpoch = maxOwnerEpoch;
    copied.ownerLastTransferTimeMs = ownerLastTransferTimeMs;
    copied.ownerLeaseDurationMs = ownerLeaseDurationMs;
    copied.ownerLeaseExpireTimeMs = ownerLeaseExpireTimeMs;

    copied.subscribedConsumerGroupIds = new HashSet<>(subscribedConsumerGroupIds);
    return copied;
  }

  public TopicMeta deepCopyWithUpdatedAttributes(final Map<String, String> updatedAttributes) {
    final Map<String, String> copiedAttributes = new HashMap<>(config.getAttribute());
    if (Objects.nonNull(updatedAttributes)) {
      copiedAttributes.putAll(updatedAttributes);
      if ((updatedAttributes.containsKey(TopicConstant.OWNER_ID_KEY)
              || updatedAttributes.containsKey(TopicConstant.OWNER_EPOCH_KEY))
          && !updatedAttributes.containsKey(TopicConstant.OWNER_LEASE_DURATION_MS_KEY)) {
        copiedAttributes.remove(TopicConstant.OWNER_LEASE_DURATION_MS_KEY);
      }
    }

    final TopicMeta copied = new TopicMeta(topicName, creationTime, copiedAttributes);
    copied.subscribedConsumerGroupIds = new HashSet<>(subscribedConsumerGroupIds);
    return copied;
  }

  public String getTopicName() {
    return topicName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public TopicConfig getConfig() {
    return config;
  }

  public boolean isOwnerFencingEnabled() {
    return Objects.nonNull(ownerId) && ownerEpoch >= 0;
  }

  public String getOwnerId() {
    return ownerId;
  }

  public long getOwnerEpoch() {
    return ownerEpoch;
  }

  public long getMaxOwnerEpoch() {
    return maxOwnerEpoch;
  }

  public long getOwnerLastTransferTimeMs() {
    return ownerLastTransferTimeMs;
  }

  public Long getOwnerLeaseExpireTimeMs() {
    return ownerLeaseExpireTimeMs;
  }

  public Long getOwnerLeaseDurationMs() {
    return ownerLeaseDurationMs;
  }

  public void transferOwner(final String ownerId, final long ownerEpoch) {
    transferOwner(ownerId, ownerEpoch, null);
  }

  public void transferOwner(
      final String ownerId, final long ownerEpoch, final Long ownerLeaseDurationMs) {
    if (Objects.isNull(ownerId) || ownerId.isEmpty()) {
      throw new IllegalArgumentException(SubscriptionMessages.OWNER_ID_SHOULD_NOT_BE_EMPTY);
    }
    if (ownerEpoch < 0) {
      throw new IllegalArgumentException(SubscriptionMessages.OWNER_EPOCH_SHOULD_NOT_BE_NEGATIVE);
    }
    if (isOwnerFencingEnabled() && ownerEpoch <= this.ownerEpoch) {
      throw new IllegalArgumentException(
          String.format(
              SubscriptionMessages.OWNER_EPOCH_SHOULD_INCREASE_MONOTONICALLY,
              this.ownerEpoch,
              ownerEpoch));
    }
    if (Objects.nonNull(ownerLeaseDurationMs) && ownerLeaseDurationMs <= 0) {
      throw new IllegalArgumentException(
          String.format(
              SubscriptionMessages.OWNER_LEASE_DURATION_SHOULD_BE_POSITIVE,
              topicName,
              ownerLeaseDurationMs));
    }

    this.ownerId = ownerId;
    this.ownerEpoch = ownerEpoch;
    this.ownerLeaseDurationMs = ownerLeaseDurationMs;
    // The lease expire time is DataNode-local runtime state derived from the owner-lease heartbeat;
    // a freshly (re)assigned owner has no lease until the first heartbeat establishes one.
    this.ownerLeaseExpireTimeMs = null;
    this.ownerLastTransferTimeMs = System.currentTimeMillis();

    config.getAttribute().put(TopicConstant.OWNER_ID_KEY, ownerId);
    config.getAttribute().put(TopicConstant.OWNER_EPOCH_KEY, String.valueOf(ownerEpoch));
    if (ownerEpoch > maxOwnerEpoch) {
      maxOwnerEpoch = ownerEpoch;
    }
    config.getAttribute().put(TopicConstant.MAX_OWNER_EPOCH_KEY, String.valueOf(maxOwnerEpoch));
    if (Objects.nonNull(ownerLeaseDurationMs)) {
      config
          .getAttribute()
          .put(TopicConstant.OWNER_LEASE_DURATION_MS_KEY, String.valueOf(ownerLeaseDurationMs));
    } else {
      config.getAttribute().remove(TopicConstant.OWNER_LEASE_DURATION_MS_KEY);
    }
  }

  public void clearOwner() {
    ownerId = null;
    ownerEpoch = -1L;
    ownerLastTransferTimeMs = -1L;
    ownerLeaseDurationMs = null;
    ownerLeaseExpireTimeMs = null;
    config.getAttribute().remove(TopicConstant.OWNER_ID_KEY);
    config.getAttribute().remove(TopicConstant.OWNER_EPOCH_KEY);
    config.getAttribute().remove(TopicConstant.OWNER_LEASE_DURATION_MS_KEY);
    // Intentionally retain maxOwnerEpoch and MAX_OWNER_EPOCH_KEY so that a later re-enable cannot
    // reuse an epoch <= any epoch ever granted (global monotonicity survives clear).
  }

  public boolean matchesOwner(final String requestOwnerId, final Long requestOwnerEpoch) {
    return !isOwnerFencingEnabled()
        || (Objects.equals(ownerId, requestOwnerId)
            && Objects.equals(ownerEpoch, requestOwnerEpoch)
            && (Objects.isNull(ownerLeaseExpireTimeMs)
                || System.currentTimeMillis() <= ownerLeaseExpireTimeMs));
  }

  /**
   * DataNode side: apply a lease renewal pushed by ConfigNode via the dedicated subscription owner
   * heartbeat. The pushed value is a relative remaining duration; it is converted to a local
   * absolute expire time using this node's own clock, so no absolute timestamp is ever compared
   * across ConfigNode and DataNode clocks. This expire time is purely DataNode-local runtime state
   * (not a topic attribute, not serialized, not part of equality) so it never propagates back. Only
   * refreshes when the pushed {@code (ownerId, ownerEpoch)} matches the current owner; stale or
   * mismatched pushes are ignored (owner identity/epoch changes arrive via the topic-meta push
   * path, not the lease heartbeat).
   */
  public void applyOwnerLeaseFromHeartbeat(
      final String ownerId, final long ownerEpoch, final long leaseRemainingMs) {
    if (!isOwnerFencingEnabled()
        || !Objects.equals(this.ownerId, ownerId)
        || this.ownerEpoch != ownerEpoch) {
      return;
    }
    this.ownerLeaseExpireTimeMs = System.currentTimeMillis() + leaseRemainingMs;
  }

  /**
   * @return true if the consumer group did not already subscribe this topic
   */
  @TestOnly
  public boolean addSubscribedConsumerGroup(final String consumerGroupId) {
    return subscribedConsumerGroupIds.add(consumerGroupId);
  }

  @TestOnly
  public void removeSubscribedConsumerGroup(final String consumerGroupId) {
    subscribedConsumerGroupIds.remove(consumerGroupId);
  }

  @TestOnly
  public Set<String> getSubscribedConsumerGroupIds() {
    return subscribedConsumerGroupIds;
  }

  @TestOnly
  public boolean isSubscribedByConsumerGroup(final String consumerGroupId) {
    return subscribedConsumerGroupIds.contains(consumerGroupId);
  }

  @TestOnly
  public boolean hasSubscribedConsumerGroup() {
    return !subscribedConsumerGroupIds.isEmpty();
  }

  ////////////////////////////////////// de/ser ////////////////////////////////

  public ByteBuffer serialize() throws IOException {
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(final OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(topicName, outputStream);
    ReadWriteIOUtils.write(creationTime, outputStream);

    ReadWriteIOUtils.write(config.getAttribute().size(), outputStream);
    for (final Map.Entry<String, String> entry : config.getAttribute().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }

    ReadWriteIOUtils.write(subscribedConsumerGroupIds.size(), outputStream);
    for (final String subscribedConsumerGroupID : subscribedConsumerGroupIds) {
      ReadWriteIOUtils.write(subscribedConsumerGroupID, outputStream);
    }
  }

  public static TopicMeta deserialize(final InputStream inputStream) throws IOException {
    final TopicMeta topicMeta = new TopicMeta();

    topicMeta.topicName = ReadWriteIOUtils.readString(inputStream);
    topicMeta.creationTime = ReadWriteIOUtils.readLong(inputStream);

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      final String key = ReadWriteIOUtils.readString(inputStream);
      final String value = ReadWriteIOUtils.readString(inputStream);
      topicMeta.config.getAttribute().put(key, value);
    }

    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      topicMeta.subscribedConsumerGroupIds.add(ReadWriteIOUtils.readString(inputStream));
    }

    topicMeta.initOwnerFromTopicAttributes(topicMeta.config.getAttribute());

    return topicMeta;
  }

  public static TopicMeta deserialize(final ByteBuffer byteBuffer) {
    final TopicMeta topicMeta = new TopicMeta();

    topicMeta.topicName = ReadWriteIOUtils.readString(byteBuffer);
    topicMeta.creationTime = ReadWriteIOUtils.readLong(byteBuffer);

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final String key = ReadWriteIOUtils.readString(byteBuffer);
      final String value = ReadWriteIOUtils.readString(byteBuffer);
      topicMeta.config.getAttribute().put(key, value);
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      topicMeta.subscribedConsumerGroupIds.add(ReadWriteIOUtils.readString(byteBuffer));
    }

    topicMeta.initOwnerFromTopicAttributes(topicMeta.config.getAttribute());

    return topicMeta;
  }

  public static void validateOwnerProgression(
      final TopicMeta currentTopicMeta, final TopicMeta updatedTopicMeta) {
    if (Objects.isNull(currentTopicMeta) || Objects.isNull(updatedTopicMeta)) {
      return;
    }

    // Global epoch monotonicity (never reused): a transfer to a different owner must use an epoch
    // strictly greater than any epoch ever granted for this topic. The baseline (maxOwnerEpoch) is
    // retained across clearOwner, so this also blocks epoch reuse after a clear -> re-enable cycle.
    // Same-owner updates (e.g. attribute / lease changes) are unaffected.
    if (updatedTopicMeta.isOwnerFencingEnabled()
        && !Objects.equals(updatedTopicMeta.getOwnerId(), currentTopicMeta.getOwnerId())
        && updatedTopicMeta.getOwnerEpoch() <= currentTopicMeta.getMaxOwnerEpoch()) {
      throw new IllegalArgumentException(
          String.format(
              SubscriptionMessages.OWNER_EPOCH_SHOULD_NEVER_BE_REUSED,
              currentTopicMeta.getTopicName(),
              currentTopicMeta.getMaxOwnerEpoch(),
              updatedTopicMeta.getOwnerId(),
              updatedTopicMeta.getOwnerEpoch()));
    }

    if (!currentTopicMeta.isOwnerFencingEnabled()) {
      return;
    }

    if (!updatedTopicMeta.isOwnerFencingEnabled()) {
      throw new IllegalArgumentException(
          String.format(
              SubscriptionMessages.OWNER_SHOULD_NOT_BE_CLEARED_BY_STALE_META,
              currentTopicMeta.getTopicName(),
              currentTopicMeta.getOwnerId(),
              currentTopicMeta.getOwnerEpoch()));
    }

    final boolean epochRollback =
        updatedTopicMeta.getOwnerEpoch() < currentTopicMeta.getOwnerEpoch();
    final boolean sameEpochOwnerChanged =
        updatedTopicMeta.getOwnerEpoch() == currentTopicMeta.getOwnerEpoch()
            && !Objects.equals(updatedTopicMeta.getOwnerId(), currentTopicMeta.getOwnerId());
    if (epochRollback || sameEpochOwnerChanged) {
      throw new IllegalArgumentException(
          String.format(
              SubscriptionMessages.OWNER_EPOCH_SHOULD_NOT_ROLL_BACK,
              currentTopicMeta.getTopicName(),
              currentTopicMeta.getOwnerId(),
              currentTopicMeta.getOwnerEpoch(),
              updatedTopicMeta.getOwnerId(),
              updatedTopicMeta.getOwnerEpoch()));
    }
  }

  private void initOwnerFromTopicAttributes(final Map<String, String> topicAttributes) {
    final TopicConfig topicConfig = new TopicConfig(topicAttributes);

    // Restore the global epoch baseline first; it persists even when the owner has been cleared.
    final Long configuredMaxOwnerEpoch = topicConfig.getLong(TopicConstant.MAX_OWNER_EPOCH_KEY);
    if (Objects.nonNull(configuredMaxOwnerEpoch)) {
      this.maxOwnerEpoch = configuredMaxOwnerEpoch;
    }

    final String configuredOwnerId = topicConfig.getString(TopicConstant.OWNER_ID_KEY);
    if (Objects.isNull(configuredOwnerId)) {
      return;
    }

    final Long configuredOwnerEpoch = topicConfig.getLong(TopicConstant.OWNER_EPOCH_KEY);
    if (Objects.isNull(configuredOwnerEpoch)) {
      throw new IllegalArgumentException(
          String.format(
              SubscriptionMessages.OWNER_EPOCH_SHOULD_BE_SET_WHEN_OWNER_ID_SET,
              TopicConstant.OWNER_ID_KEY));
    }
    final Long ownerLeaseDurationMs =
        topicConfig.getLong(TopicConstant.OWNER_LEASE_DURATION_MS_KEY);
    if (Objects.nonNull(ownerLeaseDurationMs) && ownerLeaseDurationMs <= 0) {
      throw new IllegalArgumentException(
          String.format(
              SubscriptionMessages.OWNER_LEASE_DURATION_SHOULD_BE_POSITIVE_WHEN_SET,
              TopicConstant.OWNER_LEASE_DURATION_MS_KEY));
    }
    transferOwner(configuredOwnerId, configuredOwnerEpoch, ownerLeaseDurationMs);
  }

  /////////////////////////////// utilities ///////////////////////////////

  public Map<String, String> generateExtractorAttributes(final String username) {
    return generateExtractorAttributes(username, null);
  }

  public Map<String, String> generateExtractorAttributes(
      final String username, final String password) {
    final Map<String, String> extractorAttributes = new HashMap<>();
    // disable meta sync
    extractorAttributes.put("source", "iotdb-source");
    extractorAttributes.put("inclusion", "data.insert");
    extractorAttributes.put("inclusion.exclusion", "data.delete");
    // user
    extractorAttributes.put("username", username);
    if (Objects.nonNull(password)) {
      extractorAttributes.put("password", password);
    }
    // TODO: currently set skipif to no-privileges
    extractorAttributes.put("skipif", "no-privileges");
    // sql dialect
    extractorAttributes.putAll(config.getAttributeWithSqlDialect());
    if (config.isTableTopic()) {
      // table model: database name and table name
      extractorAttributes.putAll(config.getAttributesWithSourceDatabaseAndTableName());
      // column-filter is evaluated by subscription runtime on DataNode.
      extractorAttributes.putAll(config.getAttributesWithSourceColumnFilter());
    } else {
      // tree model: path or pattern
      extractorAttributes.putAll(config.getAttributesWithSourcePathOrPattern());
    }
    // time
    extractorAttributes.putAll(config.getAttributesWithSourceTimeRange());
    // realtime mode
    extractorAttributes.putAll(config.getAttributesWithSourceRealtimeMode());
    // source mode
    extractorAttributes.putAll(config.getAttributesWithSourceMode());
    // loose range or strict
    extractorAttributes.putAll(config.getAttributesWithSourceLooseRangeOrStrict());
    // backdoor configs
    extractorAttributes.putAll(config.getAttributesWithSourcePrefix());
    return extractorAttributes;
  }

  public Map<String, String> generateProcessorAttributes() {
    return config.getAttributesWithProcessorPrefix();
  }

  public Map<String, String> generateConnectorAttributes(final String consumerGroupId) {
    final Map<String, String> connectorAttributes = new HashMap<>();
    connectorAttributes.put("sink", "subscription-sink");
    connectorAttributes.put(PipeSinkConstant.SINK_TOPIC_KEY, topicName);
    connectorAttributes.put(PipeSinkConstant.SINK_CONSUMER_GROUP_KEY, consumerGroupId);
    connectorAttributes.putAll(config.getAttributesWithSinkFormat());
    // backdoor configs
    connectorAttributes.putAll(config.getAttributesWithSinkPrefix());
    return connectorAttributes;
  }

  /////////////////////////////////  Tree & Table Isolation  /////////////////////////////////

  public boolean visibleUnder(final boolean isTableModel) {
    final Visibility visibility = VisibilityUtils.calculateFromTopicConfig(config);
    return VisibilityUtils.isCompatible(visibility, isTableModel);
  }

  public boolean visibleUnderTableModel() {
    return visibleUnder(true);
  }

  ////////////////////////////////////// Object ////////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final TopicMeta that = (TopicMeta) obj;
    return creationTime == that.creationTime
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(config, that.config)
        && Objects.equals(ownerId, that.ownerId)
        && ownerEpoch == that.ownerEpoch
        && maxOwnerEpoch == that.maxOwnerEpoch
        && Objects.equals(ownerLeaseDurationMs, that.ownerLeaseDurationMs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        topicName, creationTime, config, ownerId, ownerEpoch, maxOwnerEpoch, ownerLeaseDurationMs);
  }

  @Override
  public String toString() {
    return "TopicMeta{"
        + "topicName='"
        + topicName
        + "', creationTime="
        + creationTime
        + ", config="
        + config
        + ", ownerId='"
        + ownerId
        + "', ownerEpoch="
        + ownerEpoch
        + ", ownerLastTransferTimeMs="
        + ownerLastTransferTimeMs
        + ", ownerLeaseDurationMs="
        + ownerLeaseDurationMs
        + ", ownerLeaseExpireTimeMs="
        + ownerLeaseExpireTimeMs
        + '}';
  }
}
