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

package org.apache.iotdb.session.subscription.consumer.tree;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.subscription.consumer.AsyncCommitCallback;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionProvider;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.util.IdentifierUtils;

import org.apache.thrift.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class SubscriptionTreePullConsumer extends AbstractSubscriptionPullConsumer
    implements ISubscriptionTreePullConsumer {

  /////////////////////////////// provider ///////////////////////////////

  @Override
  protected AbstractSubscriptionProvider constructSubscriptionProvider(
      final TEndPoint endPoint,
      final String username,
      final String password,
      final String consumerId,
      final String consumerGroupId,
      final int thriftMaxFrameSize) {
    return new SubscriptionTreeProvider(
        endPoint, username, password, consumerId, consumerGroupId, thriftMaxFrameSize);
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected SubscriptionTreePullConsumer(final SubscriptionTreePullConsumerBuilder builder) {
    super(builder);
  }

  @Deprecated // keep for forward compatibility
  private SubscriptionTreePullConsumer(final SubscriptionTreePullConsumer.Builder builder) {
    super(
        new SubscriptionTreePullConsumerBuilder()
            .host(builder.host)
            .port(builder.port)
            .nodeUrls(builder.nodeUrls)
            .username(builder.username)
            .password(builder.password)
            .consumerId(builder.consumerId)
            .consumerGroupId(builder.consumerGroupId)
            .heartbeatIntervalMs(builder.heartbeatIntervalMs)
            .endpointsSyncIntervalMs(builder.endpointsSyncIntervalMs)
            .fileSaveDir(builder.fileSaveDir)
            .fileSaveFsync(builder.fileSaveFsync)
            .thriftMaxFrameSize(builder.thriftMaxFrameSize)
            .maxPollParallelism(builder.maxPollParallelism)
            .autoCommit(builder.autoCommit)
            .autoCommitIntervalMs(builder.autoCommitIntervalMs));
  }

  public SubscriptionTreePullConsumer(final Properties properties) {
    this(
        properties,
        (Boolean)
            properties.getOrDefault(
                ConsumerConstant.AUTO_COMMIT_KEY, ConsumerConstant.AUTO_COMMIT_DEFAULT_VALUE),
        (Long)
            properties.getOrDefault(
                ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_KEY,
                ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_DEFAULT_VALUE));
  }

  private SubscriptionTreePullConsumer(
      final Properties properties, final boolean autoCommit, final long autoCommitIntervalMs) {
    super(properties, autoCommit, autoCommitIntervalMs);
  }

  /////////////////////////////// interface ///////////////////////////////

  @Override
  public void open() throws SubscriptionException {
    super.open();
  }

  @Override
  public void close() throws SubscriptionException {
    super.close();
  }

  @Override
  public void subscribe(final String topicName) throws SubscriptionException {
    super.subscribe(topicName);
  }

  @Override
  public void subscribe(final String... topicNames) throws SubscriptionException {
    super.subscribe(topicNames);
  }

  @Override
  public void subscribe(final Set<String> topicNames) throws SubscriptionException {
    super.subscribe(topicNames);
  }

  @Override
  public void unsubscribe(final String topicName) throws SubscriptionException {
    super.unsubscribe(topicName);
  }

  @Override
  public void unsubscribe(final String... topicNames) throws SubscriptionException {
    super.unsubscribe(topicNames);
  }

  @Override
  public void unsubscribe(final Set<String> topicNames) throws SubscriptionException {
    super.unsubscribe(topicNames);
  }

  @Override
  public List<SubscriptionMessage> poll(final Duration timeout) throws SubscriptionException {
    return super.poll(timeout);
  }

  @Override
  public List<SubscriptionMessage> poll(final long timeoutMs) throws SubscriptionException {
    return super.poll(timeoutMs);
  }

  @Override
  public List<SubscriptionMessage> poll(final Set<String> topicNames, final Duration timeout)
      throws SubscriptionException {
    return super.poll(topicNames, timeout);
  }

  @Override
  public List<SubscriptionMessage> poll(final Set<String> topicNames, final long timeoutMs) {
    return super.poll(topicNames, timeoutMs);
  }

  @Override
  public void commitSync(final SubscriptionMessage message) throws SubscriptionException {
    super.commitSync(message);
  }

  @Override
  public void commitSync(final Iterable<SubscriptionMessage> messages)
      throws SubscriptionException {
    super.commitSync(messages);
  }

  @Override
  public CompletableFuture<Void> commitAsync(final SubscriptionMessage message) {
    return super.commitAsync(message);
  }

  @Override
  public CompletableFuture<Void> commitAsync(final Iterable<SubscriptionMessage> messages) {
    return super.commitAsync(messages);
  }

  @Override
  public void commitAsync(final SubscriptionMessage message, final AsyncCommitCallback callback) {
    super.commitAsync(message, callback);
  }

  @Override
  public void commitAsync(
      final Iterable<SubscriptionMessage> messages, final AsyncCommitCallback callback) {
    super.commitAsync(messages, callback);
  }

  @Override
  public String getConsumerId() {
    return super.getConsumerId();
  }

  @Override
  public String getConsumerGroupId() {
    return super.getConsumerGroupId();
  }

  @Override
  public boolean allTopicMessagesHaveBeenConsumed() {
    return super.allTopicMessagesHaveBeenConsumed();
  }

  /////////////////////////////// builder ///////////////////////////////

  @Deprecated // keep for forward compatibility
  public static class Builder {

    private String host;
    private Integer port;
    private List<String> nodeUrls;

    private String username = SessionConfig.DEFAULT_USER;
    private String password = SessionConfig.DEFAULT_PASSWORD;

    private String consumerId;
    private String consumerGroupId;

    private long heartbeatIntervalMs = ConsumerConstant.HEARTBEAT_INTERVAL_MS_DEFAULT_VALUE;
    private long endpointsSyncIntervalMs =
        ConsumerConstant.ENDPOINTS_SYNC_INTERVAL_MS_DEFAULT_VALUE;

    private String fileSaveDir = ConsumerConstant.FILE_SAVE_DIR_DEFAULT_VALUE;
    private boolean fileSaveFsync = ConsumerConstant.FILE_SAVE_FSYNC_DEFAULT_VALUE;

    private int thriftMaxFrameSize = SessionConfig.DEFAULT_MAX_FRAME_SIZE;
    private int maxPollParallelism = ConsumerConstant.MAX_POLL_PARALLELISM_DEFAULT_VALUE;

    private boolean autoCommit = ConsumerConstant.AUTO_COMMIT_DEFAULT_VALUE;
    private long autoCommitIntervalMs = ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_DEFAULT_VALUE;

    public Builder host(final String host) {
      this.host = host;
      return this;
    }

    public Builder port(final Integer port) {
      this.port = port;
      return this;
    }

    public Builder nodeUrls(final List<String> nodeUrls) {
      this.nodeUrls = nodeUrls;
      return this;
    }

    public Builder username(final String username) {
      this.username = username;
      return this;
    }

    public Builder password(final String password) {
      this.password = password;
      return this;
    }

    public Builder consumerId(@Nullable final String consumerId) {
      if (Objects.isNull(consumerId)) {
        return this;
      }
      this.consumerId = IdentifierUtils.checkAndParseIdentifier(consumerId);
      return this;
    }

    public Builder consumerGroupId(@Nullable final String consumerGroupId) {
      if (Objects.isNull(consumerGroupId)) {
        return this;
      }
      this.consumerGroupId = IdentifierUtils.checkAndParseIdentifier(consumerGroupId);
      return this;
    }

    public Builder heartbeatIntervalMs(final long heartbeatIntervalMs) {
      this.heartbeatIntervalMs =
          Math.max(heartbeatIntervalMs, ConsumerConstant.HEARTBEAT_INTERVAL_MS_MIN_VALUE);
      return this;
    }

    public Builder endpointsSyncIntervalMs(final long endpointsSyncIntervalMs) {
      this.endpointsSyncIntervalMs =
          Math.max(endpointsSyncIntervalMs, ConsumerConstant.ENDPOINTS_SYNC_INTERVAL_MS_MIN_VALUE);
      return this;
    }

    public Builder fileSaveDir(final String fileSaveDir) {
      this.fileSaveDir = fileSaveDir;
      return this;
    }

    public Builder fileSaveFsync(final boolean fileSaveFsync) {
      this.fileSaveFsync = fileSaveFsync;
      return this;
    }

    public Builder thriftMaxFrameSize(final int thriftMaxFrameSize) {
      this.thriftMaxFrameSize = thriftMaxFrameSize;
      return this;
    }

    public Builder maxPollParallelism(final int maxPollParallelism) {
      // Here the minimum value of max poll parallelism is set to 1 instead of 0, in order to use a
      // single thread to execute poll whenever there are idle resources available, thereby
      // achieving strict timeout.
      this.maxPollParallelism = Math.max(maxPollParallelism, 1);
      return this;
    }

    public Builder autoCommit(final boolean autoCommit) {
      this.autoCommit = autoCommit;
      return this;
    }

    public Builder autoCommitIntervalMs(final long autoCommitIntervalMs) {
      this.autoCommitIntervalMs =
          Math.max(autoCommitIntervalMs, ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_MIN_VALUE);
      return this;
    }

    public SubscriptionTreePullConsumer buildPullConsumer() {
      return new SubscriptionTreePullConsumer(this);
    }
  }
}
