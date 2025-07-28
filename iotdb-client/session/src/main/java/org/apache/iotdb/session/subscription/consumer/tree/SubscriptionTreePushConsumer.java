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
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTreePushConsumer;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionProvider;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.util.IdentifierUtils;

import org.apache.thrift.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

public class SubscriptionTreePushConsumer extends AbstractSubscriptionPushConsumer
    implements ISubscriptionTreePushConsumer {

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

  public SubscriptionTreePushConsumer(final SubscriptionTreePushConsumerBuilder builder) {
    super(builder);
  }

  @Deprecated // keep for forward compatibility
  private SubscriptionTreePushConsumer(final Builder builder) {
    super(
        new SubscriptionTreePushConsumerBuilder()
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
            .ackStrategy(builder.ackStrategy)
            .consumeListener(builder.consumeListener)
            .autoPollIntervalMs(builder.autoPollIntervalMs)
            .autoPollTimeoutMs(builder.autoPollTimeoutMs));
  }

  public SubscriptionTreePushConsumer(final Properties config) {
    this(
        config,
        (AckStrategy)
            config.getOrDefault(ConsumerConstant.ACK_STRATEGY_KEY, AckStrategy.defaultValue()),
        (ConsumeListener)
            config.getOrDefault(
                ConsumerConstant.CONSUME_LISTENER_KEY,
                (ConsumeListener) message -> ConsumeResult.SUCCESS),
        (Long)
            config.getOrDefault(
                ConsumerConstant.AUTO_POLL_INTERVAL_MS_KEY,
                ConsumerConstant.AUTO_POLL_INTERVAL_MS_DEFAULT_VALUE),
        (Long)
            config.getOrDefault(
                ConsumerConstant.AUTO_POLL_TIMEOUT_MS_KEY,
                ConsumerConstant.AUTO_POLL_TIMEOUT_MS_DEFAULT_VALUE));
  }

  private SubscriptionTreePushConsumer(
      final Properties config,
      final AckStrategy ackStrategy,
      final ConsumeListener consumeListener,
      final long autoPollIntervalMs,
      final long autoPollTimeoutMs) {
    super(config, ackStrategy, consumeListener, autoPollIntervalMs, autoPollTimeoutMs);
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
  public String getConsumerId() {
    return super.getConsumerId();
  }

  @Override
  public String getConsumerGroupId() {
    return super.getConsumerGroupId();
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

    private AckStrategy ackStrategy = AckStrategy.defaultValue();
    private ConsumeListener consumeListener = message -> ConsumeResult.SUCCESS;

    private long autoPollIntervalMs = ConsumerConstant.AUTO_POLL_INTERVAL_MS_DEFAULT_VALUE;
    private long autoPollTimeoutMs = ConsumerConstant.AUTO_POLL_TIMEOUT_MS_DEFAULT_VALUE;

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

    public Builder ackStrategy(final AckStrategy ackStrategy) {
      this.ackStrategy = ackStrategy;
      return this;
    }

    public Builder consumeListener(final ConsumeListener consumeListener) {
      this.consumeListener = consumeListener;
      return this;
    }

    public Builder autoPollIntervalMs(final long autoPollIntervalMs) {
      // avoid interval less than or equal to zero
      this.autoPollIntervalMs = Math.max(autoPollIntervalMs, 1);
      return this;
    }

    public Builder autoPollTimeoutMs(final long autoPollTimeoutMs) {
      this.autoPollTimeoutMs =
          Math.max(autoPollTimeoutMs, ConsumerConstant.AUTO_POLL_TIMEOUT_MS_MIN_VALUE);
      return this;
    }

    public SubscriptionTreePushConsumer buildPushConsumer() {
      return new SubscriptionTreePushConsumer(this);
    }
  }
}
