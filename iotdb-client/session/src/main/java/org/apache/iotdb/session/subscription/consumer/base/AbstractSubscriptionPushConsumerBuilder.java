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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;

import java.util.List;

public class AbstractSubscriptionPushConsumerBuilder extends AbstractSubscriptionConsumerBuilder {

  protected AckStrategy ackStrategy = AckStrategy.defaultValue();
  protected ConsumeListener consumeListener = message -> ConsumeResult.SUCCESS;

  protected long autoPollIntervalMs = ConsumerConstant.AUTO_POLL_INTERVAL_MS_DEFAULT_VALUE;
  protected long autoPollTimeoutMs = ConsumerConstant.AUTO_POLL_TIMEOUT_MS_DEFAULT_VALUE;

  @Override
  public AbstractSubscriptionPushConsumerBuilder host(final String host) {
    super.host(host);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder port(final Integer port) {
    super.port(port);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder nodeUrls(final List<String> nodeUrls) {
    super.nodeUrls(nodeUrls);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder username(final String username) {
    super.username(username);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder password(final String password) {
    super.password(password);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder consumerId(final String consumerId) {
    super.consumerId(consumerId);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder consumerGroupId(final String consumerGroupId) {
    super.consumerGroupId(consumerGroupId);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder heartbeatIntervalMs(
      final long heartbeatIntervalMs) {
    super.heartbeatIntervalMs(heartbeatIntervalMs);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder endpointsSyncIntervalMs(
      final long endpointsSyncIntervalMs) {
    super.endpointsSyncIntervalMs(endpointsSyncIntervalMs);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder fileSaveDir(final String fileSaveDir) {
    super.fileSaveDir(fileSaveDir);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder fileSaveFsync(final boolean fileSaveFsync) {
    super.fileSaveFsync(fileSaveFsync);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder thriftMaxFrameSize(final int thriftMaxFrameSize) {
    super.thriftMaxFrameSize(thriftMaxFrameSize);
    return this;
  }

  @Override
  public AbstractSubscriptionPushConsumerBuilder maxPollParallelism(final int maxPollParallelism) {
    super.maxPollParallelism(maxPollParallelism);
    return this;
  }

  public AbstractSubscriptionPushConsumerBuilder ackStrategy(final AckStrategy ackStrategy) {
    this.ackStrategy = ackStrategy;
    return this;
  }

  public AbstractSubscriptionPushConsumerBuilder consumeListener(
      final ConsumeListener consumeListener) {
    this.consumeListener = consumeListener;
    return this;
  }

  public AbstractSubscriptionPushConsumerBuilder autoPollIntervalMs(final long autoPollIntervalMs) {
    // avoid interval less than or equal to zero
    this.autoPollIntervalMs = Math.max(autoPollIntervalMs, 1);
    return this;
  }

  public AbstractSubscriptionPushConsumerBuilder autoPollTimeoutMs(final long autoPollTimeoutMs) {
    this.autoPollTimeoutMs =
        Math.max(autoPollTimeoutMs, ConsumerConstant.AUTO_POLL_TIMEOUT_MS_MIN_VALUE);
    return this;
  }
}
