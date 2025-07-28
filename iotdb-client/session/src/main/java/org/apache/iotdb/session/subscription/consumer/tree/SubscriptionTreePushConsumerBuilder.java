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

import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTreePushConsumer;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionPushConsumerBuilder;

import java.util.List;

public class SubscriptionTreePushConsumerBuilder extends AbstractSubscriptionPushConsumerBuilder {

  @Override
  public SubscriptionTreePushConsumerBuilder host(final String host) {
    super.host(host);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder port(final Integer port) {
    super.port(port);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder nodeUrls(final List<String> nodeUrls) {
    super.nodeUrls(nodeUrls);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder username(final String username) {
    super.username(username);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder password(final String password) {
    super.password(password);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder consumerId(final String consumerId) {
    super.consumerId(consumerId);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder consumerGroupId(final String consumerGroupId) {
    super.consumerGroupId(consumerGroupId);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder heartbeatIntervalMs(final long heartbeatIntervalMs) {
    super.heartbeatIntervalMs(heartbeatIntervalMs);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder endpointsSyncIntervalMs(
      final long endpointsSyncIntervalMs) {
    super.endpointsSyncIntervalMs(endpointsSyncIntervalMs);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder fileSaveDir(final String fileSaveDir) {
    super.fileSaveDir(fileSaveDir);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder fileSaveFsync(final boolean fileSaveFsync) {
    super.fileSaveFsync(fileSaveFsync);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder thriftMaxFrameSize(final int thriftMaxFrameSize) {
    super.thriftMaxFrameSize(thriftMaxFrameSize);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder maxPollParallelism(final int maxPollParallelism) {
    super.maxPollParallelism(maxPollParallelism);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder ackStrategy(final AckStrategy ackStrategy) {
    super.ackStrategy(ackStrategy);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder consumeListener(
      final ConsumeListener consumeListener) {
    super.consumeListener(consumeListener);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder autoPollIntervalMs(final long autoPollIntervalMs) {
    super.autoPollIntervalMs(autoPollIntervalMs);
    return this;
  }

  @Override
  public SubscriptionTreePushConsumerBuilder autoPollTimeoutMs(final long autoPollTimeoutMs) {
    super.autoPollTimeoutMs(autoPollTimeoutMs);
    return this;
  }

  public ISubscriptionTreePushConsumer build() {
    return new SubscriptionTreePushConsumer(this);
  }
}
