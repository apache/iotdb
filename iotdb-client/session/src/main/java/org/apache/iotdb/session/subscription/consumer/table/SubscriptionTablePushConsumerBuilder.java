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

package org.apache.iotdb.session.subscription.consumer.table;

import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeListener;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePushConsumer;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionPushConsumerBuilder;

import java.util.List;

public class SubscriptionTablePushConsumerBuilder extends AbstractSubscriptionPushConsumerBuilder {

  @Override
  public SubscriptionTablePushConsumerBuilder host(final String host) {
    super.host(host);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder port(final Integer port) {
    super.port(port);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder nodeUrls(final List<String> nodeUrls) {
    super.nodeUrls(nodeUrls);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder username(final String username) {
    super.username(username);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder password(final String password) {
    super.password(password);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder consumerId(final String consumerId) {
    super.consumerId(consumerId);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder consumerGroupId(final String consumerGroupId) {
    super.consumerGroupId(consumerGroupId);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder heartbeatIntervalMs(final long heartbeatIntervalMs) {
    super.heartbeatIntervalMs(heartbeatIntervalMs);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder endpointsSyncIntervalMs(
      final long endpointsSyncIntervalMs) {
    super.endpointsSyncIntervalMs(endpointsSyncIntervalMs);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder fileSaveDir(final String fileSaveDir) {
    super.fileSaveDir(fileSaveDir);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder fileSaveFsync(final boolean fileSaveFsync) {
    super.fileSaveFsync(fileSaveFsync);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder thriftMaxFrameSize(final int thriftMaxFrameSize) {
    super.thriftMaxFrameSize(thriftMaxFrameSize);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder maxPollParallelism(final int maxPollParallelism) {
    super.maxPollParallelism(maxPollParallelism);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder ackStrategy(final AckStrategy ackStrategy) {
    super.ackStrategy(ackStrategy);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder consumeListener(
      final ConsumeListener consumeListener) {
    super.consumeListener(consumeListener);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder autoPollIntervalMs(final long autoPollIntervalMs) {
    super.autoPollIntervalMs(autoPollIntervalMs);
    return this;
  }

  @Override
  public SubscriptionTablePushConsumerBuilder autoPollTimeoutMs(final long autoPollTimeoutMs) {
    super.autoPollTimeoutMs(autoPollTimeoutMs);
    return this;
  }

  public ISubscriptionTablePushConsumer build() {
    return new SubscriptionTablePushConsumer(this);
  }
}
