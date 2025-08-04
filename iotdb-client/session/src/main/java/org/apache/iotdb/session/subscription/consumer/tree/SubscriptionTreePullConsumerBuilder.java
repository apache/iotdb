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

import org.apache.iotdb.session.subscription.consumer.ISubscriptionTreePullConsumer;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionPullConsumerBuilder;

import java.util.List;

public class SubscriptionTreePullConsumerBuilder extends AbstractSubscriptionPullConsumerBuilder {

  @Override
  public SubscriptionTreePullConsumerBuilder host(final String host) {
    super.host(host);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder port(final Integer port) {
    super.port(port);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder nodeUrls(final List<String> nodeUrls) {
    super.nodeUrls(nodeUrls);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder username(final String username) {
    super.username(username);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder password(final String password) {
    super.password(password);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder consumerId(final String consumerId) {
    super.consumerId(consumerId);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder consumerGroupId(final String consumerGroupId) {
    super.consumerGroupId(consumerGroupId);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder heartbeatIntervalMs(final long heartbeatIntervalMs) {
    super.heartbeatIntervalMs(heartbeatIntervalMs);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder endpointsSyncIntervalMs(
      final long endpointsSyncIntervalMs) {
    super.endpointsSyncIntervalMs(endpointsSyncIntervalMs);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder fileSaveDir(final String fileSaveDir) {
    super.fileSaveDir(fileSaveDir);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder fileSaveFsync(final boolean fileSaveFsync) {
    super.fileSaveFsync(fileSaveFsync);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder thriftMaxFrameSize(final int thriftMaxFrameSize) {
    super.thriftMaxFrameSize(thriftMaxFrameSize);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder maxPollParallelism(final int maxPollParallelism) {
    super.maxPollParallelism(maxPollParallelism);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder autoCommit(final boolean autoCommit) {
    super.autoCommit(autoCommit);
    return this;
  }

  @Override
  public SubscriptionTreePullConsumerBuilder autoCommitIntervalMs(final long autoCommitIntervalMs) {
    super.autoCommitIntervalMs(autoCommitIntervalMs);
    return this;
  }

  public ISubscriptionTreePullConsumer build() {
    return new SubscriptionTreePullConsumer(this);
  }
}
