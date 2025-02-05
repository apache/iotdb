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

import java.util.List;

public class AbstractSubscriptionPullConsumerBuilder extends AbstractSubscriptionConsumerBuilder {

  protected boolean autoCommit = ConsumerConstant.AUTO_COMMIT_DEFAULT_VALUE;
  protected long autoCommitIntervalMs = ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_DEFAULT_VALUE;

  @Override
  public AbstractSubscriptionPullConsumerBuilder host(final String host) {
    super.host(host);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder port(final Integer port) {
    super.port(port);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder nodeUrls(final List<String> nodeUrls) {
    super.nodeUrls(nodeUrls);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder username(final String username) {
    super.username(username);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder password(final String password) {
    super.password(password);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder consumerId(final String consumerId) {
    super.consumerId(consumerId);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder consumerGroupId(final String consumerGroupId) {
    super.consumerGroupId(consumerGroupId);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder heartbeatIntervalMs(
      final long heartbeatIntervalMs) {
    super.heartbeatIntervalMs(heartbeatIntervalMs);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder endpointsSyncIntervalMs(
      final long endpointsSyncIntervalMs) {
    super.endpointsSyncIntervalMs(endpointsSyncIntervalMs);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder fileSaveDir(final String fileSaveDir) {
    super.fileSaveDir(fileSaveDir);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder fileSaveFsync(final boolean fileSaveFsync) {
    super.fileSaveFsync(fileSaveFsync);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder thriftMaxFrameSize(final int thriftMaxFrameSize) {
    super.thriftMaxFrameSize(thriftMaxFrameSize);
    return this;
  }

  @Override
  public AbstractSubscriptionPullConsumerBuilder maxPollParallelism(final int maxPollParallelism) {
    super.maxPollParallelism(maxPollParallelism);
    return this;
  }

  public AbstractSubscriptionPullConsumerBuilder autoCommit(final boolean autoCommit) {
    this.autoCommit = autoCommit;
    return this;
  }

  public AbstractSubscriptionPullConsumerBuilder autoCommitIntervalMs(
      final long autoCommitIntervalMs) {
    this.autoCommitIntervalMs =
        Math.max(autoCommitIntervalMs, ConsumerConstant.AUTO_COMMIT_INTERVAL_MS_MIN_VALUE);
    return this;
  }
}
