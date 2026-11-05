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

import org.apache.iotdb.isession.IInternalSessionBuilder;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionPullConsumerBuilder;

import java.util.List;

public class SubscriptionTablePullConsumerBuilder extends AbstractSubscriptionPullConsumerBuilder
    implements IInternalSessionBuilder<ISubscriptionTablePullConsumer> {

  @Override
  public SubscriptionTablePullConsumerBuilder host(final String host) {
    super.host(host);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder port(final Integer port) {
    super.port(port);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder nodeUrls(final List<String> nodeUrls) {
    super.nodeUrls(nodeUrls);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder username(final String username) {
    super.username(username);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder password(final String password) {
    super.password(password);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder encryptedPassword(final String encryptedPassword) {
    super.encryptedPassword(encryptedPassword);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder consumerId(final String consumerId) {
    super.consumerId(consumerId);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder consumerGroupId(final String consumerGroupId) {
    super.consumerGroupId(consumerGroupId);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder ownerId(final String ownerId) {
    super.ownerId(ownerId);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder ownerEpoch(final long ownerEpoch) {
    super.ownerEpoch(ownerEpoch);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder ownerEpoch(final Long ownerEpoch) {
    super.ownerEpoch(ownerEpoch);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder heartbeatIntervalMs(final long heartbeatIntervalMs) {
    super.heartbeatIntervalMs(heartbeatIntervalMs);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder endpointsSyncIntervalMs(
      final long endpointsSyncIntervalMs) {
    super.endpointsSyncIntervalMs(endpointsSyncIntervalMs);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder fileSaveDir(final String fileSaveDir) {
    super.fileSaveDir(fileSaveDir);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder fileSaveFsync(final boolean fileSaveFsync) {
    super.fileSaveFsync(fileSaveFsync);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder thriftMaxFrameSize(final int thriftMaxFrameSize) {
    super.thriftMaxFrameSize(thriftMaxFrameSize);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder connectionTimeoutInMs(
      final int connectionTimeoutInMs) {
    super.connectionTimeoutInMs(connectionTimeoutInMs);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder useSSL(final boolean useSSL) {
    super.useSSL(useSSL);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder trustStore(final String trustStore) {
    super.trustStore(trustStore);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder trustStorePwd(final String trustStorePwd) {
    super.trustStorePwd(trustStorePwd);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder keyStore(final String keyStore) {
    super.keyStore(keyStore);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder keyStorePwd(final String keyStorePwd) {
    super.keyStorePwd(keyStorePwd);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder sslProtocol(final String sslProtocol) {
    super.sslProtocol(sslProtocol);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder maxPollParallelism(final int maxPollParallelism) {
    super.maxPollParallelism(maxPollParallelism);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder autoCommit(final boolean autoCommit) {
    super.autoCommit(autoCommit);
    return this;
  }

  @Override
  public SubscriptionTablePullConsumerBuilder autoCommitIntervalMs(
      final long autoCommitIntervalMs) {
    super.autoCommitIntervalMs(autoCommitIntervalMs);
    return this;
  }

  public ISubscriptionTablePullConsumer build() {
    return new SubscriptionTablePullConsumer(this);
  }

  /** Builds an unopened consumer using the caller's internal endpoint source and username. */
  @Override
  public ISubscriptionTablePullConsumer buildInternal(final INodeSupplier nodeSupplier) {
    return new InternalSubscriptionTablePullConsumer(this, nodeSupplier);
  }
}
