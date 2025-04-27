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

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.session.subscription.util.IdentifierUtils;

import org.apache.thrift.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class AbstractSubscriptionConsumerBuilder {

  protected String host;
  protected Integer port;
  protected List<String> nodeUrls;

  protected String username = SessionConfig.DEFAULT_USER;
  protected String password = SessionConfig.DEFAULT_PASSWORD;

  protected String consumerId;
  protected String consumerGroupId;

  protected long heartbeatIntervalMs = ConsumerConstant.HEARTBEAT_INTERVAL_MS_DEFAULT_VALUE;
  protected long endpointsSyncIntervalMs =
      ConsumerConstant.ENDPOINTS_SYNC_INTERVAL_MS_DEFAULT_VALUE;

  protected String fileSaveDir = ConsumerConstant.FILE_SAVE_DIR_DEFAULT_VALUE;
  protected boolean fileSaveFsync = ConsumerConstant.FILE_SAVE_FSYNC_DEFAULT_VALUE;

  protected int thriftMaxFrameSize = SessionConfig.DEFAULT_MAX_FRAME_SIZE;
  protected int maxPollParallelism = ConsumerConstant.MAX_POLL_PARALLELISM_DEFAULT_VALUE;

  public AbstractSubscriptionConsumerBuilder host(final String host) {
    this.host = host;
    return this;
  }

  public AbstractSubscriptionConsumerBuilder port(final Integer port) {
    this.port = port;
    return this;
  }

  public AbstractSubscriptionConsumerBuilder nodeUrls(final List<String> nodeUrls) {
    this.nodeUrls = nodeUrls;
    return this;
  }

  public AbstractSubscriptionConsumerBuilder username(final String username) {
    this.username = username;
    return this;
  }

  public AbstractSubscriptionConsumerBuilder password(final String password) {
    this.password = password;
    return this;
  }

  public AbstractSubscriptionConsumerBuilder consumerId(@Nullable final String consumerId) {
    if (Objects.isNull(consumerId)) {
      return this;
    }
    this.consumerId = IdentifierUtils.checkAndParseIdentifier(consumerId);
    return this;
  }

  public AbstractSubscriptionConsumerBuilder consumerGroupId(
      @Nullable final String consumerGroupId) {
    if (Objects.isNull(consumerGroupId)) {
      return this;
    }
    this.consumerGroupId = IdentifierUtils.checkAndParseIdentifier(consumerGroupId);
    return this;
  }

  public AbstractSubscriptionConsumerBuilder heartbeatIntervalMs(final long heartbeatIntervalMs) {
    this.heartbeatIntervalMs =
        Math.max(heartbeatIntervalMs, ConsumerConstant.HEARTBEAT_INTERVAL_MS_MIN_VALUE);
    return this;
  }

  public AbstractSubscriptionConsumerBuilder endpointsSyncIntervalMs(
      final long endpointsSyncIntervalMs) {
    this.endpointsSyncIntervalMs =
        Math.max(endpointsSyncIntervalMs, ConsumerConstant.ENDPOINTS_SYNC_INTERVAL_MS_MIN_VALUE);
    return this;
  }

  public AbstractSubscriptionConsumerBuilder fileSaveDir(final String fileSaveDir) {
    this.fileSaveDir = fileSaveDir;
    return this;
  }

  public AbstractSubscriptionConsumerBuilder fileSaveFsync(final boolean fileSaveFsync) {
    this.fileSaveFsync = fileSaveFsync;
    return this;
  }

  public AbstractSubscriptionConsumerBuilder thriftMaxFrameSize(final int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    return this;
  }

  public AbstractSubscriptionConsumerBuilder maxPollParallelism(final int maxPollParallelism) {
    // Here the minimum value of max poll parallelism is set to 1 instead of 0, in order to use a
    // single thread to execute poll whenever there are idle resources available, thereby
    // achieving strict timeout.
    this.maxPollParallelism = Math.max(maxPollParallelism, 1);
    return this;
  }
}
