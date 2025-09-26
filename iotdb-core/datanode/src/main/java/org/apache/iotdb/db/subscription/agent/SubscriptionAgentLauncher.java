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

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllSubscriptionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaRespExceptionMessage;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaRespExceptionMessage;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.stream.Collectors;

class SubscriptionAgentLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionAgentLauncher.class);

  // For fail-over
  public static final int MAX_RETRY_TIMES = 5;

  private SubscriptionAgentLauncher() {
    // Forbidding instantiation
  }

  public static synchronized void launchSubscriptionTopicAgent() throws StartupException {
    int retry = 0;
    while (retry < MAX_RETRY_TIMES) {
      try (final ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TGetAllTopicInfoResp getAllTopicInfoResp = configNodeClient.getAllTopicInfo();
        if (getAllTopicInfoResp.getStatus().getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          final String exceptionMessage =
              String.format(
                  "Failed to get all topic info in config node, status is %s",
                  getAllTopicInfoResp.getStatus());
          LOGGER.warn(exceptionMessage);
          throw new SubscriptionException(exceptionMessage);
        }

        final TPushTopicMetaRespExceptionMessage exceptionMessage =
            SubscriptionAgent.topic()
                .handleTopicMetaChanges(
                    getAllTopicInfoResp.getAllTopicInfo().stream()
                        .map(
                            byteBuffer -> {
                              final TopicMeta topicMeta = TopicMeta.deserialize(byteBuffer);
                              LOGGER.info(
                                  "Pulled topic meta from config node: {}, recovering ...",
                                  topicMeta);
                              return topicMeta;
                            })
                        .collect(Collectors.toList()));
        if (Objects.nonNull(exceptionMessage)) {
          LOGGER.warn(exceptionMessage.getMessage());
          throw new SubscriptionException(exceptionMessage.getMessage());
        }

        return;
      } catch (final SubscriptionException | ClientManagerException | TException e) {
        retry++;
        LOGGER.warn(
            "Failed to get topic meta from config node for {} times, will retry at most {} times.",
            retry,
            MAX_RETRY_TIMES,
            e);
        try {
          SubscriptionAgentLauncher.class.wait(
              retry * SubscriptionConfig.getInstance().getSubscriptionLaunchRetryIntervalMs());
        } catch (final InterruptedException interruptedException) {
          LOGGER.info(
              "Interrupted while sleeping, will retry to get topic meta from config node.",
              interruptedException);
          Thread.currentThread().interrupt();
        }
      }
    }

    throw new StartupException("Failed to get topic meta from config node.");
  }

  public static synchronized void launchSubscriptionConsumerAgent() throws StartupException {
    int retry = 0;
    while (retry < MAX_RETRY_TIMES) {
      try (final ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TGetAllSubscriptionInfoResp getAllSubscriptionInfoResp =
            configNodeClient.getAllSubscriptionInfo();
        if (getAllSubscriptionInfoResp.getStatus().getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          final String exceptionMessage =
              String.format(
                  "Failed to get all subscription info in config node, status is %s",
                  getAllSubscriptionInfoResp.getStatus());
          LOGGER.warn(exceptionMessage);
          throw new SubscriptionException(exceptionMessage);
        }

        final TPushConsumerGroupMetaRespExceptionMessage exceptionMessage =
            SubscriptionAgent.consumer()
                .handleConsumerGroupMetaChanges(
                    getAllSubscriptionInfoResp.getAllSubscriptionInfo().stream()
                        .map(
                            byteBuffer -> {
                              final ConsumerGroupMeta consumerGroupMeta =
                                  ConsumerGroupMeta.deserialize(byteBuffer);
                              LOGGER.info(
                                  "Pulled consumer group meta from config node: {}, recovering ...",
                                  consumerGroupMeta);
                              return consumerGroupMeta;
                            })
                        .collect(Collectors.toList()));
        if (Objects.nonNull(exceptionMessage)) {
          LOGGER.warn(exceptionMessage.getMessage());
          throw new SubscriptionException(exceptionMessage.getMessage());
        }

        return;
      } catch (final SubscriptionException | ClientManagerException | TException e) {
        retry++;
        LOGGER.warn(
            "Failed to get consumer group meta from config node for {} times, will retry at most {} times.",
            retry,
            MAX_RETRY_TIMES,
            e);
        try {
          SubscriptionAgentLauncher.class.wait(
              retry * SubscriptionConfig.getInstance().getSubscriptionLaunchRetryIntervalMs());
        } catch (final InterruptedException interruptedException) {
          LOGGER.info(
              "Interrupted while sleeping, will retry to get consumer group meta from config node.",
              interruptedException);
          Thread.currentThread().interrupt();
        }
      }
    }

    throw new StartupException("Failed to get consumer group meta from config node.");
  }
}
