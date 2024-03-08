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

package org.apache.iotdb.confignode.manager.pipe.mq.coordinator;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.read.pipe.mq.subscription.ShowPipeMQSubscriptionPlan;
import org.apache.iotdb.confignode.consensus.request.read.pipe.mq.topic.ShowPipeMQTopicPlan;
import org.apache.iotdb.confignode.consensus.response.pipe.mq.PipeMQSubscriptionTableResp;
import org.apache.iotdb.confignode.consensus.response.pipe.mq.PipeMQTopicTableResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeMQInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllSubscriptionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class PipeMQCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMQCoordinator.class);

  private final ConfigManager configManager;
  private final PipeMQInfo pipeMQInfo;

  public PipeMQCoordinator(ConfigManager configManager, PipeMQInfo pipeMQInfo) {
    this.configManager = configManager;
    this.pipeMQInfo = pipeMQInfo;
  }

  public PipeMQInfo getPipeMQInfo() {
    return pipeMQInfo;
  }
  /////////////////////////////// Lock ///////////////////////////////

  public void lock() {
    pipeMQInfo.acquireWriteLock();
  }

  public void unlock() {
    pipeMQInfo.releaseWriteLock();
  }

  /////////////////////////////// Operate ///////////////////////////////
  public TSStatus createTopic(TCreateTopicReq req) {
    final TSStatus status = configManager.getProcedureManager().createTopic(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to create topic {}. Result status: {}.", req.getTopicName(), status);
    }
    return status;
  }

  public TSStatus dropTopic(String topicName) {
    final TSStatus status = configManager.getProcedureManager().dropTopic(topicName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to drop topic {}. Result status: {}.", topicName, status);
    }
    return status;
  }

  public TShowTopicResp showTopic(TShowTopicReq req) {
    try {
      return ((PipeMQTopicTableResp)
              configManager.getConsensusManager().read(new ShowPipeMQTopicPlan()))
          .filter(req.getTopicName())
          .convertToTShowTopicResp();
    } catch (Exception e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new PipeMQTopicTableResp(res, Collections.emptyList()).convertToTShowTopicResp();
    }
  }

  public TGetAllTopicInfoResp getAllTopicInfo() {
    try {
      return ((PipeMQTopicTableResp)
              configManager.getConsensusManager().read(new ShowPipeMQTopicPlan()))
          .convertToTGetAllTopicInfoResp();
    } catch (Exception e) {
      LOGGER.error("Failed to get all topic info.", e);
      return new TGetAllTopicInfoResp(
          new TSStatus(TSStatusCode.SHOW_TOPIC_ERROR.getStatusCode()).setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TSStatus createConsumer(TCreateConsumerReq req) {
    final TSStatus status = configManager.getProcedureManager().createConsumer(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Failed to create consumer {} in consumer group {}. Result status: {}.",
          req.getConsumerId(),
          req.getConsumerGroupId(),
          status);
    }
    return status;
  }

  public TSStatus dropConsumer(TCloseConsumerReq req) {
    final TSStatus status = configManager.getProcedureManager().dropConsumer(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Failed to close consumer {} in consumer group {}. Result status: {}.",
          req.getConsumerId(),
          req.getConsumerGroupId(),
          status);
    }
    return status;
  }

  public TSStatus createSubscription(TSubscribeReq req) {
    final TSStatus status = configManager.getProcedureManager().createSubscription(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Consumer {} in consumer group {} failed to subscribe topics {}. Result status: {}.",
          req.getConsumerId(),
          req.getConsumerGroupId(),
          req.getTopicNames(),
          status);
    }
    return status;
  }

  public TSStatus dropSubscription(TUnsubscribeReq req) {
    final TSStatus status = configManager.getProcedureManager().dropSubscription(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Consumer {} in consumer group {} failed to unsubscribe topics {}. Result status: {}.",
          req.getConsumerId(),
          req.getConsumerGroupId(),
          req.getTopicNames(),
          status);
    }
    return status;
  }

  public TShowSubscriptionResp showSubscription(TShowSubscriptionReq req) {
    try {
      return ((PipeMQSubscriptionTableResp)
              configManager.getConsensusManager().read(new ShowPipeMQSubscriptionPlan()))
          .filter(req.getTopicName())
          .convertToTShowSubscriptionResp();
    } catch (Exception e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new PipeMQSubscriptionTableResp(res, Collections.emptyList())
          .convertToTShowSubscriptionResp();
    }
  }

  public TGetAllSubscriptionInfoResp getAllSubscriptionInfo() {
    try {
      return ((PipeMQSubscriptionTableResp)
              configManager.getConsensusManager().read(new ShowPipeMQSubscriptionPlan()))
          .convertToTGetAllSubscriptionInfoResp();
    } catch (Exception e) {
      LOGGER.error("Failed to get all subscription info.", e);
      return new TGetAllSubscriptionInfoResp(
          new TSStatus(TSStatusCode.PIPE_ERROR.getStatusCode()).setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }
}
