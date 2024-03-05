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

package org.apache.iotdb.confignode.manager.pipe.mq.coordinator.topic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.read.mq.topic.ShowPipeMQTopicPlan;
import org.apache.iotdb.confignode.consensus.response.pipe.mq.PipeMQTopicTableResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeMQInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class PipeMQTopicCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMQTopicCoordinator.class);

  private final ConfigManager configManager;
  private final PipeMQInfo pipeMQInfo;

  public PipeMQTopicCoordinator(ConfigManager configManager, PipeMQInfo pipeMQInfo) {
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
}
