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
package org.apache.iotdb.confignode.consensus.statemachine;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.executor.ConfigPlanExecutor;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** StateMachine for PartitionRegion */
public class PartitionRegionStateMachine
    implements IStateMachine, IStateMachine.EventApi, IStateMachine.RetryPolicy {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionRegionStateMachine.class);
  private final ConfigPlanExecutor executor;
  private ConfigManager configManager;
  private final TEndPoint currentNodeTEndPoint;

  public PartitionRegionStateMachine(ConfigManager configManager, ConfigPlanExecutor executor) {
    this.executor = executor;
    this.configManager = configManager;
    this.currentNodeTEndPoint =
        new TEndPoint()
            .setIp(ConfigNodeDescriptor.getInstance().getConf().getInternalAddress())
            .setPort(ConfigNodeDescriptor.getInstance().getConf().getConsensusPort());
  }

  public ConfigManager getConfigManager() {
    return configManager;
  }

  public void setConfigManager(ConfigManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public TSStatus write(IConsensusRequest request) {
    ConfigPhysicalPlan plan;
    if (request instanceof ByteBufferConsensusRequest) {
      try {
        plan = ConfigPhysicalPlan.Factory.create(request.serializeToByteBuffer());
      } catch (Throwable e) {
        LOGGER.error(
            "Deserialization error for write plan, request: {}, bytebuffer: {}",
            request,
            request.serializeToByteBuffer(),
            e);
        return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }
    } else if (request instanceof ConfigPhysicalPlan) {
      plan = (ConfigPhysicalPlan) request;
    } else {
      LOGGER.error(
          "Unexpected write plan, request: {}, bytebuffer: {}",
          request,
          request.serializeToByteBuffer());
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return write(plan);
  }

  /** Transmit PhysicalPlan to confignode.service.executor.PlanExecutor */
  protected TSStatus write(ConfigPhysicalPlan plan) {
    TSStatus result;
    try {
      result = executor.executeNonQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException | AuthException e) {
      LOGGER.error(e.getMessage());
      result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return result;
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    ConfigPhysicalPlan plan;
    if (request instanceof ByteBufferConsensusRequest) {
      try {
        plan = ConfigPhysicalPlan.Factory.create(request.serializeToByteBuffer());
      } catch (Throwable e) {
        LOGGER.error("Deserialization error for write plan : {}", request);
        return null;
      }
    } else if (request instanceof ConfigPhysicalPlan) {
      plan = (ConfigPhysicalPlan) request;
    } else {
      LOGGER.error("Unexpected read plan : {}", request);
      return null;
    }
    return read(plan);
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    return executor.takeSnapshot(snapshotDir);
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    executor.loadSnapshot(latestSnapshotRootDir);
  }

  /** Transmit PhysicalPlan to confignode.service.executor.PlanExecutor */
  protected DataSet read(ConfigPhysicalPlan plan) {
    DataSet result;
    try {
      result = executor.executeQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException | AuthException e) {
      LOGGER.error(e.getMessage());
      result = null;
    }
    return result;
  }

  @Override
  public void notifyLeaderChanged(ConsensusGroupId groupId, int newLeaderId) {
    // We get currentNodeId here because the currentNodeId
    // couldn't initialize earlier than the PartitionRegionStateMachine
    int currentNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();

    if (currentNodeId == newLeaderId) {
      LOGGER.info(
          "Current node [nodeId: {}, ip:port: {}] becomes Leader",
          newLeaderId,
          currentNodeTEndPoint);
      configManager.getLoadManager().recoverHeartbeatCache();
      configManager.getProcedureManager().shiftExecutor(true);
      configManager.getLoadManager().startLoadBalancingService();
      configManager.getNodeManager().startHeartbeatService();
      configManager.getNodeManager().startUnknownDataNodeDetector();
      configManager.getPartitionManager().startRegionCleaner();
      configManager.getCQManager().startCQScheduler();
    } else {
      LOGGER.info(
          "Current node [nodeId:{}, ip:port: {}] is not longer the leader, the new leader is [nodeId:{}]",
          currentNodeId,
          currentNodeTEndPoint,
          newLeaderId);
      configManager.getProcedureManager().shiftExecutor(false);
      configManager.getLoadManager().stopLoadBalancingService();
      configManager.getNodeManager().stopHeartbeatService();
      configManager.getNodeManager().stopUnknownDataNodeDetector();
      configManager.getPartitionManager().stopRegionCleaner();
      configManager.getCQManager().stopCQScheduler();
    }
  }

  @Override
  public void start() {
    // do nothing
  }

  @Override
  public void stop() {
    // do nothing
  }

  @Override
  public boolean isReadOnly() {
    return CommonDescriptor.getInstance().getConfig().isReadOnly();
  }

  @Override
  public boolean shouldRetry(TSStatus writeResult) {
    // TODO implement this
    return RetryPolicy.super.shouldRetry(writeResult);
  }

  @Override
  public TSStatus updateResult(TSStatus previousResult, TSStatus retryResult) {
    // TODO implement this
    return RetryPolicy.super.updateResult(previousResult, retryResult);
  }

  @Override
  public long getSleepTime() {
    // TODO implement this
    return RetryPolicy.super.getSleepTime();
  }
}
