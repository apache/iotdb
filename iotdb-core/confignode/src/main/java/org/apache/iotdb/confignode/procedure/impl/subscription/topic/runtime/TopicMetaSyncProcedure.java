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

package org.apache.iotdb.confignode.procedure.impl.subscription.topic.runtime;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.runtime.TopicHandleMetaChangePlan;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TopicMetaSyncProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopicMetaSyncProcedure.class);

  private static final long MIN_EXECUTION_INTERVAL_MS =
      PipeConfig.getInstance().getPipeMetaSyncerSyncIntervalMinutes() * 60 * 1000 / 2;
  // No need to serialize this field
  private static final AtomicLong LAST_EXECUTION_TIME = new AtomicLong(0);

  public TopicMetaSyncProcedure() {
    super();
  }

  @Override
  protected AtomicReference<SubscriptionInfo> acquireLockInternal(
      ConfigNodeProcedureEnv configNodeProcedureEnv) {
    return configNodeProcedureEnv
        .getConfigManager()
        .getSubscriptionManager()
        .getSubscriptionCoordinator()
        .tryLock();
  }

  @Override
  protected ProcedureLockState acquireLock(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    // Skip the procedure if the last execution time is within the minimum execution interval.
    // Often used to prevent the procedure from being executed too frequently when system reboot.
    if (System.currentTimeMillis() - LAST_EXECUTION_TIME.get() < MIN_EXECUTION_INTERVAL_MS) {
      // Skip by setting the subscriptionInfo to null
      subscriptionInfo = null;
      LOGGER.info(
          "TopicMetaSyncProcedure: acquireLock, skip the procedure due to the last execution time {}",
          LAST_EXECUTION_TIME.get());
      return ProcedureLockState.LOCK_ACQUIRED;
    }

    return super.acquireLock(configNodeProcedureEnv);
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.SYNC_TOPIC_META;
  }

  @Override
  public boolean executeFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("TopicMetaSyncProcedure: executeFromValidate");

    LAST_EXECUTION_TIME.set(System.currentTimeMillis());
    return true;
  }

  @Override
  public void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("TopicMetaSyncProcedure: executeFromOperateOnConfigNodes");

    final List<TopicMeta> topicMetaList = new ArrayList<>();
    subscriptionInfo.get().getAllTopicMeta().forEach(topicMetaList::add);

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new TopicHandleMetaChangePlan(topicMetaList));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(response.getMessage());
    }
  }

  @Override
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info("TopicMetaSyncProcedure: executeFromOperateOnDataNodes");

    Map<Integer, TPushTopicMetaResp> respMap = pushTopicMetaToDataNodes(env);
    if (pushTopicMetaHasException(respMap)) {
      throw new SubscriptionException(
          String.format("Failed to push topic meta to dataNodes, details: %s", respMap));
    }
  }

  @Override
  public void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("TopicMetaSyncProcedure: rollbackFromValidate");

    // Do nothing
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("TopicMetaSyncProcedure: rollbackFromOperateOnConfigNodes");

    // Do nothing
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("TopicMetaSyncProcedure: rollbackFromOperateOnDataNodes");

    // Do nothing
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.TOPIC_META_SYNC_PROCEDURE.getTypeCode());
    super.serialize(stream);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof TopicMetaSyncProcedure;
  }

  @Override
  public int hashCode() {
    return 0;
  }
}
