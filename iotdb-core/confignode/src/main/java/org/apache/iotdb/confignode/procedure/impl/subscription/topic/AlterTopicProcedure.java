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

package org.apache.iotdb.confignode.procedure.impl.subscription.topic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class AlterTopicProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlterTopicProcedure.class);

  private TopicMeta updatedTopicMeta;

  private TopicMeta existedTopicMeta;

  public AlterTopicProcedure() {
    super();
  }

  public AlterTopicProcedure(TopicMeta updatedTopicMeta) {
    super();
    this.updatedTopicMeta = updatedTopicMeta;
  }

  /** This is only used when the subscription info lock is held by another procedure. */
  public AlterTopicProcedure(
      TopicMeta updatedTopicMeta, AtomicReference<SubscriptionInfo> subscriptionInfo) {
    super();
    this.updatedTopicMeta = updatedTopicMeta;
    this.subscriptionInfo = subscriptionInfo;
  }

  /** This should be called after {@link #executeFromValidate}. */
  public TopicMeta getExistedTopicMeta() {
    return existedTopicMeta;
  }

  /** This should be called after {@link #executeFromValidate}. */
  public TopicMeta getUpdatedTopicMeta() {
    return updatedTopicMeta;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.ALTER_TOPIC;
  }

  @Override
  public boolean executeFromValidate(ConfigNodeProcedureEnv env) throws SubscriptionException {
    LOGGER.info("AlterTopicProcedure: executeFromValidate");

    subscriptionInfo.get().validateBeforeAlteringTopic(updatedTopicMeta);

    existedTopicMeta = subscriptionInfo.get().getTopicMeta(updatedTopicMeta.getTopicName());

    return true;
  }

  @Override
  public void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("AlterTopicProcedure: executeFromOperateOnConfigNodes, try to alter topic");

    TSStatus response;
    try {
      response =
          env.getConfigManager().getConsensusManager().write(new AlterTopicPlan(updatedTopicMeta));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response =
          new TSStatus(TSStatusCode.ALTER_TOPIC_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(
          String.format(
              "Failed to alter topic (%s -> %s) on config nodes, because %s",
              existedTopicMeta, updatedTopicMeta, response));
    }
  }

  @Override
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info(
        "AlterTopicProcedure: executeFromOperateOnDataNodes({})", updatedTopicMeta.getTopicName());

    final List<TSStatus> statuses = env.pushSingleTopicOnDataNode(updatedTopicMeta.serialize());
    if (RpcUtils.squashResponseStatusList(statuses).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format(
              "Failed to alter topic (%s -> %s) on data nodes, because %s",
              existedTopicMeta, updatedTopicMeta, statuses));
    }
  }

  @Override
  public void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("AlterTopicProcedure: rollbackFromValidate({})", updatedTopicMeta.getTopicName());
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info(
        "AlterTopicProcedure: rollbackFromOperateOnConfigNodes({})",
        updatedTopicMeta.getTopicName());

    TSStatus response;
    try {
      response =
          env.getConfigManager().getConsensusManager().write(new AlterTopicPlan(existedTopicMeta));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response =
          new TSStatus(TSStatusCode.ALTER_TOPIC_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(
          String.format(
              "Failed to rollback from altering topic (%s -> %s) on config nodes, because %s",
              updatedTopicMeta, existedTopicMeta, response));
    }
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info(
        "AlterTopicProcedure: rollbackFromOperateOnDataNodes({})", updatedTopicMeta.getTopicName());

    final List<TSStatus> statuses = env.pushSingleTopicOnDataNode(existedTopicMeta.serialize());
    if (RpcUtils.squashResponseStatusList(statuses).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format(
              "Failed to rollback from altering topic (%s -> %s) on data nodes, because %s",
              updatedTopicMeta, existedTopicMeta, statuses));
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ALTER_TOPIC_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(updatedTopicMeta != null, stream);
    if (updatedTopicMeta != null) {
      updatedTopicMeta.serialize(stream);
    }

    ReadWriteIOUtils.write(existedTopicMeta != null, stream);
    if (existedTopicMeta != null) {
      existedTopicMeta.serialize(stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      updatedTopicMeta = TopicMeta.deserialize(byteBuffer);
    }

    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      existedTopicMeta = TopicMeta.deserialize(byteBuffer);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AlterTopicProcedure that = (AlterTopicProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && Objects.equals(updatedTopicMeta, that.updatedTopicMeta)
        && Objects.equals(existedTopicMeta, that.existedTopicMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(), getCurrentState(), getCycles(), updatedTopicMeta, existedTopicMeta);
  }
}
