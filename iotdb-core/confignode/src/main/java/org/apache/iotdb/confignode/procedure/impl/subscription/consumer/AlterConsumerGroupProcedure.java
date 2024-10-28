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

package org.apache.iotdb.confignode.procedure.impl.subscription.consumer;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.AlterConsumerGroupPlan;
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

public class AlterConsumerGroupProcedure extends AbstractOperateSubscriptionProcedure {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AlterConsumerGroupProcedure.class);

  protected ConsumerGroupMeta existingConsumerGroupMeta;
  protected ConsumerGroupMeta updatedConsumerGroupMeta;

  public AlterConsumerGroupProcedure() {
    super();
  }

  public AlterConsumerGroupProcedure(ConsumerGroupMeta updatedConsumerGroupMeta) {
    super();
    this.updatedConsumerGroupMeta = updatedConsumerGroupMeta;
  }

  /** This is only used when the subscription info lock is held by another procedure. */
  public AlterConsumerGroupProcedure(
      ConsumerGroupMeta updatedConsumerGroupMeta,
      AtomicReference<SubscriptionInfo> subscriptionInfo) {
    super();
    this.updatedConsumerGroupMeta = updatedConsumerGroupMeta;
    this.subscriptionInfo = subscriptionInfo;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.ALTER_CONSUMER_GROUP;
  }

  @Override
  public boolean executeFromValidate(ConfigNodeProcedureEnv env) throws SubscriptionException {
    LOGGER.info("AlterConsumerGroupProcedure: executeFromValidate, try to validate");

    validateAndGetOldAndNewMeta(env);
    return true;
  }

  protected void validateAndGetOldAndNewMeta(ConfigNodeProcedureEnv env) {
    subscriptionInfo.get().validateBeforeAlterConsumerGroup(updatedConsumerGroupMeta);

    existingConsumerGroupMeta =
        subscriptionInfo.get().getConsumerGroupMeta(updatedConsumerGroupMeta.getConsumerGroupId());
  }

  @Override
  public void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info(
        "AlterConsumerGroupProcedure: executeFromOperateOnConfigNodes({})",
        updatedConsumerGroupMeta.getConsumerGroupId());

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new AlterConsumerGroupPlan(updatedConsumerGroupMeta));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response =
          new TSStatus(TSStatusCode.ALTER_CONSUMER_ERROR.getStatusCode())
              .setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(
          String.format(
              "Failed to alter consumer group %s on config nodes, because %s",
              updatedConsumerGroupMeta.getConsumerGroupId(), response));
    }
  }

  @Override
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info(
        "AlterConsumerGroupProcedure: executeFromOperateOnDataNodes({})",
        updatedConsumerGroupMeta.getConsumerGroupId());

    final List<TSStatus> statuses =
        env.pushSingleConsumerGroupOnDataNode(updatedConsumerGroupMeta.serialize());
    if (RpcUtils.squashResponseStatusList(statuses).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format(
              "Failed to alter consumer group (%s -> %s) on data nodes, because %s",
              existingConsumerGroupMeta, updatedConsumerGroupMeta, statuses));
    }
  }

  @Override
  public void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("AlterConsumerGroupProcedure: rollbackFromValidate");
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info(
        "AlterConsumerGroupProcedure: rollbackFromOperateOnConfigNodes({})",
        updatedConsumerGroupMeta.getConsumerGroupId());

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new AlterConsumerGroupPlan(existingConsumerGroupMeta));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response =
          new TSStatus(TSStatusCode.ALTER_CONSUMER_ERROR.getStatusCode())
              .setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(
          String.format(
              "Failed to rollback from altering consumer group (%s -> %s) on config nodes, because %s",
              existingConsumerGroupMeta, updatedConsumerGroupMeta, response));
    }
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info("AlterConsumerGroupProcedure: rollbackFromOperateOnDataNodes");

    final List<TSStatus> statuses =
        env.pushSingleConsumerGroupOnDataNode(existingConsumerGroupMeta.serialize());
    if (RpcUtils.squashResponseStatusList(statuses).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format(
              "Failed to rollback from altering consumer group (%s -> %s) on data nodes, because %s",
              existingConsumerGroupMeta, updatedConsumerGroupMeta, statuses));
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ALTER_CONSUMER_GROUP_PROCEDURE.getTypeCode());
    super.serialize(stream);

    if (updatedConsumerGroupMeta != null) {
      ReadWriteIOUtils.write(true, stream);
      updatedConsumerGroupMeta.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    if (existingConsumerGroupMeta != null) {
      ReadWriteIOUtils.write(true, stream);
      existingConsumerGroupMeta.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      updatedConsumerGroupMeta = ConsumerGroupMeta.deserialize(byteBuffer);
    }

    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      existingConsumerGroupMeta = ConsumerGroupMeta.deserialize(byteBuffer);
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
    AlterConsumerGroupProcedure that = (AlterConsumerGroupProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && Objects.equals(updatedConsumerGroupMeta, that.updatedConsumerGroupMeta)
        && Objects.equals(existingConsumerGroupMeta, that.existingConsumerGroupMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        updatedConsumerGroupMeta,
        existingConsumerGroupMeta);
  }

  @TestOnly
  public void setExistingConsumerGroupMeta(ConsumerGroupMeta meta) {
    this.existingConsumerGroupMeta = meta;
  }

  @TestOnly
  public ConsumerGroupMeta getExistingConsumerGroupMeta() {
    return this.existingConsumerGroupMeta;
  }

  @TestOnly
  public void setUpdatedConsumerGroupMeta(ConsumerGroupMeta meta) {
    this.updatedConsumerGroupMeta = meta;
  }

  @TestOnly
  public ConsumerGroupMeta getUpdatedConsumerGroupMeta() {
    return this.updatedConsumerGroupMeta;
  }
}
