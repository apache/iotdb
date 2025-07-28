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
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.DropTopicPlan;
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

public class DropTopicProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTopicProcedure.class);

  private String topicName;

  public DropTopicProcedure() {
    super();
  }

  public DropTopicProcedure(String topicName) {
    super();
    this.topicName = topicName;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.DROP_TOPIC;
  }

  @Override
  protected boolean executeFromValidate(ConfigNodeProcedureEnv env) throws SubscriptionException {
    LOGGER.info("DropTopicProcedure: executeFromValidate({})", topicName);

    subscriptionInfo.get().validateBeforeDroppingTopic(topicName);
    return true;
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("DropTopicProcedure: executeFromOperateOnConfigNodes({})", topicName);

    TSStatus response;
    try {
      response = env.getConfigManager().getConsensusManager().write(new DropTopicPlan(topicName));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response =
          new TSStatus(TSStatusCode.DROP_TOPIC_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(
          String.format(
              "Failed to drop topic %s on config nodes, because %s", topicName, response));
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropTopicProcedure: executeFromOperateOnDataNodes({})", topicName);

    final List<TSStatus> statuses = env.dropSingleTopicOnDataNode(topicName);
    if (RpcUtils.squashResponseStatusList(statuses).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format("Failed to drop topic %s on data nodes, because %s", topicName, statuses));
    }
  }

  @Override
  protected void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropTopicProcedure: rollbackFromValidate({})", topicName);
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropTopicProcedure: rollbackFromCreateOnConfigNodes({})", topicName);
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropTopicProcedure: rollbackFromCreateOnDataNodes({})", topicName);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_TOPIC_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(topicName, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    topicName = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DropTopicProcedure that = (DropTopicProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && Objects.equals(topicName, that.topicName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getCurrentState(), getCycles(), topicName);
  }
}
