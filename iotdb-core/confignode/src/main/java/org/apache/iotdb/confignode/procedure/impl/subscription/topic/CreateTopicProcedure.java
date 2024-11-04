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
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.DropTopicPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
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
import java.util.Map;
import java.util.Objects;

public class CreateTopicProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTopicProcedure.class);

  private TCreateTopicReq createTopicReq;
  private TopicMeta topicMeta;

  public CreateTopicProcedure() {
    super();
  }

  public CreateTopicProcedure(TCreateTopicReq createTopicReq) throws SubscriptionException {
    super();
    this.createTopicReq = createTopicReq;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.CREATE_TOPIC;
  }

  @Override
  protected boolean executeFromValidate(ConfigNodeProcedureEnv env) throws SubscriptionException {
    LOGGER.info("CreateTopicProcedure: executeFromValidate");

    // 1. check if the topic exists
    if (!subscriptionInfo.get().validateBeforeCreatingTopic(createTopicReq)) {
      return false;
    }

    // 2. create the topic meta
    topicMeta =
        new TopicMeta(
            createTopicReq.getTopicName(),
            System.currentTimeMillis(),
            createTopicReq.getTopicAttributes());
    return true;
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("CreateTopicProcedure: executeFromOperateOnConfigNodes({})", topicMeta);

    TSStatus response;
    try {
      response = env.getConfigManager().getConsensusManager().write(new CreateTopicPlan(topicMeta));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response =
          new TSStatus(TSStatusCode.CREATE_TOPIC_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(
          String.format(
              "Failed to create topic %s on config nodes, because %s", topicMeta, response));
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info("CreateTopicProcedure: executeFromOperateOnDataNodes({})", topicMeta);

    final List<TSStatus> statuses = env.pushSingleTopicOnDataNode(topicMeta.serialize());
    if (RpcUtils.squashResponseStatusList(statuses).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format(
              "Failed to create topic %s on data nodes, because %s", topicMeta, statuses));
    }
  }

  @Override
  protected void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateTopicProcedure: rollbackFromValidate({})", topicMeta);
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("CreateTopicProcedure: rollbackFromCreateOnConfigNodes({})", topicMeta);

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new DropTopicPlan(topicMeta.getTopicName()));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response =
          new TSStatus(TSStatusCode.DROP_TOPIC_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(
          String.format(
              "Failed to rollback creating topic %s on config nodes, because %s",
              topicMeta, response));
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("CreateTopicProcedure: rollbackFromCreateOnDataNodes({})", topicMeta);

    final List<TSStatus> statuses = env.dropSingleTopicOnDataNode(topicMeta.getTopicName());
    if (RpcUtils.squashResponseStatusList(statuses).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format(
              "Failed to rollback creating topic %s on data nodes, because %s",
              topicMeta, statuses));
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_TOPIC_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(createTopicReq.getTopicName(), stream);
    final int size = createTopicReq.getTopicAttributesSize();
    ReadWriteIOUtils.write(size, stream);
    if (size != 0) {
      for (Map.Entry<String, String> entry : createTopicReq.getTopicAttributes().entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue(), stream);
      }
    }

    ReadWriteIOUtils.write(topicMeta != null, stream);
    if (topicMeta != null) {
      topicMeta.serialize(stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    createTopicReq = new TCreateTopicReq();
    createTopicReq.setTopicName(ReadWriteIOUtils.readString(byteBuffer));
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      createTopicReq.putToTopicAttributes(
          ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }

    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      topicMeta = TopicMeta.deserialize(byteBuffer);
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
    CreateTopicProcedure that = (CreateTopicProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && Objects.equals(createTopicReq, that.createTopicReq)
        && Objects.equals(topicMeta, that.topicMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getCurrentState(), getCycles(), createTopicReq, topicMeta);
  }
}
