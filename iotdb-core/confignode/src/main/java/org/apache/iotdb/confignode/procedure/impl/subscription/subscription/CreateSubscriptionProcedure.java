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

package org.apache.iotdb.confignode.procedure.impl.subscription.subscription;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.OperateMultiplePipesPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterMultipleTopicsPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.AlterTopicProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

// TODO: check if it also needs meta sync to keep CN and DN in sync
public class CreateSubscriptionProcedure extends AbstractOperateSubscriptionAndPipeProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateSubscriptionProcedure.class);

  private TSubscribeReq subscribeReq;

  private AlterConsumerGroupProcedure alterConsumerGroupProcedure;
  private List<AlterTopicProcedure> alterTopicProcedures = new ArrayList<>();
  private List<CreatePipeProcedureV2> createPipeProcedures = new ArrayList<>();

  // Record failed index of procedures to rollback properly.
  // We only record fail index when executing on config nodes, because when executing on data nodes
  // fails, we just push all meta to data nodes.
  private int alterTopicProcedureFailIndexOnCN = -1;
  private int createPipeProcedureFailIndexOnCN = -1;

  public CreateSubscriptionProcedure() {
    super();
  }

  public CreateSubscriptionProcedure(TSubscribeReq subscribeReq) {
    this.subscribeReq = subscribeReq;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.CREATE_SUBSCRIPTION;
  }

  @Override
  protected boolean executeFromValidate(ConfigNodeProcedureEnv env) throws SubscriptionException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromValidate");

    subscriptionInfo.get().validateBeforeSubscribe(subscribeReq);

    // alterConsumerGroupProcedure
    final ConsumerGroupMeta updatedConsumerGroupMeta =
        subscriptionInfo.get().deepCopyConsumerGroupMeta(subscribeReq.getConsumerGroupId());
    updatedConsumerGroupMeta.addSubscription(
        subscribeReq.getConsumerId(), subscribeReq.getTopicNames());
    alterConsumerGroupProcedure =
        new AlterConsumerGroupProcedure(updatedConsumerGroupMeta, subscriptionInfo);

    // alterTopicProcedures & createPipeProcedures
    for (String topic : subscribeReq.getTopicNames()) {
      TopicMeta updatedTopicMeta = subscriptionInfo.get().deepCopyTopicMeta(topic);

      if (updatedTopicMeta.addSubscribedConsumerGroup(subscribeReq.getConsumerGroupId())) {
        createPipeProcedures.add(
            new CreatePipeProcedureV2(
                new TCreatePipeReq()
                    .setPipeName(
                        PipeStaticMeta.generateSubscriptionPipeName(
                            topic, subscribeReq.getConsumerGroupId()))
                    .setExtractorAttributes(updatedTopicMeta.generateExtractorAttributes())
                    .setProcessorAttributes(updatedTopicMeta.generateProcessorAttributes())
                    .setConnectorAttributes(
                        updatedTopicMeta.generateConnectorAttributes(
                            subscribeReq.getConsumerGroupId())),
                pipeTaskInfo));

        alterTopicProcedures.add(new AlterTopicProcedure(updatedTopicMeta, subscriptionInfo));
      }
    }

    alterConsumerGroupProcedure.executeFromValidate(env);

    for (AlterTopicProcedure alterTopicProcedure : alterTopicProcedures) {
      alterTopicProcedure.executeFromValidate(env);
    }

    for (CreatePipeProcedureV2 createPipeProcedure : createPipeProcedures) {
      createPipeProcedure.executeFromValidateTask(env);
      createPipeProcedure.executeFromCalculateInfoForTask(env);
    }
    return true;
  }

  // TODO: check periodically if the subscription is still valid but no working pipe?
  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromOperateOnConfigNodes");

    alterConsumerGroupProcedure.executeFromOperateOnConfigNodes(env);

    TSStatus response;

    List<AlterTopicPlan> alterTopicPlans =
        alterTopicProcedures.stream()
            .map(AlterTopicProcedure::getUpdatedTopicMeta)
            .map(AlterTopicPlan::new)
            .collect(Collectors.toList());
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new AlterMultipleTopicsPlan(alterTopicPlans));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && response.getSubStatusSize() > 0) {
      // Record the failed index for rollback
      alterTopicProcedureFailIndexOnCN = response.getSubStatusSize() - 1;
    }

    List<ConfigPhysicalPlan> createPipePlans =
        createPipeProcedures.stream()
            .map(CreatePipeProcedureV2::constructPlan)
            .collect(Collectors.toList());
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new OperateMultiplePipesPlanV2(createPipePlans));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && response.getSubStatusSize() > 0) {
      // Record the failed index for rollback
      createPipeProcedureFailIndexOnCN = response.getSubStatusSize() - 1;
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromOperateOnDataNodes");

    alterConsumerGroupProcedure.executeFromOperateOnDataNodes(env);

    // push topic meta to data nodes
    List<ByteBuffer> topicMetaBinaryList = new ArrayList<>();
    for (AlterTopicProcedure alterTopicProcedure : alterTopicProcedures) {
      topicMetaBinaryList.add(alterTopicProcedure.getUpdatedTopicMeta().serialize());
    }
    if (pushTopicMetaHasException(env.pushMultiTopicMetaToDataNodes(topicMetaBinaryList))) {
      // If not all topic meta are pushed successfully, the meta can be pushed during meta sync.
      LOGGER.warn(
          "Failed to alter topics when creating subscription, metadata will be synchronized later.");
    }

    // push pipe meta to data nodes
    List<String> pipeNames =
        createPipeProcedures.stream()
            .map(CreatePipeProcedureV2::getPipeName)
            .collect(Collectors.toList());
    String exceptionMessage =
        AbstractOperatePipeProcedureV2.parsePushPipeMetaExceptionForPipe(
            null, pushMultiPipeMetaToDataNodes(pipeNames, env));
    if (!exceptionMessage.isEmpty()) {
      // If not all pipe meta are pushed successfully, the meta can be pushed during meta sync.
      LOGGER.warn(
          "Failed to create pipes {} when creating subscription, details: {}, metadata will be synchronized later.",
          pipeNames,
          exceptionMessage);
    }
  }

  @Override
  protected void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromValidate");
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromOperateOnConfigNodes");

    // TODO: roll back from the last executed procedure to the first executed
    alterConsumerGroupProcedure.rollbackFromOperateOnConfigNodes(env);

    TSStatus response;

    // rollback alterTopicProcedures
    List<AlterTopicPlan> alterTopicRollbackPlans = new ArrayList<>();
    for (int i = 0;
        i <= Math.min(alterTopicProcedureFailIndexOnCN, alterTopicProcedures.size());
        i++) {
      alterTopicRollbackPlans.add(
          new AlterTopicPlan(alterTopicProcedures.get(i).getExistedTopicMeta()));
    }
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new AlterMultipleTopicsPlan(alterTopicRollbackPlans));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    // if failed to rollback, throw exception
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(response.getMessage());
    }

    // rollback createPipeProcedures
    List<ConfigPhysicalPlan> dropPipePlans = new ArrayList<>();
    for (int i = 0;
        i <= Math.min(createPipeProcedureFailIndexOnCN, createPipeProcedures.size());
        i++) {
      dropPipePlans.add(new DropPipePlanV2(createPipeProcedures.get(i).getPipeName()));
    }
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new OperateMultiplePipesPlanV2(dropPipePlans));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    // if failed to rollback, throw exception
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(response.getMessage());
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromOperateOnDataNodes");

    // TODO: roll back from the last executed procedure to the first executed
    alterConsumerGroupProcedure.rollbackFromOperateOnDataNodes(env);

    // Push all topic metas to datanode, may be time-consuming
    if (pushTopicMetaHasException(pushTopicMetaToDataNodes(env))) {
      LOGGER.warn(
          "Failed to rollback alter topics when creating subscription, metadata will be synchronized later.");
    }

    // Push all pipe metas to datanode, may be time-consuming
    String exceptionMessage =
        AbstractOperatePipeProcedureV2.parsePushPipeMetaExceptionForPipe(
            null, AbstractOperatePipeProcedureV2.pushPipeMetaToDataNodes(env, pipeTaskInfo));
    if (!exceptionMessage.isEmpty()) {
      LOGGER.warn(
          "Failed to rollback create pipes when creating subscription, details: {}, metadata will be synchronized later.",
          exceptionMessage);
    }
  }

  // TODO: we still need some strategies to clean the subscription if it's not valid anymore

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_SUBSCRIPTION_PROCEDURE.getTypeCode());

    super.serialize(stream);

    ReadWriteIOUtils.write(subscribeReq.getConsumerId(), stream);
    ReadWriteIOUtils.write(subscribeReq.getConsumerGroupId(), stream);
    final int size = subscribeReq.getTopicNamesSize();
    ReadWriteIOUtils.write(size, stream);
    if (size != 0) {
      for (String topicName : subscribeReq.getTopicNames()) {
        ReadWriteIOUtils.write(topicName, stream);
      }
    }

    // serialize consumerGroupProcedure
    if (alterConsumerGroupProcedure != null) {
      ReadWriteIOUtils.write(true, stream);
      alterConsumerGroupProcedure.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    // serialize topic procedures
    if (alterTopicProcedures != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(alterTopicProcedures.size(), stream);
      for (AlterTopicProcedure topicProcedure : alterTopicProcedures) {
        topicProcedure.serialize(stream);
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    // serialize pipe procedures
    if (createPipeProcedures != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(createPipeProcedures.size(), stream);
      for (CreatePipeProcedureV2 pipeProcedure : createPipeProcedures) {
        pipeProcedure.serialize(stream);
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    subscribeReq =
        new TSubscribeReq()
            .setConsumerId(ReadWriteIOUtils.readString(byteBuffer))
            .setConsumerGroupId(ReadWriteIOUtils.readString(byteBuffer))
            .setTopicNames(new HashSet<>());
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      subscribeReq.getTopicNames().add(ReadWriteIOUtils.readString(byteBuffer));
    }

    // deserialize consumerGroupProcedure
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      // This readShort should return ALTER_CONSUMER_GROUP_PROCEDURE, and we ignore it.
      ReadWriteIOUtils.readShort(byteBuffer);

      alterConsumerGroupProcedure = new AlterConsumerGroupProcedure();
      alterConsumerGroupProcedure.deserialize(byteBuffer);
    }

    // deserialize topic procedures
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; ++i) {
        // This readShort should return ALTER_TOPIC_PROCEDURE, and we ignore it.
        ReadWriteIOUtils.readShort(byteBuffer);

        AlterTopicProcedure topicProcedure = new AlterTopicProcedure();
        topicProcedure.deserialize(byteBuffer);
        alterTopicProcedures.add(topicProcedure);
      }
    }

    // deserialize pipe procedures
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; ++i) {
        // This readShort should return CREATE_PIPE_PROCEDURE or START_PIPE_PROCEDURE.
        short typeCode = ReadWriteIOUtils.readShort(byteBuffer);
        if (typeCode == ProcedureType.CREATE_PIPE_PROCEDURE_V2.getTypeCode()) {
          CreatePipeProcedureV2 createPipeProcedureV2 = new CreatePipeProcedureV2();
          createPipeProcedureV2.deserialize(byteBuffer);
          createPipeProcedures.add(createPipeProcedureV2);
        }
      }
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
    CreateSubscriptionProcedure that = (CreateSubscriptionProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && Objects.equals(subscribeReq, that.subscribeReq)
        && Objects.equals(alterConsumerGroupProcedure, that.alterConsumerGroupProcedure)
        && Objects.equals(alterTopicProcedures, that.alterTopicProcedures)
        && Objects.equals(createPipeProcedures, that.createPipeProcedures);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        subscribeReq,
        alterConsumerGroupProcedure,
        alterTopicProcedures,
        createPipeProcedures);
  }

  @TestOnly
  public void setAlterConsumerGroupProcedure(
      AlterConsumerGroupProcedure alterConsumerGroupProcedure) {
    this.alterConsumerGroupProcedure = alterConsumerGroupProcedure;
  }

  @TestOnly
  public AlterConsumerGroupProcedure getAlterConsumerGroupProcedure() {
    return this.alterConsumerGroupProcedure;
  }

  @TestOnly
  public void setAlterTopicProcedures(List<AlterTopicProcedure> alterTopicProcedures) {
    this.alterTopicProcedures = alterTopicProcedures;
  }

  @TestOnly
  public List<AlterTopicProcedure> getAlterTopicProcedures() {
    return this.alterTopicProcedures;
  }

  @TestOnly
  public void setCreatePipeProcedures(List<CreatePipeProcedureV2> createPipeProcedures) {
    this.createPipeProcedures = createPipeProcedures;
  }

  @TestOnly
  public List<CreatePipeProcedureV2> getCreatePipeProcedures() {
    return this.createPipeProcedures;
  }
}
