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
import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.AlterTopicProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
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
import java.util.Set;
import java.util.stream.Collectors;

// TODO: check if it also needs meta sync to keep CN and DN in sync
public class DropSubscriptionProcedure extends AbstractOperateSubscriptionAndPipeProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropSubscriptionProcedure.class);

  private TUnsubscribeReq unsubscribeReq;

  private AlterConsumerGroupProcedure alterConsumerGroupProcedure;
  private List<AlterTopicProcedure> alterTopicProcedures = new ArrayList<>();
  private List<DropPipeProcedureV2> dropPipeProcedures = new ArrayList<>();

  // Record failed index of procedures to rollback properly.
  // We only record fail index when executing on config nodes, because when executing on data nodes
  // fails, we just push all meta to data nodes.
  private int alterTopicProcedureFailIndexOnCN = -1;
  private int dropPipeProcedureFailIndexOnCN = -1;

  public DropSubscriptionProcedure() {
    super();
  }

  public DropSubscriptionProcedure(TUnsubscribeReq unsubscribeReq) {
    this.unsubscribeReq = unsubscribeReq;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.DROP_SUBSCRIPTION;
  }

  @Override
  protected void executeFromValidate(ConfigNodeProcedureEnv env) throws SubscriptionException {
    LOGGER.info("DropSubscriptionProcedure: executeFromValidate");

    subscriptionInfo.get().validateBeforeUnsubscribe(unsubscribeReq);

    // Construct AlterConsumerGroupProcedure
    ConsumerGroupMeta updatedConsumerGroupMeta =
        subscriptionInfo.get().deepCopyConsumerGroupMeta(unsubscribeReq.getConsumerGroupId());

    // Get topics subscribed by no consumers in this group after this un-subscription
    Set<String> topicsUnsubByGroup =
        updatedConsumerGroupMeta.removeSubscription(
            unsubscribeReq.getConsumerId(), unsubscribeReq.getTopicNames());
    alterConsumerGroupProcedure =
        new AlterConsumerGroupProcedure(updatedConsumerGroupMeta, subscriptionInfo);

    for (String topic : unsubscribeReq.getTopicNames()) {
      if (topicsUnsubByGroup.contains(topic)) {
        // Topic will be subscribed by no consumers in this group

        TopicMeta updatedTopicMeta = subscriptionInfo.get().deepCopyTopicMeta(topic);
        updatedTopicMeta.removeSubscribedConsumerGroup(unsubscribeReq.getConsumerGroupId());

        alterTopicProcedures.add(new AlterTopicProcedure(updatedTopicMeta, subscriptionInfo));
        dropPipeProcedures.add(
            new DropPipeProcedureV2(
                PipeStaticMeta.generateSubscriptionPipeName(
                    topic, unsubscribeReq.getConsumerGroupId()),
                pipeTaskInfo));
      }
    }

    alterConsumerGroupProcedure.executeFromValidate(env);

    for (AlterTopicProcedure alterTopicProcedure : alterTopicProcedures) {
      alterTopicProcedure.executeFromValidate(env);
    }

    for (DropPipeProcedureV2 dropPipeProcedure : dropPipeProcedures) {
      dropPipeProcedure.executeFromValidateTask(env);
      dropPipeProcedure.executeFromCalculateInfoForTask(env);
    }
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("DropSubscriptionProcedure: executeFromOperateOnConfigNodes");

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

    List<ConfigPhysicalPlan> dropPipePlans =
        dropPipeProcedures.stream()
            .map(proc -> new DropPipePlanV2(proc.getPipeName()))
            .collect(Collectors.toList());
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
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && response.getSubStatusSize() > 0) {
      // Record the failed index for rollback
      dropPipeProcedureFailIndexOnCN = response.getSubStatusSize() - 1;
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info("DropSubscriptionProcedure: executeFromOperateOnDataNodes");

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
        dropPipeProcedures.stream()
            .map(DropPipeProcedureV2::getPipeName)
            .collect(Collectors.toList());
    String exceptionMessage =
        AbstractOperatePipeProcedureV2.parsePushPipeMetaExceptionForPipe(
            null, dropMultiPipeOnDataNodes(pipeNames, env));
    if (!exceptionMessage.isEmpty()) {
      // If not all pipe meta are pushed successfully, the meta can be pushed during meta sync.
      LOGGER.warn(
          "Failed to drop pipes {} when dropping subscription, details: {}, metadata will be synchronized later.",
          pipeNames,
          exceptionMessage);
    }
  }

  @Override
  protected void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromLock");
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromOperateOnConfigNodes");

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

    // Do nothing to rollback dropPipeProcedures
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromOperateOnDataNodes");

    alterConsumerGroupProcedure.rollbackFromOperateOnDataNodes(env);

    // Push all topic metas to datanode, may be time-consuming
    if (pushTopicMetaHasException(pushTopicMetaToDataNodes(env))) {
      LOGGER.warn(
          "Failed to rollback alter topics when dropping subscription, metadata will be synchronized later.");
    }

    // Push all pipe metas to datanode, may be time-consuming
    String exceptionMessage =
        AbstractOperatePipeProcedureV2.parsePushPipeMetaExceptionForPipe(
            null, AbstractOperatePipeProcedureV2.pushPipeMetaToDataNodes(env, pipeTaskInfo));
    if (!exceptionMessage.isEmpty()) {
      LOGGER.warn(
          "Failed to rollback create pipes when dropping subscription, details: {}, metadata will be synchronized later.",
          exceptionMessage);
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_SUBSCRIPTION_PROCEDURE.getTypeCode());

    super.serialize(stream);

    ReadWriteIOUtils.write(unsubscribeReq.getConsumerId(), stream);
    ReadWriteIOUtils.write(unsubscribeReq.getConsumerGroupId(), stream);
    final int size = unsubscribeReq.getTopicNamesSize();
    ReadWriteIOUtils.write(size, stream);
    if (size != 0) {
      for (String topicName : unsubscribeReq.getTopicNames()) {
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
    if (dropPipeProcedures != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(dropPipeProcedures.size(), stream);
      for (AbstractOperatePipeProcedureV2 pipeProcedure : dropPipeProcedures) {
        pipeProcedure.serialize(stream);
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    unsubscribeReq =
        new TUnsubscribeReq()
            .setConsumerId(ReadWriteIOUtils.readString(byteBuffer))
            .setConsumerGroupId(ReadWriteIOUtils.readString(byteBuffer))
            .setTopicNames(new HashSet<>());
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      unsubscribeReq.getTopicNames().add(ReadWriteIOUtils.readString(byteBuffer));
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
        // This readShort should return DROP_PIPE_PROCEDURE.
        short typeCode = ReadWriteIOUtils.readShort(byteBuffer);
        if (typeCode == ProcedureType.DROP_PIPE_PROCEDURE_V2.getTypeCode()) {
          DropPipeProcedureV2 dropPipeProcedureV2 = new DropPipeProcedureV2();
          dropPipeProcedureV2.deserialize(byteBuffer);
          dropPipeProcedures.add(dropPipeProcedureV2);
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
    DropSubscriptionProcedure that = (DropSubscriptionProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && Objects.equals(unsubscribeReq, that.unsubscribeReq)
        && Objects.equals(alterConsumerGroupProcedure, that.alterConsumerGroupProcedure)
        && Objects.equals(alterTopicProcedures, that.alterTopicProcedures)
        && Objects.equals(dropPipeProcedures, that.dropPipeProcedures);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        unsubscribeReq,
        alterConsumerGroupProcedure,
        alterTopicProcedures,
        dropPipeProcedures);
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
  public void setDropPipeProcedures(List<DropPipeProcedureV2> dropPipeProcedures) {
    this.dropPipeProcedures = dropPipeProcedures;
  }

  @TestOnly
  public List<DropPipeProcedureV2> getDropPipeProcedures() {
    return this.dropPipeProcedures;
  }
}
