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
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.OperateMultiplePipesPlanV2;
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

public class DropSubscriptionProcedure extends AbstractOperateSubscriptionAndPipeProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropSubscriptionProcedure.class);

  private TUnsubscribeReq unsubscribeReq;

  // execution order: drop pipe -> alter consumer group
  // rollback order: alter consumer group -> drop pipe (no-op)
  // NOTE: The 'drop pipe' operation must be performed before 'alter consumer group'.
  private List<DropPipeProcedureV2> dropPipeProcedures = new ArrayList<>();
  private AlterConsumerGroupProcedure alterConsumerGroupProcedure;

  // TODO: remove this variable later
  private final List<AlterTopicProcedure> alterTopicProcedures = new ArrayList<>(); // unused now

  public DropSubscriptionProcedure() {
    super();
  }

  public DropSubscriptionProcedure(final TUnsubscribeReq unsubscribeReq) {
    this.unsubscribeReq = unsubscribeReq;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.DROP_SUBSCRIPTION;
  }

  @Override
  protected boolean executeFromValidate(final ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("DropSubscriptionProcedure: executeFromValidate");

    subscriptionInfo.get().validateBeforeUnsubscribe(unsubscribeReq);

    // Construct AlterConsumerGroupProcedure
    final ConsumerGroupMeta updatedConsumerGroupMeta =
        subscriptionInfo.get().deepCopyConsumerGroupMeta(unsubscribeReq.getConsumerGroupId());

    // Get topics subscribed by no consumers in this group after this un-subscription
    final Set<String> topicsUnsubByGroup =
        updatedConsumerGroupMeta.removeSubscription(
            unsubscribeReq.getConsumerId(), unsubscribeReq.getTopicNames());
    alterConsumerGroupProcedure =
        new AlterConsumerGroupProcedure(updatedConsumerGroupMeta, subscriptionInfo);

    for (final String topic : unsubscribeReq.getTopicNames()) {
      if (topicsUnsubByGroup.contains(topic)) {
        // Topic will be subscribed by no consumers in this group
        dropPipeProcedures.add(
            new DropPipeProcedureV2(
                PipeStaticMeta.generateSubscriptionPipeName(
                    topic, unsubscribeReq.getConsumerGroupId()),
                pipeTaskInfo));
      }
    }

    // Validate DropPipeProcedureV2s
    for (final DropPipeProcedureV2 dropPipeProcedure : dropPipeProcedures) {
      dropPipeProcedure.executeFromValidateTask(env);
      dropPipeProcedure.executeFromCalculateInfoForTask(env);
    }

    // Validate AlterConsumerGroupProcedure
    alterConsumerGroupProcedure.executeFromValidate(env);
    return true;
  }

  @Override
  protected void executeFromOperateOnConfigNodes(final ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("DropSubscriptionProcedure: executeFromOperateOnConfigNodes");

    // Execute DropPipeProcedureV2s
    final List<ConfigPhysicalPlan> dropPipePlans =
        dropPipeProcedures.stream()
            .map(proc -> new DropPipePlanV2(proc.getPipeName()))
            .collect(Collectors.toList());
    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new OperateMultiplePipesPlanV2(dropPipePlans));
    } catch (final ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && response.getSubStatusSize() > 0) {
      throw new SubscriptionException(
          String.format(
              "Failed to drop subscription with request %s on config nodes, because %s",
              unsubscribeReq, response));
    }

    // Execute AlterConsumerGroupProcedure
    alterConsumerGroupProcedure.executeFromOperateOnConfigNodes(env);
  }

  @Override
  protected void executeFromOperateOnDataNodes(final ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info("DropSubscriptionProcedure: executeFromOperateOnDataNodes");

    // Push pipe meta to data nodes
    final List<String> pipeNames =
        dropPipeProcedures.stream()
            .map(DropPipeProcedureV2::getPipeName)
            .collect(Collectors.toList());
    final String exceptionMessage =
        AbstractOperatePipeProcedureV2.parsePushPipeMetaExceptionForPipe(
            null, dropMultiPipeOnDataNodes(pipeNames, env));
    if (!exceptionMessage.isEmpty()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format(
              "Failed to drop pipes %s when dropping subscription with request %s, because %s",
              pipeNames, unsubscribeReq, exceptionMessage));
    }

    // Push consumer group meta to data nodes
    alterConsumerGroupProcedure.executeFromOperateOnDataNodes(env);
  }

  @Override
  protected void rollbackFromValidate(final ConfigNodeProcedureEnv env) {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromLock");
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(final ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromOperateOnConfigNodes");

    // Rollback AlterConsumerGroupProcedure
    alterConsumerGroupProcedure.rollbackFromOperateOnConfigNodes(env);

    // Do nothing to rollback DropPipeProcedureV2s
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(final ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromOperateOnDataNodes");

    // Rollback AlterConsumerGroupProcedure
    alterConsumerGroupProcedure.rollbackFromOperateOnDataNodes(env);

    // Do nothing to rollback DropPipeProcedureV2s
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_SUBSCRIPTION_PROCEDURE.getTypeCode());

    super.serialize(stream);

    ReadWriteIOUtils.write(unsubscribeReq.getConsumerId(), stream);
    ReadWriteIOUtils.write(unsubscribeReq.getConsumerGroupId(), stream);
    final int size = unsubscribeReq.getTopicNamesSize();
    ReadWriteIOUtils.write(size, stream);
    if (size != 0) {
      for (final String topicName : unsubscribeReq.getTopicNames()) {
        ReadWriteIOUtils.write(topicName, stream);
      }
    }

    // Serialize AlterConsumerGroupProcedure
    if (alterConsumerGroupProcedure != null) {
      ReadWriteIOUtils.write(true, stream);
      alterConsumerGroupProcedure.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    // Serialize AlterTopicProcedures
    if (alterTopicProcedures != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(alterTopicProcedures.size(), stream);
      for (final AlterTopicProcedure topicProcedure : alterTopicProcedures) {
        topicProcedure.serialize(stream);
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    // Serialize DropPipeProcedureV2s
    if (dropPipeProcedures != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(dropPipeProcedures.size(), stream);
      for (final AbstractOperatePipeProcedureV2 pipeProcedure : dropPipeProcedures) {
        pipeProcedure.serialize(stream);
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
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

    // Deserialize AlterConsumerGroupProcedure
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      // This readShort should return ALTER_CONSUMER_GROUP_PROCEDURE, and we ignore it.
      ReadWriteIOUtils.readShort(byteBuffer);

      alterConsumerGroupProcedure = new AlterConsumerGroupProcedure();
      alterConsumerGroupProcedure.deserialize(byteBuffer);
    }

    // Deserialize AlterTopicProcedures
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; ++i) {
        // This readShort should return ALTER_TOPIC_PROCEDURE, and we ignore it.
        ReadWriteIOUtils.readShort(byteBuffer);

        final AlterTopicProcedure topicProcedure = new AlterTopicProcedure();
        topicProcedure.deserialize(byteBuffer);
        alterTopicProcedures.add(topicProcedure);
      }
    }

    // Deserialize DropPipeProcedureV2s
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; ++i) {
        // This readShort should return DROP_PIPE_PROCEDURE.
        final short typeCode = ReadWriteIOUtils.readShort(byteBuffer);
        if (typeCode == ProcedureType.DROP_PIPE_PROCEDURE_V2.getTypeCode()) {
          final DropPipeProcedureV2 dropPipeProcedureV2 = new DropPipeProcedureV2();
          dropPipeProcedureV2.deserialize(byteBuffer);
          dropPipeProcedures.add(dropPipeProcedureV2);
        }
      }
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DropSubscriptionProcedure that = (DropSubscriptionProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && Objects.equals(unsubscribeReq, that.unsubscribeReq)
        && Objects.equals(alterConsumerGroupProcedure, that.alterConsumerGroupProcedure)
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
        dropPipeProcedures);
  }

  @TestOnly
  public void setAlterConsumerGroupProcedure(
      final AlterConsumerGroupProcedure alterConsumerGroupProcedure) {
    this.alterConsumerGroupProcedure = alterConsumerGroupProcedure;
  }

  @TestOnly
  public AlterConsumerGroupProcedure getAlterConsumerGroupProcedure() {
    return this.alterConsumerGroupProcedure;
  }

  @TestOnly
  public void setDropPipeProcedures(final List<DropPipeProcedureV2> dropPipeProcedures) {
    this.dropPipeProcedures = dropPipeProcedures;
  }

  @TestOnly
  public List<DropPipeProcedureV2> getDropPipeProcedures() {
    return this.dropPipeProcedures;
  }
}
