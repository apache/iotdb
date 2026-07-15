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
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.OperateMultiplePipesPlanV2;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.runtime.CommitProgressSyncProcedure;
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
import java.util.Set;
import java.util.stream.Collectors;

public class CreateSubscriptionProcedure extends AbstractOperateSubscriptionAndPipeProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateSubscriptionProcedure.class);

  private TSubscribeReq subscribeReq;

  // execution order: alter consumer group -> create pipe
  // rollback order: create pipe -> alter consumer group
  // NOTE: The 'alter consumer group' operation must be performed before 'create pipe'.
  private AlterConsumerGroupProcedure alterConsumerGroupProcedure;
  private List<CreatePipeProcedureV2> createPipeProcedures = new ArrayList<>();

  private Set<String> consensusTopicNames = new HashSet<>();

  // TODO: remove this variable later
  private final List<AlterTopicProcedure> alterTopicProcedures = new ArrayList<>(); // unused now

  public CreateSubscriptionProcedure() {
    super();
  }

  public CreateSubscriptionProcedure(final TSubscribeReq subscribeReq) {
    this.subscribeReq = subscribeReq;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.CREATE_SUBSCRIPTION;
  }

  @Override
  protected boolean executeFromValidate(final ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info(ProcedureMessages.CREATESUBSCRIPTIONPROCEDURE_EXECUTEFROMVALIDATE);

    alterConsumerGroupProcedure = null;
    createPipeProcedures = new ArrayList<>();
    consensusTopicNames = new HashSet<>();

    subscriptionInfo.get().validateBeforeSubscribe(subscribeReq);

    final String consumerId = subscribeReq.getConsumerId();
    final String consumerGroupId = subscribeReq.getConsumerGroupId();
    final ConsumerGroupMeta consumerGroupMeta =
        subscriptionInfo.get().getConsumerGroupMeta(consumerGroupId);
    final ConsumerMeta consumerMeta = consumerGroupMeta.getConsumerMeta(consumerId);

    // Construct AlterConsumerGroupProcedure
    final ConsumerGroupMeta updatedConsumerGroupMeta =
        subscriptionInfo.get().deepCopyConsumerGroupMeta(consumerGroupId);
    updatedConsumerGroupMeta.addSubscription(
        subscribeReq.getConsumerId(), subscribeReq.getTopicNames());
    alterConsumerGroupProcedure =
        new AlterConsumerGroupProcedure(updatedConsumerGroupMeta, subscriptionInfo);

    // Construct CreatePipeProcedureV2s (for non-consensus topics)
    for (final String topicName : subscribeReq.getTopicNames()) {
      final TopicMeta topicMeta = subscriptionInfo.get().deepCopyTopicMeta(topicName);

      final String topicMode = topicMeta.getConfig().getMode();
      final boolean isConsensusBasedTopic = topicMeta.getConfig().isConsensusMode();

      if (isConsensusBasedTopic) {
        // skip pipe creation
        consensusTopicNames.add(topicName);
        LOGGER.info(
            ProcedureMessages
                    .LOG_CREATESUBSCRIPTIONPROCEDURE_TOPIC_ARG_USES_CONSENSUS_SUBSCRIPTION_MODE_031CF049
                + ProcedureMessages.LOG_MODE_ARG_SKIPPING_PIPE_CREATION_5F4D1026,
            topicName,
            topicMode);
        continue;
      }

      final String pipeName =
          PipeStaticMeta.generateSubscriptionPipeName(topicName, consumerGroupId);
      if (!subscriptionInfo.get().isTopicSubscribedByConsumerGroup(topicName, consumerGroupId)
          // even if there existed subscription meta, if there is no corresponding pipe meta, it
          // will try to create the pipe
          || !pipeTaskInfo.get().isPipeExisted(pipeName, topicMeta.visibleUnderTableModel())) {
        createPipeProcedures.add(
            new CreatePipeProcedureV2(
                new TCreatePipeReq()
                    .setPipeName(pipeName)
                    .setExtractorAttributes(
                        topicMeta.generateExtractorAttributes(
                            consumerMeta.getUsername(), consumerMeta.getSubscriptionAuthPassword()))
                    .setProcessorAttributes(topicMeta.generateProcessorAttributes())
                    .setConnectorAttributes(topicMeta.generateConnectorAttributes(consumerGroupId)),
                pipeTaskInfo));
      }
    }

    // Validate AlterConsumerGroupProcedure
    alterConsumerGroupProcedure.executeFromValidate(env);

    // Validate CreatePipeProcedureV2s
    for (final CreatePipeProcedureV2 createPipeProcedure : createPipeProcedures) {
      createPipeProcedure.executeFromValidateTask(env);
      createPipeProcedure.executeFromCalculateInfoForTask(env);
    }

    return true;
  }

  @Override
  protected void executeFromOperateOnConfigNodes(final ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info(ProcedureMessages.CREATESUBSCRIPTIONPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES);

    // Execute AlterConsumerGroupProcedure
    alterConsumerGroupProcedure.executeFromOperateOnConfigNodes(env);

    // Execute CreatePipeProcedureV2s
    final List<ConfigPhysicalPlan> createPipePlans =
        createPipeProcedures.stream()
            .map(CreatePipeProcedureV2::constructPlan)
            .collect(Collectors.toList());
    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new OperateMultiplePipesPlanV2(createPipePlans));
    } catch (final ConsensusException e) {
      LOGGER.warn(ConfigNodeMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(
          String.format(
              ProcedureMessages.FAILED_TO_CREATE_SUBSCRIPTION_WITH_REQUEST_ON_CONFIG_NODES_BECAUSE,
              subscribeReq,
              response));
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(final ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info(ProcedureMessages.CREATESUBSCRIPTIONPROCEDURE_EXECUTEFROMOPERATEONDATANODES);

    // Push consumer group meta to data nodes
    alterConsumerGroupProcedure.executeFromOperateOnDataNodes(env);

    final Set<String> newlySubscribedConsensusTopicNames =
        getNewlySubscribedConsensusTopicNames(
            alterConsumerGroupProcedure.getExistingConsumerGroupMeta(),
            alterConsumerGroupProcedure.getUpdatedConsumerGroupMeta(),
            consensusTopicNames);
    if (!newlySubscribedConsensusTopicNames.isEmpty()) {
      LOGGER.info(
          ProcedureMessages
              .LOG_CREATESUBSCRIPTIONPROCEDURE_SYNCHRONIZING_COMMIT_PROGRESS_AFTER_CONSUMER_GROUP_NEWLY_SUBSCRIBED_CONSENSUS_TOPICS_ARG_F5687D36,
          newlySubscribedConsensusTopicNames);
      CommitProgressSyncProcedure.syncCommitProgressFromDataNodesRequired(
          env, subscriptionInfo.get());
    }

    if (!consensusTopicNames.isEmpty()) {
      LOGGER.info(
          ProcedureMessages
                  .LOG_CREATESUBSCRIPTIONPROCEDURE_CONSENSUS_BASED_TOPICS_ARG_WILL_HANDLED_DATANODE_90A9C2FD
              + ProcedureMessages.LOG_VIA_CONSUMER_GROUP_META_PUSH_NO_PIPE_CREATION_NEEDED_D56CFE31,
          consensusTopicNames);
    }

    // Push pipe meta to data nodes (only for non-consensus pipe-based topics)
    if (!createPipeProcedures.isEmpty()) {
      final List<PipeStaticMeta> pipeStaticMetas =
          createPipeProcedures.stream()
              .map(CreatePipeProcedureV2::getPipeStaticMeta)
              .collect(Collectors.toList());
      final String exceptionMessage =
          AbstractOperatePipeProcedureV2.parsePushPipeMetaExceptionForPipe(
              null, pushMultiPipeMetaToDataNodes(pipeStaticMetas, env));
      if (!exceptionMessage.isEmpty()) {
        // throw exception instead of logging warn, do not rely on metadata synchronization
        throw new SubscriptionException(
            String.format(
                ProcedureMessages
                    .FAILED_TO_CREATE_PIPES_WHEN_CREATING_SUBSCRIPTION_WITH_REQUEST_DETAILS,
                pipeStaticMetas.stream()
                    .map(PipeStaticMeta::getPipeName)
                    .collect(Collectors.toList()),
                subscribeReq,
                exceptionMessage));
      }
    }
  }

  @Override
  protected void rollbackFromValidate(final ConfigNodeProcedureEnv env) {
    LOGGER.info(ProcedureMessages.CREATESUBSCRIPTIONPROCEDURE_ROLLBACKFROMVALIDATE);
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(final ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info(ProcedureMessages.CREATESUBSCRIPTIONPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES);

    // Rollback CreatePipeProcedureV2s
    final List<ConfigPhysicalPlan> dropPipePlans =
        createPipeProcedures.stream()
            .map(
                procedure ->
                    new DropPipePlanV2(
                        procedure.getPipeName(),
                        procedure.getPipeStaticMeta().visibleUnderTableModel()))
            .collect(Collectors.toList());
    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new OperateMultiplePipesPlanV2(dropPipePlans));
    } catch (final ConsensusException e) {
      LOGGER.warn(ConfigNodeMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(
          String.format(
              ProcedureMessages
                  .FAILED_TO_ROLLBACK_CREATING_SUBSCRIPTION_WITH_REQUEST_ON_CONFIG_NODES,
              subscribeReq,
              response));
    }

    // Rollback AlterConsumerGroupProcedure
    alterConsumerGroupProcedure.rollbackFromOperateOnConfigNodes(env);
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(final ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info(ProcedureMessages.CREATESUBSCRIPTIONPROCEDURE_ROLLBACKFROMOPERATEONDATANODES);

    // Push all pipe metas to datanode, may be time-consuming
    final String exceptionMessage =
        AbstractOperatePipeProcedureV2.parsePushPipeMetaExceptionForPipe(
            null, AbstractOperatePipeProcedureV2.pushPipeMetaToDataNodes(env, pipeTaskInfo));
    if (!exceptionMessage.isEmpty()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format(
              ProcedureMessages
                  .FAILED_TO_ROLLBACK_CREATE_PIPES_WHEN_CREATING_SUBSCRIPTION_WITH_REQUEST,
              subscribeReq,
              exceptionMessage));
    }

    // Rollback AlterConsumerGroupProcedure
    alterConsumerGroupProcedure.rollbackFromOperateOnDataNodes(env);
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_SUBSCRIPTION_PROCEDURE.getTypeCode());

    super.serialize(stream);

    ReadWriteIOUtils.write(subscribeReq.getConsumerId(), stream);
    ReadWriteIOUtils.write(subscribeReq.getConsumerGroupId(), stream);
    final int size = subscribeReq.getTopicNamesSize();
    ReadWriteIOUtils.write(size, stream);
    if (size != 0) {
      for (final String topicName : subscribeReq.getTopicNames()) {
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

    // Serialize CreatePipeProcedureV2s
    if (createPipeProcedures != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(createPipeProcedures.size(), stream);
      for (final CreatePipeProcedureV2 pipeProcedure : createPipeProcedures) {
        pipeProcedure.serialize(stream);
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    // Serialize consensus topic names
    ReadWriteIOUtils.write(consensusTopicNames.size(), stream);
    for (final String consensusTopicName : consensusTopicNames) {
      ReadWriteIOUtils.write(consensusTopicName, stream);
    }
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
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

    // Deserialize CreatePipeProcedureV2s
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; ++i) {
        // This readShort should return CREATE_PIPE_PROCEDURE or START_PIPE_PROCEDURE.
        final short typeCode = ReadWriteIOUtils.readShort(byteBuffer);
        if (typeCode == ProcedureType.CREATE_PIPE_PROCEDURE_V2.getTypeCode()) {
          final CreatePipeProcedureV2 createPipeProcedureV2 = new CreatePipeProcedureV2();
          createPipeProcedureV2.deserialize(byteBuffer);
          createPipeProcedures.add(createPipeProcedureV2);
        }
      }
    }

    // Deserialize consensus topic names
    if (byteBuffer.hasRemaining()) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; ++i) {
        consensusTopicNames.add(ReadWriteIOUtils.readString(byteBuffer));
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
    final CreateSubscriptionProcedure that = (CreateSubscriptionProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && Objects.equals(subscribeReq, that.subscribeReq)
        && Objects.equals(alterConsumerGroupProcedure, that.alterConsumerGroupProcedure)
        && Objects.equals(createPipeProcedures, that.createPipeProcedures)
        && Objects.equals(consensusTopicNames, that.consensusTopicNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        subscribeReq,
        alterConsumerGroupProcedure,
        createPipeProcedures,
        consensusTopicNames);
  }

  static Set<String> getNewlySubscribedConsensusTopicNames(
      final ConsumerGroupMeta existingConsumerGroupMeta,
      final ConsumerGroupMeta updatedConsumerGroupMeta,
      final Set<String> consensusTopicNames) {
    final Set<String> newlySubscribedConsensusTopicNames =
        new HashSet<>(
            ConsumerGroupMeta.getTopicsNewlySubByGroup(
                existingConsumerGroupMeta, updatedConsumerGroupMeta));
    newlySubscribedConsensusTopicNames.retainAll(consensusTopicNames);
    return newlySubscribedConsensusTopicNames;
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
  public void setCreatePipeProcedures(final List<CreatePipeProcedureV2> createPipeProcedures) {
    this.createPipeProcedures = createPipeProcedures;
  }

  @TestOnly
  public List<CreatePipeProcedureV2> getCreatePipeProcedures() {
    return this.createPipeProcedures;
  }
}
