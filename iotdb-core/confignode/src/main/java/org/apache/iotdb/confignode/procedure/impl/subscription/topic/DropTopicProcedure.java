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
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.CommitProgressKeeper;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.OperateMultiplePipesPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.AlterConsumerGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.runtime.CommitProgressHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.DropTopicPlan;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.impl.subscription.subscription.AbstractOperateSubscriptionAndPipeProcedure;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DropTopicProcedure extends AbstractOperateSubscriptionAndPipeProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropTopicProcedure.class);

  private String topicName;
  private boolean isTableModel;
  private boolean isTableModelSet;
  private TopicMeta existedTopicMeta;
  private final List<ConsumerGroupMeta> retainedConsumerGroupMetas = new ArrayList<>();
  private final List<DropPipeProcedureV2> retainedPipeProcedures = new ArrayList<>();
  private Map<String, ByteBuffer> retainedProgress = new HashMap<>();
  private boolean serializeRetainedProgressState;

  public DropTopicProcedure() {
    super();
  }

  public DropTopicProcedure(String topicName) {
    super();
    this.topicName = topicName;
  }

  public DropTopicProcedure(String topicName, boolean isTableModel) {
    this(topicName);
    this.isTableModel = isTableModel;
    this.isTableModelSet = true;
  }

  public DropTopicProcedure(final boolean serializeRetainedProgressState) {
    super();
    this.serializeRetainedProgressState = serializeRetainedProgressState;
  }

  public boolean shouldSerializeRetainedProgressState() {
    return serializeRetainedProgressState
        || Objects.nonNull(existedTopicMeta)
        || !retainedConsumerGroupMetas.isEmpty()
        || !retainedPipeProcedures.isEmpty();
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.DROP_TOPIC;
  }

  @Override
  protected boolean executeFromValidate(ConfigNodeProcedureEnv env) throws SubscriptionException {
    LOGGER.info(ProcedureMessages.DROPTOPICPROCEDURE_EXECUTEFROMVALIDATE, topicName);

    retainedConsumerGroupMetas.clear();
    retainedPipeProcedures.clear();
    retainedProgress.clear();

    if (isTableModelSet) {
      subscriptionInfo.get().validateBeforeDroppingTopic(topicName, isTableModel);
    } else {
      subscriptionInfo.get().validateBeforeDroppingTopic(topicName);
    }

    existedTopicMeta =
        isTableModelSet
            ? subscriptionInfo.get().deepCopyTopicMeta(topicName, isTableModel)
            : subscriptionInfo.get().deepCopyTopicMeta(topicName);
    if (Objects.nonNull(existedTopicMeta)
        && existedTopicMeta.getConfig().isProgressRetainedAfterUnsubscribe()) {
      final boolean model = existedTopicMeta.visibleUnderTableModel();
      for (final ConsumerGroupMeta consumerGroupMeta :
          subscriptionInfo.get().getConsumerGroupsRetainingTopic(topicName, model)) {
        retainedConsumerGroupMetas.add(consumerGroupMeta);
        retainedProgress.putAll(
            subscriptionInfo
                .get()
                .getCommitProgressKeeper()
                .getTopicProgress(consumerGroupMeta.getConsumerGroupId(), topicName));
        final String pipeName =
            PipeStaticMeta.generateSubscriptionPipeName(
                topicName, consumerGroupMeta.getConsumerGroupId());
        if (pipeTaskInfo.get().isPipeExisted(pipeName, model)) {
          final DropPipeProcedureV2 dropPipeProcedure =
              new DropPipeProcedureV2(pipeName, model, pipeTaskInfo);
          dropPipeProcedure.executeFromValidateTask(env);
          dropPipeProcedure.executeFromCalculateInfoForTask(env);
          retainedPipeProcedures.add(dropPipeProcedure);
        }
      }
    }
    return true;
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info(ProcedureMessages.DROPTOPICPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES, topicName);

    final boolean model = getTopicModel();
    writeConsensusPlan(
        env,
        existedTopicMeta == null && !isTableModelSet
            ? new DropTopicPlan(topicName)
            : new DropTopicPlan(topicName, model));
    if (!retainedPipeProcedures.isEmpty()) {
      final List<ConfigPhysicalPlan> dropPipePlans =
          retainedPipeProcedures.stream()
              .map(proc -> new DropPipePlanV2(proc.getPipeName(), model))
              .collect(java.util.stream.Collectors.toList());
      writeConsensusPlan(env, new OperateMultiplePipesPlanV2(dropPipePlans));
    }
    for (final ConsumerGroupMeta consumerGroupMeta : getUpdatedRetainedConsumerGroupMetas()) {
      writeConsensusPlan(env, new AlterConsumerGroupPlan(consumerGroupMeta));
    }
  }

  private void writeConsensusPlan(final ConfigNodeProcedureEnv env, final ConfigPhysicalPlan plan)
      throws SubscriptionException {
    TSStatus response;
    try {
      response = env.getConfigManager().getConsensusManager().write(plan);
    } catch (ConsensusException e) {
      LOGGER.warn(ConfigNodeMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
      response =
          new TSStatus(TSStatusCode.DROP_TOPIC_ERROR.getStatusCode()).setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(
          String.format(
              ProcedureMessages.FAILED_TO_DROP_TOPIC_ON_CONFIG_NODES_BECAUSE, topicName, response));
    }
  }

  private List<ConsumerGroupMeta> getUpdatedRetainedConsumerGroupMetas() {
    final List<ConsumerGroupMeta> updatedMetas = new ArrayList<>();
    for (final ConsumerGroupMeta existingMeta : retainedConsumerGroupMetas) {
      final ConsumerGroupMeta updatedMeta = existingMeta.deepCopy();
      updatedMeta.removeRetainedTopic(topicName);
      updatedMetas.add(updatedMeta);
    }
    return updatedMetas;
  }

  private boolean getTopicModel() {
    return existedTopicMeta == null ? isTableModel : existedTopicMeta.visibleUnderTableModel();
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info(ProcedureMessages.DROPTOPICPROCEDURE_EXECUTEFROMOPERATEONDATANODES, topicName);

    if (!retainedPipeProcedures.isEmpty()) {
      final String exceptionMessage =
          AbstractOperatePipeProcedureV2.parsePushPipeMetaExceptionForPipe(
              null, dropMultiPipeOnDataNodes(retainedPipeProcedures, env));
      if (!exceptionMessage.isEmpty()) {
        throw new SubscriptionException(exceptionMessage);
      }
    }
    for (final ConsumerGroupMeta consumerGroupMeta : getUpdatedRetainedConsumerGroupMetas()) {
      final List<TSStatus> consumerGroupStatuses =
          env.pushSingleConsumerGroupOnDataNode(consumerGroupMeta.serialize());
      if (RpcUtils.squashResponseStatusList(consumerGroupStatuses).getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new SubscriptionException(consumerGroupStatuses.toString());
      }
    }

    final List<TSStatus> statuses =
        existedTopicMeta == null && !isTableModelSet
            ? env.dropSingleTopicOnDataNode(topicName)
            : env.dropSingleTopicOnDataNode(topicName, getTopicModel());
    if (RpcUtils.squashResponseStatusList(statuses).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format(
              ProcedureMessages.FAILED_TO_DROP_TOPIC_ON_DATA_NODES_BECAUSE, topicName, statuses));
    }
  }

  @Override
  protected void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info(ProcedureMessages.DROPTOPICPROCEDURE_ROLLBACKFROMVALIDATE, topicName);
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info(ProcedureMessages.DROPTOPICPROCEDURE_ROLLBACKFROMCREATEONCONFIGNODES, topicName);
    if (Objects.nonNull(existedTopicMeta)) {
      writeConsensusPlan(env, new CreateTopicPlan(existedTopicMeta));
      for (final DropPipeProcedureV2 dropPipeProcedure : retainedPipeProcedures) {
        final PipeMeta pipeMeta = dropPipeProcedure.getPipeMetaToDrop();
        if (!pipeTaskInfo
            .get()
            .isPipeExisted(
                pipeMeta.getStaticMeta().getPipeName(),
                pipeMeta.getStaticMeta().visibleUnderTableModel())) {
          writeConsensusPlan(
              env, new CreatePipePlanV2(pipeMeta.getStaticMeta(), pipeMeta.getRuntimeMeta()));
        }
      }
      for (final ConsumerGroupMeta consumerGroupMeta : retainedConsumerGroupMetas) {
        writeConsensusPlan(env, new AlterConsumerGroupPlan(consumerGroupMeta));
      }
      restoreRetainedProgress(env);
    }
  }

  private void restoreRetainedProgress(final ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    if (retainedProgress.isEmpty()) {
      return;
    }
    final Map<String, ByteBuffer> progress =
        subscriptionInfo.get().getCommitProgressKeeper().getAllRegionProgress();
    progress.putAll(retainedProgress);
    writeConsensusPlan(env, new CommitProgressHandleMetaChangePlan(progress));
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info(ProcedureMessages.DROPTOPICPROCEDURE_ROLLBACKFROMCREATEONDATANODES, topicName);
    if (Objects.nonNull(existedTopicMeta)) {
      for (final ConsumerGroupMeta consumerGroupMeta : retainedConsumerGroupMetas) {
        final List<TSStatus> statuses =
            env.pushSingleConsumerGroupOnDataNode(consumerGroupMeta.serialize());
        if (RpcUtils.squashResponseStatusList(statuses).getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new SubscriptionException(statuses.toString());
        }
      }
      final List<TSStatus> statuses = env.pushSingleTopicOnDataNode(existedTopicMeta.serialize());
      if (RpcUtils.squashResponseStatusList(statuses).getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new SubscriptionException(statuses.toString());
      }
      if (!retainedPipeProcedures.isEmpty()) {
        final String exceptionMessage =
            AbstractOperatePipeProcedureV2.parsePushPipeMetaExceptionForPipe(
                null, AbstractOperatePipeProcedureV2.pushPipeMetaToDataNodes(env, pipeTaskInfo));
        if (!exceptionMessage.isEmpty()) {
          throw new SubscriptionException(exceptionMessage);
        }
      }
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(
        (shouldSerializeRetainedProgressState()
                ? ProcedureType.DROP_TOPIC_WITH_RETAINED_PROGRESS_PROCEDURE
                : ProcedureType.DROP_TOPIC_PROCEDURE)
            .getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(topicName, stream);
    if (shouldSerializeRetainedProgressState()) {
      ReadWriteIOUtils.write(Objects.nonNull(existedTopicMeta), stream);
      if (Objects.nonNull(existedTopicMeta)) {
        existedTopicMeta.serialize(stream);
      }
      ReadWriteIOUtils.write(isTableModelSet, stream);
      if (isTableModelSet) {
        ReadWriteIOUtils.write(isTableModel, stream);
      }
      ReadWriteIOUtils.write(retainedConsumerGroupMetas.size(), stream);
      for (final ConsumerGroupMeta consumerGroupMeta : retainedConsumerGroupMetas) {
        consumerGroupMeta.serialize(stream);
      }
      ReadWriteIOUtils.write(retainedPipeProcedures.size(), stream);
      for (final DropPipeProcedureV2 pipeProcedure : retainedPipeProcedures) {
        pipeProcedure.serialize(stream);
      }
      CommitProgressKeeper.serializeRegionProgressMapToStream(retainedProgress, stream);
    } else if (isTableModelSet) {
      ReadWriteIOUtils.write(isTableModel, stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    topicName = ReadWriteIOUtils.readString(byteBuffer);
    if (shouldSerializeRetainedProgressState()) {
      if (ReadWriteIOUtils.readBool(byteBuffer)) {
        existedTopicMeta = TopicMeta.deserialize(byteBuffer);
      }
      isTableModelSet = ReadWriteIOUtils.readBool(byteBuffer);
      if (isTableModelSet) {
        isTableModel = ReadWriteIOUtils.readBool(byteBuffer);
      }
      int size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; i++) {
        retainedConsumerGroupMetas.add(ConsumerGroupMeta.deserialize(byteBuffer));
      }
      size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; i++) {
        ReadWriteIOUtils.readShort(byteBuffer);
        final DropPipeProcedureV2 pipeProcedure = new DropPipeProcedureV2();
        pipeProcedure.deserialize(byteBuffer);
        retainedPipeProcedures.add(pipeProcedure);
      }
      retainedProgress = CommitProgressKeeper.deserializeRegionProgressFromBuffer(byteBuffer);
    } else {
      isTableModelSet = byteBuffer.hasRemaining();
      if (isTableModelSet) {
        isTableModel = ReadWriteIOUtils.readBool(byteBuffer);
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
    DropTopicProcedure that = (DropTopicProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && Objects.equals(topicName, that.topicName)
        && isTableModel == that.isTableModel
        && isTableModelSet == that.isTableModelSet
        && Objects.equals(existedTopicMeta, that.existedTopicMeta)
        && Objects.equals(retainedConsumerGroupMetas, that.retainedConsumerGroupMetas)
        && Objects.equals(retainedPipeProcedures, that.retainedPipeProcedures)
        && Objects.equals(retainedProgress, that.retainedProgress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        topicName,
        isTableModel,
        isTableModelSet,
        existedTopicMeta,
        retainedConsumerGroupMetas,
        retainedPipeProcedures,
        retainedProgress);
  }
}
