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
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.impl.subscription.subscription.AbstractOperateSubscriptionAndPipeProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class AlterTopicProcedure extends AbstractOperateSubscriptionAndPipeProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlterTopicProcedure.class);

  private TopicMeta updatedTopicMeta;

  private TopicMeta existedTopicMeta;

  // Non-null only for client ALTER TOPIC requests. These are merged again after the procedure has
  // acquired the subscription lock, so a request cannot overwrite attributes committed after its
  // initial TopicMeta snapshot was built.
  private Map<String, String> updatedTopicAttributes;

  private final List<ConsumerGroupMeta> retainedConsumerGroupMetas = new ArrayList<>();
  private final List<DropPipeProcedureV2> retainedPipeProcedures = new ArrayList<>();
  private Map<String, ByteBuffer> retainedProgress = new HashMap<>();
  private boolean serializeRetainedProgressState;

  public AlterTopicProcedure() {
    super();
  }

  public AlterTopicProcedure(final boolean shouldMergeUpdatedTopicAttributes) {
    super();
    if (shouldMergeUpdatedTopicAttributes) {
      updatedTopicAttributes = new HashMap<>();
    }
  }

  public AlterTopicProcedure(
      final boolean shouldMergeUpdatedTopicAttributes,
      final boolean serializeRetainedProgressState) {
    this(shouldMergeUpdatedTopicAttributes);
    this.serializeRetainedProgressState = serializeRetainedProgressState;
  }

  public AlterTopicProcedure(TopicMeta updatedTopicMeta) {
    super();
    this.updatedTopicMeta = updatedTopicMeta;
  }

  public AlterTopicProcedure(
      TopicMeta updatedTopicMeta, Map<String, String> updatedTopicAttributes) {
    super();
    this.updatedTopicMeta = updatedTopicMeta;
    this.updatedTopicAttributes =
        Objects.isNull(updatedTopicAttributes) ? null : new HashMap<>(updatedTopicAttributes);
  }

  /** This is only used when the subscription info lock is held by another procedure. */
  public AlterTopicProcedure(
      TopicMeta updatedTopicMeta, AtomicReference<SubscriptionInfo> subscriptionInfo) {
    super();
    this.updatedTopicMeta = updatedTopicMeta;
    this.subscriptionInfo = subscriptionInfo;
  }

  /** This is only used when the subscription info lock is held by another procedure. */
  public AlterTopicProcedure(
      TopicMeta updatedTopicMeta,
      Map<String, String> updatedTopicAttributes,
      AtomicReference<SubscriptionInfo> subscriptionInfo) {
    this(updatedTopicMeta, updatedTopicAttributes);
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

  public boolean shouldMergeUpdatedTopicAttributes() {
    return Objects.nonNull(updatedTopicAttributes);
  }

  public boolean shouldSerializeRetainedProgressState() {
    return serializeRetainedProgressState
        || !retainedConsumerGroupMetas.isEmpty()
        || !retainedPipeProcedures.isEmpty();
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.ALTER_TOPIC;
  }

  @Override
  public boolean executeFromValidate(ConfigNodeProcedureEnv env) throws SubscriptionException {
    LOGGER.info(ProcedureMessages.ALTERTOPICPROCEDURE_EXECUTEFROMVALIDATE);

    retainedConsumerGroupMetas.clear();
    retainedPipeProcedures.clear();
    retainedProgress.clear();

    existedTopicMeta =
        subscriptionInfo
            .get()
            .deepCopyTopicMeta(
                updatedTopicMeta.getTopicName(), updatedTopicMeta.visibleUnderTableModel());
    if (Objects.nonNull(updatedTopicAttributes) && Objects.nonNull(existedTopicMeta)) {
      subscriptionInfo.get().validateUpdatedTopicAttributes(updatedTopicAttributes);
      updatedTopicMeta = existedTopicMeta.deepCopyWithUpdatedAttributes(updatedTopicAttributes);
    }

    subscriptionInfo.get().validateBeforeAlteringTopic(updatedTopicMeta);

    if (Objects.nonNull(existedTopicMeta)
        && existedTopicMeta.getConfig().isProgressRetainedAfterUnsubscribe()
        && !updatedTopicMeta.getConfig().isProgressRetainedAfterUnsubscribe()) {
      prepareRetainedProgressCleanup(env);
    }

    return true;
  }

  private void prepareRetainedProgressCleanup(final ConfigNodeProcedureEnv env) {
    final String topicName = updatedTopicMeta.getTopicName();
    final boolean isTableModel = updatedTopicMeta.visibleUnderTableModel();
    for (final ConsumerGroupMeta consumerGroupMeta :
        subscriptionInfo.get().getConsumerGroupsRetainingTopic(topicName, isTableModel)) {
      retainedConsumerGroupMetas.add(consumerGroupMeta);
      retainedProgress.putAll(
          subscriptionInfo
              .get()
              .getCommitProgressKeeper()
              .getTopicProgress(consumerGroupMeta.getConsumerGroupId(), topicName));
      final String pipeName =
          PipeStaticMeta.generateSubscriptionPipeName(
              topicName, consumerGroupMeta.getConsumerGroupId());
      if (pipeTaskInfo.get().isPipeExisted(pipeName, isTableModel)) {
        final DropPipeProcedureV2 dropPipeProcedure =
            new DropPipeProcedureV2(pipeName, isTableModel, pipeTaskInfo);
        dropPipeProcedure.executeFromValidateTask(env);
        dropPipeProcedure.executeFromCalculateInfoForTask(env);
        retainedPipeProcedures.add(dropPipeProcedure);
      }
    }
  }

  @Override
  public void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info(
        ProcedureMessages.ALTERTOPICPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES_TRY_TO_ALTER_TOPIC);

    if (!retainedPipeProcedures.isEmpty()) {
      final List<ConfigPhysicalPlan> dropPipePlans =
          retainedPipeProcedures.stream()
              .map(
                  procedure ->
                      new DropPipePlanV2(
                          procedure.getPipeName(), updatedTopicMeta.visibleUnderTableModel()))
              .collect(Collectors.toList());
      writeConsensusPlan(env, new OperateMultiplePipesPlanV2(dropPipePlans));
    }
    for (final ConsumerGroupMeta consumerGroupMeta : getUpdatedRetainedConsumerGroupMetas()) {
      writeConsensusPlan(env, new AlterConsumerGroupPlan(consumerGroupMeta));
    }
    writeAlterTopicPlan(env, updatedTopicMeta);
  }

  private void writeAlterTopicPlan(final ConfigNodeProcedureEnv env, final TopicMeta topicMeta)
      throws SubscriptionException {
    writeConsensusPlan(env, new AlterTopicPlan(topicMeta));
  }

  private void writeConsensusPlan(final ConfigNodeProcedureEnv env, final ConfigPhysicalPlan plan)
      throws SubscriptionException {
    final TSStatus response;
    try {
      response = env.getConfigManager().getConsensusManager().write(plan);
    } catch (ConsensusException e) {
      LOGGER.warn(ConfigNodeMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
      throw new SubscriptionException(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new SubscriptionException(
          String.format(
              ProcedureMessages.FAILED_TO_ALTER_TOPIC_ON_CONFIG_NODES_BECAUSE,
              existedTopicMeta,
              updatedTopicMeta,
              response));
    }
  }

  private List<ConsumerGroupMeta> getUpdatedRetainedConsumerGroupMetas() {
    final List<ConsumerGroupMeta> updatedMetas = new ArrayList<>();
    for (final ConsumerGroupMeta existingMeta : retainedConsumerGroupMetas) {
      final ConsumerGroupMeta updatedMeta = existingMeta.deepCopy();
      updatedMeta.removeRetainedTopic(updatedTopicMeta.getTopicName());
      updatedMetas.add(updatedMeta);
    }
    return updatedMetas;
  }

  @Override
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info(
        ProcedureMessages.ALTERTOPICPROCEDURE_EXECUTEFROMOPERATEONDATANODES,
        updatedTopicMeta.getTopicName());

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
          env.pushSingleConsumerGroupOnDataNode(consumerGroupMeta.serialize()).values().stream()
              .map(TPushConsumerGroupMetaResp::getStatus)
              .toList();
      if (RpcUtils.squashResponseStatusList(consumerGroupStatuses).getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new SubscriptionException(consumerGroupStatuses.toString());
      }
    }

    final List<TSStatus> statuses = env.pushSingleTopicOnDataNode(updatedTopicMeta.serialize());
    if (RpcUtils.squashResponseStatusList(statuses).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format(
              ProcedureMessages.FAILED_TO_ALTER_TOPIC_ON_DATA_NODES_BECAUSE,
              existedTopicMeta,
              updatedTopicMeta,
              statuses));
    }
  }

  @Override
  public void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        ProcedureMessages.ALTERTOPICPROCEDURE_ROLLBACKFROMVALIDATE,
        updatedTopicMeta.getTopicName());
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info(
        ProcedureMessages.ALTERTOPICPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES,
        updatedTopicMeta.getTopicName());

    writeAlterTopicPlan(env, existedTopicMeta);
    if (!retainedPipeProcedures.isEmpty()) {
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
    }
    for (final ConsumerGroupMeta consumerGroupMeta : retainedConsumerGroupMetas) {
      writeConsensusPlan(env, new AlterConsumerGroupPlan(consumerGroupMeta));
    }
    restoreRetainedProgress(env);
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
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException, IOException {
    LOGGER.info(
        ProcedureMessages.ALTERTOPICPROCEDURE_ROLLBACKFROMOPERATEONDATANODES,
        updatedTopicMeta.getTopicName());

    for (final ConsumerGroupMeta consumerGroupMeta : retainedConsumerGroupMetas) {
      final List<TSStatus> consumerGroupStatuses =
          env.pushSingleConsumerGroupOnDataNode(consumerGroupMeta.serialize()).values().stream()
              .map(TPushConsumerGroupMetaResp::getStatus)
              .toList();
      if (RpcUtils.squashResponseStatusList(consumerGroupStatuses).getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new SubscriptionException(consumerGroupStatuses.toString());
      }
    }

    final List<TSStatus> statuses = env.pushSingleTopicOnDataNode(existedTopicMeta.serialize());
    if (RpcUtils.squashResponseStatusList(statuses).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // throw exception instead of logging warn, do not rely on metadata synchronization
      throw new SubscriptionException(
          String.format(
              ProcedureMessages.FAILED_TO_ROLLBACK_FROM_ALTERING_TOPIC_ON_DATA_NODES_BECAUSE,
              updatedTopicMeta,
              existedTopicMeta,
              statuses));
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

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(getProcedureType().getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(updatedTopicMeta != null, stream);
    if (updatedTopicMeta != null) {
      updatedTopicMeta.serialize(stream);
    }

    ReadWriteIOUtils.write(existedTopicMeta != null, stream);
    if (existedTopicMeta != null) {
      existedTopicMeta.serialize(stream);
    }

    if (shouldMergeUpdatedTopicAttributes()) {
      ReadWriteIOUtils.write(updatedTopicAttributes, stream);
    }
    if (shouldSerializeRetainedProgressState()) {
      ReadWriteIOUtils.write(retainedConsumerGroupMetas.size(), stream);
      for (final ConsumerGroupMeta consumerGroupMeta : retainedConsumerGroupMetas) {
        consumerGroupMeta.serialize(stream);
      }
      ReadWriteIOUtils.write(retainedPipeProcedures.size(), stream);
      for (final DropPipeProcedureV2 pipeProcedure : retainedPipeProcedures) {
        pipeProcedure.serialize(stream);
      }
      CommitProgressKeeper.serializeRegionProgressMapToStream(retainedProgress, stream);
    }
  }

  private ProcedureType getProcedureType() {
    if (shouldSerializeRetainedProgressState()) {
      return shouldMergeUpdatedTopicAttributes()
          ? ProcedureType.ALTER_TOPIC_WITH_ATTRIBUTES_AND_RETAINED_PROGRESS_PROCEDURE
          : ProcedureType.ALTER_TOPIC_WITH_RETAINED_PROGRESS_PROCEDURE;
    }
    return shouldMergeUpdatedTopicAttributes()
        ? ProcedureType.ALTER_TOPIC_WITH_ATTRIBUTES_PROCEDURE
        : ProcedureType.ALTER_TOPIC_PROCEDURE;
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

    if (shouldMergeUpdatedTopicAttributes()) {
      updatedTopicAttributes = ReadWriteIOUtils.readMap(byteBuffer);
    }
    if (shouldSerializeRetainedProgressState()) {
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
        && Objects.equals(existedTopicMeta, that.existedTopicMeta)
        && Objects.equals(updatedTopicAttributes, that.updatedTopicAttributes)
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
        updatedTopicMeta,
        existedTopicMeta,
        updatedTopicAttributes,
        retainedConsumerGroupMetas,
        retainedPipeProcedures,
        retainedProgress);
  }
}
