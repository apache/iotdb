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

import org.apache.iotdb.commons.exception.SubscriptionException;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.StartPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.AlterTopicProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

// TODO: check if lock is properly acquired
// TODO: check if it also needs meta sync to keep CN and DN in sync
public class CreateSubscriptionProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateSubscriptionProcedure.class);

  private TSubscribeReq subscribeReq;

  private AlterConsumerGroupProcedure alterConsumerGroupProcedure;
  private List<AlterTopicProcedure> alterTopicProcedures = new ArrayList<>();
  private List<AbstractOperatePipeProcedureV2> createPipeProcedures = new ArrayList<>();

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
  protected void unlockPipeProcedure() {
    for (AbstractOperatePipeProcedureV2 createPipeProcedure : createPipeProcedures) {
      createPipeProcedure.unsetPipeTaskInfo();
    }
  }

  @Override
  protected void executeFromValidate(ConfigNodeProcedureEnv env) throws SubscriptionException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromValidate");

    subscriptionInfo.get().validateBeforeSubscribe(subscribeReq);

    // alterConsumerGroupProcedure
    final ConsumerGroupMeta updatedConsumerGroupMeta =
        // TODO: fixme deepCopy() is not locked by the subscription procedure lock
        subscriptionInfo.get().getConsumerGroupMeta(subscribeReq.getConsumerGroupId()).deepCopy();
    updatedConsumerGroupMeta.addSubscription(
        subscribeReq.getConsumerId(), subscribeReq.getTopicNames());
    alterConsumerGroupProcedure = new AlterConsumerGroupProcedure(updatedConsumerGroupMeta);

    // alterTopicProcedures & createPipeProcedures
    for (String topic : subscribeReq.getTopicNames()) {
      // TODO: fixme deepCopy() is not locked by the subscription procedure lock
      TopicMeta updatedTopicMeta = subscriptionInfo.get().getTopicMeta(topic).deepCopy();

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
                            subscribeReq.getConsumerGroupId()))));

        alterTopicProcedures.add(new AlterTopicProcedure(updatedTopicMeta));
      }
    }

    for (int i = 0, topicCount = alterTopicProcedures.size(); i < topicCount; ++i) {
      // TODO: temporary fix
      createPipeProcedures.get(i).setPipeTaskInfo(pipeTaskInfo);
      createPipeProcedures.get(i).executeFromValidateTask(env);
      createPipeProcedures.get(i).executeFromCalculateInfoForTask(env);
    }
  }

  // TODO: record execution result for each procedure and only rollback the executed ones
  // TODO: check periodically if the subscription is still valid but no working pipe?
  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromOperateOnConfigNodes");

    alterConsumerGroupProcedure.executeFromOperateOnConfigNodes(env);

    for (AlterTopicProcedure alterTopicProcedure : alterTopicProcedures) {
      alterTopicProcedure.executeFromOperateOnConfigNodes(env);
    }

    for (AbstractOperatePipeProcedureV2 createPipeProcedure : createPipeProcedures) {
      createPipeProcedure.executeFromWriteConfigNodeConsensus(env);
    }
  }

  // TODO: record execution result for each procedure and only rollback the executed ones
  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws SubscriptionException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromOperateOnDataNodes");

    alterConsumerGroupProcedure.executeFromOperateOnDataNodes(env);

    for (AlterTopicProcedure alterTopicProcedure : alterTopicProcedures) {
      alterTopicProcedure.executeFromOperateOnDataNodes(env);
    }

    try {
      for (AbstractOperatePipeProcedureV2 createPipeProcedure : createPipeProcedures) {
        createPipeProcedure.executeFromOperateOnDataNodes(env);
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to create or start pipe task for subscription on datanode, because {}",
          e.getMessage());
      throw new SubscriptionException(
          "Failed to create or start pipe task for subscription on datanode, because "
              + e.getMessage());
    }
  }

  @Override
  protected void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromValidate");
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromOperateOnConfigNodes");

    alterConsumerGroupProcedure.rollbackFromOperateOnConfigNodes(env);

    for (AlterTopicProcedure alterTopicProcedure : alterTopicProcedures) {
      alterTopicProcedure.rollbackFromOperateOnConfigNodes(env);
    }

    for (AbstractOperatePipeProcedureV2 createPipeProcedure : createPipeProcedures) {
      createPipeProcedure.rollbackFromWriteConfigNodeConsensus(env);
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromOperateOnDataNodes");

    alterConsumerGroupProcedure.rollbackFromOperateOnDataNodes(env);

    for (AlterTopicProcedure alterTopicProcedure : alterTopicProcedures) {
      alterTopicProcedure.rollbackFromOperateOnDataNodes(env);
    }

    try {
      for (AbstractOperatePipeProcedureV2 createPipeProcedure : createPipeProcedures) {
        createPipeProcedure.rollbackFromOperateOnDataNodes(env);
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to rollback create or start pipe task for subscription on datanode, because {}",
          e.getMessage());
      throw new SubscriptionException(
          "Failed to rollback create or start pipe task for subscription on datanode, because "
              + e.getMessage());
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
      for (AbstractOperatePipeProcedureV2 pipeProcedure : createPipeProcedures) {
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
          // TODO: check if we actually need it?
        } else if (typeCode == ProcedureType.START_PIPE_PROCEDURE_V2.getTypeCode()) {
          StartPipeProcedureV2 startPipeProcedureV2 = new StartPipeProcedureV2();
          startPipeProcedureV2.deserialize(byteBuffer);
          createPipeProcedures.add(startPipeProcedureV2);
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
  public void setCreatePipeProcedures(List<AbstractOperatePipeProcedureV2> createPipeProcedures) {
    this.createPipeProcedures = createPipeProcedures;
  }

  @TestOnly
  public List<AbstractOperatePipeProcedureV2> getCreatePipeProcedures() {
    return this.createPipeProcedures;
  }
}
