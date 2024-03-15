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
import org.apache.iotdb.pipe.api.exception.PipeException;
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

public class CreateSubscriptionProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateSubscriptionProcedure.class);

  private TSubscribeReq subscribeReq;

  private AlterConsumerGroupProcedure consumerGroupProcedure;
  private List<AlterTopicProcedure> topicProcedures = new ArrayList<>();
  private List<AbstractOperatePipeProcedureV2> pipeProcedures = new ArrayList<>();

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
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromOperateOnConfigNodes");

    int topicCount = topicProcedures.size();
    for (int i = 0; i < topicCount; ++i) {
      pipeProcedures.get(i).executeFromValidateTask(env);
    }

    for (int i = 0; i < topicCount; ++i) {
      pipeProcedures.get(i).executeFromCalculateInfoForTask(env);
    }

    consumerGroupProcedure.executeFromOperateOnConfigNodes(env);

    for (int i = 0; i < topicCount; ++i) {
      topicProcedures.get(i).executeFromOperateOnConfigNodes(env);
      pipeProcedures.get(i).executeFromWriteConfigNodeConsensus(env);
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromOperateOnDataNodes");

    consumerGroupProcedure.executeFromOperateOnDataNodes(env);

    int topicCount = topicProcedures.size();
    for (int i = 0; i < topicCount; ++i) {
      topicProcedures.get(i).executeFromOperateOnDataNodes(env);
      try {
        pipeProcedures.get(i).executeFromOperateOnDataNodes(env);
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to create or start pipe task for subscription on datanode, because {}",
            e.getMessage());
        throw new PipeException(e.getMessage());
      }
    }
  }

  @Override
  protected void executeFromValidate(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("CreateSubscriptionProcedure: executeFromValidate");

    // check if the consumer and all topics exists
    try {
      subscriptionInfo.get().validateBeforeSubscribe(subscribeReq);
    } catch (PipeException e) {
      LOGGER.error(
          "CreateSubscriptionProcedure: executeFromValidate, validateBeforeSubscribe failed", e);
      throw e;
    }

    // Construct AlterConsumerGroupProcedure
    ConsumerGroupMeta updatedConsumerGroupMeta =
        subscriptionInfo.get().getConsumerGroupMeta(subscribeReq.getConsumerGroupId()).copy();
    updatedConsumerGroupMeta.addSubscription(
        subscribeReq.getConsumerId(), subscribeReq.getTopicNames());
    consumerGroupProcedure = new AlterConsumerGroupProcedure(updatedConsumerGroupMeta);

    for (String topic : subscribeReq.getTopicNames()) {
      // Construct AlterTopicProcedures
      TopicMeta updatedTopicMeta = subscriptionInfo.get().getTopicMeta(topic).deepCopy();

      // Construct pipe procedures
      if (updatedTopicMeta.addSubscribedConsumerGroup(subscribeReq.getConsumerGroupId())) {
        // Consumer group not subscribed this topic
        // TODO: now all configs are put to extractor attributes. need to put to processor and
        // connector attributes correctly.
        pipeProcedures.add(
            new CreatePipeProcedureV2(
                new TCreatePipeReq()
                    .setPipeName(topic + "_" + subscribeReq.getConsumerGroupId())
                    .setExtractorAttributes(updatedTopicMeta.getConfig().getAttribute())));

        topicProcedures.add(new AlterTopicProcedure(updatedTopicMeta));
      } else {
        // Consumer group already subscribed this topic
        pipeProcedures.add(
            new StartPipeProcedureV2(topic + "_" + subscribeReq.getConsumerGroupId()));

        topicProcedures.add(null);
      }
    }
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromOperateOnConfigNodes");

    int topicCount = topicProcedures.size();
    for (int i = topicCount - 1; i >= 0; --i) {
      pipeProcedures.get(i).rollbackFromWriteConfigNodeConsensus(env);
      if (topicProcedures.get(i) != null) {
        topicProcedures.get(i).rollbackFromOperateOnConfigNodes(env);
      }
    }

    consumerGroupProcedure.rollbackFromOperateOnConfigNodes(env);

    for (int i = topicCount - 1; i >= 0; --i) {
      pipeProcedures.get(i).executeFromCalculateInfoForTask(env);
    }

    for (int i = topicCount - 1; i >= 0; --i) {
      pipeProcedures.get(i).executeFromValidateTask(env);
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromOperateOnDataNodes");

    int topicCount = topicProcedures.size();
    for (int i = topicCount - 1; i >= 0; --i) {
      try {
        pipeProcedures.get(i).rollbackFromOperateOnDataNodes(env);
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to roll back create or start pipe task for subscription on datanode, because {}",
            e.getMessage());
        throw new PipeException(e.getMessage());
      }

      if (topicProcedures.get(i) != null) {
        topicProcedures.get(i).rollbackFromOperateOnDataNodes(env);
      }
    }

    consumerGroupProcedure.rollbackFromOperateOnDataNodes(env);
  }

  @Override
  protected void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreateSubscriptionProcedure: rollbackFromValidate");
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_SUBSCRIPTION_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(subscribeReq.getConsumerId(), stream);
    ReadWriteIOUtils.write(subscribeReq.getConsumerGroupId(), stream);
    ReadWriteIOUtils.write(subscribeReq.getTopicNamesSize(), stream);
    for (String topicName : subscribeReq.getTopicNames()) {
      ReadWriteIOUtils.write(topicName, stream);
    }

    // serialize consumerGroupProcedure
    if (consumerGroupProcedure != null) {
      ReadWriteIOUtils.write(true, stream);
      consumerGroupProcedure.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    // serialize topic procedures
    if (topicProcedures != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(topicProcedures.size(), stream);
      for (AlterTopicProcedure topicProcedure : topicProcedures) {
        // Topic procedure may be null
        if (topicProcedure != null) {
          ReadWriteIOUtils.write(true, stream);
          topicProcedure.serialize(stream);
        } else {
          ReadWriteIOUtils.write(false, stream);
        }
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    // serialize pipe procedures
    if (pipeProcedures != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(pipeProcedures.size(), stream);
      for (AbstractOperatePipeProcedureV2 pipeProcedure : pipeProcedures) {
        // Pipe procedure can't be null
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

      consumerGroupProcedure = new AlterConsumerGroupProcedure();
      consumerGroupProcedure.deserialize(byteBuffer);
    }

    // deserialize topic procedures
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; ++i) {
        if (ReadWriteIOUtils.readBool(byteBuffer)) {
          // This readShort should return ALTER_TOPIC_PROCEDURE, and we ignore it.
          ReadWriteIOUtils.readShort(byteBuffer);

          AlterTopicProcedure topicProcedure = new AlterTopicProcedure();
          topicProcedure.deserialize(byteBuffer);
          topicProcedures.add(topicProcedure);
        } else {
          topicProcedures.add(null);
        }
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
          pipeProcedures.add(createPipeProcedureV2);
        } else if (typeCode == ProcedureType.START_PIPE_PROCEDURE_V2.getTypeCode()) {
          StartPipeProcedureV2 startPipeProcedureV2 = new StartPipeProcedureV2();
          startPipeProcedureV2.deserialize(byteBuffer);
          pipeProcedures.add(startPipeProcedureV2);
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
    return this.subscribeReq.getConsumerId().equals(that.subscribeReq.getConsumerId())
        && this.subscribeReq.getConsumerGroupId().equals(that.subscribeReq.getConsumerGroupId())
        && this.subscribeReq.getTopicNames().equals(that.subscribeReq.getTopicNames());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        subscribeReq.getConsumerId(),
        subscribeReq.getConsumerGroupId(),
        subscribeReq.getTopicNames());
  }

  @TestOnly
  public void setConsumerGroupProcedure(AlterConsumerGroupProcedure consumerGroupProcedure) {
    this.consumerGroupProcedure = consumerGroupProcedure;
  }

  @TestOnly
  public AlterConsumerGroupProcedure getConsumerGroupProcedure() {
    return this.consumerGroupProcedure;
  }

  @TestOnly
  public void setTopicProcedures(List<AlterTopicProcedure> topicProcedures) {
    this.topicProcedures = topicProcedures;
  }

  @TestOnly
  public List<AlterTopicProcedure> getTopicProcedures() {
    return this.topicProcedures;
  }

  @TestOnly
  public void setPipeProcedures(List<AbstractOperatePipeProcedureV2> pipeProcedures) {
    this.pipeProcedures = pipeProcedures;
  }

  @TestOnly
  public List<AbstractOperatePipeProcedureV2> getPipeProcedures() {
    return this.pipeProcedures;
  }
}
