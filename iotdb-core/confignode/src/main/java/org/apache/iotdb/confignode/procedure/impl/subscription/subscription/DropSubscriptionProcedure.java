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
import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.AlterTopicProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
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
import java.util.Set;

// TODO: check if lock is properly acquired
// TODO: check if it also needs meta sync to keep CN and DN in sync
// TODO: fixme according to CreateSubscriptionProcedure
public class DropSubscriptionProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropSubscriptionProcedure.class);

  private TUnsubscribeReq unsubscribeReq;

  private AlterConsumerGroupProcedure alterConsumerGroupProcedure;
  private List<AlterTopicProcedure> alterTopicProcedures = new ArrayList<>();
  private List<AbstractOperatePipeProcedureV2> dropPipeProcedures = new ArrayList<>();

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
  protected void executeFromValidate(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropSubscriptionProcedure: executeFromValidate");

    subscriptionInfo.get().validateBeforeUnsubscribe(unsubscribeReq);

    // Construct AlterConsumerGroupProcedure
    ConsumerGroupMeta updatedConsumerGroupMeta =
        subscriptionInfo.get().getConsumerGroupMeta(unsubscribeReq.getConsumerGroupId()).deepCopy();

    // Get topics subscribed by no consumers in this group after this un-subscription
    Set<String> topicsUnsubByGroup =
        updatedConsumerGroupMeta.removeSubscription(
            unsubscribeReq.getConsumerId(), unsubscribeReq.getTopicNames());
    alterConsumerGroupProcedure = new AlterConsumerGroupProcedure(updatedConsumerGroupMeta);

    for (String topic : unsubscribeReq.getTopicNames()) {
      if (topicsUnsubByGroup.contains(topic)) {
        // Topic will be subscribed by no consumers in this group

        TopicMeta updatedTopicMeta = subscriptionInfo.get().getTopicMeta(topic).deepCopy();
        updatedTopicMeta.removeSubscribedConsumerGroup(unsubscribeReq.getConsumerGroupId());

        alterTopicProcedures.add(new AlterTopicProcedure(updatedTopicMeta));
        dropPipeProcedures.add(
            new DropPipeProcedureV2(topic + "_" + unsubscribeReq.getConsumerGroupId()));
      }
    }
  }

  @Override
  protected void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropSubscriptionProcedure: executeFromOperateOnConfigNodes");

    int topicCount = alterTopicProcedures.size();
    for (int i = 0; i < topicCount; ++i) {
      dropPipeProcedures.get(i).executeFromValidateTask(env);
    }

    for (int i = 0; i < topicCount; ++i) {
      dropPipeProcedures.get(i).executeFromCalculateInfoForTask(env);
    }

    alterConsumerGroupProcedure.executeFromOperateOnConfigNodes(env);

    for (int i = 0; i < topicCount; ++i) {
      alterTopicProcedures.get(i).executeFromOperateOnConfigNodes(env);
      dropPipeProcedures.get(i).executeFromWriteConfigNodeConsensus(env);
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropSubscriptionProcedure: executeFromOperateOnDataNodes");

    alterConsumerGroupProcedure.executeFromOperateOnDataNodes(env);

    int topicCount = alterTopicProcedures.size();
    for (int i = 0; i < topicCount; ++i) {
      alterTopicProcedures.get(i).executeFromOperateOnDataNodes(env);
      try {
        dropPipeProcedures.get(i).executeFromOperateOnDataNodes(env);
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to stop pipe task for subscription on datanode, because {}", e.getMessage());
        throw new PipeException(e.getMessage());
      }
    }
  }

  @Override
  protected void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromLock");
  }

  @Override
  protected void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromOperateOnConfigNodes");

    int topicCount = alterTopicProcedures.size();
    for (int i = topicCount - 1; i >= 0; --i) {
      dropPipeProcedures.get(i).rollbackFromWriteConfigNodeConsensus(env);
      alterTopicProcedures.get(i).rollbackFromOperateOnConfigNodes(env);
    }

    alterConsumerGroupProcedure.rollbackFromOperateOnConfigNodes(env);

    for (int i = topicCount - 1; i >= 0; --i) {
      dropPipeProcedures.get(i).executeFromCalculateInfoForTask(env);
    }

    for (int i = topicCount - 1; i >= 0; --i) {
      dropPipeProcedures.get(i).executeFromValidateTask(env);
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropSubscriptionProcedure: rollbackFromOperateOnDataNodes");

    int topicCount = alterTopicProcedures.size();
    for (int i = topicCount - 1; i >= 0; --i) {
      try {
        dropPipeProcedures.get(i).rollbackFromOperateOnDataNodes(env);
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to roll back stop pipe task for subscription on datanode, because {}",
            e.getMessage());
        throw new PipeException(e.getMessage());
      }
      alterTopicProcedures.get(i).rollbackFromOperateOnDataNodes(env);
    }

    alterConsumerGroupProcedure.rollbackFromOperateOnDataNodes(env);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_SUBSCRIPTION_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(unsubscribeReq.getConsumerId(), stream);
    ReadWriteIOUtils.write(unsubscribeReq.getConsumerGroupId(), stream);
    ReadWriteIOUtils.write(unsubscribeReq.getTopicNamesSize(), stream);
    for (String topicName : unsubscribeReq.getTopicNames()) {
      ReadWriteIOUtils.write(topicName, stream);
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
    return this.unsubscribeReq.getConsumerId().equals(that.unsubscribeReq.getConsumerId())
        && this.unsubscribeReq.getConsumerGroupId().equals(that.unsubscribeReq.getConsumerGroupId())
        && this.unsubscribeReq.getTopicNames().equals(that.unsubscribeReq.getTopicNames());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        unsubscribeReq.getConsumerId(),
        unsubscribeReq.getConsumerGroupId(),
        unsubscribeReq.getTopicNames());
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
  public void setDropPipeProcedures(List<AbstractOperatePipeProcedureV2> dropPipeProcedures) {
    this.dropPipeProcedures = dropPipeProcedures;
  }

  @TestOnly
  public List<AbstractOperatePipeProcedureV2> getDropPipeProcedures() {
    return this.dropPipeProcedures;
  }
}
