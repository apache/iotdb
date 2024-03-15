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

package org.apache.iotdb.confignode.procedure.impl.subscription.consumer;

import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CreateConsumerProcedure extends AlterConsumerGroupProcedure {
  private TCreateConsumerReq createConsumerReq;

  public CreateConsumerProcedure() {
    super();
  }

  public CreateConsumerProcedure(TCreateConsumerReq createConsumerReq) throws PipeException {
    super();
    this.createConsumerReq = createConsumerReq;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.CREATE_CONSUMER;
  }

  @Override
  protected void validateAndGetOldAndNewMeta(ConfigNodeProcedureEnv env) {
    try {
      subscriptionInfo.get().validateBeforeCreatingConsumer(createConsumerReq);
    } catch (PipeException e) {
      // The consumer has already been created, we should end the procedure
      LOGGER.warn(
          "Consumer {} in consumer group {} is already created, end the CreateConsumerProcedure",
          createConsumerReq.getConsumerId(),
          createConsumerReq.getConsumerGroupId());
      setFailure(new ProcedureException(e.getMessage()));
      throw e;
    }

    existingConsumerGroupMeta =
        subscriptionInfo.get().getConsumerGroupMeta(createConsumerReq.getConsumerGroupId());

    final long createTime = System.currentTimeMillis();
    final ConsumerMeta newConsumerMeta =
        new ConsumerMeta(
            createConsumerReq.getConsumerId(),
            createTime,
            createConsumerReq.getConsumerAttributes());
    if (existingConsumerGroupMeta == null) {
      updatedConsumerGroupMeta =
          new ConsumerGroupMeta(
              createConsumerReq.getConsumerGroupId(), createTime, newConsumerMeta);
    } else {
      updatedConsumerGroupMeta = existingConsumerGroupMeta.deepCopy();
      updatedConsumerGroupMeta.addConsumer(newConsumerMeta);
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_CONSUMER_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(createConsumerReq.getConsumerId(), stream);
    ReadWriteIOUtils.write(createConsumerReq.getConsumerGroupId(), stream);
    ReadWriteIOUtils.write(createConsumerReq.getConsumerAttributesSize(), stream);
    for (Map.Entry<String, String> entry : createConsumerReq.getConsumerAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    // This readShort should return ALTER_CONSUMER_GROUP_PROCEDURE, and we ignore it.
    ReadWriteIOUtils.readShort(byteBuffer);

    super.deserialize(byteBuffer);
    createConsumerReq =
        new TCreateConsumerReq()
            .setConsumerId(ReadWriteIOUtils.readString(byteBuffer))
            .setConsumerGroupId(ReadWriteIOUtils.readString(byteBuffer))
            .setConsumerAttributes(new HashMap<>());
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      createConsumerReq
          .getConsumerAttributes()
          .put(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
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
    CreateConsumerProcedure that = (CreateConsumerProcedure) o;
    return Objects.equals(
            this.createConsumerReq.getConsumerId(), that.createConsumerReq.getConsumerId())
        && Objects.equals(
            this.createConsumerReq.getConsumerGroupId(),
            that.createConsumerReq.getConsumerGroupId())
        && Objects.equals(
            this.createConsumerReq.getConsumerAttributes(),
            that.createConsumerReq.getConsumerAttributes());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        createConsumerReq.getConsumerId(),
        createConsumerReq.getConsumerGroupId(),
        createConsumerReq.getConsumerAttributes());
  }
}
