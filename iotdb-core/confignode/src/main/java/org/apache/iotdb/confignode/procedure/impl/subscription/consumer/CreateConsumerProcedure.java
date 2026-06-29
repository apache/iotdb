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
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.utils.ReadWriteIOUtils;

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
    subscriptionInfo.get().validateBeforeCreatingConsumer(createConsumerReq);

    existingConsumerGroupMeta =
        subscriptionInfo.get().getConsumerGroupMeta(createConsumerReq.getConsumerGroupId());

    final long creationTime = System.currentTimeMillis();
    final ConsumerMeta newConsumerMeta =
        new ConsumerMeta(
            createConsumerReq.getConsumerId(),
            creationTime,
            createConsumerReq.getConsumerAttributes());

    if (Objects.isNull(existingConsumerGroupMeta)) {
      updatedConsumerGroupMeta =
          new ConsumerGroupMeta(
              createConsumerReq.getConsumerGroupId(), creationTime, newConsumerMeta);
    } else {
      existingConsumerGroupMeta.checkAuthorityBeforeJoinConsumerGroup(newConsumerMeta);
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
    final int size = createConsumerReq.getConsumerAttributes().size();
    ReadWriteIOUtils.write(size, stream);
    if (size != 0) {
      for (Map.Entry<String, String> entry : createConsumerReq.getConsumerAttributes().entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue(), stream);
      }
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
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      createConsumerReq
          .getConsumerAttributes()
          .put(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o)
        && Objects.equals(createConsumerReq, ((CreateConsumerProcedure) o).createConsumerReq);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), createConsumerReq);
  }
}
