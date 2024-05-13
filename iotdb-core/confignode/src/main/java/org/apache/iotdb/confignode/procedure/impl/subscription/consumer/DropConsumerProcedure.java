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

import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DropConsumerProcedure extends AlterConsumerGroupProcedure {

  private TCloseConsumerReq dropConsumerReq;

  public DropConsumerProcedure() {
    super();
  }

  public DropConsumerProcedure(TCloseConsumerReq dropConsumerReq) throws PipeException {
    super();
    this.dropConsumerReq = dropConsumerReq;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.DROP_CONSUMER;
  }

  @Override
  protected void validateAndGetOldAndNewMeta(ConfigNodeProcedureEnv env) {
    subscriptionInfo.get().validateBeforeDroppingConsumer(dropConsumerReq);

    existingConsumerGroupMeta =
        subscriptionInfo.get().getConsumerGroupMeta(dropConsumerReq.getConsumerGroupId());

    updatedConsumerGroupMeta = existingConsumerGroupMeta.deepCopy();
    updatedConsumerGroupMeta.removeConsumer(dropConsumerReq.getConsumerId());
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_CONSUMER_PROCEDURE.getTypeCode());

    super.serialize(stream);

    ReadWriteIOUtils.write(dropConsumerReq.getConsumerId(), stream);
    ReadWriteIOUtils.write(dropConsumerReq.getConsumerGroupId(), stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    // This readShort should return ALTER_CONSUMER_GROUP_PROCEDURE, and we ignore it.
    ReadWriteIOUtils.readShort(byteBuffer);

    super.deserialize(byteBuffer);

    dropConsumerReq =
        new TCloseConsumerReq()
            .setConsumerId(ReadWriteIOUtils.readString(byteBuffer))
            .setConsumerGroupId(ReadWriteIOUtils.readString(byteBuffer));
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o)
        && Objects.equals(dropConsumerReq, ((DropConsumerProcedure) o).dropConsumerReq);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), dropConsumerReq);
  }
}
