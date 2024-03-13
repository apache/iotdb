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

import org.apache.iotdb.commons.subscription.meta.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.ConsumerMeta;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.pipe.api.exception.PipeException;

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
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.CREATE_CONSUMER;
  }

  @Override
  public void validateAndGetOldAndNewMeta(ConfigNodeProcedureEnv env) {
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
      updatedConsumerGroupMeta = existingConsumerGroupMeta.copy();
      updatedConsumerGroupMeta.addConsumer(newConsumerMeta);
    }
  }
}
