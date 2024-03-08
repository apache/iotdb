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

package org.apache.iotdb.confignode.procedure.impl.pipe.mq.consumer;

import org.apache.iotdb.confignode.manager.pipe.mq.coordinator.PipeMQCoordinator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class DropPipeMQConsumerProcedure extends AlterPipeMQConsumerGroupProcedure {
  private TCloseConsumerReq dropConsumerReq;

  public DropPipeMQConsumerProcedure(TCloseConsumerReq dropConsumerReq) throws PipeException {
    super();
    this.dropConsumerReq = dropConsumerReq;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.DROP_CONSUMER;
  }

  @Override
  public void validateAndGetOldAndNewMeta(
      ConfigNodeProcedureEnv env, PipeMQCoordinator pipeMQCoordinator) {
    try {
      pipeMQCoordinator.getPipeMQInfo().validateBeforeDroppingConsumer(dropConsumerReq);
    } catch (PipeException e) {
      // The consumer does not exist, we should end the procedure
      LOGGER.warn(
          "Consumer {} in consumer group {} does not exist, end the DropPipeMQConsumerProcedure",
          dropConsumerReq.getConsumerId(),
          dropConsumerReq.getConsumerGroupId());
      setFailure(new ProcedureException(e.getMessage()));
      pipeMQCoordinator.unlock();
      throw e;
    }

    existingPipeMQConsumerGroupMeta =
        pipeMQCoordinator
            .getPipeMQInfo()
            .getPipeMQConsumerGroupMeta(dropConsumerReq.getConsumerGroupId());

    updatedPipeMQConsumerGroupMeta = existingPipeMQConsumerGroupMeta.copy();
    updatedPipeMQConsumerGroupMeta.removeConsumer(dropConsumerReq.getConsumerId());
  }
}
