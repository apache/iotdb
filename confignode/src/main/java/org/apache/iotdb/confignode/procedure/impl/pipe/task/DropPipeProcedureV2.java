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
package org.apache.iotdb.confignode.procedure.impl.pipe.task;

import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.mpp.rpc.thrift.TOperatePipeOnDataNodeReq;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DropPipeProcedureV2 extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropPipeProcedureV2.class);

  private String pipeName;

  public DropPipeProcedureV2() {
    super();
  }

  public DropPipeProcedureV2(String pipeName) throws PipeException {
    super();
    this.pipeName = pipeName;
  }

  @Override
  PipeTaskOperation getOperation() {
    return PipeTaskOperation.DROP_PIPE;
  }

  @Override
  boolean executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeManagementException {
    LOGGER.info("DropPipeProcedureV2: executeFromValidateTask({})", pipeName);

    return env.getConfigManager()
        .getPipeManager()
        .getPipeInfo()
        .getPipeTaskInfo()
        .checkBeforeDropPipe(pipeName);
  }

  @Override
  void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeManagementException {
    LOGGER.info("DropPipeProcedureV2: executeFromCalculateInfoForTask({})", pipeName);
    // Do nothing
  }

  @Override
  void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env)
      throws PipeManagementException {
    LOGGER.info("DropPipeProcedureV2: executeFromWriteConfigNodeConsensus({})", pipeName);

    final ConsensusWriteResponse response =
        env.getConfigManager().getConsensusManager().write(new DropPipePlanV2(pipeName));
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }
  }

  @Override
  void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeManagementException {
    LOGGER.info("DropPipeProcedureV2: executeFromOperateOnDataNodes({})", pipeName);

    final TOperatePipeOnDataNodeReq request =
        new TOperatePipeOnDataNodeReq()
            .setPipeName(pipeName)
            .setOperation((byte) PipeTaskOperation.DROP_PIPE.ordinal());
    if (RpcUtils.squashResponseStatusList(env.operatePipeOnDataNodes(request)).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeManagementException(
          String.format("Failed to drop pipe instance [%s] on data nodes", pipeName));
    }
  }

  @Override
  protected void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipeProcedureV2: rollbackFromValidateTask({})", pipeName);
    // Do nothing
  }

  @Override
  protected void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipeProcedureV2: rollbackFromCalculateInfoForTask({})", pipeName);
    // Do nothing
  }

  @Override
  protected void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})", pipeName);
    // Do nothing
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipeProcedureV2: rollbackFromOperateOnDataNodes({})", pipeName);
    // Do nothing
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_PIPE_PROCEDURE_V2.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(pipeName, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    pipeName = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DropPipeProcedureV2 that = (DropPipeProcedureV2) o;
    return pipeName.equals(that.pipeName);
  }

  @Override
  public int hashCode() {
    return pipeName.hashCode();
  }
}
