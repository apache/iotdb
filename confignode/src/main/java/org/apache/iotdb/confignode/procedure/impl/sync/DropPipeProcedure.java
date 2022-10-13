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
package org.apache.iotdb.confignode.procedure.impl.sync;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.sync.OperatePipeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

// TODO(sync): drop logic need to be updated
public class DropPipeProcedure extends AbstractOperatePipeProcedure {
  private static final Logger LOGGER = LoggerFactory.getLogger(DropPipeProcedure.class);

  private String pipeName;

  public DropPipeProcedure() {
    super();
  }

  public DropPipeProcedure(String pipeName) throws PipeException {
    super();
    this.pipeName = pipeName;
  }

  @Override
  boolean executeCheckCanSkip(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("Start to drop PIPE [{}]", pipeName);
    PipeInfo pipeInfo = env.getConfigManager().getSyncManager().getPipeInfo(pipeName);
    return pipeInfo.getStatus().equals(PipeStatus.DROP);
  }

  @Override
  void executePreOperatePipeOnConfigNode(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("Start to pre-drop PIPE [{}] on Config Nodes", pipeName);
    TSStatus status =
        env.getConfigManager().getSyncManager().setPipeStatus(pipeName, PipeStatus.PREPARE_DROP);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.getMessage());
    }
  }

  @Override
  void executeOperatePipeOnDataNode(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("Start to broadcast drop PIPE [{}] on Data Nodes", pipeName);
    TSStatus status =
        RpcUtils.squashResponseStatusList(
            env.getConfigManager()
                .getSyncManager()
                .operatePipeOnDataNodes(pipeName, SyncOperation.DROP_PIPE));
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Fail to drop PIPE [%s] on Data Nodes because %s", pipeName, status.getMessage()));
    }
  }

  @Override
  void executeOperatePipeOnConfigNode(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("Start to drop PIPE [{}] on Config Nodes", pipeName);
    TSStatus status =
        env.getConfigManager().getSyncManager().setPipeStatus(pipeName, PipeStatus.DROP);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.getMessage());
    }
  }

  @Override
  SyncOperation getOperation() {
    return SyncOperation.DROP_PIPE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, OperatePipeState state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.DROP_PIPE_PROCEDURE.ordinal());
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DropPipeProcedure that = (DropPipeProcedure) o;
    return Objects.equals(pipeName, that.pipeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeName);
  }
}
