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
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

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
    // throw PipeNotExistException if pipe not exist
    PipeInfo pipeInfo = env.getConfigManager().getSyncManager().getPipeInfo(pipeName);
    return false;
  }

  @Override
  void executePreOperatePipeOnConfigNode(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("Start to pre-drop PIPE [{}] on Config Nodes", pipeName);
    TSStatus status =
        env.getConfigManager().getSyncManager().setPipeStatus(pipeName, PipeStatus.DROP);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.getMessage());
    }
  }

  @Override
  void executeOperatePipeOnDataNode(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("Start to broadcast drop PIPE [{}] on Data Nodes", pipeName);
    Map<Integer, TSStatus> responseMap =
        env.getConfigManager()
            .getSyncManager()
            .operatePipeOnDataNodes(pipeName, SyncOperation.DROP_PIPE);
    TSStatus status = RpcUtils.squashResponseStatusList(new ArrayList<>(responseMap.values()));
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Fail to drop PIPE [%s] because %s. Please execute [DROP PIPE %s] later to retry.",
              pipeName,
              StringUtils.join(
                  responseMap.values().stream()
                      .filter(i -> i.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
                      .map(TSStatus::getMessage)
                      .toArray(),
                  ", "),
              pipeName));
    }
  }

  @Override
  void executeOperatePipeOnConfigNode(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("Start to drop PIPE [{}] on Config Nodes", pipeName);
    TSStatus status = env.getConfigManager().getSyncManager().dropPipe(pipeName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.getMessage());
    }
  }

  @Override
  SyncOperation getOperation() {
    return SyncOperation.DROP_PIPE;
  }

  @Override
  protected boolean isRollbackSupported(OperatePipeState state) {
    return state == OperatePipeState.OPERATE_CHECK;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, OperatePipeState state)
      throws IOException, InterruptedException, ProcedureException {
    LOGGER.info("Roll back DropPipeProcedure at STATE [{}]", state);
    if (state == OperatePipeState.OPERATE_CHECK) {
      env.getConfigManager().getSyncManager().unlockSyncMetadata();
    } else {
      LOGGER.error("Unsupported roll back STATE [{}]", state);
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_PIPE_PROCEDURE.getTypeCode());
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
