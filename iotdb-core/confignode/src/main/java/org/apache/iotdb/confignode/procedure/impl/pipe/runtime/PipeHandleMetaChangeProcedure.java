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

package org.apache.iotdb.confignode.procedure.impl.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class PipeHandleMetaChangeProcedure extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHandleMetaChangeProcedure.class);

  private boolean needWriteConsensusOnConfigNodes = false;
  private boolean needPushPipeMetaToDataNodes = false;

  public PipeHandleMetaChangeProcedure() {
    super();
  }

  public PipeHandleMetaChangeProcedure(
      boolean needWriteConsensusOnConfigNodes, boolean needPushPipeMetaToDataNodes) {
    super();
    this.needWriteConsensusOnConfigNodes = needWriteConsensusOnConfigNodes;
    this.needPushPipeMetaToDataNodes = needPushPipeMetaToDataNodes;
  }

  @Override
  protected AtomicReference<PipeTaskInfo> acquireLockInternal(
      ConfigNodeProcedureEnv configNodeProcedureEnv) {
    return configNodeProcedureEnv
        .getConfigManager()
        .getPipeManager()
        .getPipeTaskCoordinator()
        .tryLock();
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.HANDLE_PIPE_META_CHANGE;
  }

  @Override
  public boolean executeFromValidateTask(ConfigNodeProcedureEnv env) {
    PipeLogger.log(
        LOGGER::info, ProcedureMessages.PIPEHANDLEMETACHANGEPROCEDURE_EXECUTEFROMVALIDATETASK);

    // Do nothing
    return true;
  }

  @Override
  public void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    PipeLogger.log(
        LOGGER::info,
        ProcedureMessages.PIPEHANDLEMETACHANGEPROCEDURE_EXECUTEFROMCALCULATEINFOFORTASK);

    // Do nothing
  }

  @Override
  public void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    PipeLogger.log(
        LOGGER::info,
        ProcedureMessages.PIPEHANDLEMETACHANGEPROCEDURE_EXECUTEFROMWRITECONFIGNODECONSENSUS);

    if (!needWriteConsensusOnConfigNodes) {
      return;
    }

    final List<PipeMeta> pipeMetaList = new ArrayList<>();
    for (final PipeMeta pipeMeta : pipeTaskInfo.get().getPipeMetaList()) {
      pipeMetaList.add(pipeMeta);
    }

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new PipeHandleMetaChangePlan(pipeMetaList));
    } catch (ConsensusException e) {
      PipeLogger.log(
          LOGGER::warn,
          e,
          ConfigNodeMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    PipeLogger.log(
        LOGGER::info, ProcedureMessages.PIPEHANDLEMETACHANGEPROCEDURE_EXECUTEFROMHANDLEONDATANODES);

    if (!needPushPipeMetaToDataNodes) {
      return;
    }

    pushPipeMetaToDataNodesBestEffort(env);
  }

  @Override
  public void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    PipeLogger.log(
        LOGGER::info, ProcedureMessages.PIPEHANDLEMETACHANGEPROCEDURE_ROLLBACKFROMVALIDATETASK);

    // Do nothing
  }

  @Override
  public void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    PipeLogger.log(
        LOGGER::info,
        ProcedureMessages.PIPEHANDLEMETACHANGEPROCEDURE_ROLLBACKFROMCALCULATEINFOFORTASK);

    // Do nothing
  }

  @Override
  public void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    PipeLogger.log(
        LOGGER::info,
        ProcedureMessages.PIPEHANDLEMETACHANGEPROCEDURE_ROLLBACKFROMWRITECONFIGNODECONSENSUS);

    // Do nothing
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    PipeLogger.log(
        LOGGER::info,
        ProcedureMessages.PIPEHANDLEMETACHANGEPROCEDURE_ROLLBACKFROMOPERATEONDATANODES);

    // Do nothing
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.PIPE_HANDLE_META_CHANGE_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(needWriteConsensusOnConfigNodes, stream);
    ReadWriteIOUtils.write(needPushPipeMetaToDataNodes, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    needWriteConsensusOnConfigNodes = ReadWriteIOUtils.readBool(byteBuffer);
    needPushPipeMetaToDataNodes = ReadWriteIOUtils.readBool(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PipeHandleMetaChangeProcedure)) {
      return false;
    }
    PipeHandleMetaChangeProcedure that = (PipeHandleMetaChangeProcedure) o;
    return getProcId() == that.getProcId()
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && needWriteConsensusOnConfigNodes == that.needWriteConsensusOnConfigNodes
        && needPushPipeMetaToDataNodes == that.needPushPipeMetaToDataNodes;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        needWriteConsensusOnConfigNodes,
        needPushPipeMetaToDataNodes);
  }
}
