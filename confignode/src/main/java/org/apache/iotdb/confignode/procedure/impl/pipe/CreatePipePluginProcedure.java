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

package org.apache.iotdb.confignode.procedure.impl.pipe;

import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipePluginInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.pipe.CreatePipePluginState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CreatePipePluginProcedure extends AbstractNodeProcedure<CreatePipePluginState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreatePipePluginProcedure.class);

  private static final int RETRY_THRESHOLD = 5;

  private PipePluginMeta pipePluginMeta;
  private byte[] jarFile;

  public CreatePipePluginProcedure() {
    super();
  }

  public CreatePipePluginProcedure(PipePluginMeta pipePluginMeta, byte[] jarFile) {
    super();
    this.pipePluginMeta = pipePluginMeta;
    this.jarFile = jarFile;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, CreatePipePluginState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (pipePluginMeta == null) {
      return Flow.NO_MORE_STATE;
    }

    PipePluginInfo pipePluginInfo =
        env.getConfigManager().getPipeManager().getPipePluginCoordinator().getPipePluginInfo();

    boolean needToSaveJar =
        pipePluginInfo.isJarNeededToBeSavedWhenCreatingPipePlugin(pipePluginMeta.getJarName());
    try {
      switch (state) {
        case LOCK:
          LOGGER.info("Locking pipe plugin {}", pipePluginMeta.getPluginName());
          pipePluginInfo.acquirePipePluginInfoLock();
          pipePluginInfo.validateBeforeCreatingPipePlugin(
              pipePluginMeta.getPluginName(),
              pipePluginMeta.getJarName(),
              pipePluginMeta.getJarMD5());
          setNextState(CreatePipePluginState.CREATE_ON_CONFIG_NODE);
          break;

        case CREATE_ON_CONFIG_NODE:
          ConfigManager configNodeManager = env.getConfigManager();
          LOGGER.info("Creating pipe plugin {} on config node", pipePluginMeta.getPluginName());

          final CreatePipePluginPlan createPluginPlan =
              new CreatePipePluginPlan(pipePluginMeta, needToSaveJar ? new Binary(jarFile) : null);

          ConsensusWriteResponse response =
              configNodeManager.getConsensusManager().write(createPluginPlan);

          if (!response.isSuccessful()) {
            throw new PipeManagementException(response.getErrorMessage());
          }

          setNextState(CreatePipePluginState.CREATE_ON_DATA_NODES);
          break;

        case CREATE_ON_DATA_NODES:
          LOGGER.info("Creating pipe plugin {} on data nodes", pipePluginMeta.getPluginName());

          needToSaveJar =
              pipePluginInfo.isJarNeededToBeSavedWhenCreatingPipePlugin(
                  pipePluginMeta.getJarName());
          if (RpcUtils.squashResponseStatusList(
                      env.createPipePluginOnDataNodes(
                          pipePluginMeta, needToSaveJar ? jarFile : null))
                  .getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            setNextState(CreatePipePluginState.UNLOCK);
          } else {
            throw new PipeManagementException(
                String.format(
                    "Failed to create pipe plugin instance [%s] on data nodes",
                    pipePluginMeta.getPluginName()));
          }
          break;

        case UNLOCK:
          LOGGER.info("Unlocking pipe plugin {}", pipePluginMeta.getPluginName());
          pipePluginInfo.releasePipePluginInfoLock();
          return Flow.NO_MORE_STATE;
      }
    } catch (PipeManagementException e) {
      // if the pipe plugin is already created, we should end the procedure
      LOGGER.warn(
          "Pipe plugin {} is already created, end the procedure ", pipePluginMeta.getPluginName());
      pipePluginInfo.releasePipePluginInfoLock();
      return Flow.NO_MORE_STATE;
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        LOGGER.error("CreatePipePluginProcedure failed in state {}, will rollback", state, e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error(
            "Retrievable error trying to create pipe plugin [{}], state: {}",
            pipePluginMeta.getPluginName(),
            state,
            e);
        if (getCycles() > RETRY_THRESHOLD) {
          LOGGER.error(
              "Fail to create pipe plugin [{}] after {} retries",
              pipePluginMeta.getPluginName(),
              getCycles());
          setFailure(new ProcedureException(e.getMessage()));
        }
      }
    }
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, CreatePipePluginState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case CREATE_ON_CONFIG_NODE:
        LOGGER.info(
            "Start [CREATE_ON_CONFIG_NODE] rollback of pipe plugin [{}]",
            pipePluginMeta.getPluginName());
        env.getConfigManager()
            .getConsensusManager()
            .write(new DropPipePluginPlan(pipePluginMeta.getPluginName()));
        break;

      case CREATE_ON_DATA_NODES:
        LOGGER.info(
            "Start [CREATE_ON_DATA_NODES] rollback of pipe plugin [{}]",
            pipePluginMeta.getPluginName());

        if (RpcUtils.squashResponseStatusList(
                    env.dropPipePluginOnDataNodes(pipePluginMeta.getPluginName(), false))
                .getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new ProcedureException(
              String.format(
                  "Failed to rollback pipe plugin [%s] on data nodes",
                  pipePluginMeta.getPluginName()));
        }
        break;

      default:
        break;
    }
  }

  @Override
  protected boolean isRollbackSupported(CreatePipePluginState state) {
    switch (state) {
      case CREATE_ON_CONFIG_NODE:
      case CREATE_ON_DATA_NODES:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected CreatePipePluginState getState(int stateId) {
    return CreatePipePluginState.values()[stateId];
  }

  @Override
  protected int getStateId(CreatePipePluginState createPipePluginState) {
    return createPipePluginState.ordinal();
  }

  @Override
  protected CreatePipePluginState getInitialState() {
    return CreatePipePluginState.LOCK;
  }

  @TestOnly
  public byte[] getJarFile() {
    return jarFile;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_PIPE_PLUGIN_PROCEDURE.getTypeCode());
    super.serialize(stream);
    pipePluginMeta.serialize(stream);
    ReadWriteIOUtils.write(ByteBuffer.wrap(jarFile), stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    pipePluginMeta = PipePluginMeta.deserialize(byteBuffer);
    jarFile = ReadWriteIOUtils.readBinary(byteBuffer).getValues();
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof CreatePipePluginProcedure) {
      CreatePipePluginProcedure thatProcedure = (CreatePipePluginProcedure) that;
      return thatProcedure.getProcId() == getProcId()
          && thatProcedure.getState() == this.getState()
          && thatProcedure.pipePluginMeta.equals(pipePluginMeta);
    }
    return false;
  }
}
