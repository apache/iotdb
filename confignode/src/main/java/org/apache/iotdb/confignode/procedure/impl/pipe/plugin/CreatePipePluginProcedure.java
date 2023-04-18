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

package org.apache.iotdb.confignode.procedure.impl.pipe.plugin;

import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.PipePluginCoordinator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.AddConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.pipe.plugin.CreatePipePluginState;
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

/**
 * This class extends {@link AbstractNodeProcedure} to make sure that when a {@link
 * CreatePipePluginProcedure} is executed, the {@link AddConfigNodeProcedure}, {@link
 * RemoveConfigNodeProcedure} or {@link RemoveDataNodeProcedure} will not be executed at the same
 * time.
 */
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

    try {
      switch (state) {
        case LOCK:
          return executeFromLock(env);
        case CREATE_ON_CONFIG_NODES:
          return executeFromCreateOnConfigNodes(env);
        case CREATE_ON_DATA_NODES:
          return executeFromCreateOnDataNodes(env);
        case UNLOCK:
          return executeFromUnlock(env);
      }
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
    return Flow.HAS_MORE_STATE;
  }

  private Flow executeFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreatePipePluginProcedure: executeFromLock({})", pipePluginMeta.getPluginName());
    final PipePluginCoordinator pipePluginCoordinator =
        env.getConfigManager().getPipeManager().getPipePluginCoordinator();

    pipePluginCoordinator.lock();

    try {
      pipePluginCoordinator
          .getPipePluginInfo()
          .validateBeforeCreatingPipePlugin(
              pipePluginMeta.getPluginName(),
              pipePluginMeta.getJarName(),
              pipePluginMeta.getJarMD5());
    } catch (PipeManagementException e) {
      // The pipe plugin has already created, we should end the procedure
      LOGGER.warn(
          "Pipe plugin {} is already created, end the CreatePipePluginProcedure({})",
          pipePluginMeta.getPluginName(),
          pipePluginMeta.getPluginName());
      setFailure(new ProcedureException(e.getMessage()));
      pipePluginCoordinator.unlock();
      return Flow.NO_MORE_STATE;
    }

    setNextState(CreatePipePluginState.CREATE_ON_CONFIG_NODES);
    return Flow.HAS_MORE_STATE;
  }

  private Flow executeFromCreateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipePluginProcedure: executeFromCreateOnConfigNodes({})",
        pipePluginMeta.getPluginName());

    final ConfigManager configNodeManager = env.getConfigManager();

    final boolean needToSaveJar =
        configNodeManager
            .getPipeManager()
            .getPipePluginCoordinator()
            .getPipePluginInfo()
            .isJarNeededToBeSavedWhenCreatingPipePlugin(pipePluginMeta.getJarName());
    final CreatePipePluginPlan createPluginPlan =
        new CreatePipePluginPlan(pipePluginMeta, needToSaveJar ? new Binary(jarFile) : null);

    final ConsensusWriteResponse response =
        configNodeManager.getConsensusManager().write(createPluginPlan);
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }

    setNextState(CreatePipePluginState.CREATE_ON_DATA_NODES);
    return Flow.HAS_MORE_STATE;
  }

  private Flow executeFromCreateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info(
        "CreatePipePluginProcedure: executeFromCreateOnDataNodes({})",
        pipePluginMeta.getPluginName());

    if (RpcUtils.squashResponseStatusList(env.createPipePluginOnDataNodes(pipePluginMeta, jarFile))
            .getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(CreatePipePluginState.UNLOCK);
      return Flow.HAS_MORE_STATE;
    }

    throw new PipeManagementException(
        String.format(
            "Failed to create pipe plugin instance [%s] on data nodes",
            pipePluginMeta.getPluginName()));
  }

  private Flow executeFromUnlock(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreatePipePluginProcedure: executeFromUnlock({})", pipePluginMeta.getPluginName());

    env.getConfigManager().getPipeManager().getPipePluginCoordinator().unlock();

    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, CreatePipePluginState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case LOCK:
        rollbackFromLock(env);
        break;
      case CREATE_ON_CONFIG_NODES:
        rollbackFromCreateOnConfigNodes(env);
        break;
      case CREATE_ON_DATA_NODES:
        rollbackFromCreateOnDataNodes(env);
        break;
    }
  }

  private void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("CreatePipePluginProcedure: rollbackFromLock({})", pipePluginMeta.getPluginName());

    env.getConfigManager().getPipeManager().getPipePluginCoordinator().unlock();
  }

  private void rollbackFromCreateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipePluginProcedure: rollbackFromCreateOnConfigNodes({})",
        pipePluginMeta.getPluginName());

    env.getConfigManager()
        .getConsensusManager()
        .write(new DropPipePluginPlan(pipePluginMeta.getPluginName()));
  }

  private void rollbackFromCreateOnDataNodes(ConfigNodeProcedureEnv env) throws ProcedureException {
    LOGGER.info(
        "CreatePipePluginProcedure: rollbackFromCreateOnDataNodes({})",
        pipePluginMeta.getPluginName());

    if (RpcUtils.squashResponseStatusList(
                env.dropPipePluginOnDataNodes(pipePluginMeta.getPluginName(), false))
            .getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new ProcedureException(
          String.format(
              "Failed to rollback pipe plugin [%s] on data nodes", pipePluginMeta.getPluginName()));
    }
  }

  @Override
  protected boolean isRollbackSupported(CreatePipePluginState state) {
    switch (state) {
      case LOCK:
      case CREATE_ON_CONFIG_NODES:
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

  @TestOnly
  public byte[] getJarFile() {
    return jarFile;
  }
}
