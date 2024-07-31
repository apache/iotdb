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

import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.manager.pipe.coordinator.plugin.PipePluginCoordinator;
import org.apache.iotdb.confignode.manager.pipe.coordinator.task.PipeTaskCoordinator;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.AddConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.pipe.plugin.DropPipePluginState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class extends {@link AbstractNodeProcedure} to make sure that when a {@link
 * DropPipePluginProcedure} is executed, the {@link AddConfigNodeProcedure}, {@link
 * RemoveConfigNodeProcedure} or {@link RemoveDataNodeProcedure} will not be executed at the same
 * time.
 */
public class DropPipePluginProcedure extends AbstractNodeProcedure<DropPipePluginState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropPipePluginProcedure.class);

  private static final int RETRY_THRESHOLD = 5;

  private String pluginName;

  public DropPipePluginProcedure() {
    super();
  }

  public DropPipePluginProcedure(String pluginName) {
    super();
    this.pluginName = pluginName;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DropPipePluginState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (pluginName == null) {
      return Flow.NO_MORE_STATE;
    }

    try {
      switch (state) {
        case LOCK:
          return executeFromLock(env);
        case DROP_ON_DATA_NODES:
          return executeFromDropOnDataNodes(env);
        case DROP_ON_CONFIG_NODES:
          return executeFromDropOnConfigNodes(env);
        case UNLOCK:
          return executeFromUnlock(env);
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        LOGGER.warn("DropPipePluginProcedure failed in state {}, will rollback", state, e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error(
            "Retrievable error trying to drop pipe plugin [{}], state: {}", pluginName, state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          LOGGER.error("Fail to drop pipe plugin [{}] after {} retries", pluginName, getCycles());
          setFailure(new ProcedureException(e.getMessage()));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private Flow executeFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipePluginProcedure: executeFromLock({})", pluginName);

    final PipeTaskCoordinator pipeTaskCoordinator =
        env.getConfigManager().getPipeManager().getPipeTaskCoordinator();
    final PipePluginCoordinator pipePluginCoordinator =
        env.getConfigManager().getPipeManager().getPipePluginCoordinator();

    final AtomicReference<PipeTaskInfo> pipeTaskInfo = pipeTaskCoordinator.lock();
    pipePluginCoordinator.lock();

    try {
      pipePluginCoordinator.getPipePluginInfo().validateBeforeDroppingPipePlugin(pluginName);
      pipeTaskInfo.get().validatePipePluginUsageByPipe(pluginName);
    } catch (PipeException e) {
      // if the pipe plugin is a built-in plugin, we should not drop it
      LOGGER.warn(e.getMessage());
      pipePluginCoordinator.unlock();
      pipeTaskCoordinator.unlock();
      setFailure(new ProcedureException(e.getMessage()));
      return Flow.NO_MORE_STATE;
    }

    try {
      env.getConfigManager().getConsensusManager().write(new DropPipePluginPlan(pluginName));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
    }
    setNextState(DropPipePluginState.DROP_ON_DATA_NODES);
    return Flow.HAS_MORE_STATE;
  }

  private Flow executeFromDropOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipePluginProcedure: executeFromDropOnDataNodes({})", pluginName);

    if (RpcUtils.squashResponseStatusList(env.dropPipePluginOnDataNodes(pluginName, true)).getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(DropPipePluginState.DROP_ON_CONFIG_NODES);
      return Flow.HAS_MORE_STATE;
    }

    throw new PipeException(
        String.format("Failed to drop pipe plugin %s on data nodes", pluginName));
  }

  private Flow executeFromDropOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipePluginProcedure: executeFromDropOnConfigNodes({})", pluginName);

    try {
      env.getConfigManager().getConsensusManager().write(new DropPipePluginPlan(pluginName));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
    }

    setNextState(DropPipePluginState.UNLOCK);
    return Flow.HAS_MORE_STATE;
  }

  private Flow executeFromUnlock(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipePluginProcedure: executeFromUnlock({})", pluginName);

    env.getConfigManager().getPipeManager().getPipePluginCoordinator().unlock();
    env.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, DropPipePluginState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case LOCK:
        rollbackFromLock(env);
        break;
      case DROP_ON_DATA_NODES:
        rollbackFromDropOnDataNodes(env);
        break;
      case DROP_ON_CONFIG_NODES:
        rollbackFromDropOnConfigNodes(env);
        break;
    }
  }

  private void rollbackFromLock(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipePluginProcedure: rollbackFromLock({})", pluginName);

    env.getConfigManager().getPipeManager().getPipePluginCoordinator().unlock();
    env.getConfigManager().getPipeManager().getPipeTaskCoordinator().unlock();
  }

  private void rollbackFromDropOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipePluginProcedure: rollbackFromDropOnDataNodes({})", pluginName);

    // do nothing but wait for rolling back to the previous state: LOCK
    // TODO: we should drop the pipe plugin on data nodes properly with RuntimeAgent's help
  }

  private void rollbackFromDropOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipePluginProcedure: rollbackFromDropOnConfigNodes({})", pluginName);

    // do nothing but wait for rolling back to the previous state: DROP_ON_DATA_NODES
    // TODO: we should drop the pipe plugin on config nodes properly with RuntimeCoordinator's help
  }

  @Override
  protected boolean isRollbackSupported(DropPipePluginState state) {
    switch (state) {
      case LOCK:
      case DROP_ON_DATA_NODES:
      case DROP_ON_CONFIG_NODES:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected DropPipePluginState getState(int stateId) {
    return DropPipePluginState.values()[stateId];
  }

  @Override
  protected int getStateId(DropPipePluginState dropPipePluginState) {
    return dropPipePluginState.ordinal();
  }

  @Override
  protected DropPipePluginState getInitialState() {
    return DropPipePluginState.LOCK;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_PIPE_PLUGIN_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(pluginName, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    pluginName = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof DropPipePluginProcedure) {
      final DropPipePluginProcedure thatProcedure = (DropPipePluginProcedure) that;
      return thatProcedure.getProcId() == getProcId()
          && thatProcedure.getCurrentState().equals(this.getCurrentState())
          && thatProcedure.getCycles() == this.getCycles()
          && (thatProcedure.pluginName).equals(pluginName);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getCurrentState(), getCycles(), pluginName);
  }
}
