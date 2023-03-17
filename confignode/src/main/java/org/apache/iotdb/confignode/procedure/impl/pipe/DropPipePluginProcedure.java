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

import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.persistence.pipe.PipePluginInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.pipe.DropPipePluginState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

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
    PipePluginInfo pipePluginInfo =
        env.getConfigManager().getPipeManager().getPipePluginCoordinator().getPipePluginInfo();

    try {
      switch (state) {
        case LOCK:
          LOGGER.info("Locking pipe plugin {}", pluginName);

          pipePluginInfo.acquirePipePluginInfoLock();

          LOGGER.info("Lock pipe plugin {} successfully", pluginName);
          pipePluginInfo.validateBeforeDroppingPipePlugin(pluginName);

          env.getConfigManager().getConsensusManager().write(new DropPipePluginPlan(pluginName));
          setNextState(DropPipePluginState.DROP_ON_DATA_NODES);

        case DROP_ON_DATA_NODES:
          LOGGER.info("Dropping pipe plugin {} on data nodes", pluginName);

          if (RpcUtils.squashResponseStatusList(env.dropPipePluginOnDataNodes(pluginName, false))
                  .getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            setNextState(DropPipePluginState.DROP_ON_CONFIG_NODE);
          } else {
            throw new PipeManagementException(
                String.format("Failed to drop pipe plugin %s on data nodes", pluginName));
          }

        case DROP_ON_CONFIG_NODE:
          LOGGER.info("Dropping pipe plugin {} on config node", pluginName);

          env.getConfigManager().getConsensusManager().write(new DropPipePluginPlan(pluginName));
          setNextState(DropPipePluginState.UNLOCK);
          break;

        case UNLOCK:
          LOGGER.info("Unlocking pipe plugin {}", pluginName);
          pipePluginInfo.releasePipePluginInfoLock();
          return Flow.NO_MORE_STATE;
      }
    } catch (PipeManagementException e) {
      // if the pipe plugin is not exist, we should end the procedure
      LOGGER.warn(e.getMessage());
      pipePluginInfo.releasePipePluginInfoLock();
      return Flow.NO_MORE_STATE;
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

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, DropPipePluginState state)
      throws IOException, InterruptedException, ProcedureException {
    if (state == DropPipePluginState.LOCK) {
      LOGGER.info("Start [LOCK] rollback of pipe plugin [{}]", pluginName);

      env.getConfigManager()
          .getPipeManager()
          .getPipePluginCoordinator()
          .getPipePluginInfo()
          .releasePipePluginInfoLock();
    }
  }

  @Override
  protected boolean isRollbackSupported(DropPipePluginState state) {
    return state == DropPipePluginState.LOCK;
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
      DropPipePluginProcedure thatProcedure = (DropPipePluginProcedure) that;
      return thatProcedure.getProcId() == getProcId()
          && thatProcedure.getState() == this.getState()
          && (thatProcedure.pluginName).equals(this.pluginName);
    }
    return false;
  }
}
