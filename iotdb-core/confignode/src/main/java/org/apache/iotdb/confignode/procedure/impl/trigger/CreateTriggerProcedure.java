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

package org.apache.iotdb.confignode.procedure.impl.trigger;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.AddTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.DeleteTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerStateInTablePlan;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.TriggerInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.CreateTriggerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CreateTriggerProcedure extends AbstractNodeProcedure<CreateTriggerState> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTriggerProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TriggerInformation triggerInformation;
  private Binary jarFile;

  public CreateTriggerProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public CreateTriggerProcedure(
      final TriggerInformation triggerInformation,
      final Binary jarFile,
      final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.triggerInformation = triggerInformation;
    this.jarFile = jarFile;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final CreateTriggerState state) {
    if (triggerInformation == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case INIT:
          LOG.info(ProcedureMessages.START_TO_CREATE_TRIGGER, triggerInformation.getTriggerName());

          TriggerInfo triggerInfo = env.getConfigManager().getTriggerManager().getTriggerInfo();
          triggerInfo.acquireTriggerTableLock();
          triggerInfo.validate(
              triggerInformation.getTriggerName(),
              triggerInformation.getJarName(),
              triggerInformation.getJarFileMD5());
          setNextState(CreateTriggerState.VALIDATED);
          break;

        case VALIDATED:
          ConfigManager configManager = env.getConfigManager();

          LOG.info(
              ProcedureMessages
                  .LOG_START_ADD_TRIGGER_ARG_TRIGGERTABLE_CONFIG_NODES_NEEDTOSAVEJAR_ARG_0C23D81E,
              triggerInformation.getTriggerName(),
              jarFile != null);

          TSStatus response;

          response =
              configManager
                  .getConsensusManager()
                  .write(new AddTriggerInTablePlan(triggerInformation, jarFile));

          if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            throw new TriggerManagementException(response.getMessage());
          }

          setNextState(CreateTriggerState.CONFIG_NODE_INACTIVE);
          break;

        case CONFIG_NODE_INACTIVE:
          LOG.info(
              ProcedureMessages.LOG_START_CREATE_TRIGGERINSTANCE_ARG_DATA_NODES_917C3313,
              triggerInformation.getTriggerName());

          if (RpcUtils.squashResponseStatusList(
                      env.createTriggerOnDataNodes(triggerInformation, jarFile))
                  .getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            setNextState(CreateTriggerState.DATA_NODE_INACTIVE);
          } else {
            throw new TriggerManagementException(
                String.format(
                    ProcedureMessages.FAIL_TO_CREATE_TRIGGERINSTANCE_ON_DATA_NODES,
                    triggerInformation.getTriggerName()));
          }
          break;

        case DATA_NODE_INACTIVE:
          LOG.info(
              ProcedureMessages.LOG_START_ACTIVE_TRIGGER_ARG_DATA_NODES_A4AB8131,
              triggerInformation.getTriggerName());

          if (RpcUtils.squashResponseStatusList(
                      env.activeTriggerOnDataNodes(triggerInformation.getTriggerName()))
                  .getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            setNextState(CreateTriggerState.DATA_NODE_ACTIVE);
          } else {
            throw new TriggerManagementException(
                String.format(
                    ProcedureMessages.FAIL_TO_ACTIVE_TRIGGERINSTANCE_ON_DATA_NODES,
                    triggerInformation.getTriggerName()));
          }
          break;

        case DATA_NODE_ACTIVE:
          LOG.info(
              ProcedureMessages.LOG_START_ACTIVE_TRIGGER_ARG_CONFIG_NODES_153A5D40,
              triggerInformation.getTriggerName());
          env.getConfigManager()
              .getConsensusManager()
              .write(
                  isGeneratedByPipe
                      ? new PipeEnrichedPlan(
                          new UpdateTriggerStateInTablePlan(
                              triggerInformation.getTriggerName(), TTriggerState.ACTIVE))
                      : new UpdateTriggerStateInTablePlan(
                          triggerInformation.getTriggerName(), TTriggerState.ACTIVE));
          setNextState(CreateTriggerState.CONFIG_NODE_ACTIVE);
          break;

        case CONFIG_NODE_ACTIVE:
          env.getConfigManager().getTriggerManager().getTriggerInfo().releaseTriggerTableLock();
          return Flow.NO_MORE_STATE;

        default:
          throw new IllegalArgumentException(ProcedureMessages.UNKNOWN_CREATETRIGGERSTATE + state);
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        LOG.warn(ProcedureMessages.CREATE_TRIGGER_FAILED, triggerInformation.getTriggerName(), e);
        setFailure(new ProcedureException(e));
      } else {
        LOG.error(
            ProcedureMessages.LOG_RETRIEVABLE_ERROR_TRYING_CREATE_TRIGGER_ARG_STATE_ARG_44976C4E,
            triggerInformation.getTriggerName(),
            state,
            e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format(
                      ProcedureMessages.FAIL_TO_CREATE_TRIGGER_AT_STATE,
                      triggerInformation.getTriggerName(),
                      state)));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, CreateTriggerState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case INIT:
        LOG.info(
            ProcedureMessages.START_INIT_ROLLBACK_OF_TRIGGER, triggerInformation.getTriggerName());

        env.getConfigManager().getTriggerManager().getTriggerInfo().releaseTriggerTableLock();
        break;

      case VALIDATED:
        LOG.info(
            ProcedureMessages.START_VALIDATED_ROLLBACK_OF_TRIGGER,
            triggerInformation.getTriggerName());

        try {
          env.getConfigManager()
              .getConsensusManager()
              .write(
                  isGeneratedByPipe
                      ? new PipeEnrichedPlan(
                          new DeleteTriggerInTablePlan(triggerInformation.getTriggerName()))
                      : new DeleteTriggerInTablePlan(triggerInformation.getTriggerName()));
        } catch (ConsensusException e) {
          LOG.warn(ProcedureMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
        }
        break;

      case CONFIG_NODE_INACTIVE:
        LOG.info(
            ProcedureMessages.LOG_START_CONFIG_NODE_INACTIVE_ROLLBACK_TRIGGER_ARG_536929E5,
            triggerInformation.getTriggerName());

        if (RpcUtils.squashResponseStatusList(
                    env.dropTriggerOnDataNodes(triggerInformation.getTriggerName(), false))
                .getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new TriggerManagementException(
              String.format(
                  ProcedureMessages.FAIL_TO_CONFIG_NODE_INACTIVE_ROLLBACK_OF_TRIGGER,
                  triggerInformation.getTriggerName()));
        }
        break;

      case DATA_NODE_INACTIVE:
        LOG.info(
            ProcedureMessages.LOG_START_DATA_NODE_INACTIVE_ROLLBACK_TRIGGER_ARG_38C93D64,
            triggerInformation.getTriggerName());

        if (RpcUtils.squashResponseStatusList(
                    env.inactiveTriggerOnDataNodes(triggerInformation.getTriggerName()))
                .getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new TriggerManagementException(
              String.format(
                  ProcedureMessages.FAIL_TO_DATA_NODE_INACTIVE_ROLLBACK_OF_TRIGGER,
                  triggerInformation.getTriggerName()));
        }
        break;

      default:
        break;
    }
  }

  @Override
  protected boolean isRollbackSupported(CreateTriggerState state) {
    return true;
  }

  @Override
  protected CreateTriggerState getState(int stateId) {
    return CreateTriggerState.values()[stateId];
  }

  @Override
  protected int getStateId(CreateTriggerState createTriggerState) {
    return createTriggerState.ordinal();
  }

  @Override
  protected CreateTriggerState getInitialState() {
    return CreateTriggerState.INIT;
  }

  public Binary getJarFile() {
    return jarFile;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_CREATE_TRIGGER_PROCEDURE.getTypeCode()
            : ProcedureType.CREATE_TRIGGER_PROCEDURE.getTypeCode());
    super.serialize(stream);
    triggerInformation.serialize(stream);
    if (jarFile == null) {
      ReadWriteIOUtils.write(true, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
      ReadWriteIOUtils.write(jarFile, stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    triggerInformation = TriggerInformation.deserialize(byteBuffer);
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      return;
    }
    jarFile = ReadWriteIOUtils.readBinary(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof CreateTriggerProcedure) {
      CreateTriggerProcedure thatProc = (CreateTriggerProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && Objects.equals(thatProc.getCurrentState(), this.getCurrentState())
          && thatProc.getCycles() == this.getCycles()
          && thatProc.isGeneratedByPipe == this.isGeneratedByPipe
          && thatProc.triggerInformation.equals(this.triggerInformation);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(), getCurrentState(), getCycles(), isGeneratedByPipe, triggerInformation);
  }
}
