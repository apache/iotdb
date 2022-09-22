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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.confignode.consensus.request.write.trigger.AddTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerStateInTablePlan;
import org.apache.iotdb.confignode.persistence.TriggerInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.CreateTriggerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** remove config node procedure */
public class CreateTriggerProcedure extends AbstractNodeProcedure<CreateTriggerState> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTriggerProcedure.class);
  private static final int retryThreshold = 5;

  private TriggerInformation triggerInformation;
  private Binary jarFile;

  public CreateTriggerProcedure(TriggerInformation triggerInformation, Binary jarFile) {
    super();
    this.triggerInformation = triggerInformation;
    this.jarFile = jarFile;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, CreateTriggerState state) {
    if (triggerInformation == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case INIT:
          TriggerInfo triggerInfo = env.getConfigManager().getTriggerManager().getTriggerInfo();
          LOG.info("Start to create trigger [{}]", triggerInformation.getTriggerName());
          env.getConfigManager().getTriggerManager().getTriggerInfo().acquireTriggerTableLock();
          boolean needToSaveJar =
              triggerInfo.validate(
                  triggerInformation.getTriggerName(),
                  triggerInformation.getJarName(),
                  triggerInformation.getJarMD5());
          env.getConfigManager()
              .getConsensusManager()
              .write(new AddTriggerInTablePlan(triggerInformation, needToSaveJar ? jarFile : null));
          setNextState(CreateTriggerState.CONFIG_NODE_INACTIVE);
          break;
        case CONFIG_NODE_INACTIVE:
          LOG.info(
              "Start to create triggerInstance [{}] on Data Nodes",
              triggerInformation.getTriggerName());
          if (RpcUtils.squashResponseStatusList(
                      env.createTriggerOnDataNodes(triggerInformation, jarFile))
                  .getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            setNextState(CreateTriggerState.DATA_NODE_INACTIVE);
          } else {
            throw new TriggerManagementException(
                String.format(
                    "Fail to create triggerInstance [{}] on Data Nodes",
                    triggerInformation.getTriggerName()));
          }
          break;
        case DATA_NODE_INACTIVE:
          LOG.info(
              "Start to active trigger [{}] on Data Nodes", triggerInformation.getTriggerName());
          if (RpcUtils.squashResponseStatusList(
                      env.activeTriggerOnDataNodes(triggerInformation.getTriggerName()))
                  .getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            setNextState(CreateTriggerState.DATA_NODE_ACTIVE);
          } else {
            throw new TriggerManagementException(
                String.format(
                    "Fail to active triggerInstance [{}] on Data Nodes",
                    triggerInformation.getTriggerName()));
          }
          break;
        case DATA_NODE_ACTIVE:
          LOG.info(
              "Start to active trigger [{}] on Config Nodes", triggerInformation.getTriggerName());
          env.getConfigManager()
              .getConsensusManager()
              .write(
                  new UpdateTriggerStateInTablePlan(
                      triggerInformation.getTriggerName(), TTriggerState.ACTIVE));
          setNextState(CreateTriggerState.CONFIG_NODE_ACTIVE);
          break;
        case CONFIG_NODE_ACTIVE:
          env.getConfigManager().getTriggerManager().getTriggerInfo().releaseTriggerTableLock();
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Create trigger failed " + state));
      } else {
        LOG.error(
            "Retrievable error trying to create trigger [{}], state [{}]",
            triggerInformation.getTriggerName(),
            state,
            e);
        if (getCycles() > retryThreshold) {
          setFailure(new ProcedureException("State stuck at " + state));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, CreateTriggerState state)
      throws IOException, InterruptedException, ProcedureException {
    // TODO
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

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.CREATE_TRIGGER_PROCEDURE.ordinal());
    super.serialize(stream);
    triggerInformation.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    triggerInformation = TriggerInformation.deserialize(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof CreateTriggerProcedure) {
      CreateTriggerProcedure thatProc = (CreateTriggerProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && thatProc.triggerInformation.equals(this.triggerInformation);
    }
    return false;
  }
}
