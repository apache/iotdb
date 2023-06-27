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

import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.confignode.consensus.request.write.trigger.DeleteTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerStateInTablePlan;
import org.apache.iotdb.confignode.persistence.TriggerInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.DropTriggerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** drop trigger procedure */
public class DropTriggerProcedure extends AbstractNodeProcedure<DropTriggerState> {
  private static final Logger LOG = LoggerFactory.getLogger(DropTriggerProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private String triggerName;

  public DropTriggerProcedure() {
    super();
  }

  public DropTriggerProcedure(String triggerName) {
    super();
    this.triggerName = triggerName;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DropTriggerState state) {
    if (triggerName == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case INIT:
          LOG.info("Start to drop trigger [{}]", triggerName);

          TriggerInfo triggerInfo = env.getConfigManager().getTriggerManager().getTriggerInfo();
          triggerInfo.acquireTriggerTableLock();

          triggerInfo.validate(triggerName);

          env.getConfigManager()
              .getConsensusManager()
              .write(new UpdateTriggerStateInTablePlan(triggerName, TTriggerState.DROPPING));
          setNextState(DropTriggerState.CONFIG_NODE_DROPPING);
          break;

        case CONFIG_NODE_DROPPING:
          LOG.info("Start to drop trigger [{}] on Data Nodes", triggerName);

          // TODO consider using reference counts to determine whether to remove jar
          if (RpcUtils.squashResponseStatusList(env.dropTriggerOnDataNodes(triggerName, false))
                  .getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            setNextState(DropTriggerState.DATA_NODE_DROPPED);
          } else {
            throw new TriggerManagementException(
                String.format("Fail to drop trigger [%s] on Data Nodes", triggerName));
          }
          break;

        case DATA_NODE_DROPPED:
          LOG.info("Start to drop trigger [{}] on Config Nodes", triggerName);
          env.getConfigManager()
              .getConsensusManager()
              .write(new DeleteTriggerInTablePlan(triggerName));
          setNextState(DropTriggerState.CONFIG_NODE_DROPPED);
          break;

        case CONFIG_NODE_DROPPED:
          env.getConfigManager().getTriggerManager().getTriggerInfo().releaseTriggerTableLock();
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        LOG.error("Fail in DropTriggerProcedure", e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOG.error(
            "Retrievable error trying to drop trigger [{}], state [{}]", triggerName, state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format("Fail to drop trigger [%s] at STATE [%s]", triggerName, state)));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, DropTriggerState state)
      throws IOException, InterruptedException, ProcedureException {
    if (state == DropTriggerState.INIT) {
      LOG.info("Start [INIT] rollback of trigger [{}]", triggerName);

      env.getConfigManager().getTriggerManager().getTriggerInfo().releaseTriggerTableLock();
    }
  }

  @Override
  protected boolean isRollbackSupported(DropTriggerState state) {
    return true;
  }

  @Override
  protected DropTriggerState getState(int stateId) {
    return DropTriggerState.values()[stateId];
  }

  @Override
  protected int getStateId(DropTriggerState dropTriggerState) {
    return dropTriggerState.ordinal();
  }

  @Override
  protected DropTriggerState getInitialState() {
    return DropTriggerState.INIT;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_TRIGGER_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(triggerName, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    triggerName = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof DropTriggerProcedure) {
      DropTriggerProcedure thatProc = (DropTriggerProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && (thatProc.triggerName).equals(this.triggerName);
    }
    return false;
  }
}
