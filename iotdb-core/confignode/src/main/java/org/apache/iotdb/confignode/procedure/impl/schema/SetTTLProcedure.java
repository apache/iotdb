/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.SetTTLState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class SetTTLProcedure extends StateMachineProcedure<ConfigNodeProcedureEnv, SetTTLState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SetTTLProcedure.class);

  private SetTTLPlan plan;

  public SetTTLProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public SetTTLProcedure(SetTTLPlan plan, final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.plan = plan;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, SetTTLState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case SET_CONFIGNODE_TTL:
          setConfigNodeTTL(env);
          return Flow.HAS_MORE_STATE;
        case UPDATE_DATANODE_CACHE:
          updateDataNodeTTL(env);
          return Flow.NO_MORE_STATE;
        default:
          return Flow.NO_MORE_STATE;
      }
    } finally {
      LOGGER.info("SetTTL-[{}] costs {}ms", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void setConfigNodeTTL(ConfigNodeProcedureEnv env) {
    TSStatus res;
    try {
      res =
          env.getConfigManager()
              .getConsensusManager()
              .write(isGeneratedByPipe ? new PipeEnrichedPlan(this.plan) : this.plan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
    }
    if (res.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.info("Failed to execute plan {} because {}", plan, res.message);
      setFailure(new ProcedureException(new IoTDBException(res.message, res.code)));
    } else {
      setNextState(SetTTLState.UPDATE_DATANODE_CACHE);
    }
  }

  private void updateDataNodeTTL(ConfigNodeProcedureEnv env) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<TSetTTLReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.SET_TTL,
            new TSetTTLReq(
                Collections.singletonList(String.join(".", plan.getPathPattern())),
                plan.getTTL(),
                plan.isDataBase()),
            dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related schemaengine cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to update ttl cache of dataNode.");
        setFailure(
            new ProcedureException(new MetadataException("Update dataNode ttl cache failed")));
        return;
      }
    }
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, SetTTLState setTTLState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected SetTTLState getState(int stateId) {
    return SetTTLState.values()[stateId];
  }

  @Override
  protected int getStateId(SetTTLState setTTLState) {
    return setTTLState.ordinal();
  }

  @Override
  protected SetTTLState getInitialState() {
    return SetTTLState.SET_CONFIGNODE_TTL;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_SET_TTL_PROCEDURE.getTypeCode()
            : ProcedureType.SET_TTL_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(plan.serializeToByteBuffer(), stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      ReadWriteIOUtils.readInt(byteBuffer);
      this.plan = (SetTTLPlan) ConfigPhysicalPlan.Factory.create(byteBuffer);
    } catch (IOException e) {
      LOGGER.error("IO error when deserialize setTTL plan.", e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return this.plan.equals(((SetTTLProcedure) o).plan)
        && this.isGeneratedByPipe == (((SetTTLProcedure) o).isGeneratedByPipe);
  }

  @Override
  public int hashCode() {
    return Objects.hash(plan, isGeneratedByPipe);
  }
}
