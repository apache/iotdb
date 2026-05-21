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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.ttl.TTLCache;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class SetTTLProcedure extends StateMachineProcedure<ConfigNodeProcedureEnv, SetTTLState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SetTTLProcedure.class);
  private static final long TTL_NOT_EXIST = Long.MIN_VALUE;

  private SetTTLPlan plan;
  private long previousTTL = TTL_NOT_EXIST;
  private long previousDatabaseWildcardTTL = TTL_NOT_EXIST;
  private boolean previousTTLStateCaptured = false;

  public SetTTLProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public SetTTLProcedure(SetTTLPlan plan, final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.plan = plan;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, SetTTLState state)
      throws InterruptedException {
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
      LOGGER.info(
          ProcedureMessages.SETTTL_COSTS_MS, state, (System.currentTimeMillis() - startTime));
    }
  }

  protected void setConfigNodeTTL(final ConfigNodeProcedureEnv env) {
    capturePreviousTTLState(env);
    final TSStatus res = writeConfigNodePlan(env, plan);
    if (res.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.info(ProcedureMessages.FAILED_TO_EXECUTE_PLAN_BECAUSE, plan, res.message);
      setFailure(new ProcedureException(new IoTDBException(res)));
    } else {
      setNextState(SetTTLState.UPDATE_DATANODE_CACHE);
    }
  }

  protected void updateDataNodeTTL(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    final DataNodeAsyncRequestContext<TSetTTLReq, TSStatus> clientHandler =
        sendTTLRequest(
            dataNodeLocationMap,
            buildSetTTLReq(plan.getPathPattern(), plan.getTTL(), plan.isDataBase()));
    if (hasFailedDataNode(clientHandler)) {
      LOGGER.error(ProcedureMessages.FAILED_TO_UPDATE_TTL_CACHE_OF_DATANODE);
      setFailure(
          new ProcedureException(
              new MetadataException(ProcedureMessages.UPDATE_DATANODE_TTL_CACHE_FAILED)));
    }
  }

  private void capturePreviousTTLState(final ConfigNodeProcedureEnv env) {
    if (previousTTLStateCaptured) {
      return;
    }
    final Map<String, Long> ttlMap = env.getConfigManager().getTTLManager().getAllTTL();
    previousTTL = getTTLOrDefault(ttlMap, plan.getPathPattern());
    if (plan.isDataBase()) {
      previousDatabaseWildcardTTL =
          getTTLOrDefault(ttlMap, getDatabaseWildcardPathPattern(plan.getPathPattern()));
    }
    previousTTLStateCaptured = true;
  }

  protected TSStatus writeConfigNodePlan(
      final ConfigNodeProcedureEnv env, final SetTTLPlan setTTLPlan) {
    try {
      return env.getConfigManager()
          .getConsensusManager()
          .write(isGeneratedByPipe ? new PipeEnrichedPlan(setTTLPlan) : setTTLPlan);
    } catch (ConsensusException e) {
      LOGGER.warn(ConfigNodeMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
      final TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  protected DataNodeAsyncRequestContext<TSetTTLReq, TSStatus> sendTTLRequest(
      final Map<Integer, TDataNodeLocation> dataNodeLocationMap, final TSetTTLReq req) {
    final DataNodeAsyncRequestContext<TSetTTLReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.SET_TTL, req, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler;
  }

  private TSetTTLReq buildSetTTLReq(
      final String[] pathPattern, final long ttl, final boolean isDataBase) {
    return new TSetTTLReq(
        Collections.singletonList(String.join(".", pathPattern)), ttl, isDataBase);
  }

  private boolean hasFailedDataNode(
      final DataNodeAsyncRequestContext<TSetTTLReq, TSStatus> clientHandler) {
    if (!clientHandler.getRequestIndices().isEmpty()) {
      return true;
    }
    for (TSStatus status : clientHandler.getResponseMap().values()) {
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return true;
      }
    }
    return false;
  }

  private long getTTLOrDefault(final Map<String, Long> ttlMap, final String[] pathPattern) {
    return ttlMap.getOrDefault(String.join(".", pathPattern), TTL_NOT_EXIST);
  }

  private String[] getDatabaseWildcardPathPattern(final String[] pathPattern) {
    final String[] pathNodes = Arrays.copyOf(pathPattern, pathPattern.length + 1);
    pathNodes[pathNodes.length - 1] = IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
    return pathNodes;
  }

  private void rollbackConfigNodeTTL(final ConfigNodeProcedureEnv env) throws ProcedureException {
    restoreTTLOnConfigNode(env, plan.getPathPattern(), previousTTL);
    if (plan.isDataBase()) {
      restoreTTLOnConfigNode(
          env, getDatabaseWildcardPathPattern(plan.getPathPattern()), previousDatabaseWildcardTTL);
    }
  }

  private void restoreTTLOnConfigNode(
      final ConfigNodeProcedureEnv env, final String[] pathPattern, final long ttl)
      throws ProcedureException {
    final SetTTLPlan rollbackPlan =
        new SetTTLPlan(pathPattern, ttl == TTL_NOT_EXIST ? TTLCache.NULL_TTL : ttl);
    rollbackPlan.setDataBase(false);
    final TSStatus status = writeConfigNodePlan(env, rollbackPlan);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new ProcedureException(
          new MetadataException(
              "Rollback ConfigNode ttl failed for "
                  + String.join(".", pathPattern)
                  + ": "
                  + status.getMessage()));
    }
  }

  private void rollbackDataNodeTTL(final ConfigNodeProcedureEnv env) throws ProcedureException {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    restoreTTLOnDataNodes(dataNodeLocationMap, plan.getPathPattern(), previousTTL);
    if (plan.isDataBase()) {
      restoreTTLOnDataNodes(
          dataNodeLocationMap,
          getDatabaseWildcardPathPattern(plan.getPathPattern()),
          previousDatabaseWildcardTTL);
    }
  }

  private void restoreTTLOnDataNodes(
      final Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      final String[] pathPattern,
      final long ttl)
      throws ProcedureException {
    if (dataNodeLocationMap.isEmpty()) {
      return;
    }
    final DataNodeAsyncRequestContext<TSetTTLReq, TSStatus> clientHandler =
        sendTTLRequest(
            dataNodeLocationMap,
            buildSetTTLReq(pathPattern, ttl == TTL_NOT_EXIST ? TTLCache.NULL_TTL : ttl, false));
    if (hasFailedDataNode(clientHandler)) {
      throw new ProcedureException(
          new MetadataException(
              "Rollback dataNode ttl cache failed for " + String.join(".", pathPattern)));
    }
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final SetTTLState setTTLState)
      throws IOException, InterruptedException, ProcedureException {
    if (setTTLState != SetTTLState.UPDATE_DATANODE_CACHE || !previousTTLStateCaptured) {
      return;
    }
    ProcedureException rollbackFailure = null;
    try {
      rollbackConfigNodeTTL(env);
    } catch (ProcedureException e) {
      LOGGER.error("Failed to rollback ConfigNode ttl state.", e);
      rollbackFailure = e;
    }
    try {
      rollbackDataNodeTTL(env);
    } catch (ProcedureException e) {
      LOGGER.error("Failed to rollback DataNode ttl cache.", e);
      if (rollbackFailure == null) {
        rollbackFailure = e;
      }
    }
    if (rollbackFailure != null) {
      throw rollbackFailure;
    }
  }

  @Override
  protected boolean isRollbackSupported(final SetTTLState state) {
    return state == SetTTLState.UPDATE_DATANODE_CACHE;
  }

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
    stream.writeBoolean(previousTTLStateCaptured);
    stream.writeLong(previousTTL);
    stream.writeLong(previousDatabaseWildcardTTL);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      final int length = ReadWriteIOUtils.readInt(byteBuffer);
      final int position = byteBuffer.position();
      this.plan = (SetTTLPlan) ConfigPhysicalPlan.Factory.create(byteBuffer);
      byteBuffer.position(position + length);
      if (byteBuffer.remaining() >= 17) {
        this.previousTTLStateCaptured = byteBuffer.get() != 0;
        this.previousTTL = byteBuffer.getLong();
        this.previousDatabaseWildcardTTL = byteBuffer.getLong();
      }
    } catch (IOException e) {
      LOGGER.error(ProcedureMessages.IO_ERROR_WHEN_DESERIALIZE_SETTTL_PLAN, e);
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
    final SetTTLProcedure that = (SetTTLProcedure) o;
    return this.isGeneratedByPipe == that.isGeneratedByPipe
        && this.previousTTL == that.previousTTL
        && this.previousDatabaseWildcardTTL == that.previousDatabaseWildcardTTL
        && this.previousTTLStateCaptured == that.previousTTLStateCaptured
        && this.plan.equals(that.plan);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        plan,
        isGeneratedByPipe,
        previousTTL,
        previousDatabaseWildcardTTL,
        previousTTLStateCaptured);
  }
}
