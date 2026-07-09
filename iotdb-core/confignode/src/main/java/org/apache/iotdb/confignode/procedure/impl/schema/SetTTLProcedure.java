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
import org.apache.iotdb.confignode.manager.lease.ClusterCachePropagator;
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
  // Distinguishes no previous TTL from TTLCache.NULL_TTL, the explicit unset marker for rollback.
  private static final long TTL_NOT_EXIST = Long.MIN_VALUE;
  private static final int ROLLBACK_STATE_BYTES = Byte.BYTES + Long.BYTES * 2;

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
        case CAPTURE_PREVIOUS_TTL:
          capturePreviousTTLState(env);
          setNextState(SetTTLState.SET_CONFIGNODE_TTL);
          return Flow.HAS_MORE_STATE;
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

  void setConfigNodeTTL(final ConfigNodeProcedureEnv env) {
    if (!previousTTLStateCaptured) {
      capturePreviousTTLState(env);
      setNextState(SetTTLState.SET_CONFIGNODE_TTL);
      return;
    }
    final TSStatus res = writeConfigNodePlan(env, plan);
    if (res.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.info(ProcedureMessages.FAILED_TO_EXECUTE_PLAN_BECAUSE, plan, res.message);
      setFailure(new ProcedureException(new IoTDBException(res)));
    } else {
      setNextState(SetTTLState.UPDATE_DATANODE_CACHE);
    }
  }

  void updateDataNodeTTL(final ConfigNodeProcedureEnv env) {
    if (!broadcastTTLAndDecide(
        env, buildSetTTLReq(plan.getPathPattern(), plan.getTTL(), plan.isDataBase()))) {
      LOGGER.error(ProcedureMessages.FAILED_TO_UPDATE_TTL_CACHE_OF_DATANODE);
      setFailure(
          new ProcedureException(
              new MetadataException(ProcedureMessages.UPDATE_DATANODE_TTL_CACHE_FAILED)));
    }
  }

  /**
   * Broadcast the TTL update to all DataNodes and decide whether it is safe to proceed: proceed
   * once every unreachable DataNode is provably self-fenced (it fails closed on TTL in compaction
   * and resyncs on recovery) instead of hard-failing on the first unreachable DataNode.
   * Package-private and overridable for tests.
   */
  boolean broadcastTTLAndDecide(final ConfigNodeProcedureEnv env, final TSetTTLReq req) {
    return new ClusterCachePropagator(SchemaUtils.filterFencedDataNode(env.getConfigManager()))
        .propagate(targets -> sendTTLRequest(targets, req).getResponseMap());
  }

  private void capturePreviousTTLState(final ConfigNodeProcedureEnv env) {
    if (previousTTLStateCaptured) {
      return;
    }
    previousTTL = getTTLOrDefault(env, plan.getPathPattern());
    if (plan.isDataBase()) {
      previousDatabaseWildcardTTL =
          getTTLOrDefault(env, getDatabaseWildcardPathPattern(plan.getPathPattern()));
    }
    previousTTLStateCaptured = true;
  }

  TSStatus writeConfigNodePlan(final ConfigNodeProcedureEnv env, final SetTTLPlan setTTLPlan) {
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

  DataNodeAsyncRequestContext<TSetTTLReq, TSStatus> sendTTLRequest(
      final Map<Integer, TDataNodeLocation> dataNodeLocationMap, final TSetTTLReq req) {
    final DataNodeAsyncRequestContext<TSetTTLReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.SET_TTL, req, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(
            clientHandler,
            ClusterCachePropagator.BROADCAST_RPC_RETRY,
            ClusterCachePropagator.BROADCAST_RPC_TIMEOUT_MS);
    return clientHandler;
  }

  private TSetTTLReq buildSetTTLReq(
      final String[] pathPattern, final long ttl, final boolean isDataBase) {
    return new TSetTTLReq(
        Collections.singletonList(String.join(".", pathPattern)), ttl, isDataBase);
  }

  private long getTTLOrDefault(final ConfigNodeProcedureEnv env, final String[] pathPattern) {
    final long ttl = env.getConfigManager().getTTLManager().getTTL(pathPattern);
    return ttl == TTLCache.NULL_TTL ? TTL_NOT_EXIST : ttl;
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
    // TTL_NOT_EXIST means the original ttl was absent; NULL_TTL asks the executor to unset it.
    final SetTTLPlan rollbackPlan =
        new SetTTLPlan(pathPattern, ttl == TTL_NOT_EXIST ? TTLCache.NULL_TTL : ttl);
    // Database rollback restores the database path and db.** separately, so avoid auto-expansion.
    rollbackPlan.setDataBase(false);
    final TSStatus status = writeConfigNodePlan(env, rollbackPlan);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new ProcedureException(
          new MetadataException(
              ProcedureMessages.EXCEPTION_ROLLBACK_CONFIGNODE_TTL_FAILED_6D4FB59A
                  + String.join(ConfigNodeMessages.EXCEPTION_DOT_9D9B854A, pathPattern)
                  + ConfigNodeMessages.MESSAGE_COLON_CEFF3F4D
                  + status.getMessage()));
    }
  }

  private void rollbackDataNodeTTL(final ConfigNodeProcedureEnv env) throws ProcedureException {
    restoreTTLOnDataNodes(env, plan.getPathPattern(), previousTTL);
    if (plan.isDataBase()) {
      restoreTTLOnDataNodes(
          env, getDatabaseWildcardPathPattern(plan.getPathPattern()), previousDatabaseWildcardTTL);
    }
  }

  private void restoreTTLOnDataNodes(
      final ConfigNodeProcedureEnv env, final String[] pathPattern, final long ttl)
      throws ProcedureException {
    // Same proceed-past-fenced semantics as the forward update: a down DataNode must not block
    // rollback (it resyncs TTL on recovery); only a live unacked DataNode fails it.
    if (!broadcastTTLAndDecide(
        env, buildSetTTLReq(pathPattern, ttl == TTL_NOT_EXIST ? TTLCache.NULL_TTL : ttl, false))) {
      throw new ProcedureException(
          new MetadataException(
              ProcedureMessages.EXCEPTION_ROLLBACK_DATANODE_TTL_CACHE_FAILED_AF9C7102
                  + String.join(ConfigNodeMessages.EXCEPTION_DOT_9D9B854A, pathPattern)));
    }
  }

  /**
   * Best-effort rollback: restore both sides, throw the earliest failure, and suppress later ones.
   */
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
      LOGGER.error(ProcedureMessages.LOG_FAILED_ROLLBACK_CONFIGNODE_TTL_STATE_9666EF54, e);
      rollbackFailure = e;
    }
    try {
      rollbackDataNodeTTL(env);
    } catch (ProcedureException e) {
      LOGGER.error(ProcedureMessages.LOG_FAILED_ROLLBACK_DATANODE_TTL_CACHE_436C008A, e);
      if (rollbackFailure == null) {
        rollbackFailure = e;
      } else {
        rollbackFailure.addSuppressed(e);
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
    return SetTTLState.CAPTURE_PREVIOUS_TTL;
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
      // The serialized plan buffer may include padding; skip to the actual payload end.
      byteBuffer.position(position + length);
      if (byteBuffer.remaining() >= ROLLBACK_STATE_BYTES) {
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
