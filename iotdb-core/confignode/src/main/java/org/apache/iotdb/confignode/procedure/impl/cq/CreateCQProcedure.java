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

package org.apache.iotdb.confignode.procedure.impl.cq;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.read.cq.ShowCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.ActiveCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.AddCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.DropCQPlan;
import org.apache.iotdb.confignode.consensus.response.cq.ShowCQResp;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.cq.CQManager;
import org.apache.iotdb.confignode.manager.cq.CQScheduleTask;
import org.apache.iotdb.confignode.persistence.cq.CQInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.cq.CreateCQState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.iotdb.confignode.procedure.state.cq.CreateCQState.INACTIVE;
import static org.apache.iotdb.confignode.procedure.state.cq.CreateCQState.SCHEDULED;

public class CreateCQProcedure extends AbstractNodeProcedure<CreateCQState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateCQProcedure.class);

  private static final int RETRY_THRESHOLD = 5;

  private final ScheduledExecutorService executor;

  private TCreateCQReq req;

  private String cqToken;

  private long firstExecutionTime;

  private static final String CONSENSUS_WRITE_ERROR =
      "Failed in the write API executing the consensus layer due to: ";

  public CreateCQProcedure(ScheduledExecutorService executor) {
    super();
    this.executor = executor;
  }

  public CreateCQProcedure(TCreateCQReq req, ScheduledExecutorService executor) {
    super();
    this.req = req;
    this.cqToken = generateCQToken();
    this.executor = executor;
    this.firstExecutionTime =
        CQScheduleTask.getFirstExecutionTime(req.boundaryTime, req.everyInterval);
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, CreateCQState state)
      throws InterruptedException {

    try {
      switch (state) {
        case INIT:
          addCQ(env);
          return Flow.HAS_MORE_STATE;
        case INACTIVE:
          submitScheduleTask(
              env,
              new CQScheduleTask(
                  req, firstExecutionTime, cqToken, executor, env.getConfigManager()));
          setNextState(SCHEDULED);
          break;
        case SCHEDULED:
          if (isStateDeserialized()) {
            recoverScheduledTask(env);
          }
          activeCQ(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new IllegalArgumentException(ProcedureMessages.UNKNOWN_CREATECQSTATE + state);
      }
    } catch (Exception t) {
      if (isRollbackSupported(state)) {
        LOGGER.error(ProcedureMessages.FAIL_IN_CREATECQPROCEDURE, t);
        setFailure(new ProcedureException(t));
      } else {
        LOGGER.error(
            ProcedureMessages.RETRIEVABLE_ERROR_TRYING_TO_CREATE_CQ_STATE, req.getCqId(), state, t);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format(
                      ProcedureMessages.FAIL_TO_CREATE_TRIGGER_AT_STATE, req.getCqId(), state)));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private void addCQ(ConfigNodeProcedureEnv env) {
    TSStatus res;
    try {
      res =
          env.getConfigManager()
              .getConsensusManager()
              .write(new AddCQPlan(req, cqToken, firstExecutionTime));
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
    }
    if (res.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.debug(ProcedureMessages.FINISH_INIT_CQ_SUCCESSFULLY, req.cqId);
      setNextState(INACTIVE);
    } else if (res.code == TSStatusCode.CQ_ALREADY_EXIST.getStatusCode()) {
      LOGGER.info(ProcedureMessages.FAILED_TO_INIT_CQ_BECAUSE_SUCH_CQ_ALREADY_EXISTS, req.cqId);
      setFailure(new ProcedureException(new IoTDBException(res)));
    } else {
      LOGGER.warn(ProcedureMessages.FAILED_TO_INIT_CQ_BECAUSE_OF_UNKNOWN_REASONS, req.cqId, res);
      setFailure(new ProcedureException(new IoTDBException(res)));
    }
  }

  private void activeCQ(ConfigNodeProcedureEnv env) {
    TSStatus res;
    try {
      res = env.getConfigManager().getConsensusManager().write(new ActiveCQPlan(req.cqId, cqToken));
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
    }
    if (res.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.debug(ProcedureMessages.FINISH_SCHEDULING_CQ_SUCCESSFULLY, req.cqId);
    } else if (res.code == TSStatusCode.NO_SUCH_CQ.getStatusCode()) {
      LOGGER.warn(
          ProcedureMessages.FAILED_TO_ACTIVE_CQ_BECAUSE_OF_NO_SUCH_CQ, req.cqId, res.message);
    } else if (res.code == TSStatusCode.CQ_ALREADY_ACTIVE.getStatusCode()) {
      LOGGER.warn(ProcedureMessages.FAILED_TO_ACTIVE_CQ_BECAUSE_THIS_CQ_HAS_ALREADY_BEEN, req.cqId);
    } else {
      LOGGER.warn(
          ProcedureMessages.FAILED_TO_ACTIVE_CQ_SUCCESSFULLY_BECAUSE_OF_UNKNOWN_REASONS,
          req.cqId,
          res);
    }
  }

  void recoverScheduledTask(ConfigNodeProcedureEnv env) throws ConsensusException {
    Optional<CQInfo.CQEntry> cqEntry = getCurrentCQEntry(env);
    if (!cqEntry.isPresent()) {
      LOGGER.info(
          "Skip recovering the schedule task of CQ {} because its metadata is unavailable.",
          req.cqId);
      return;
    }
    submitScheduleTask(env, new CQScheduleTask(cqEntry.get(), executor, env.getConfigManager()));
  }

  Optional<CQInfo.CQEntry> getCurrentCQEntry(ConfigNodeProcedureEnv env) throws ConsensusException {
    ShowCQResp response =
        (ShowCQResp) env.getConfigManager().getConsensusManager().read(new ShowCQPlan(req.cqId));
    return response.getCqList().stream()
        .filter(entry -> cqToken.equals(entry.getCqToken()))
        .findFirst();
  }

  private static String generateCQToken() {
    return UUID.randomUUID().toString();
  }

  private void submitScheduleTask(ConfigNodeProcedureEnv env, CQScheduleTask cqScheduleTask) {
    CQManager cqManager = env.getConfigManager().getCQManager();
    if (!cqManager.markCQLocallyScheduled(req.cqId, cqToken, cqScheduleTask)) {
      return;
    }
    try {
      cqScheduleTask.submitSelf();
    } catch (RuntimeException e) {
      cqManager.unmarkCQLocallyScheduled(req.cqId, cqToken);
      throw e;
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, CreateCQState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case INIT:
      case SCHEDULED:
        // do nothing
        break;
      case INACTIVE:
        LOGGER.info(ProcedureMessages.START_INACTIVE_ROLLBACK_OF_CQ, req.cqId);
        TSStatus res;
        try {
          res =
              env.getConfigManager().getConsensusManager().write(new DropCQPlan(req.cqId, cqToken));
        } catch (ConsensusException e) {
          LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
          res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
          res.setMessage(e.getMessage());
        }
        if (res.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.info(ProcedureMessages.FINISH_INACTIVE_ROLLBACK_OF_CQ_SUCCESSFULLY, req.cqId);
        } else if (res.code == TSStatusCode.NO_SUCH_CQ.getStatusCode()) {
          LOGGER.warn(
              ProcedureMessages.FAILED_TO_DO_INACTIVE_ROLLBACK_OF_CQ_BECAUSE_OF_NO,
              req.cqId,
              res.message);
        } else {
          LOGGER.warn(
              ProcedureMessages.FAILED_TO_DO_INACTIVE_ROLLBACK_OF_CQ_BECAUSE_OF_UNKNOWN,
              req.cqId,
              res);
        }

        break;
      default:
        throw new IllegalArgumentException(ProcedureMessages.UNKNOWN_CREATECQSTATE + state);
    }
  }

  @Override
  protected boolean isRollbackSupported(CreateCQState createCQState) {
    return createCQState == INACTIVE;
  }

  @Override
  protected CreateCQState getState(int stateId) {
    return CreateCQState.values()[stateId];
  }

  @Override
  protected int getStateId(CreateCQState createCQState) {
    return createCQState.ordinal();
  }

  @Override
  protected CreateCQState getInitialState() {
    return CreateCQState.INIT;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_CQ_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTCreateCQReq(req, stream);
    ReadWriteIOUtils.write(cqToken, stream);
    ReadWriteIOUtils.write(firstExecutionTime, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.req = ThriftCommonsSerDeUtils.deserializeTCreateCQReq(byteBuffer);
    this.cqToken = ReadWriteIOUtils.readString(byteBuffer);
    this.firstExecutionTime = ReadWriteIOUtils.readLong(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateCQProcedure that = (CreateCQProcedure) o;
    return getProcId() == that.getProcId()
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && isGeneratedByPipe == that.isGeneratedByPipe
        && firstExecutionTime == that.firstExecutionTime
        && Objects.equals(req, that.req)
        && Objects.equals(cqToken, that.cqToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        isGeneratedByPipe,
        req,
        cqToken,
        firstExecutionTime);
  }

  public String getCqId() {
    return req == null ? null : req.getCqId();
  }

  public String getCqToken() {
    return cqToken;
  }
}
