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
import org.apache.iotdb.confignode.consensus.request.write.cq.ActiveCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.AddCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.DropCQPlan;
import org.apache.iotdb.confignode.manager.cq.CQScheduleTask;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.cq.CreateCQState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.external.commons.codec.digest.DigestUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.iotdb.confignode.procedure.state.cq.CreateCQState.INACTIVE;
import static org.apache.iotdb.confignode.procedure.state.cq.CreateCQState.SCHEDULED;

public class CreateCQProcedure extends AbstractNodeProcedure<CreateCQState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateCQProcedure.class);

  private static final int RETRY_THRESHOLD = 5;

  private final ScheduledExecutorService executor;

  private TCreateCQReq req;

  private String md5;

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
    this.md5 = DigestUtils.md2Hex(req.cqId);
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
          CQScheduleTask cqScheduleTask =
              new CQScheduleTask(req, firstExecutionTime, md5, executor, env.getConfigManager());
          cqScheduleTask.submitSelf();
          setNextState(SCHEDULED);
          break;
        case SCHEDULED:
          activeCQ(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new IllegalArgumentException("Unknown CreateCQState: " + state);
      }
    } catch (Exception t) {
      if (isRollbackSupported(state)) {
        LOGGER.error("Fail in CreateCQProcedure", t);
        setFailure(new ProcedureException(t));
      } else {
        LOGGER.error(
            "Retrievable error trying to create cq [{}], state [{}]", req.getCqId(), state, t);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format(
                      "Fail to create trigger [%s] at STATE [%s]", req.getCqId(), state)));
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
              .write(new AddCQPlan(req, md5, firstExecutionTime));
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
    }
    if (res.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.debug("Finish init CQ {} successfully", req.cqId);
      setNextState(INACTIVE);
    } else if (res.code == TSStatusCode.CQ_ALREADY_EXIST.getStatusCode()) {
      LOGGER.info("Failed to init CQ {} because such cq already exists", req.cqId);
      setFailure(new ProcedureException(new IoTDBException(res)));
    } else {
      LOGGER.warn("Failed to init CQ {} because of unknown reasons {}", req.cqId, res);
      setFailure(new ProcedureException(new IoTDBException(res)));
    }
  }

  private void activeCQ(ConfigNodeProcedureEnv env) {
    TSStatus res;
    try {
      res = env.getConfigManager().getConsensusManager().write(new ActiveCQPlan(req.cqId, md5));
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
    }
    if (res.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.debug("Finish Scheduling CQ {} successfully", req.cqId);
    } else if (res.code == TSStatusCode.NO_SUCH_CQ.getStatusCode()) {
      LOGGER.warn("Failed to active CQ {} because of no such cq: {}", req.cqId, res.message);
    } else if (res.code == TSStatusCode.CQ_ALREADY_ACTIVE.getStatusCode()) {
      LOGGER.warn("Failed to active CQ {} because this cq has already been active", req.cqId);
    } else {
      LOGGER.warn(
          "Failed to active CQ {} successfully because of unknown reasons {}", req.cqId, res);
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
        LOGGER.info("Start [INACTIVE] rollback of CQ {}", req.cqId);
        TSStatus res;
        try {
          res = env.getConfigManager().getConsensusManager().write(new DropCQPlan(req.cqId, md5));
        } catch (ConsensusException e) {
          LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
          res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
          res.setMessage(e.getMessage());
        }
        if (res.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.info("Finish [INACTIVE] rollback of CQ {} successfully", req.cqId);
        } else if (res.code == TSStatusCode.NO_SUCH_CQ.getStatusCode()) {
          LOGGER.warn(
              "Failed to do [INACTIVE] rollback of CQ {} because of no such cq: {}",
              req.cqId,
              res.message);
        } else {
          LOGGER.warn(
              "Failed to do [INACTIVE] rollback of CQ {} because of unknown reasons {}",
              req.cqId,
              res);
        }

        break;
      default:
        throw new IllegalArgumentException("Unknown CreateCQState: " + state);
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
    ReadWriteIOUtils.write(md5, stream);
    ReadWriteIOUtils.write(firstExecutionTime, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.req = ThriftCommonsSerDeUtils.deserializeTCreateCQReq(byteBuffer);
    this.md5 = ReadWriteIOUtils.readString(byteBuffer);
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
        && getCurrentState().equals(that.getCurrentState())
        && getCycles() == that.getCycles()
        && isGeneratedByPipe == that.isGeneratedByPipe
        && firstExecutionTime == that.firstExecutionTime
        && Objects.equals(req, that.req)
        && Objects.equals(md5, that.md5);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        isGeneratedByPipe,
        req,
        md5,
        firstExecutionTime);
  }
}
