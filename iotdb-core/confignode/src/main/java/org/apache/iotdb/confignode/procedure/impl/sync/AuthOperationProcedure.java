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

package org.apache.iotdb.confignode.procedure.impl.sync;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.AuthOperationProcedureState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.confignode.procedure.state.AuthOperationProcedureState.DATANODE_AUTHCACHE_INVALIDING;

public class AuthOperationProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AuthOperationProcedureState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthOperationProcedure.class);

  private String user;
  private String role;

  private AuthorPlan plan;

  private int timeoutMS;
  private static final String CONSENSUS_WRITE_ERROR =
      "Failed in the write API executing the consensus layer due to: ";

  private static final int RETRY_THRESHOLD = 2;
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private List<Pair<TDataNodeConfiguration, Long>> dataNodesToInvalid;

  private List<TDataNodeConfiguration> datanodes;

  public AuthOperationProcedure() {
    super();
  }

  public AuthOperationProcedure(AuthorPlan plan, List<TDataNodeConfiguration> alldns) {
    super();
    this.user = plan.getUserName();
    this.role = plan.getRoleName();
    this.plan = plan;
    this.datanodes = alldns;
    this.timeoutMS = commonConfig.getDatanodeTokenTimeoutMS();
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, AuthOperationProcedureState state) {
    try {
      switch (state) {
        case INIT:
          writePlan(env);
          return Flow.HAS_MORE_STATE;
        case DATANODE_AUTHCACHE_INVALIDING:
          TInvalidatePermissionCacheReq req = new TInvalidatePermissionCacheReq();
          TSStatus status;
          req.setUsername(user);
          req.setRoleName(role);
          Iterator<Pair<TDataNodeConfiguration, Long>> it = dataNodesToInvalid.iterator();
          while (it.hasNext()) {
            if (it.next().getRight() + this.timeoutMS < System.currentTimeMillis()) {
              it.remove();
              continue;
            }
            status =
                SyncDataNodeClientPool.getInstance()
                    .sendSyncRequestToDataNodeWithRetry(
                        it.next().getLeft().getLocation().getInternalEndPoint(),
                        req,
                        DataNodeRequestType.INVALIDATE_PERMISSION_CACHE);
            if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              it.remove();
            }
          }
          if (dataNodesToInvalid.isEmpty()) {
            return Flow.NO_MORE_STATE;
          } else {
            setNextState(AuthOperationProcedureState.DATANODE_AUTHCACHE_INVALIDING);
          }
          break;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        LOGGER.error("Fail when execute {} ", plan);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error("Retrievable error trying to execute plan {}, state: {}", plan, state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format("Fail to execute plan [%s] at state[%s]", plan.toString(), state)));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private void writePlan(ConfigNodeProcedureEnv env) {
    TSStatus res;
    try {
      res = env.getConfigManager().getConsensusManager().write(this.plan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
    }
    if (res.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.debug("Execute auth plan {} success.", plan);
      setNextState(DATANODE_AUTHCACHE_INVALIDING);
      dataNodesToInvalid = new ArrayList<>();
      for (TDataNodeConfiguration item : datanodes) {
        this.dataNodesToInvalid.add(new Pair<>(item, System.currentTimeMillis()));
      }
    } else {
      LOGGER.info("Failed to execute plan {} because {}", plan, res.message);
      setFailure(new ProcedureException(new IoTDBException(res.message, res.code)));
    }
  }

  @Override
  protected boolean isRollbackSupported(AuthOperationProcedureState state) {
    return state == AuthOperationProcedureState.INIT;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, AuthOperationProcedureState state) {}

  @Override
  protected AuthOperationProcedureState getState(int stateId) {
    return AuthOperationProcedureState.values()[stateId];
  }

  @Override
  protected int getStateId(AuthOperationProcedureState state) {
    return state.ordinal();
  }

  @Override
  protected AuthOperationProcedureState getInitialState() {
    return AuthOperationProcedureState.INIT;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.INVALID_DATANODE_AUTH_CACHE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(user, stream);
    ReadWriteIOUtils.write(role, stream);
    ReadWriteIOUtils.write(dataNodesToInvalid.size(), stream);
    for (Pair<TDataNodeConfiguration, Long> item : dataNodesToInvalid) {
      ThriftCommonsSerDeUtils.serializeTDataNodeConfiguration(item.getLeft(), stream);
      ReadWriteIOUtils.write(item.getRight(), stream);
    }
    ReadWriteIOUtils.write(timeoutMS, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.user = ReadWriteIOUtils.readString(byteBuffer);
    this.role = ReadWriteIOUtils.readString(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    this.dataNodesToInvalid = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      TDataNodeConfiguration datanode =
          ThriftCommonsSerDeUtils.deserializeTDataNodeConfiguration(byteBuffer);
      Long timestamp = ReadWriteIOUtils.readLong(byteBuffer);
      this.dataNodesToInvalid.add(new Pair<TDataNodeConfiguration, Long>(datanode, timestamp));
    }
    this.timeoutMS = ReadWriteIOUtils.readInt(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthOperationProcedure that = (AuthOperationProcedure) o;
    return plan.equals(that.plan)
        && Objects.equals(dataNodesToInvalid, ((AuthOperationProcedure) o).dataNodesToInvalid);
  }
}
