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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.CreateRegionState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.commons.utils.KillPoint.KillPoint.setKillPoint;
import static org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler.simplifiedLocation;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;

/**
 * Create a single region replica on one DataNode.
 *
 * <p>This replaces the queue-based CREATE path in {@code PartitionManager.maintainRegionReplicas}.
 * Region creation already returns a final, unambiguous status from a single synchronous RPC, so
 * this procedure simply wraps that RPC, gaining persistence and retry across ConfigNode leader
 * changes.
 */
public class CreateRegionProcedure extends RegionOperationProcedure<CreateRegionState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateRegionProcedure.class);
  private static final int MAX_RETRY = 5;

  private String storageGroup;
  private TRegionReplicaSet regionReplicaSet;
  private TDataNodeLocation targetDataNode;

  public CreateRegionProcedure() {
    super();
  }

  public CreateRegionProcedure(
      String storageGroup, TRegionReplicaSet regionReplicaSet, TDataNodeLocation targetDataNode) {
    super(regionReplicaSet.getRegionId());
    this.storageGroup = storageGroup;
    this.regionReplicaSet = regionReplicaSet;
    this.targetDataNode = targetDataNode;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, CreateRegionState state)
      throws InterruptedException {
    if (regionId == null || targetDataNode == null) {
      return Flow.NO_MORE_STATE;
    }
    RegionMaintainHandler handler = env.getRegionMaintainHandler();
    try {
      switch (state) {
        case CREATE_REGION:
          LOGGER.info(
              ProcedureMessages.PID_CREATEREGION_STARTED_WILL_BE_CREATED_ON_DATANODE,
              getProcId(),
              regionId,
              simplifiedLocation(targetDataNode));
          TSStatus status = handler.createRegion(regionReplicaSet, storageGroup, targetDataNode);
          setKillPoint(state);
          if (status.getCode() != SUCCESS_STATUS.getStatusCode()) {
            if (getCycles() < MAX_RETRY) {
              LOGGER.warn(
                  ProcedureMessages.PID_CREATEREGION_CREATE_FAILED_WILL_RETRY,
                  getProcId(),
                  regionId,
                  simplifiedLocation(targetDataNode),
                  getCycles() + 1,
                  status);
              setNextState(CreateRegionState.CREATE_REGION);
              return Flow.HAS_MORE_STATE;
            }
            LOGGER.warn(ProcedureMessages.PID_CREATEREGION_STATE_FAILED, getProcId(), state);
            return Flow.NO_MORE_STATE;
          }
          // Requirement: every successfully completed maintain task must be logged.
          LOGGER.info(
              ProcedureMessages
                  .PID_CREATEREGION_SUCCESS_HAS_BEEN_CREATED_ON_DATANODE_PROCEDURE_TOOK,
              getProcId(),
              regionId,
              simplifiedLocation(targetDataNode),
              CommonDateTimeUtils.convertMillisecondToDurationStr(
                  System.currentTimeMillis() - getSubmittedTime()),
              DateTimeUtils.convertLongToDate(getSubmittedTime(), "ms"));
          return Flow.NO_MORE_STATE;
        default:
          throw new ProcedureException(ProcedureMessages.UNSUPPORTED_STATE + state.name());
      }
    } catch (Exception e) {
      LOGGER.error(ProcedureMessages.PID_CREATEREGION_STATE_FAILED, getProcId(), state, e);
      return Flow.NO_MORE_STATE;
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, CreateRegionState state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected CreateRegionState getState(int stateId) {
    return CreateRegionState.values()[stateId];
  }

  @Override
  protected int getStateId(CreateRegionState createRegionState) {
    return createRegionState.ordinal();
  }

  @Override
  protected CreateRegionState getInitialState() {
    return CreateRegionState.CREATE_REGION;
  }

  public TDataNodeLocation getTargetDataNode() {
    return targetDataNode;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_REGION_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(storageGroup, stream);
    ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(regionReplicaSet, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(targetDataNode, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      storageGroup = ReadWriteIOUtils.readString(byteBuffer);
      regionReplicaSet = ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(byteBuffer);
      targetDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      regionId = regionReplicaSet.getRegionId();
    } catch (ThriftSerDeException e) {
      LOGGER.error(ProcedureMessages.ERROR_IN_DESERIALIZE, this.getClass(), e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CreateRegionProcedure)) {
      return false;
    }
    CreateRegionProcedure procedure = (CreateRegionProcedure) obj;
    return Objects.equals(this.storageGroup, procedure.storageGroup)
        && Objects.equals(this.regionReplicaSet, procedure.regionReplicaSet)
        && Objects.equals(this.targetDataNode, procedure.targetDataNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageGroup, regionReplicaSet, targetDataNode);
  }

  @Override
  public String toString() {
    return "CreateRegionProcedure{"
        + "regionId="
        + regionId
        + ", storageGroup="
        + storageGroup
        + ", targetDataNode="
        + simplifiedLocation(targetDataNode)
        + '}';
  }
}
