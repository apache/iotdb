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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.NotifyRegionMigrationState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** A procedure that notifies all DNs of the ongoing region migration procedure. */
public class NotifyRegionMigrationProcedure
    extends RegionOperationProcedure<NotifyRegionMigrationState> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(NotifyRegionMigrationProcedure.class);

  private boolean isStart;

  public NotifyRegionMigrationProcedure() {
    super();
  }

  public NotifyRegionMigrationProcedure(TConsensusGroupId consensusGroupId, boolean isStart) {
    super(consensusGroupId);
    this.isStart = isStart;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, NotifyRegionMigrationState state)
      throws InterruptedException {
    if (regionId == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      LOGGER.info(
          "[pid{}][NotifyRegionMigration] started, region id is {}.", getProcId(), regionId);
      env.notifyRegionMigrationToAllDataNodes(regionId, isStart);
    } catch (Exception e) {
      LOGGER.error("[pid{}][NotifyRegionMigration] state {} failed", getProcId(), state, e);
      return Flow.NO_MORE_STATE;
    }
    LOGGER.info("[pid{}][NotifyRegionMigration] state {} complete", getProcId(), state);
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, NotifyRegionMigrationState state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected NotifyRegionMigrationState getState(int stateId) {
    return NotifyRegionMigrationState.values()[stateId];
  }

  @Override
  protected int getStateId(NotifyRegionMigrationState state) {
    return state.ordinal();
  }

  @Override
  protected NotifyRegionMigrationState getInitialState() {
    return NotifyRegionMigrationState.INIT;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.NOTIFY_REGION_MIGRATION_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(regionId, stream);
    stream.writeBoolean(isStart);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      regionId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
      isStart = (byteBuffer.get() != (byte) 0);
    } catch (ThriftSerDeException e) {
      LOGGER.error("Error in deserialize {}", this.getClass(), e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof NotifyRegionMigrationProcedure)) {
      return false;
    }
    NotifyRegionMigrationProcedure procedure = (NotifyRegionMigrationProcedure) obj;
    return this.regionId.equals(procedure.regionId) && this.isStart == procedure.isStart;
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionId, isStart);
  }

  @Override
  public String toString() {
    return "NotifyRegionMigrationProcedure{"
        + "regionId="
        + regionId
        + ", isStart="
        + isStart
        + '}';
  }
}
